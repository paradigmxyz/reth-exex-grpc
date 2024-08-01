use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use eyre::{bail, Result};
use kona_derive::{
    online::{AlloyL2ChainProvider, EthereumDataSource},
    traits::{ChainProvider, L2ChainProvider, OriginProvider, Pipeline, StepResult},
    types::{BlockInfo, L2BlockInfo, RollupConfig, StageError},
};
use reth::{cli::Cli, transaction_pool::TransactionPool};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use superchain_registry::ROLLUP_CONFIGS;
use tracing::{debug, error, info, warn};

mod blobs;
use blobs::ExExBlobProvider;

mod cli_ext;
use cli_ext::{KonaArgsExt, ValidationMode};

mod pipeline;
use pipeline::{new_local_pipeline, LocalAttributesBuilder, LocalPipeline};

mod providers;
use providers::LocalChainProvider;

mod validation;
use validation::{AttributesValidator, EngineApiValidator, TrustedValidator};

#[derive(Debug)]
pub(crate) struct KonaExEx<Node: FullNodeComponents> {
    /// The rollup configuration
    cfg: Arc<RollupConfig>,
    /// The context of the Execution Extension
    ctx: ExExContext<Node>,
    /// The chain provider to follow the chain state based on ExEx events
    chain_provider: LocalChainProvider,
    /// The L2 chain provider to fetch L2 block information and optionally
    /// verify newly derived payloads against
    l2_provider: AlloyL2ChainProvider,
    /// The blob provider to fetch blobs from the beacon client
    blob_provider: ExExBlobProvider,
    /// The validator to verify newly derived payloads
    validator: Box<dyn AttributesValidator + Send>,
    /// The derivation pipeline
    pipeline: LocalPipeline,
    /// The current L2 block we are processing
    l2_block_cursor: L2BlockInfo,
    /// A map of L1 anchor blocks to their L2 cursor
    l1_to_l2_block_cursor: HashMap<BlockInfo, L2BlockInfo>,
    /// Whether the ExEx is synced to the L2 genesis
    is_synced_to_l2_genesis: bool,
    /// Whether we should advance the L2 block cursor
    should_advance_l2_block_cursor: bool,
    /// The number of derived payloads so far
    derived_payloads_count: u64,
}

impl<Node: FullNodeComponents> KonaExEx<Node> {
    /// Creates a new instance of the Kona Execution Extension.
    pub async fn new(ctx: ExExContext<Node>, args: KonaArgsExt, cfg: Arc<RollupConfig>) -> Self {
        let mut chain_provider = LocalChainProvider::new();
        chain_provider.insert_l2_genesis_block(cfg.genesis.l1);

        let l2_provider = AlloyL2ChainProvider::new_http(args.l2_rpc_url.clone(), cfg.clone());
        let blob_provider = ExExBlobProvider::new_from_beacon_client(args.beacon_client_url);
        let dap = EthereumDataSource::new(chain_provider.clone(), blob_provider.clone(), &cfg);

        let attributes =
            LocalAttributesBuilder::new(cfg.clone(), l2_provider.clone(), chain_provider.clone());

        let validator: Box<dyn AttributesValidator + Send> = match args.validation_mode {
            ValidationMode::Trusted => Box::new(TrustedValidator::new_http(
                args.l2_rpc_url,
                cfg.canyon_time.unwrap_or_default(),
            )),
            ValidationMode::EngineApi => Box::new(EngineApiValidator::new_http(
                // The presence of these variables is enforced by the clap configuration,
                // so we can safely unwrap them here.
                args.l2_engine_api_url.unwrap(),
                args.l2_engine_jwt.unwrap(),
            )),
        };

        // Initialize the L2 cursor to the L2 genesis block
        let l2_block_cursor = l2_genesis_info_from_cfg(&cfg);
        let tip =
            chain_provider.block_info_by_number(l2_block_cursor.l1_origin.number).await.unwrap();

        // Finally create the derivation pipeline
        let pipeline = new_local_pipeline(
            cfg.clone(),
            chain_provider.clone(),
            l2_provider.clone(),
            dap,
            attributes,
            tip,
        );

        Self {
            cfg,
            ctx,
            validator,
            pipeline,
            chain_provider,
            l2_provider,
            blob_provider,
            l2_block_cursor,
            l1_to_l2_block_cursor: HashMap::new(),
            is_synced_to_l2_genesis: false,
            should_advance_l2_block_cursor: false,
            derived_payloads_count: 0,
        }
    }

    /// Steps the L2 derivation pipeline and validates the prepared attributes.
    async fn step_l2(&mut self) {
        match self.pipeline.step(self.l2_block_cursor).await {
            StepResult::PreparedAttributes => info!(target: "loop", "Prepared attributes"),
            StepResult::AdvancedOrigin => info!(target: "loop", "Advanced origin"),
            StepResult::OriginAdvanceErr(err) => {
                warn!(target: "loop", ?err, "Error advancing origin")
            }
            StepResult::StepFailed(err) => match err {
                StageError::NotEnoughData => {
                    debug!(target: "loop", "Not enough data to step derivation pipeline");
                }
                _ => {
                    error!(target: "loop", ?err, "Error stepping derivation pipeline");
                }
            },
        }

        // Peek the the next prepared attributes and validate them
        match self.pipeline.peek() {
            None => debug!(target: "loop", "No prepared attributes to validate"),
            Some(attributes) => match self.validator.validate(attributes).await {
                Ok(true) => info!(target: "loop", "Attributes validated"),
                Ok(false) => {
                    warn!(target: "loop", "Attributes failed validation");
                    // If the validation fails, take the attributes out and continue
                    let _ = self.pipeline.next();
                    return;
                }
                Err(err) => {
                    error!(target: "loop", ?err, "Error validating attributes");
                    // If the attributes fail validation, retry them without taking them
                    // out of the pipeline
                    return;
                }
            },
        };

        // Take the next attributes from the pipeline since they're valid.
        let Some(attributes) = self.pipeline.next() else {
            error!(target: "loop", "Must have valid attributes");
            return;
        };

        // If we validated some attributes, we should advance the cursor.
        self.derived_payloads_count += 1;
        self.should_advance_l2_block_cursor = true;

        println!(
            "Validated Payload Attributes {} [L2 Block Num: {}] [L2 Timestamp: {}] [L1 Origin Block Num: {}]",
            self.derived_payloads_count,
            attributes.parent.block_info.number as i64 + 1,
            attributes.attributes.timestamp,
            self.pipeline.origin().unwrap().number,
        );
        debug!(target: "loop", "attributes: {:#?}", attributes);
    }

    async fn advance_l2_cursor(&mut self) -> Result<()> {
        let next_l2_block = self.l2_block_cursor.block_info.number + 1;
        match self.l2_provider.l2_block_info_by_number(next_l2_block).await {
            Ok(bi) => {
                let tip = self
                    .chain_provider
                    .block_info_by_number(bi.l1_origin.number)
                    .await
                    .map_err(|e| eyre::eyre!(e))?;

                self.l1_to_l2_block_cursor.insert(tip, bi);
                self.l2_block_cursor = bi;
                self.should_advance_l2_block_cursor = false;
            }
            Err(err) => {
                error!(target: "loop", ?err, "Failed to fetch next pending l2 safe head: {}", next_l2_block)
            }
        }

        Ok(())
    }

    async fn handle_exex_notification(&mut self, notification: ExExNotification) -> Result<()> {
        if let Some(reverted_chain) = notification.reverted_chain() {
            self.chain_provider.commit(reverted_chain.clone());
            let l1_block_info = info_from_header(&reverted_chain.tip().block);

            // Insert blobs in the blob provider for the reverted chain
            for block in reverted_chain.blocks_iter() {
                let tx_hashes = block.transactions().map(|tx| tx.hash).collect::<Vec<_>>();
                let blobs = self.ctx.pool().get_all_blobs(tx_hashes)?;
                let blobs = blobs.into_iter().flat_map(|b| b.1.blobs).collect::<Vec<_>>();
                self.blob_provider.insert_blobs(block.hash(), blobs);
            }

            // Find the l2 block cursor associated with the reverted L1 chain tip
            let l2_cursor = if let Some(c) = self.l1_to_l2_block_cursor.get(&l1_block_info) {
                *c
            } else {
                bail!("Critical: Failed to get previous cursor for old chain tip");
            };

            // Reset the pipeline to the previous L2 block cursor
            self.l2_block_cursor = l2_cursor;
            if let Err(err) = self.pipeline.reset(l2_cursor.block_info, l1_block_info).await {
                bail!("Critical: Failed to reset pipeline: {:?}", err);
            }
        }

        if let Some(committed_chain) = notification.committed_chain() {
            let tip_number = committed_chain.tip().number; // TODO: ensure this is the right tip
            self.chain_provider.commit(committed_chain);

            if tip_number >= self.cfg.genesis.l1.number {
                debug!(target: "loop", "Chain synced to rollup genesis with L1 block number: {}", tip_number);
                self.is_synced_to_l2_genesis = true;
            }

            if let Err(err) = self.ctx.events.send(ExExEvent::FinishedHeight(tip_number)) {
                bail!("Critical: Failed to send ExEx event: {:?}", err);
            }
        }

        Ok(())
    }

    /// Starts the Kona Execution Extension loop.
    pub async fn start(mut self) -> Result<()> {
        loop {
            if self.is_synced_to_l2_genesis {
                self.step_l2().await;
            }

            if self.should_advance_l2_block_cursor {
                if let Err(err) = self.advance_l2_cursor().await {
                    error!(target: "loop", ?err, "Failed to advance L2 block cursor");
                }
            }

            if let Ok(notification) = self.ctx.notifications.try_recv() {
                if let Err(err) = self.handle_exex_notification(notification).await {
                    error!(target: "loop", ?err, "Failed to handle ExEx notification");
                }
            }

            if self.ctx.notifications.is_closed() {
                warn!(target: "loop", "ExEx notification channel closed, exiting");
                bail!("ExEx notification channel closed");
            }
        }
    }
}

fn main() -> Result<()> {
    Cli::<KonaArgsExt>::parse().run(|builder, args| async move {
        let Some(cfg) = ROLLUP_CONFIGS.get(&args.l2_chain_id).cloned() else {
            bail!("Rollup configuration not found for L2 chain id: {}", args.l2_chain_id);
        };

        let node = EthereumNode::default();
        let kona = move |ctx| async { Ok(KonaExEx::new(ctx, args, Arc::new(cfg)).await.start()) };
        let handle = builder.node(node).install_exex("Kona", kona).launch().await?;
        handle.wait_for_node_exit().await
    })
}

/// Helper to extract block info from a sealed block
fn info_from_header(block: &reth::primitives::SealedBlock) -> BlockInfo {
    BlockInfo {
        hash: block.hash(),
        number: block.number,
        timestamp: block.timestamp,
        parent_hash: block.parent_hash,
    }
}

/// Helper to extract L2 genesis block info from a rollup configuration
fn l2_genesis_info_from_cfg(cfg: &Arc<RollupConfig>) -> L2BlockInfo {
    L2BlockInfo {
        block_info: BlockInfo {
            hash: cfg.genesis.l2.hash,
            number: cfg.genesis.l2.number,
            ..Default::default()
        },
        l1_origin: cfg.genesis.l1,
        seq_num: 0,
    }
}
