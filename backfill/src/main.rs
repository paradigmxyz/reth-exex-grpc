mod rpc;

use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use clap::{Args, Parser};
use eyre::OptionExt;
use futures::{FutureExt, TryStreamExt};
use jsonrpsee::tracing::instrument;
use reth::{
    primitives::{BlockId, BlockNumber, BlockNumberOrTag},
    providers::{BlockIdReader, BlockReader, HeaderProvider, StateProviderFactory},
};
use reth_evm::execute::BlockExecutorProvider;
use reth_execution_types::Chain;
use reth_exex::{BackfillJob, BackfillJobFactory, ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::{error, info};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::rpc::{BackfillRpcExt, BackfillRpcExtApiServer};

/// The message type used to communicate with the ExEx.
enum BackfillMessage {
    /// Start a backfill job for the given range.
    ///
    /// The job ID will be sent on the provided channel.
    Start { range: RangeInclusive<BlockNumber>, response_tx: oneshot::Sender<eyre::Result<u64>> },
    /// Cancel the backfill job with the given ID.
    ///
    /// The cancellation result will be sent on the provided channel.
    Cancel { job_id: u64, response_tx: oneshot::Sender<eyre::Result<()>> },
    /// Finish the backfill job with the given ID, if it exists.
    Finish { job_id: u64 },
}

/// The ExEx that consumes new [`ExExNotification`]s and processes new backfill requests by
/// [`BackfillRpcExt`].
struct BackfillExEx<Node: FullNodeComponents> {
    /// The context of the ExEx.
    ctx: ExExContext<Node>,
    /// Sender for backfill messages.
    backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
    /// Receiver for backfill messages.
    backfill_rx: mpsc::UnboundedReceiver<BackfillMessage>,
    /// Factory for backfill jobs.
    backfill_job_factory: BackfillJobFactory<Node::Executor, Node::Provider>,
    /// Semaphore to limit the number of concurrent backfills.
    backfill_semaphore: Arc<Semaphore>,
    /// Next backfill job ID.
    next_backfill_job_id: u64,
    /// Mapping of backfill job IDs to backfill jobs.
    backfill_jobs: HashMap<u64, oneshot::Sender<oneshot::Sender<()>>>,
}

impl<Node: FullNodeComponents> BackfillExEx<Node> {
    /// Creates a new instance of the ExEx.
    fn new(
        ctx: ExExContext<Node>,
        backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
        backfill_rx: mpsc::UnboundedReceiver<BackfillMessage>,
        backfill_limit: usize,
    ) -> Self {
        let backfill_job_factory =
            BackfillJobFactory::new(ctx.block_executor().clone(), ctx.provider().clone());
        Self {
            ctx,
            backfill_tx,
            backfill_rx,
            backfill_job_factory,
            backfill_semaphore: Arc::new(Semaphore::new(backfill_limit)),
            next_backfill_job_id: 0,
            backfill_jobs: HashMap::new(),
        }
    }

    /// Starts listening for notifications and backfill requests.
    async fn start(mut self) -> eyre::Result<()> {
        loop {
            tokio::select! {
                Some(notification) = self.ctx.notifications.recv() => {
                    self.handle_notification(notification).await?;
                }
                Some(message) = self.backfill_rx.recv() => {
                    self.handle_backfill_message(message).await;
                }
            }
        }
    }

    /// Handles the given notification and calls [`process_committed_chain`] for a committed
    /// chain, if any.
    async fn handle_notification(&self, notification: ExExNotification) -> eyre::Result<()> {
        match &notification {
            ExExNotification::ChainCommitted { new } => {
                info!(committed_chain = ?new.range(), "Received commit");
            }
            ExExNotification::ChainReorged { old, new } => {
                info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
            }
            ExExNotification::ChainReverted { old } => {
                info!(reverted_chain = ?old.range(), "Received revert");
            }
        };

        if let Some(committed_chain) = notification.committed_chain() {
            process_committed_chain(&committed_chain)?;

            self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().number))?;
        }

        Ok(())
    }

    /// Handles the given backfill message.
    async fn handle_backfill_message(&mut self, message: BackfillMessage) {
        match message {
            BackfillMessage::Start { range, response_tx } => {
                let _ = response_tx
                    .send(self.start_backfill(range))
                    .inspect_err(|_| error!("Failed to send backfill start response"));
            }
            BackfillMessage::Cancel { job_id, response_tx } => {
                let _ = response_tx
                    .send(self.cancel_backfill(job_id).await)
                    .inspect_err(|_| error!("Failed to send backfill cancel response"));

                self.backfill_jobs.remove(&job_id);
            }
            BackfillMessage::Finish { job_id } => {
                self.backfill_jobs.remove(&job_id);
            }
        }
    }

    /// Backfills the given range of blocks in parallel. Requires acquiring a
    /// semaphore permit that limits the number of concurrent backfills. The backfill job is
    /// spawned in a separate task.
    ///
    /// Returns the backfill job ID or an error if the semaphore permit could not be acquired.
    fn start_backfill(&mut self, range: RangeInclusive<BlockNumber>) -> eyre::Result<u64> {
        let permit = self
            .backfill_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|err| eyre::eyre!("concurrent backfills limit reached: {err:?}"))?;

        let job_id = self.next_backfill_job_id;
        self.next_backfill_job_id += 1;

        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.backfill_jobs.insert(job_id, cancel_tx);

        let job = self
            .backfill_job_factory
            // Create a backfill job for the given range
            .backfill(range);
        let backfill_tx = self.backfill_tx.clone();

        // Spawn the backfill job in a separate task
        self.ctx.task_executor().spawn(async move {
            Self::backfill(permit, job_id, job, backfill_tx, cancel_rx).await
        });

        Ok(job_id)
    }

    /// Cancels the backfill job with the given ID.
    async fn cancel_backfill(&mut self, job_id: u64) -> eyre::Result<()> {
        let Some(cancel_tx) = self.backfill_jobs.remove(&job_id) else {
            eyre::bail!("backfill job not found");
        };

        let (tx, rx) = oneshot::channel();
        cancel_tx.send(tx).map_err(|_| eyre::eyre!("failed to cancel backfill job"))?;
        let _ = rx.await;

        Ok(())
    }

    /// Calls the [`process_committed_chain`] method for each backfilled block.
    ///
    /// Listens on the `cancel_rx` channel for cancellation requests.
    #[instrument(level = "info", skip(_permit, job, backfill_tx, cancel_rx))]
    async fn backfill(
        _permit: OwnedSemaphorePermit,
        job_id: u64,
        job: BackfillJob<Node::Executor, Node::Provider>,
        backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
        cancel_rx: oneshot::Receiver<oneshot::Sender<()>>,
    ) {
        let backfill = backfill_with_job(job);

        tokio::select! {
            result = backfill => {
                if let Err(err) = result {
                    error!(%err, "Backfill error occurred");
                }

                let _ = backfill_tx.send(BackfillMessage::Finish { job_id });
            }
            sender = cancel_rx => {
                info!("Backfill job cancelled");

                if let Ok(sender) = sender {
                    let _ = sender.send(());
                }
            }
        }
    }
}

/// Backfills the given range of blocks in parallel, calling the
/// [`process_committed_chain`] method for each block.
async fn backfill_with_job<
    E: BlockExecutorProvider + Send,
    P: BlockReader + HeaderProvider + StateProviderFactory + Clone + Send + Unpin + 'static,
>(
    job: BackfillJob<E, P>,
) -> eyre::Result<()> {
    job
        // Convert the backfill job into a parallel stream
        .into_stream()
        // Covert the block execution error into `eyre`
        .map_err(Into::into)
        // Process each block, returning early if an error occurs
        .try_for_each(|chain| async move { process_committed_chain(&chain) })
        .await
}

/// Processes the committed chain and logs the number of blocks and transactions.
fn process_committed_chain(chain: &Chain) -> eyre::Result<()> {
    // Calculate the number of blocks and transactions in the committed chain
    let blocks = chain.blocks().len();
    let transactions = chain.blocks().values().map(|block| block.body.len()).sum::<usize>();

    info!(first_block = %chain.execution_outcome().first_block, %blocks, %transactions, "Processed committed blocks");
    Ok(())
}

#[derive(Debug, Clone, Args)]
struct BackfillArgsExt {
    /// Start backfill from the specified block number
    #[clap(long = "backfill-from-block")]
    pub from_block: Option<u64>,

    /// Stop backfill at the specified block number
    #[clap(long = "backfill-to-block")]
    pub to_block: Option<BlockId>,
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::<BackfillArgsExt>::parse().run(|builder, args| async move {
        // Create a channel for backfill requests. Sender will go to the RPC server, receiver will
        // be used by the ExEx.
        let (backfill_tx, backfill_rx) = mpsc::unbounded_channel();
        let rpc_backfill_tx = backfill_tx.clone();
        let exex_backfill_tx = backfill_tx.clone();

        let handle = builder
            .node(EthereumNode::default())
            // Extend the RPC server with the backfill RPC module.
            .extend_rpc_modules(move |ctx| {
                ctx.modules
                    .merge_configured(BackfillRpcExt { backfill_tx: rpc_backfill_tx }.into_rpc())?;
                Ok(())
            })
            // Install the backfill ExEx.
            .install_exex("Backfill", move |ctx| {
                // Rust seems to trigger a bogus higher-ranked lifetime error when using
                // just an async closure here -- using `spawn_blocking` avoids this
                // particular issue.
                //
                // To avoid the higher ranked lifetime error we use `spawn_blocking`
                // which will move the closure to another blocking-allowed thread,
                // then execute.
                //
                // Source: https://github.com/vados-cosmonic/wasmCloud/commit/440e8c377f6b02f45eacb02692e4d2fabd53a0ec
                tokio::task::spawn_blocking(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let exex = BackfillExEx::new(ctx, exex_backfill_tx, backfill_rx, 10);

                        let to_block = args
                            .to_block
                            // If the end of range block number is not provided, but the start of
                            // range is, use the latest block number as
                            // the end of range.
                            .or(args
                                .from_block
                                .is_some()
                                .then_some(BlockNumberOrTag::Latest.into()));
                        if let Some(to_block) = to_block {
                            let to_block =
                                exex.ctx.provider().block_number_for_id(to_block)?.ok_or_eyre(
                                    "end of range block number for backfill is not found",
                                )?;

                            let job = exex
                                .backfill_job_factory
                                .backfill(args.from_block.unwrap_or(1)..=to_block);

                            backfill_with_job(job).await.map_err(|err| {
                                eyre::eyre!("failed to backfill for the provided args: {err:?}")
                            })?;
                        }

                        eyre::Ok(exex.start())
                    })
                })
                .map(|result| result.map_err(Into::into).and_then(|result| result))
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
