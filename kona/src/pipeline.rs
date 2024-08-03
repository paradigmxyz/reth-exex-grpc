//! Kona Pipeline types and helper functions.

use std::sync::Arc;

use kona_derive::{
    online::{AlloyL2ChainProvider, DerivationPipeline, EthereumDataSource, PipelineBuilder},
    stages::{
        AttributesQueue, BatchQueue, ChannelBank, ChannelReader, FrameQueue, L1Retrieval,
        L1Traversal, StatefulAttributesBuilder,
    },
    types::{BlockInfo, RollupConfig},
};

use crate::{blobs::ExExBlobProvider, providers::LocalChainProvider};

pub(crate) type LocalPipeline =
    DerivationPipeline<LocalAttributesQueue<LocalDataProvider>, AlloyL2ChainProvider>;

pub(crate) type LocalDataProvider = EthereumDataSource<LocalChainProvider, ExExBlobProvider>;

pub(crate) type LocalAttributesBuilder =
    StatefulAttributesBuilder<LocalChainProvider, AlloyL2ChainProvider>;

pub(crate) type LocalAttributesQueue<DAP> = AttributesQueue<
    BatchQueue<
        ChannelReader<ChannelBank<FrameQueue<L1Retrieval<DAP, L1Traversal<LocalChainProvider>>>>>,
        AlloyL2ChainProvider,
    >,
    LocalAttributesBuilder,
>;

/// Creates a new pipeline with a [`LocalChainProvider`] and an online [`AlloyL2ChainProvider`].
pub(crate) fn new_local_pipeline(
    cfg: Arc<RollupConfig>,
    chain_provider: LocalChainProvider,
    l2_provider: AlloyL2ChainProvider,
    dap: EthereumDataSource<LocalChainProvider, ExExBlobProvider>,
    attributes: LocalAttributesBuilder,
    tip: BlockInfo,
) -> LocalPipeline {
    PipelineBuilder::new()
        .rollup_config(cfg.clone())
        .dap_source(dap)
        .l2_chain_provider(l2_provider.clone())
        .chain_provider(chain_provider.clone())
        .builder(attributes)
        .origin(tip)
        .build()
}
