use crate::{
    exex::ExEx,
    network::{proto::data::SignedTicker, OracleNetwork},
    offchain_data::{DataFeederError, DataFeeds},
};
use alloy_rlp::{BytesMut, Encodable};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use futures::{FutureExt, Stream, StreamExt};
use reth::api::NodeTypes;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_api::FullNodeComponents;
use reth_tracing::tracing::{error, info, trace};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// The Oracle struct is a long running task that orchestrates discovery of new peers,
/// decoding data from chain events (ExEx) and gossiping it to peers.
pub(crate) struct Oracle<Node: FullNodeComponents, D>
where
    D: Stream<Item = Result<DataFeeds, DataFeederError>> + Send + 'static,
{
    /// The network task for this node.
    /// It is composed by a discovery task and a sub protocol RLPx task.
    network: OracleNetwork,
    /// The execution extension task for this node.
    exex: ExEx<Node>,
    /// The offchain data feed stream.
    data_feed: D,
    /// The signer to sign the data feed.
    signer: PrivateKeySigner,
    /// Half of the broadcast channel to send data to connected peers.
    to_peers: tokio::sync::broadcast::Sender<SignedTicker>,
}

impl<Node: FullNodeComponents, D> Oracle<Node, D>
where
    D: Stream<Item = Result<DataFeeds, DataFeederError>> + Send + 'static,
{
    pub(crate) fn new(
        exex: ExEx<Node>,
        network: OracleNetwork,
        data_feed: D,
        to_peers: tokio::sync::broadcast::Sender<SignedTicker>,
    ) -> Self {
        Self { exex, network, data_feed, signer: PrivateKeySigner::random(), to_peers }
    }

    /// Returns the signer used by the oracle.
    #[allow(dead_code)]
    pub(crate) fn signer(&self) -> &PrivateKeySigner {
        &self.signer
    }

    /// Returns the signed ticker broadcast channel.
    #[allow(dead_code)]
    pub(crate) fn signed_ticks(&self) -> &tokio::sync::broadcast::Sender<SignedTicker> {
        &self.to_peers
    }
}

impl<Node, D> Future for Oracle<Node, D>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
    D: Stream<Item = Result<DataFeeds, DataFeederError>> + Send + 'static + std::marker::Unpin,
{
    type Output = eyre::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        // Poll the network future until its drained
        loop {
            match this.network.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    info!("Discv5 task completed successfully");
                }
                Poll::Ready(Err(e)) => {
                    error!(?e, "Discv5 task encountered an error");
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    // Exit match and continue to poll exex
                    break;
                }
            }
        }

        // Poll the data feed future until it's drained
        while let Poll::Ready(item) = this.data_feed.poll_next_unpin(cx) {
            match item {
                Some(Ok(ticker_data)) => {
                    let DataFeeds::Binance(ticker) = ticker_data;
                    let mut buffer = BytesMut::new();
                    ticker.encode(&mut buffer);
                    let signature = this.signer.sign_message_sync(&buffer)?;
                    let signed_ticker = SignedTicker::new(ticker, signature, this.signer.address());

                    if this.to_peers.send(signed_ticker.clone()).is_ok() {
                        let signer = signed_ticker.signer;
                        trace!(target: "oracle", ?signer, "Sent signed ticker");
                    }
                }
                Some(Err(e)) => {
                    error!(?e, "Data feed task encountered an error");
                    return Poll::Ready(Err(e.into()));
                }
                None => break,
            }
        }

        // Poll the exex future until its drained
        loop {
            match this.exex.poll_unpin(cx)? {
                Poll::Ready(t) => t,
                Poll::Pending => return Poll::Pending,
            };
        }
    }
}
