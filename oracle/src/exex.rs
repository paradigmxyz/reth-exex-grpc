use alloy_consensus::BlockHeader;
use eyre::Result;
use futures::{Future, FutureExt, TryStreamExt};
use reth::{api::NodeTypes, providers::ExecutionOutcome};
use reth_ethereum_primitives::EthPrimitives;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_tracing::tracing::info;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

/// The ExEx struct, representing the initialization and execution of the ExEx.
pub struct ExEx<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    /// Execution outcome of the chain
    execution_outcome: ExecutionOutcome,
}

impl<Node: FullNodeComponents> ExEx<Node> {
    pub(crate) fn new(ctx: ExExContext<Node>) -> Self {
        Self { ctx, execution_outcome: ExecutionOutcome::default() }
    }
}

impl<Node> Future for ExEx<Node>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Continuously poll the ExExContext notifications
        let this = self.get_mut();
        while let Some(notification) = ready!(this.ctx.notifications.try_next().poll_unpin(cx))? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    info!(committed_chain = ?new.range(), "Received commit");
                }
                ExExNotification::ChainReorged { old, new } => {
                    // revert to block before the reorg
                    this.execution_outcome.revert_to(new.first().number() - 1);
                    info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
                }
                ExExNotification::ChainReverted { old } => {
                    this.execution_outcome.revert_to(old.first().number() - 1);
                    info!(reverted_chain = ?old.range(), "Received revert");
                }
            };

            if let Some(committed_chain) = notification.committed_chain() {
                // extend the state with the new chain
                this.execution_outcome.extend(committed_chain.execution_outcome().clone());
                this.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }
        Poll::Ready(Ok(()))
    }
}
