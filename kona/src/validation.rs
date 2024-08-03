//! Contains logic to validate derivation pipeline outputs.

use std::fmt::Debug;

use alloy::{
    providers::{Provider, ReqwestProvider},
    rpc::types::{BlockNumberOrTag, BlockTransactionsKind, Header},
    transports::TransportResult,
};
use async_trait::async_trait;
use kona_derive::types::{L2AttributesWithParent, L2PayloadAttributes, RawTransaction};
use reqwest::{Client, StatusCode, Url};
use reth::rpc::types::engine::{Claims, JwtSecret};
use tracing::{error, warn};

/// AttributesValidator
///
/// A trait that defines the interface for validating [`L2AttributesWithParent`].
#[async_trait]
pub(crate) trait AttributesValidator: Debug {
    /// Validates the given [`L2AttributesWithParent`] and returns true
    /// if the attributes are valid, false otherwise.
    async fn validate(&self, attributes: &L2AttributesWithParent) -> eyre::Result<bool>;
}

/// TrustedValidator
///
/// Validates the [`L2AttributesWithParent`] by fetching the associated L2 block from
/// a trusted L2 RPC and constructing the L2 Attributes from the block.
#[derive(Debug, Clone)]
pub struct TrustedValidator {
    /// The L2 provider.
    provider: ReqwestProvider,
    /// The canyon activation timestamp.
    canyon_activation: u64,
}

impl TrustedValidator {
    /// Creates a new [`TrustedValidator`].
    pub fn new(provider: ReqwestProvider, canyon: u64) -> Self {
        Self { provider, canyon_activation: canyon }
    }

    /// Creates a new [`TrustedValidator`] from the provided [Url].
    pub fn new_http(url: Url, canyon: u64) -> Self {
        let inner = ReqwestProvider::new_http(url);
        Self::new(inner, canyon)
    }

    /// Fetches a block [Header] and a list of raw RLP encoded transactions from the L2 provider.
    ///
    /// This method needs to fetch the non-hydrated block and then
    /// fetch the raw transactions using the `debug_*` namespace.
    pub(crate) async fn get_block(
        &self,
        tag: BlockNumberOrTag,
    ) -> eyre::Result<(Header, Vec<RawTransaction>)> {
        // Don't hydrate the block so we only get a list of transaction hashes.
        let block = self
            .provider
            .get_block(tag.into(), BlockTransactionsKind::Hashes)
            .await
            .map_err(|e| eyre::eyre!(e))?
            .ok_or(eyre::eyre!("Block not found"))?;
        // For each transaction hash, fetch the raw transaction RLP.
        let mut txs = vec![];
        for tx in block.transactions.hashes() {
            let tx: TransportResult<RawTransaction> =
                self.provider.raw_request("debug_getRawTransaction".into(), [tx]).await;
            if let Ok(tx) = tx {
                txs.push(tx);
            } else {
                warn!("Failed to fetch transaction: {:?}", tx);
            }
        }
        Ok((block.header, txs))
    }

    /// Gets the payload for the specified [BlockNumberOrTag].
    pub(crate) async fn get_payload(
        &self,
        tag: BlockNumberOrTag,
    ) -> eyre::Result<L2PayloadAttributes> {
        let (header, transactions) = self.get_block(tag).await?;
        Ok(L2PayloadAttributes {
            timestamp: header.timestamp,
            prev_randao: header.mix_hash.unwrap_or_default(),
            fee_recipient: header.miner,
            // Withdrawals on optimism are always empty, *after* canyon (Shanghai) activation
            withdrawals: (header.timestamp >= self.canyon_activation).then_some(Vec::default()),
            parent_beacon_block_root: header.parent_beacon_block_root,
            transactions,
            no_tx_pool: true,
            gas_limit: Some(header.gas_limit as u64),
        })
    }
}

#[async_trait]
impl AttributesValidator for TrustedValidator {
    async fn validate(&self, attributes: &L2AttributesWithParent) -> eyre::Result<bool> {
        let expected = attributes.parent.block_info.number + 1;
        let tag = BlockNumberOrTag::from(expected);
        match self.get_payload(tag).await {
            Ok(payload) => Ok(attributes.attributes == payload),
            Err(err) => {
                error!(target: "validation", ?err, "Failed to fetch payload for block {}", expected);
                eyre::bail!("Failed to fetch payload for block {}: {:?}", expected, err);
            }
        }
    }
}

/// EngineApiValidator
///
/// Validates the [`L2AttributesWithParent`] by sending the attributes to an L2 engine API.
/// The engine API will return a VALID or INVALID response.
#[derive(Debug, Clone)]
pub struct EngineApiValidator {
    /// The engine API URL.
    url: Url,
    /// The reqwest client.
    client: Client,
    /// The JWT secret token for the engine API.
    jwt_secret: JwtSecret,
}

impl EngineApiValidator {
    /// Creates a new [`EngineApiValidator`] from the provided [Url] and [JwtSecret].
    pub fn new_http(url: Url, jwt: JwtSecret) -> Self {
        Self { url, client: Client::new(), jwt_secret: jwt }
    }
}

#[async_trait]
impl AttributesValidator for EngineApiValidator {
    async fn validate(&self, attributes: &L2AttributesWithParent) -> eyre::Result<bool> {
        let request_body = serde_json::json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "engine_newPayloadV2",
            "params": [attributes.attributes]
        });

        let claims = Claims::default();
        let jwt = self.jwt_secret.encode(&claims)?;

        let response = self
            .client
            .post(self.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", jwt))
            .json(&request_body)
            .send()
            .await?;

        let status = response.status();
        let body = response.json::<serde_json::Value>().await?;
        match status {
            StatusCode::OK => Ok(body
                .pointer("/result/status")
                .and_then(|status| status.as_str())
                .map_or(false, |status| status == "VALID")),
            _ => {
                error!(target: "validation", ?body, "Engine API returned status: {}", status);
                eyre::bail!("Engine API returned status: {} and body: {:#?}", status, body);
            }
        }
    }
}
