//! Enrichment providers. A provider layers counterpart and status data from a protocol
//! API onto a transfer that was already decoded from Solana data. Enrichment can raise a
//! transfer's status but never gates persistence and never moves a status backwards.
//!
//! WormholeScan is the protocol-API provider. [`evm_first::EvmFirstProvider`] wraps it
//! when EVM watching is enabled, so a stored bridge log answers first and WormholeScan
//! is only the fallback.

use crate::domain::bridge::BridgeEvent;
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeTransfer};
use crate::error::AppError;
use async_trait::async_trait;
use serde_json::Value;

pub mod evm_first;
pub mod reenrich;
pub mod wormholescan;

/// A provider response the caller already fetched, so a cold analysis can run it in
/// parallel with the Solana fetch instead of paying for it afterwards.
pub enum Prefetch<'a> {
    /// Nothing fetched yet: the provider calls its API itself.
    None,
    /// The provider's response (or its error), fetched by the caller.
    Done(Result<&'a Value, &'a AppError>),
}

pub struct EnrichInput<'a> {
    /// The Solana signature the transfer was decoded from.
    pub source_tx_hash: &'a str,
    pub prefetch: Prefetch<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichOutcome {
    /// The provider's data matches the on-chain message and was applied.
    Applied,
    /// The provider has no usable record yet. The transfer is unchanged.
    NoVaa,
    /// The provider's record describes a different message. Ignored, the chain wins.
    Mismatch,
    /// The provider could not be reached (error, rate limit, overload). Nothing was
    /// learned, so a caller should retry later.
    Unavailable,
}

pub struct Enriched {
    pub outcome: EnrichOutcome,
    /// The bridge event the provider response normalises to, when it applied.
    pub bridge_event: Option<BridgeEvent>,
}

impl Enriched {
    pub fn of(outcome: EnrichOutcome) -> Self {
        Self {
            outcome,
            bridge_event: None,
        }
    }
}

#[async_trait]
pub trait EnrichmentProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Layers the provider's data onto `transfer`. Never fails: a provider problem is an
    /// [`EnrichOutcome`], because enrichment must not block persistence.
    async fn enrich(&self, input: EnrichInput<'_>, transfer: &mut BridgeTransfer) -> Enriched;

    /// The destination tx of a message, when the provider knows it.
    async fn destination_tx(&self, id: &BridgeMessageId) -> Option<String>;
}
