//! An enrichment provider that answers from stored EVM logs before it asks
//! WormholeScan. It wraps the existing provider rather than replacing it: an unwatched
//! destination, or a transfer whose event predates the watcher, still falls through.

use crate::domain::bridge_transfer::{BridgeMessageId, BridgeTransfer};
use crate::enrichment::{EnrichInput, EnrichOutcome, Enriched, EnrichmentProvider};
use crate::evm::lookup::EvmEventLookup;
use crate::protocols::Direction;
use async_trait::async_trait;
use std::sync::Arc;

pub struct EvmFirstProvider {
    inner: Arc<dyn EnrichmentProvider>,
    lookup: EvmEventLookup,
}

impl EvmFirstProvider {
    pub fn new(inner: Arc<dyn EnrichmentProvider>, lookup: EvmEventLookup) -> Self {
        Self { inner, lookup }
    }
}

#[async_trait]
impl EnrichmentProvider for EvmFirstProvider {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn enrich(&self, input: EnrichInput<'_>, transfer: &mut BridgeTransfer) -> Enriched {
        let missing = match transfer.direction() {
            Direction::Inbound => transfer.source_tx_hash.is_none(),
            Direction::Outbound => transfer.destination_tx_hash.is_none(),
        };
        if missing {
            match self.lookup.apply(transfer).await {
                Ok(true) => {
                    tracing::debug!(
                        chain = transfer.destination_chain.wormhole_id(),
                        "tx hash filled from stored EVM logs, WormholeScan not called"
                    );
                    return Enriched::of(EnrichOutcome::Applied);
                }
                Ok(false) => {}
                // A lookup failure must not block enrichment: fall through to the
                // provider that can still answer.
                Err(error) => tracing::debug!(
                    %error,
                    "EVM event lookup failed, falling back to WormholeScan"
                ),
            }
        }
        self.inner.enrich(input, transfer).await
    }

    async fn destination_tx(&self, id: &BridgeMessageId) -> Option<String> {
        match self.lookup.destination_tx(id).await {
            Ok(Some(hash)) => Some(hash),
            Ok(None) => self.inner.destination_tx(id).await,
            Err(error) => {
                tracing::debug!(%error, "EVM redeem lookup failed, falling back to WormholeScan");
                self.inner.destination_tx(id).await
            }
        }
    }
}
