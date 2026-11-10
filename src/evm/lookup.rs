//! Transfer-side lookup. When an enrichment or re-enrichment pass builds or updates a
//! row whose source tx (inbound) or destination tx (outbound) is still null, the stored
//! EVM events answer first. WormholeScan is only asked when this misses.
//!
//! There is no age limit here: `EVM_JOIN_MAX_AGE_SECS` bounds the event-side retry
//! sweep, not this lookup.

use super::db;
use super::decode::EventKind;
use crate::chains::{ChainRegistry, RawChainRef};
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeTransfer, ChainId};
use crate::protocols::Direction;
use crate::services::detect;
use sqlx::PgPool;

pub struct EvmEventLookup {
    pool: PgPool,
    chains: ChainRegistry,
}

impl EvmEventLookup {
    pub fn new(pool: PgPool, chains: ChainRegistry) -> Self {
        Self { pool, chains }
    }

    /// Fills the missing side of `transfer` from a stored event. Returns whether it
    /// changed the transfer. A database failure is returned to the caller, which must
    /// fall back to WormholeScan rather than fail enrichment.
    pub async fn apply(&self, transfer: &mut BridgeTransfer) -> anyhow::Result<bool> {
        let direction = transfer.direction();
        let (event_kind, expected_chain) = match direction {
            Direction::Inbound => {
                if transfer.source_tx_hash.is_some() {
                    return Ok(false);
                }
                (
                    EventKind::LogMessagePublished,
                    transfer.source_chain.clone(),
                )
            }
            Direction::Outbound => {
                if transfer.destination_tx_hash.is_some() {
                    return Ok(false);
                }
                (
                    EventKind::TransferRedeemed,
                    transfer.destination_chain.clone(),
                )
            }
        };

        let Some(chain_id) = self.chain_key(expected_chain) else {
            return Ok(false);
        };
        let id = &transfer.message_id;
        let Some(hash) = db::lookup_tx_hash(
            &self.pool,
            chain_id,
            event_kind,
            id.emitter_chain,
            &id.emitter_address.to_ascii_lowercase(),
            id.sequence,
        )
        .await?
        else {
            return Ok(false);
        };

        let snapshot = self.chains.snapshot();
        Ok(match direction {
            Direction::Inbound => detect::apply_source_tx(transfer, &hash, &snapshot),
            Direction::Outbound => detect::apply_chain_destination_tx(transfer, &hash, &snapshot),
        })
    }

    /// The destination tx of a message, when a watched EVM chain redeemed it. Used by
    /// the WormholeScan-driven fallback path before it asks WormholeScan.
    pub async fn destination_tx(&self, id: &BridgeMessageId) -> anyhow::Result<Option<String>> {
        db::lookup_redeem_tx(
            &self.pool,
            id.emitter_chain,
            &id.emitter_address.to_ascii_lowercase(),
            id.sequence,
        )
        .await
    }

    fn chain_key(&self, chain: ChainId) -> Option<i16> {
        self.chains
            .resolve(&RawChainRef::wormhole(chain.wormhole_id()))
            .key()
            .map(|key| key.0)
    }
}
