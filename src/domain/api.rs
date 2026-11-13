//! The generalised `/api/v2` response. One transaction carries every transfer decoded
//! from it; chain references keep the protocol's own namespace alongside the resolved
//! chain, so an unknown chain is data, never an error.

use crate::domain::transaction::TxStatus;
use crate::protocols::{Direction, MessageId, Via};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub hash: String,
    /// The chain the observed Solana transaction lives on.
    pub chain: ChainView,
    pub status: TxStatus,
    pub slot: u64,
    pub timestamp: Option<i64>,
    pub fee_lamports: u64,
    pub signer: Option<String>,
    /// Every transfer decoded from this transaction, ordered by `event_index`.
    pub transfers: Vec<TransferView>,
}

impl TransactionResponse {
    /// Terminal enough for the long cache TTL: every transfer has completed.
    pub fn is_complete(&self) -> bool {
        !self.transfers.is_empty()
            && self
                .transfers
                .iter()
                .all(|t| matches!(t.status, TransferStatus::Completed))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferView {
    /// The bridge protocol, for example `wormhole`.
    pub protocol: String,
    /// The protocol sub-program that decoded this transfer, e.g. `token_bridge`.
    pub program: String,
    /// Human-facing product name for `(protocol, program)`, e.g. `Portal`.
    pub protocol_name: String,
    pub direction: Direction,
    /// The router or outer protocol, when there is one.
    pub via: Option<Via>,
    /// The Solana signature this transfer was decoded from.
    pub observed_tx_hash: String,
    /// Position among the transfers of one transaction.
    pub event_index: u16,
    /// Protocol-specific message identity, tagged with its protocol.
    pub message_id: MessageId,
    pub source: ChainView,
    pub destination: ChainView,
    pub source_wallet: Option<String>,
    pub destination_wallet: Option<String>,
    pub source_tx_hash: Option<String>,
    pub destination_tx_hash: Option<String>,
    pub source_explorer_url: Option<String>,
    pub destination_explorer_url: Option<String>,
    pub token: Option<TokenView>,
    pub token_symbol: Option<String>,
    pub amount_raw: Option<String>,
    pub amount_formatted: Option<String>,
    pub status: TransferStatus,
}

/// A chain as the protocol wrote it, plus the registry's answer when it knows one.
/// `chain` is `null` for an unresolved id; `protocol` and `id` are always present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainView {
    pub protocol: String,
    pub id: String,
    pub chain: Option<ChainDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainDetails {
    pub id: i16,
    pub slug: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenView {
    /// Where the token lives. `None` for rows written before the token chain was stored.
    pub chain: Option<ChainView>,
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Detected,
    Pending,
    Completed,
    Failed,
    Refunded,
}

impl TransferStatus {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Detected" => Some(Self::Detected),
            "Pending" => Some(Self::Pending),
            "Completed" => Some(Self::Completed),
            "Failed" => Some(Self::Failed),
            "Refunded" => Some(Self::Refunded),
            _ => None,
        }
    }
}

/// One page of the `/api/v2/transfers` list. Items use the same [`TransferView`]
/// the analyse response carries, so field names match. There is deliberately no
/// total count: it would require a full scan of the filtered set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferListResponse {
    pub items: Vec<TransferView>,
    /// `None` on the last page. Opaque to clients: echo it back as `cursor`.
    pub next_cursor: Option<String>,
}
