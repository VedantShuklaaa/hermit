//! What one analysis produces: the v1 transaction body plus every transfer decoded from
//! the transaction, in the order the adapters found them. Persistence writes one row per
//! decoded transfer; `/api/v1` keeps reading the first one.

use crate::domain::bridge_transfer::{BridgeStatus, BridgeTransfer};
use crate::domain::transaction::NormalisedTransaction;
use crate::protocols::DetectedTransfer;

#[derive(Debug, Clone)]
pub struct DecodedTransfer {
    /// The adapter's view: identity, `event_index`, raw chain references, `via`.
    pub detected: DetectedTransfer,
    /// The frozen v1 body, for the protocols `/api/v1` can represent. `None` for a
    /// protocol whose identity v1 has no field for, such as CCTP.
    pub bridge: Option<BridgeTransfer>,
    /// The protocol-agnostic values every `bridge_transfers` row needs.
    pub row: RowValues,
}

/// A `bridge_transfers` row's columns, independent of any one protocol's API shape.
#[derive(Debug, Clone)]
pub struct RowValues {
    pub source_tx_hash: Option<String>,
    pub source_wallet: Option<String>,
    pub source_explorer_url: Option<String>,
    pub destination_wallet: Option<String>,
    pub destination_tx_hash: Option<String>,
    pub destination_explorer_url: Option<String>,
    pub token: Option<String>,
    pub token_symbol: Option<String>,
    pub amount: Option<String>,
    pub amount_formatted: Option<String>,
    pub status: BridgeStatus,
}

#[derive(Debug, Clone)]
pub struct Analysis {
    /// The v1 body for the first transfer. The pipeline keeps it for the cached
    /// analysis column and for `/api/v1`.
    pub tx: NormalisedTransaction,
    /// Every decoded transfer. Empty for the WormholeScan-driven path, which can only
    /// produce one transfer without raw chain references.
    pub transfers: Vec<DecodedTransfer>,
}
