//! DB-backed chain registry. The tables come from the `chain_registry` migration and
//! [`db`] loads them into an in-memory [`Snapshot`].
//!
//! Adapters stay pure by emitting a [`RawChainRef`] in the protocol's own chain-ID
//! namespace. Resolution is an in-memory lookup against a [`Snapshot`].
//!
//! Only `active` rows resolve, in both tables. That single rule keeps an unverified
//! proposal (`pending_review`, for example from the later LLM upgrade) from ever
//! labelling a real transfer.

pub mod db;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Namespace of Wormhole chain IDs (`u16`).
pub const NS_WORMHOLE: &str = "wormhole";
/// Namespace of EVM chain IDs.
pub const NS_EVM: &str = "evm";
/// Namespace of Circle CCTP domain IDs (`u32`).
pub const NS_CCTP: &str = "cctp";

/// A chain identifier exactly as a protocol wrote it, plus which namespace it is in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RawChainRef {
    pub protocol: String,
    pub id: String,
}

impl RawChainRef {
    pub fn new(protocol: impl Into<String>, id: impl ToString) -> Self {
        Self {
            protocol: protocol.into(),
            id: id.to_string(),
        }
    }

    pub fn wormhole(id: u16) -> Self {
        Self::new(NS_WORMHOLE, id)
    }

    pub fn cctp(id: u32) -> Self {
        Self::new(NS_CCTP, id)
    }
}

/// `chains.id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChainKey(pub i16);

/// A resolved chain, or the raw reference kept as-is. Never an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainRef {
    Resolved(ChainKey),
    Unresolved { protocol: String, id: String },
}

impl ChainRef {
    pub fn key(&self) -> Option<ChainKey> {
        match self {
            ChainRef::Resolved(k) => Some(*k),
            ChainRef::Unresolved { .. } => None,
        }
    }

    pub fn is_resolved(&self) -> bool {
        matches!(self, ChainRef::Resolved(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryStatus {
    Active,
    PendingReview,
    Rejected,
}

impl RegistryStatus {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(Self::Active),
            "pending_review" => Some(Self::PendingReview),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChainRow {
    pub id: i16,
    pub slug: String,
    pub name: String,
    pub kind: String,
    /// Template with a `{hash}` placeholder.
    pub explorer_tx_url: Option<String>,
    pub status: RegistryStatus,
}

#[derive(Debug, Clone)]
pub struct MappingRow {
    pub protocol: String,
    pub protocol_chain_id: String,
    pub chain_id: i16,
    pub status: RegistryStatus,
}

/// Immutable lookup tables, built once per refresh.
#[derive(Debug, Default)]
pub struct Snapshot {
    chains: HashMap<i16, ChainRow>,
    by_ref: HashMap<(String, String), i16>,
}

impl Snapshot {
    /// Keeps only active chains, and active mappings that point at an active chain.
    pub fn build(chains: Vec<ChainRow>, mappings: Vec<MappingRow>) -> Self {
        let chains: HashMap<i16, ChainRow> = chains
            .into_iter()
            .filter(|c| c.status == RegistryStatus::Active)
            .map(|c| (c.id, c))
            .collect();

        let by_ref = mappings
            .into_iter()
            .filter(|m| m.status == RegistryStatus::Active && chains.contains_key(&m.chain_id))
            .map(|m| ((m.protocol, m.protocol_chain_id), m.chain_id))
            .collect();

        Self { chains, by_ref }
    }

    pub fn resolve(&self, raw: &RawChainRef) -> ChainRef {
        match self.by_ref.get(&(raw.protocol.clone(), raw.id.clone())) {
            Some(id) => ChainRef::Resolved(ChainKey(*id)),
            None => ChainRef::Unresolved {
                protocol: raw.protocol.clone(),
                id: raw.id.clone(),
            },
        }
    }

    pub fn chain(&self, key: ChainKey) -> Option<&ChainRow> {
        self.chains.get(&key.0)
    }

    pub fn slug(&self, key: ChainKey) -> Option<&str> {
        self.chain(key).map(|c| c.slug.as_str())
    }

    /// The active chain with this canonical key. An unknown or inactive slug is `None`,
    /// never an error: the caller decides how to report it.
    pub fn chain_id_by_slug(&self, slug: &str) -> Option<ChainKey> {
        self.chains
            .values()
            .find(|chain| chain.slug == slug)
            .map(|chain| ChainKey(chain.id))
    }

    pub fn explorer_tx_url(&self, key: ChainKey, tx_hash: &str) -> Option<String> {
        self.chain(key)?
            .explorer_tx_url
            .as_ref()
            .map(|t| t.replace("{hash}", tx_hash))
    }

    pub fn explorer_tx_url_for(&self, raw: &RawChainRef, tx_hash: &str) -> Option<String> {
        self.explorer_tx_url(self.resolve(raw).key()?, tx_hash)
    }
}

/// Shared handle. Readers take a cheap `Arc` clone of the current snapshot; a
/// refresh swaps in a new one.
#[derive(Debug, Clone, Default)]
pub struct ChainRegistry {
    inner: Arc<RwLock<Arc<Snapshot>>>,
}

impl ChainRegistry {
    pub fn new(snapshot: Snapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(snapshot))),
        }
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn replace(&self, snapshot: Snapshot) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(snapshot);
    }

    pub fn resolve(&self, raw: &RawChainRef) -> ChainRef {
        self.snapshot().resolve(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(id: i16, slug: &str, status: RegistryStatus) -> ChainRow {
        ChainRow {
            id,
            slug: slug.to_string(),
            name: slug.to_string(),
            kind: "evm".to_string(),
            explorer_tx_url: Some(format!("https://{slug}.example/tx/{{hash}}")),
            status,
        }
    }

    fn mapping(protocol: &str, id: &str, chain_id: i16, status: RegistryStatus) -> MappingRow {
        MappingRow {
            protocol: protocol.to_string(),
            protocol_chain_id: id.to_string(),
            chain_id,
            status,
        }
    }

    fn registry() -> ChainRegistry {
        ChainRegistry::new(Snapshot::build(
            vec![
                chain(1, "solana", RegistryStatus::Active),
                chain(2, "ethereum", RegistryStatus::Active),
                chain(3, "newchain", RegistryStatus::PendingReview),
            ],
            vec![
                mapping(NS_WORMHOLE, "1", 1, RegistryStatus::Active),
                mapping(NS_WORMHOLE, "2", 2, RegistryStatus::Active),
                mapping(NS_EVM, "1", 2, RegistryStatus::Active),
                // active mapping to a chain that is not active: must not resolve
                mapping(NS_WORMHOLE, "900", 3, RegistryStatus::Active),
                // pending mapping to an active chain: must not resolve
                mapping(NS_WORMHOLE, "901", 2, RegistryStatus::PendingReview),
            ],
        ))
    }

    #[test]
    fn same_number_resolves_per_namespace() {
        let r = registry();
        // Wormhole 1 is Solana, EVM 1 is Ethereum.
        assert_eq!(
            r.resolve(&RawChainRef::wormhole(1)),
            ChainRef::Resolved(ChainKey(1))
        );
        assert_eq!(
            r.resolve(&RawChainRef::new(NS_EVM, 1)),
            ChainRef::Resolved(ChainKey(2))
        );
    }

    #[test]
    fn unknown_ids_are_unresolved_not_errors() {
        let r = registry();
        assert_eq!(
            r.resolve(&RawChainRef::wormhole(65000)),
            ChainRef::Unresolved {
                protocol: NS_WORMHOLE.to_string(),
                id: "65000".to_string()
            }
        );
    }

    #[test]
    fn only_active_rows_resolve() {
        let r = registry();
        assert!(!r.resolve(&RawChainRef::wormhole(900)).is_resolved());
        assert!(!r.resolve(&RawChainRef::wormhole(901)).is_resolved());
    }

    #[test]
    fn explorer_url_fills_the_template() {
        let snap = registry().snapshot();
        assert_eq!(
            snap.explorer_tx_url(ChainKey(1), "abc").as_deref(),
            Some("https://solana.example/tx/abc")
        );
        assert_eq!(snap.explorer_tx_url(ChainKey(3), "abc"), None);
    }

    #[test]
    fn replace_swaps_the_snapshot_for_existing_handles() {
        let r = registry();
        let handle = r.clone();
        assert!(!handle.resolve(&RawChainRef::wormhole(4)).is_resolved());

        r.replace(Snapshot::build(
            vec![chain(4, "bsc", RegistryStatus::Active)],
            vec![mapping(NS_WORMHOLE, "4", 4, RegistryStatus::Active)],
        ));
        assert_eq!(
            handle.resolve(&RawChainRef::wormhole(4)),
            ChainRef::Resolved(ChainKey(4))
        );
    }

    #[test]
    fn explorer_url_for_follows_the_resolved_chain() {
        let snap = Snapshot::build(
            vec![chain(1, "solana", RegistryStatus::Active)],
            vec![mapping(NS_WORMHOLE, "1", 1, RegistryStatus::Active)],
        );
        assert_eq!(
            snap.explorer_tx_url_for(&RawChainRef::wormhole(1), "abc")
                .as_deref(),
            Some("https://solana.example/tx/abc")
        );
        assert_eq!(
            snap.explorer_tx_url_for(&RawChainRef::wormhole(9), "abc"),
            None
        );
    }
}
