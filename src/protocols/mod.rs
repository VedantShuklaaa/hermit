//! Protocol-agnostic core: one [`BridgeAdapter`] per bridge protocol.
//!
//! Adapters are pure. They read a [`SolanaTx`] and either return the transfers it
//! contains or a [`Reject`] with a reason. No IO, no clock, no database.

pub mod amount;
pub mod compose;
pub mod registry;
pub mod solana_tx;
pub mod wormhole;

use crate::chains::RawChainRef;
use amount::RawAmount;
use serde::{Deserialize, Serialize};
pub use solana_tx::{AccountData, Instruction, SolanaTx};

/// The Token Bridge sub-program. It is the default `program` discriminator, so rows
/// written before the discriminator existed mean exactly this.
pub const TOKEN_BRIDGE_PROGRAM: &str = "token_bridge";

/// The NFT Bridge sub-program. It shares Wormhole's core bridge and message namespace
/// with the Token Bridge, but is a different Solana program with its own payloads.
pub const NFT_BRIDGE_PROGRAM: &str = "nft_bridge";

/// The serde default for a transfer that predates the discriminator.
pub fn default_program() -> String {
    TOKEN_BRIDGE_PROGRAM.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Wormhole,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Wormhole => "wormhole",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "wormhole" => Some(Protocol::Wormhole),
            _ => None,
        }
    }
}

/// Which way a transfer moves relative to Solana.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// A send out of Solana.
    Outbound,
    /// A redeem or fill into Solana.
    Inbound,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Outbound => "Outbound",
            Direction::Inbound => "Inbound",
        }
    }
}

/// Protocol-specific message identity. One variant per protocol, added in that
/// protocol's phase (CCTP domain/nonce, LayerZero GUID, deBridge order ID, ...).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum MessageId {
    Wormhole {
        emitter_chain: u16,
        emitter_address: String,
        sequence: u64,
    },
}

impl MessageId {
    pub fn protocol(&self) -> Protocol {
        match self {
            MessageId::Wormhole { .. } => Protocol::Wormhole,
        }
    }

    /// Canonical key, unique per protocol. Stored as `bridge_transfers.message_key`.
    pub fn key(&self) -> String {
        match self {
            MessageId::Wormhole {
                emitter_chain,
                emitter_address,
                sequence,
            } => format!("{emitter_chain}/{emitter_address}/{sequence}"),
        }
    }
}

/// The router or outer protocol a transfer went through, when there is one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Via {
    pub name: String,
    /// The outer protocol's own reference (for example an order ID).
    pub reference: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRef {
    /// Where the token lives, in the protocol's chain namespace.
    pub chain: RawChainRef,
    /// Address in the token's home chain's own format.
    pub address: String,
}

/// One bridge transfer decoded from a Solana transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectedTransfer {
    /// Which program inside the protocol decoded this: `token_bridge` or
    /// `nft_bridge`. It is what `/api/v1` can represent and what `/api/v2` exposes
    /// additively.
    pub program: String,
    pub message: MessageId,
    pub direction: Direction,
    /// The Solana signature this was decoded from.
    pub observed_tx: String,
    /// Position among the transfers of one tx, so several can coexist.
    pub event_index: u16,
    pub source: RawChainRef,
    pub destination: RawChainRef,
    pub source_wallet: Option<String>,
    pub destination_wallet: Option<String>,
    pub token: Option<TokenRef>,
    /// Symbol carried by the bridge payload itself, when it has one. Chain data, so
    /// it wins over token metadata from an indexer.
    pub token_symbol: Option<String>,
    pub amount_raw: Option<RawAmount>,
    pub via: Option<Via>,
}

/// Why a transaction that touches a bridge program was not turned into a transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    /// Admin, attest, governance, registration, ...
    NotTransfer(&'static str),
    /// A known program and a variant we do not decode yet.
    Unsupported(&'static str),
    /// The layout did not match what the program documents.
    Malformed(&'static str),
}

impl Reject {
    pub fn kind(&self) -> &'static str {
        match self {
            Reject::NotTransfer(_) => "not_transfer",
            Reject::Unsupported(_) => "unsupported",
            Reject::Malformed(_) => "malformed",
        }
    }

    pub fn reason(&self) -> &'static str {
        match self {
            Reject::NotTransfer(r) | Reject::Unsupported(r) | Reject::Malformed(r) => r,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classification {
    Transfers(Vec<DetectedTransfer>),
    Rejected(Reject),
}

pub trait BridgeAdapter: Send + Sync {
    fn protocol(&self) -> Protocol;

    /// Verified program IDs. `SOLANA_BRIDGE_PROGRAMS` can override them.
    fn default_program_ids(&self) -> &'static [&'static str];

    /// Pure: same input, same output. Must never panic on malformed input.
    fn classify(&self, tx: &SolanaTx) -> Classification;

    /// Accounts the adapter needs read from the chain before `classify`, as pubkeys.
    /// Pure: derived from the tx alone. Most adapters need none.
    fn required_accounts(&self, _tx: &SolanaTx) -> Vec<String> {
        Vec::new()
    }
}
