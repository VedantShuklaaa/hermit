use crate::protocols::Direction;
use serde::{Deserialize, Serialize};

/// The chain as `/api/v1` and stored analyses spell it. Frozen: logic never branches on
/// it, chain resolution and explorer URLs go through `chains::ChainRegistry`. It is retired
/// with `/api/v1` (A10).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ChainId {
    Solana,
    Ethereum,
    Bsc,
    Avalanche,
    Base,
    Arbitrum,
    Optimism,
    Polygon,
    Gnosis,
    Near,
    Unknown(u16),
}

impl ChainId {
    pub fn from_wormhole_id(id: u16) -> Self {
        match id {
            1 => ChainId::Solana,
            2 => ChainId::Ethereum,
            4 => ChainId::Bsc,
            5 => ChainId::Polygon,
            6 => ChainId::Avalanche,
            15 => ChainId::Near,
            23 => ChainId::Arbitrum,
            24 => ChainId::Optimism,
            25 => ChainId::Gnosis,
            30 => ChainId::Base,
            other => ChainId::Unknown(other),
        }
    }

    pub fn wormhole_id(&self) -> u16 {
        match self {
            ChainId::Solana => 1,
            ChainId::Ethereum => 2,
            ChainId::Bsc => 4,
            ChainId::Polygon => 5,
            ChainId::Avalanche => 6,
            ChainId::Near => 15,
            ChainId::Arbitrum => 23,
            ChainId::Optimism => 24,
            ChainId::Gnosis => 25,
            ChainId::Base => 30,
            ChainId::Unknown(id) => *id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BridgeStatus {
    Detected,
    Pending,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeMessageId {
    pub emitter_chain: u16,
    pub emitter_address: String,
    pub sequence: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeTransfer {
    /// The protocol sub-program that produced this transfer (`token_bridge` today).
    /// It is skipped when serialising, so the frozen `/api/v1` analysis stays byte for
    /// byte identical and older analyses deserialize with the Token Bridge default.
    #[serde(default = "crate::protocols::default_program", skip_serializing)]
    pub program: String,
    pub source_chain: ChainId,
    /// `None` for an inbound redeem until the origin chain's tx is learned from enrichment.
    pub source_tx_hash: Option<String>,
    pub source_wallet: Option<String>,
    pub source_explorer_url: Option<String>,
    pub destination_chain: ChainId,
    pub destination_wallet: Option<String>,
    pub destination_tx_hash: Option<String>,
    pub destination_explorer_url: Option<String>,
    pub token: Option<String>,
    pub token_symbol: Option<String>,
    pub amount: Option<String>, // raw, Wormhole-normalized (unchanged behavior)
    pub amount_formatted: Option<String>, // new — human-readable, decimals-aware
    pub message_id: BridgeMessageId,
    pub status: BridgeStatus,
}

impl BridgeTransfer {
    /// Which way the transfer moves relative to Solana. The Solana tx the pipeline observes
    /// is the send for an outbound transfer and the redeem for an inbound one, so a transfer
    /// that lands on Solana from another chain is inbound. Derived, so the stored analyses
    /// and the frozen `/api/v1` shape do not change.
    pub fn direction(&self) -> Direction {
        if self.destination_chain == ChainId::Solana && self.source_chain != ChainId::Solana {
            Direction::Inbound
        } else {
            Direction::Outbound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer(source: ChainId, destination: ChainId) -> BridgeTransfer {
        BridgeTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain: source,
            source_tx_hash: None,
            source_wallet: None,
            source_explorer_url: None,
            destination_chain: destination,
            destination_wallet: None,
            destination_tx_hash: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount: None,
            amount_formatted: None,
            message_id: BridgeMessageId {
                emitter_chain: 2,
                emitter_address: "aa".into(),
                sequence: 1,
            },
            status: BridgeStatus::Detected,
        }
    }

    #[test]
    fn a_send_out_of_solana_is_outbound() {
        assert_eq!(
            transfer(ChainId::Solana, ChainId::Ethereum).direction(),
            Direction::Outbound
        );
    }

    #[test]
    fn a_redeem_into_solana_is_inbound() {
        assert_eq!(
            transfer(ChainId::Ethereum, ChainId::Solana).direction(),
            Direction::Inbound
        );
        assert_eq!(
            transfer(ChainId::Unknown(9999), ChainId::Solana).direction(),
            Direction::Inbound
        );
    }

    #[test]
    fn a_transfer_that_does_not_touch_solana_is_outbound() {
        assert_eq!(
            transfer(ChainId::Ethereum, ChainId::Bsc).direction(),
            Direction::Outbound
        );
    }
}
