//! The `/api/v1` compatibility mapper. `/api/v1` keeps its frozen Wormhole-shaped body
//! until it is retired; this maps the generalised `/api/v2` response back to it.

use crate::chains::NS_WORMHOLE;
use crate::domain::api::{ChainView, TransactionResponse, TransferStatus, TransferView};
use crate::domain::bridge::BridgeEvent;
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use crate::domain::transaction::{Chain, NormalisedTransaction};
use crate::error::AppError;
use crate::protocols::{MessageId, TOKEN_BRIDGE_PROGRAM};

/// `/api/v2` -> the frozen `/api/v1` body. The first transfer (lowest `event_index`) is
/// the one v1 can represent.
///
/// `bridge_event` is not part of the generalised model, so the caller passes the stored
/// v1 event through unchanged.
pub fn to_v1(
    response: &TransactionResponse,
    bridge_event: Option<BridgeEvent>,
) -> Result<NormalisedTransaction, AppError> {
    // v1 can only represent the Token Bridge. A transaction with rows from another
    // program is rejected explicitly rather than mapped as a Token Bridge transfer.
    let bridge_transfer = match response
        .transfers
        .iter()
        .find(|transfer| transfer.program == TOKEN_BRIDGE_PROGRAM)
    {
        Some(transfer) => Some(transfer_to_v1(transfer)?),
        None => match response.transfers.first() {
            Some(other) => {
                return Err(AppError::Normalisation(format!(
                    "program {} has no /api/v1 representation",
                    other.program
                )));
            }
            None => None,
        },
    };
    let bridge_event = bridge_transfer.as_ref().and(bridge_event);

    Ok(NormalisedTransaction {
        hash: response.hash.clone(),
        chain: observed_chain(&response.chain)?,
        status: response.status.clone(),
        slot: response.slot,
        timestamp: response.timestamp,
        fee_lamports: response.fee_lamports,
        signer: response.signer.clone(),
        bridge_event,
        bridge_transfer,
    })
}

fn transfer_to_v1(transfer: &TransferView) -> Result<BridgeTransfer, AppError> {
    if transfer.protocol != "wormhole" {
        return Err(AppError::Normalisation(format!(
            "protocol {} has no /api/v1 representation",
            transfer.protocol
        )));
    }
    if transfer.program != TOKEN_BRIDGE_PROGRAM {
        return Err(AppError::Normalisation(format!(
            "program {} has no /api/v1 representation",
            transfer.program
        )));
    }

    let MessageId::Wormhole {
        emitter_chain,
        emitter_address,
        sequence,
    } = &transfer.message_id;

    let status = match transfer.status {
        TransferStatus::Detected => BridgeStatus::Detected,
        TransferStatus::Pending => BridgeStatus::Pending,
        TransferStatus::Completed => BridgeStatus::Completed,
        TransferStatus::Failed | TransferStatus::Refunded => {
            return Err(AppError::Normalisation(format!(
                "status {:?} has no /api/v1 representation",
                transfer.status
            )));
        }
    };

    Ok(BridgeTransfer {
        program: transfer.program.clone(),
        source_chain: chain_id(&transfer.source)?,
        source_tx_hash: transfer.source_tx_hash.clone(),
        source_wallet: transfer.source_wallet.clone(),
        source_explorer_url: transfer.source_explorer_url.clone(),
        destination_chain: chain_id(&transfer.destination)?,
        destination_wallet: transfer.destination_wallet.clone(),
        destination_tx_hash: transfer.destination_tx_hash.clone(),
        destination_explorer_url: transfer.destination_explorer_url.clone(),
        token: transfer.token.as_ref().map(|token| token.address.clone()),
        token_symbol: transfer.token_symbol.clone(),
        amount: transfer.amount_raw.clone(),
        amount_formatted: transfer.amount_formatted.clone(),
        message_id: BridgeMessageId {
            emitter_chain: *emitter_chain,
            emitter_address: emitter_address.clone(),
            sequence: *sequence,
        },
        status,
    })
}

/// The observed chain is Solana in this service. A resolved registry row is preferred;
/// the raw id keeps v1 working when the registry has not loaded.
fn observed_chain(view: &ChainView) -> Result<Chain, AppError> {
    view.chain
        .as_ref()
        .and_then(|chain| Chain::from_label(&chain.slug))
        .or_else(|| (view.protocol == NS_WORMHOLE && view.id == "1").then_some(Chain::Solana))
        .ok_or_else(|| {
            AppError::Normalisation(format!(
                "unsupported observed chain: {}:{}",
                view.protocol, view.id
            ))
        })
}

/// Mirrors the v1 rule in `detect::wormhole_id`: another namespace or a non-numeric id
/// is a normalisation error (422); a numeric id the registry does not know is
/// `ChainId::Unknown(id)`, never a failure.
fn chain_id(view: &ChainView) -> Result<ChainId, AppError> {
    if view.protocol != NS_WORMHOLE {
        return Err(AppError::Normalisation(format!(
            "chain reference from {}, expected {NS_WORMHOLE}",
            view.protocol
        )));
    }
    let id: u16 = view
        .id
        .parse()
        .map_err(|_| AppError::Normalisation(format!("bad wormhole chain id {:?}", view.id)))?;
    Ok(ChainId::from_wormhole_id(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::api::ChainDetails;

    fn chain(protocol: &str, id: &str) -> ChainView {
        ChainView {
            protocol: protocol.to_string(),
            id: id.to_string(),
            chain: None,
        }
    }

    fn transfer(sequence: u64, source: ChainView) -> TransferView {
        TransferView {
            protocol: "wormhole".to_string(),
            program: TOKEN_BRIDGE_PROGRAM.to_string(),
            direction: crate::protocols::Direction::Outbound,
            via: None,
            observed_tx_hash: "sig".to_string(),
            event_index: 0,
            message_id: MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence,
            },
            destination: chain(NS_WORMHOLE, "2"),
            source,
            source_wallet: None,
            destination_wallet: None,
            source_tx_hash: Some("sig".to_string()),
            destination_tx_hash: None,
            source_explorer_url: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount_raw: None,
            amount_formatted: None,
            status: TransferStatus::Completed,
        }
    }

    fn response(transfers: Vec<TransferView>) -> TransactionResponse {
        TransactionResponse {
            hash: "sig".to_string(),
            chain: ChainView {
                protocol: NS_WORMHOLE.to_string(),
                id: "1".to_string(),
                chain: Some(ChainDetails {
                    id: 1,
                    slug: "solana".to_string(),
                    name: "Solana".to_string(),
                }),
            },
            status: crate::domain::transaction::TxStatus::Success,
            slot: 1,
            timestamp: None,
            fee_lamports: 0,
            signer: None,
            transfers,
        }
    }

    #[test]
    fn a_non_numeric_wormhole_id_is_a_normalisation_error() {
        // v1's `detect::wormhole_id` rejects the same input with the same error.
        let response = response(vec![transfer(1, chain(NS_WORMHOLE, "not-a-number"))]);
        assert!(matches!(
            to_v1(&response, None),
            Err(AppError::Normalisation(_))
        ));
    }

    #[test]
    fn another_namespace_is_a_normalisation_error() {
        let response = response(vec![transfer(1, chain("cctp", "1"))]);
        assert!(matches!(
            to_v1(&response, None),
            Err(AppError::Normalisation(_))
        ));
    }

    #[test]
    fn an_unknown_numeric_id_is_unknown_not_an_error() {
        let response = response(vec![transfer(1, chain(NS_WORMHOLE, "9999"))]);
        let v1 = to_v1(&response, None).expect("numeric unknown is representable");
        assert_eq!(
            v1.bridge_transfer.unwrap().source_chain,
            ChainId::Unknown(9999)
        );
    }

    #[test]
    fn the_first_transfer_is_the_one_v1_represents() {
        let mut second = transfer(2, chain(NS_WORMHOLE, "2"));
        second.event_index = 1;
        let response = response(vec![transfer(1, chain(NS_WORMHOLE, "1")), second]);
        let v1 = to_v1(&response, None).expect("frozen shape");
        assert_eq!(v1.bridge_transfer.unwrap().message_id.sequence, 1);
    }

    #[test]
    fn a_transaction_without_transfers_still_maps() {
        let v1 = to_v1(&response(vec![]), None).expect("frozen shape");
        assert!(v1.bridge_transfer.is_none());
        assert!(v1.bridge_event.is_none());
    }
}
