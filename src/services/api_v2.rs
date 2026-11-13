//! Builds the generalised `/api/v2` response from stored rows, or from a v1 analysis
//! when the transaction has no stored transfer rows yet.

use crate::chains::{ChainRef, RawChainRef, Snapshot};
use crate::db::repository::{StoredTransactionWithTransfers, StoredTransfer};
use crate::domain::api::{
    ChainDetails, ChainView, TokenView, TransactionResponse, TransferStatus, TransferView,
};
use crate::domain::transaction::{NormalisedTransaction, TxStatus};
use crate::error::AppError;
use crate::protocols::{Direction, MessageId, Via};

/// Stored transaction + rows -> `/api/v2` response, ordered by `event_index`.
pub fn assemble(
    stored: StoredTransactionWithTransfers,
    chains: &Snapshot,
) -> Result<TransactionResponse, AppError> {
    let tx = stored.transaction;
    let observed =
        RawChainRef::wormhole(u16::try_from(tx.chain).map_err(|_| {
            AppError::Normalisation(format!("bad observed chain id: {}", tx.chain))
        })?);

    let transfers = stored
        .transfers
        .iter()
        .map(|row| transfer_view(row, chains))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TransactionResponse {
        hash: tx.hash,
        chain: chain_view(&observed, chains),
        status: tx_status(&tx.status)?,
        slot: u64::try_from(tx.slot)
            .map_err(|_| AppError::Normalisation(format!("bad slot: {}", tx.slot)))?,
        timestamp: tx.timestamp.map(|t| t.timestamp()),
        fee_lamports: u64::try_from(tx.fee_lamports)
            .map_err(|_| AppError::Normalisation(format!("bad fee: {}", tx.fee_lamports)))?,
        signer: tx.signer,
        transfers,
    })
}

/// A transaction that has no stored transfer rows (or has not been persisted yet).
/// The v1 analysis is the only source, so it carries one transfer at most.
pub fn from_v1(tx: &NormalisedTransaction, chains: &Snapshot) -> TransactionResponse {
    let observed = RawChainRef::wormhole(tx.chain.wormhole_id());
    let transfers = tx
        .bridge_transfer
        .iter()
        .map(|t| TransferView {
            protocol: "wormhole".to_string(),
            program: t.program.clone(),
            direction: t.direction(),
            via: None,
            observed_tx_hash: tx.hash.clone(),
            event_index: 0,
            message_id: MessageId::Wormhole {
                emitter_chain: t.message_id.emitter_chain,
                emitter_address: t.message_id.emitter_address.clone(),
                sequence: t.message_id.sequence,
            },
            source: chain_view(&RawChainRef::wormhole(t.source_chain.wormhole_id()), chains),
            destination: chain_view(
                &RawChainRef::wormhole(t.destination_chain.wormhole_id()),
                chains,
            ),
            source_wallet: t.source_wallet.clone(),
            destination_wallet: t.destination_wallet.clone(),
            source_tx_hash: t.source_tx_hash.clone(),
            destination_tx_hash: t.destination_tx_hash.clone(),
            source_explorer_url: t.source_explorer_url.clone(),
            destination_explorer_url: t.destination_explorer_url.clone(),
            token: t.token.as_ref().map(|address| TokenView {
                chain: None,
                address: address.clone(),
            }),
            token_symbol: t.token_symbol.clone(),
            amount_raw: t.amount.clone(),
            amount_formatted: t.amount_formatted.clone(),
            status: transfer_status(t.status.clone()),
        })
        .collect();

    TransactionResponse {
        hash: tx.hash.clone(),
        chain: chain_view(&observed, chains),
        status: tx.status.clone(),
        slot: tx.slot,
        timestamp: tx.timestamp,
        fee_lamports: tx.fee_lamports,
        signer: tx.signer.clone(),
        transfers,
    }
}

/// One stored row as the v2 transfer model. Shared by the analyse response and the
/// `/api/v2/transfers` list so the two cannot drift.
pub(crate) fn transfer_view(
    row: &StoredTransfer,
    chains: &Snapshot,
) -> Result<TransferView, AppError> {
    let direction = match row.direction.as_str() {
        "Outbound" => Direction::Outbound,
        "Inbound" => Direction::Inbound,
        other => return Err(AppError::Normalisation(format!("bad direction: {other}"))),
    };
    let status = TransferStatus::parse(&row.status)
        .ok_or_else(|| AppError::Normalisation(format!("bad status: {}", row.status)))?;
    let message_id = message_id(row)?;

    let source = RawChainRef::new(row.source_ref_protocol.clone(), row.source_ref_id.clone());
    let destination = RawChainRef::new(
        row.destination_ref_protocol.clone(),
        row.destination_ref_id.clone(),
    );
    let via = row.via.as_ref().map(|name| Via {
        name: name.clone(),
        reference: row.via_ref.clone(),
    });
    let token = match (&row.token, &row.token_ref_protocol, &row.token_ref_id) {
        (Some(address), Some(protocol), Some(id)) => Some(TokenView {
            chain: Some(chain_view(
                &RawChainRef::new(protocol.clone(), id.clone()),
                chains,
            )),
            address: address.clone(),
        }),
        (Some(address), _, _) => Some(TokenView {
            chain: None,
            address: address.clone(),
        }),
        _ => None,
    };

    Ok(TransferView {
        protocol: row.protocol.clone(),
        program: row.program.clone(),
        direction,
        via,
        observed_tx_hash: row.observed_tx_hash.clone(),
        event_index: u16::try_from(row.event_index).map_err(|_| {
            AppError::Normalisation(format!("bad event index: {}", row.event_index))
        })?,
        message_id,
        source: chain_view(&source, chains),
        destination: chain_view(&destination, chains),
        source_wallet: row.source_wallet.clone(),
        destination_wallet: row.destination_wallet.clone(),
        source_tx_hash: row.source_tx_hash.clone(),
        destination_tx_hash: row.destination_tx_hash.clone(),
        source_explorer_url: row.source_explorer_url.clone(),
        destination_explorer_url: row.destination_explorer_url.clone(),
        token,
        token_symbol: row.token_symbol.clone(),
        amount_raw: row.amount.clone(),
        amount_formatted: row.amount_formatted.clone(),
        status,
    })
}

fn message_id(row: &StoredTransfer) -> Result<MessageId, AppError> {
    match row.protocol.as_str() {
        "wormhole" => {
            let emitter_chain = row.emitter_chain.ok_or_else(|| missing("emitter_chain"))?;
            let emitter_chain = u16::try_from(emitter_chain).map_err(|_| {
                AppError::Normalisation(format!("bad emitter chain: {emitter_chain}"))
            })?;
            let emitter_address = row
                .emitter_address
                .clone()
                .ok_or_else(|| missing("emitter_address"))?;
            let sequence = row
                .sequence
                .as_deref()
                .ok_or_else(|| missing("sequence"))?
                .parse::<u64>()
                .map_err(|_| {
                    AppError::Normalisation(format!("bad sequence: {:?}", row.sequence))
                })?;
            Ok(MessageId::Wormhole {
                emitter_chain,
                emitter_address,
                sequence,
            })
        }
        "cctp" => {
            let source_domain = row
                .cctp_source_domain
                .ok_or_else(|| missing("source_domain"))?;
            let source_domain = u32::try_from(source_domain).map_err(|_| {
                AppError::Normalisation(format!("bad source domain: {source_domain}"))
            })?;
            let nonce = row.cctp_nonce.clone().ok_or_else(|| missing("nonce"))?;
            Ok(MessageId::Cctp {
                source_domain,
                nonce,
            })
        }
        "debridge" => {
            if row.message_key.is_empty() {
                return Err(missing("order_id"));
            }
            Ok(MessageId::Debridge {
                order_id: row.message_key.clone(),
            })
        }
        other => Err(AppError::Normalisation(format!(
            "unsupported protocol: {other}"
        ))),
    }
}

fn missing(field: &str) -> AppError {
    AppError::Normalisation(format!("missing {field}"))
}

fn chain_view(raw: &RawChainRef, chains: &Snapshot) -> ChainView {
    let resolved = match chains.resolve(raw) {
        ChainRef::Resolved(key) => chains.chain(key).map(|chain| ChainDetails {
            id: chain.id,
            slug: chain.slug.clone(),
            name: chain.name.clone(),
        }),
        ChainRef::Unresolved { .. } => None,
    };
    ChainView {
        protocol: raw.protocol.clone(),
        id: raw.id.clone(),
        chain: resolved,
    }
}

fn tx_status(value: &str) -> Result<TxStatus, AppError> {
    match value {
        "Success" => Ok(TxStatus::Success),
        "Failed" => Ok(TxStatus::Failed),
        other => Err(AppError::Normalisation(format!(
            "unknown transaction status: {other}"
        ))),
    }
}

fn transfer_status(value: crate::domain::bridge_transfer::BridgeStatus) -> TransferStatus {
    use crate::domain::bridge_transfer::BridgeStatus;
    match value {
        BridgeStatus::Detected => TransferStatus::Detected,
        BridgeStatus::Pending => TransferStatus::Pending,
        BridgeStatus::Completed => TransferStatus::Completed,
    }
}
