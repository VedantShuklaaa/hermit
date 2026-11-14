//! CCTP persistence: the message identity is `(source_domain, nonce)`, so the existing
//! unique `(protocol, message_key)` rule gives one row per message, an unseeded origin
//! domain is stored unresolved rather than rejected, and a redelivery never lowers the
//! status. The `/api/v2` shape is checked from stored columns.

mod common;

use bridge::chains::{RawChainRef, Snapshot};
use bridge::db::repository::persist_analysis;
use bridge::db::repository::{StoredTransaction, StoredTransactionWithTransfers, StoredTransfer};
use bridge::domain::analysis::DecodedTransfer;
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use bridge::protocols::amount::RawAmount;
use bridge::protocols::{DetectedTransfer, Direction, MessageId, TokenRef};
use bridge::services::{api_v2, detect};
use serde_json::json;

const HASH: &str = "cctp-persistence-test";

fn decoded(source_domain: u32, nonce: &str, status: BridgeStatus) -> DecodedTransfer {
    let detected = DetectedTransfer {
        program: "cctp".to_string(),
        message: MessageId::Cctp {
            source_domain,
            nonce: nonce.to_string(),
        },
        direction: Direction::Inbound,
        delivered: true,
        observed_tx: HASH.to_string(),
        event_index: 0,
        source: RawChainRef::cctp(source_domain),
        destination: RawChainRef::cctp(5),
        source_wallet: Some("0xsource".to_string()),
        destination_wallet: Some("SolanaRecipient".to_string()),
        token: Some(TokenRef {
            chain: RawChainRef::cctp(source_domain),
            address: "0xtoken".to_string(),
        }),
        token_symbol: None,
        amount_raw: RawAmount::parse("1000000"),
        via: None,
    };
    let mut row = detect::cctp_row_values(&detected, &Snapshot::default());
    row.status = status;
    DecodedTransfer {
        detected,
        bridge: None,
        row,
    }
}

fn transaction() -> NormalisedTransaction {
    NormalisedTransaction {
        hash: HASH.to_string(),
        chain: Chain::Solana,
        status: TxStatus::Success,
        slot: 123,
        timestamp: Some(1_700_000_000),
        fee_lamports: 5000,
        signer: Some("signer".to_string()),
        bridge_event: None,
        bridge_transfer: None,
    }
}

#[tokio::test]
async fn one_cctp_message_is_one_row_and_its_status_never_moves_backwards() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let nonce = "ab".repeat(32);

    // Domain 9999 is not seeded: the chain reference stays raw, never an error.
    let first = decoded(9999, &nonce, BridgeStatus::Completed);
    persist_analysis(pool, &transaction(), std::slice::from_ref(&first))
        .await
        .expect("first persist");

    // A later pass that knows less must not duplicate the row or lower the status.
    let mut later = decoded(9999, &nonce, BridgeStatus::Detected);
    later.row.destination_wallet = None;
    persist_analysis(pool, &transaction(), std::slice::from_ref(&later))
        .await
        .expect("redelivery");

    let (count, program, status, source_chain_id, source_ref_id): (
        i64,
        String,
        String,
        Option<i16>,
        String,
    ) = sqlx::query_as(
        "SELECT COUNT(*) OVER (), program, status, source_chain_id, source_ref_id
         FROM bridge_transfers WHERE protocol = 'cctp' AND message_key = $1",
    )
    .bind(format!("9999/{nonce}"))
    .fetch_one(pool)
    .await
    .expect("the transfer row");

    assert_eq!(count, 1, "one row per (protocol, message_key)");
    assert_eq!(program, "cctp");
    assert_eq!(
        status, "Completed",
        "a redelivery must not move it backwards"
    );
    assert_eq!(
        source_chain_id, None,
        "domain 9999 is unresolved, not an error"
    );
    assert_eq!(source_ref_id, "9999");

    let (domain, stored_nonce, version): (i32, String, i32) = sqlx::query_as(
        "SELECT source_domain, nonce, version FROM cctp_messages
         WHERE transfer_id = (
             SELECT id FROM bridge_transfers WHERE protocol = 'cctp' AND message_key = $1
         )",
    )
    .bind(format!("9999/{nonce}"))
    .fetch_one(pool)
    .await
    .expect("the cctp detail row");
    assert_eq!(
        (domain, stored_nonce.as_str(), version),
        (9999, nonce.as_str(), 1)
    );
}

#[test]
fn the_v2_message_id_is_the_origin_domain_and_the_32_byte_nonce() {
    let nonce = "cd".repeat(32);
    let stored = StoredTransactionWithTransfers {
        transaction: StoredTransaction {
            hash: HASH.to_string(),
            chain: Chain::Solana.wormhole_id() as i16,
            status: "Success".to_string(),
            slot: 123,
            timestamp: None,
            fee_lamports: 5000,
            signer: None,
        },
        transfers: vec![StoredTransfer {
            program: "cctp".to_string(),
            protocol: "cctp".to_string(),
            message_key: format!("0/{nonce}"),
            direction: "Inbound".to_string(),
            observed_tx_hash: HASH.to_string(),
            event_index: 0,
            via: None,
            via_ref: None,
            source_ref_protocol: "cctp".to_string(),
            source_ref_id: "0".to_string(),
            source_tx_hash: None,
            source_wallet: Some("0xsource".to_string()),
            source_explorer_url: None,
            destination_ref_protocol: "cctp".to_string(),
            destination_ref_id: "5".to_string(),
            destination_wallet: Some("SolanaRecipient".to_string()),
            destination_tx_hash: Some(HASH.to_string()),
            destination_explorer_url: None,
            token_ref_protocol: Some("cctp".to_string()),
            token_ref_id: Some("0".to_string()),
            token: Some("0xtoken".to_string()),
            token_symbol: None,
            amount: Some("1000000".to_string()),
            amount_formatted: None,
            status: "Completed".to_string(),
            emitter_chain: None,
            emitter_address: None,
            sequence: None,
            cctp_source_domain: Some(0),
            cctp_nonce: Some(nonce.clone()),
        }],
    };

    let response = api_v2::assemble(stored, &Snapshot::default()).expect("v2 assemble");
    assert_eq!(response.transfers.len(), 1);
    assert_eq!(response.transfers[0].protocol, "cctp");
    assert_eq!(
        serde_json::to_value(&response.transfers[0].message_id).expect("message id json"),
        json!({ "protocol": "cctp", "source_domain": 0, "nonce": nonce })
    );
}
