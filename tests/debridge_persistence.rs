//! deBridge DLN persistence: a source order and its destination fill carry the same
//! order id, so the existing unique `(protocol, message_key)` rule gives them one row.
//! The source leg is `Pending`, the fill is `Completed`, and the upsert merges both tx
//! hashes without ever lowering the status.

mod common;

use bridge::chains::{RawChainRef, Snapshot};
use bridge::db::repository::persist_analysis;
use bridge::domain::analysis::DecodedTransfer;
use bridge::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use bridge::protocols::amount::RawAmount;
use bridge::protocols::{DetectedTransfer, Direction, MessageId, TokenRef};
use bridge::services::detect;
use sqlx::PgPool;

const SOURCE_HASH: &str = "debridge-source-order-test";
const FILL_HASH: &str = "debridge-fulfill-order-test";

fn transaction(hash: &str, slot: u64) -> NormalisedTransaction {
    NormalisedTransaction {
        hash: hash.to_string(),
        chain: Chain::Solana,
        status: TxStatus::Success,
        slot,
        timestamp: Some(1_700_000_000),
        fee_lamports: 5000,
        signer: Some("signer".to_string()),
        bridge_event: None,
        bridge_transfer: None,
    }
}

fn detected(order_id: &str, direction: Direction, observed_tx: &str) -> DetectedTransfer {
    DetectedTransfer {
        program: "debridge".to_string(),
        message: MessageId::Debridge {
            order_id: order_id.to_string(),
        },
        direction,
        delivered: direction == Direction::Inbound,
        observed_tx: observed_tx.to_string(),
        event_index: 0,
        source: RawChainRef::debridge("8453"),
        destination: RawChainRef::debridge("7565164"),
        source_wallet: Some("0x2672113e354667adce50c38296ee7dfd65ac6666".to_string()),
        destination_wallet: Some("5FV8hNqbBpMwsUXhazL2Givohy92MUTkv6MvcPrmGQ2A".to_string()),
        token: Some(TokenRef {
            chain: RawChainRef::debridge("8453"),
            address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string(),
        }),
        token_symbol: None,
        amount_raw: RawAmount::parse("438490169"),
        via: None,
    }
}

fn source(order_id: &str) -> DecodedTransfer {
    let detected = detected(order_id, Direction::Outbound, SOURCE_HASH);
    let row = detect::debridge_row_values(&detected, &Snapshot::default());
    DecodedTransfer {
        detected,
        bridge: None,
        row,
    }
}

fn fill(order_id: &str) -> DecodedTransfer {
    let detected = detected(order_id, Direction::Inbound, FILL_HASH);
    let row = detect::debridge_destination_row_values(&detected, &Snapshot::default());
    DecodedTransfer {
        detected,
        bridge: None,
        row,
    }
}

type Row = (
    i64,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<i16>,
    Option<i16>,
);

async fn one_row(pool: &PgPool, order_id: &str) -> Row {
    sqlx::query_as(
        "SELECT COUNT(*) OVER (), status, source_tx_hash, destination_tx_hash, direction,
                source_chain_id, destination_chain_id
         FROM bridge_transfers WHERE protocol = 'debridge' AND message_key = $1",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("the transfer row")
}

#[tokio::test]
async fn a_source_order_and_its_fill_converge_on_one_completed_row() {
    let db = common::test_db().await;
    let order_id = "ab".repeat(32);

    persist_analysis(&db.pool, &transaction(SOURCE_HASH, 1), &[source(&order_id)])
        .await
        .expect("source persist");
    persist_analysis(&db.pool, &transaction(FILL_HASH, 2), &[fill(&order_id)])
        .await
        .expect("fill persist");

    let (count, status, source_tx, destination_tx, direction, source_chain, destination_chain) =
        one_row(&db.pool, &order_id).await;
    assert_eq!(count, 1, "one row per (protocol, message_key)");
    assert_eq!(status, "Completed", "the fill completes the source row");
    assert_eq!(source_tx.as_deref(), Some(SOURCE_HASH));
    assert_eq!(destination_tx.as_deref(), Some(FILL_HASH));
    assert_eq!(
        direction, "Outbound",
        "the first observation set the direction"
    );
    assert_eq!(
        (source_chain.is_some(), destination_chain.is_some()),
        (true, true),
        "both DLN chain IDs are seeded and resolve"
    );
}

#[tokio::test]
async fn a_fill_seen_before_its_source_stays_completed() {
    let db = common::test_db().await;
    let order_id = "cd".repeat(32);

    persist_analysis(&db.pool, &transaction(FILL_HASH, 1), &[fill(&order_id)])
        .await
        .expect("fill persist");
    persist_analysis(&db.pool, &transaction(SOURCE_HASH, 2), &[source(&order_id)])
        .await
        .expect("source persist");

    let (count, status, source_tx, destination_tx, direction, _, _) =
        one_row(&db.pool, &order_id).await;
    assert_eq!(count, 1, "one row per (protocol, message_key)");
    assert_eq!(
        status, "Completed",
        "a later source observation must not move the status back to Pending"
    );
    assert_eq!(source_tx.as_deref(), Some(SOURCE_HASH));
    assert_eq!(destination_tx.as_deref(), Some(FILL_HASH));
    assert_eq!(
        direction, "Inbound",
        "the first observation set the direction"
    );
}
