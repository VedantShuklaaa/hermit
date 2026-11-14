//! Mayan Swift V2 persistence: a source `init_order` and its destination `fulfill`
//! carry the same order hash, so the existing unique `(protocol, message_key)` rule
//! gives them one row. The source leg is `Pending`, the fill is `Completed`, and the
//! upsert merges both tx hashes without ever lowering the status.

mod common;

use bridge::chains::{RawChainRef, Snapshot};
use bridge::db::repository::persist_analysis;
use bridge::domain::analysis::DecodedTransfer;
use bridge::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use bridge::protocols::amount::RawAmount;
use bridge::protocols::{DetectedTransfer, Direction, MessageId, TokenRef};
use bridge::services::detect;
use sqlx::PgPool;

const SOURCE_HASH: &str = "mayan-source-order-test";
const FILL_HASH: &str = "mayan-fulfill-order-test";

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

fn detected(order_hash: &str, direction: Direction, observed_tx: &str) -> DetectedTransfer {
    let (source, destination) = if direction == Direction::Outbound {
        (RawChainRef::wormhole(1), RawChainRef::wormhole(4))
    } else {
        (RawChainRef::wormhole(4), RawChainRef::wormhole(1))
    };
    DetectedTransfer {
        program: "swift_v2".to_string(),
        message: MessageId::Mayan {
            order_hash: order_hash.to_string(),
        },
        direction,
        delivered: direction == Direction::Inbound,
        observed_tx: observed_tx.to_string(),
        event_index: 0,
        source,
        destination,
        source_wallet: None,
        destination_wallet: None,
        token: Some(TokenRef {
            chain: RawChainRef::wormhole(4),
            address: "0x55d398326f99059ff775485246999027b3197955".to_string(),
        }),
        token_symbol: None,
        amount_raw: RawAmount::parse("95749899"),
        via: None,
    }
}

fn source(order_hash: &str) -> DecodedTransfer {
    let detected = detected(order_hash, Direction::Outbound, SOURCE_HASH);
    let row = detect::mayan_row_values(&detected, &Snapshot::default());
    DecodedTransfer {
        detected,
        bridge: None,
        row,
    }
}

fn fill(order_hash: &str) -> DecodedTransfer {
    let detected = detected(order_hash, Direction::Inbound, FILL_HASH);
    let row = detect::mayan_destination_row_values(&detected, &Snapshot::default());
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

async fn one_row(pool: &PgPool, order_hash: &str) -> Row {
    sqlx::query_as(
        "SELECT COUNT(*) OVER (), status, source_tx_hash, destination_tx_hash, direction,
                source_chain_id, destination_chain_id
         FROM bridge_transfers WHERE protocol = 'mayan' AND message_key = $1",
    )
    .bind(order_hash)
    .fetch_one(pool)
    .await
    .expect("the transfer row")
}

#[tokio::test]
async fn a_source_order_and_its_fill_converge_on_one_completed_row() {
    let db = common::test_db().await;
    let order_hash = "ab".repeat(32);

    persist_analysis(
        &db.pool,
        &transaction(SOURCE_HASH, 1),
        &[source(&order_hash)],
    )
    .await
    .expect("source persist");
    persist_analysis(&db.pool, &transaction(FILL_HASH, 2), &[fill(&order_hash)])
        .await
        .expect("fill persist");

    let (count, status, source_tx, destination_tx, direction, source_chain, destination_chain) =
        one_row(&db.pool, &order_hash).await;
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
        "both Wormhole chain IDs are seeded and resolve"
    );
}

#[tokio::test]
async fn a_fill_seen_before_its_source_stays_completed() {
    let db = common::test_db().await;
    let order_hash = "cd".repeat(32);

    persist_analysis(&db.pool, &transaction(FILL_HASH, 1), &[fill(&order_hash)])
        .await
        .expect("fill persist");
    persist_analysis(
        &db.pool,
        &transaction(SOURCE_HASH, 2),
        &[source(&order_hash)],
    )
    .await
    .expect("source persist");

    let (count, status, source_tx, destination_tx, direction, _, _) =
        one_row(&db.pool, &order_hash).await;
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
