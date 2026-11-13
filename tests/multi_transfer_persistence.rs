//! A10: one Solana transaction can hold several bridge transfers. Each decoded transfer
//! is stored as its own `bridge_transfers` row, told apart by `event_index`, with its own
//! message identity, raw chain references, router and token.

mod common;

use bridge::chains::RawChainRef;
use bridge::db::repository::{get_transaction_with_transfers, persist_analysis};
use bridge::domain::analysis::DecodedTransfer;
use bridge::domain::bridge_transfer::ChainId;
use bridge::protocols::amount::RawAmount;
use bridge::protocols::{DetectedTransfer, Direction, MessageId, TokenRef, Via};
use sqlx::Row;

const HASH: &str = "multi-transfer-signature-123";

fn transfer(sequence: u64, event_index: u16, destination: ChainId) -> DecodedTransfer {
    let destination_id = destination.wormhole_id();
    let mut tx = common::sample_tx(HASH, sequence, destination, Some("1000000"));
    let mut bridge = tx.bridge_transfer.take().expect("sample transfer");
    bridge.token = Some("token-mint-address".to_string());

    let row = bridge::services::detect::row_values(&bridge);

    DecodedTransfer {
        detected: DetectedTransfer {
            program: bridge::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "emitter-addr".to_string(),
                sequence,
            },
            direction: Direction::Outbound,
            observed_tx: HASH.to_string(),
            event_index,
            source: RawChainRef::wormhole(1),
            destination: RawChainRef::wormhole(destination_id),
            source_wallet: Some("SomeSignerPubkey".to_string()),
            destination_wallet: None,
            token: Some(TokenRef {
                chain: RawChainRef::wormhole(1),
                address: "token-mint-address".to_string(),
            }),
            token_symbol: None,
            amount_raw: RawAmount::parse("1000000"),
            via: Some(Via {
                name: "cctp".to_string(),
                reference: Some("order-1".to_string()),
            }),
        },
        bridge: Some(bridge),
        row,
    }
}

fn analysis() -> (
    bridge::domain::transaction::NormalisedTransaction,
    Vec<DecodedTransfer>,
) {
    let mut tx = common::sample_tx(HASH, 700, ChainId::Ethereum, Some("1000000"));
    let transfers = vec![
        transfer(700, 0, ChainId::Ethereum),
        transfer(701, 1, ChainId::Polygon),
    ];
    // `/api/v1` keeps reading the first transfer, as before A10.
    tx.bridge_transfer = transfers[0].bridge.clone();
    (tx, transfers)
}

#[tokio::test]
async fn every_decoded_transfer_is_persisted_with_its_event_index() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let (tx, transfers) = analysis();

    persist_analysis(pool, &tx, &transfers)
        .await
        .expect("persist analysis");

    let rows = sqlx::query(
        r#"
        SELECT b.event_index::int AS event_index, b.message_key,
               b.source_ref_protocol, b.source_ref_id,
               b.destination_ref_protocol, b.destination_ref_id,
               b.destination_chain_id, b.via, b.via_ref,
               b.token_ref_protocol, b.token_ref_id, b.token_chain_id,
               b.amount::text AS amount, w.emitter_chain, w.sequence::text AS sequence
        FROM bridge_transfers b
        JOIN wormhole_messages w ON w.transfer_id = b.id
        WHERE b.observed_tx_hash = $1
        ORDER BY b.event_index
        "#,
    )
    .bind(HASH)
    .fetch_all(pool)
    .await
    .expect("read transfers");

    assert_eq!(rows.len(), 2, "both transfers must be stored");

    let polygon_id: i16 = sqlx::query_scalar("SELECT id FROM chains WHERE slug = 'polygon'")
        .fetch_one(pool)
        .await
        .expect("polygon chain row");
    let ethereum_id: i16 = sqlx::query_scalar("SELECT id FROM chains WHERE slug = 'ethereum'")
        .fetch_one(pool)
        .await
        .expect("ethereum chain row");

    for (position, row) in rows.iter().enumerate() {
        let index: i32 = row.get("event_index");
        assert_eq!(index, position as i32, "event_index keeps adapter order");

        let sequence: String = row.get("sequence");
        assert_eq!(
            row.get::<String, _>("message_key"),
            format!("1/emitter-addr/{sequence}"),
            "each transfer has its own message identity"
        );

        assert_eq!(row.get::<String, _>("source_ref_protocol"), "wormhole");
        assert_eq!(row.get::<String, _>("source_ref_id"), "1");
        assert_eq!(row.get::<String, _>("destination_ref_protocol"), "wormhole");
        assert_eq!(row.get::<String, _>("via"), "cctp");
        assert_eq!(row.get::<String, _>("via_ref"), "order-1");
        assert_eq!(row.get::<String, _>("token_ref_protocol"), "wormhole");
        assert_eq!(row.get::<String, _>("token_ref_id"), "1");
        assert_eq!(row.get::<Option<i16>, _>("token_chain_id"), Some(1));
        assert_eq!(row.get::<String, _>("amount"), "1000000");
    }

    assert_eq!(rows[0].get::<String, _>("destination_ref_id"), "2");
    assert_eq!(
        rows[0].get::<Option<i16>, _>("destination_chain_id"),
        Some(ethereum_id),
        "raw destination reference resolves through the registry"
    );
    assert_eq!(rows[1].get::<String, _>("destination_ref_id"), "5");
    assert_eq!(
        rows[1].get::<Option<i16>, _>("destination_chain_id"),
        Some(polygon_id),
    );

    let read_back = get_transaction_with_transfers(pool, HASH)
        .await
        .expect("read back")
        .expect("transaction row");
    assert_eq!(
        read_back.transfers.len(),
        2,
        "the v2 read path sees every transfer"
    );
    assert_eq!(
        read_back
            .transfers
            .iter()
            .map(|t| t.event_index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[tokio::test]
async fn redelivering_a_multi_transfer_transaction_keeps_one_row_per_transfer() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let (tx, transfers) = analysis();

    persist_analysis(pool, &tx, &transfers)
        .await
        .expect("first persist");
    // A crash after the commit makes XAUTOCLAIM deliver the same message again.
    persist_analysis(pool, &tx, &transfers)
        .await
        .expect("redelivery");

    let transfer_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM bridge_transfers WHERE observed_tx_hash = $1")
            .bind(HASH)
            .fetch_one(pool)
            .await
            .expect("count transfers");
    let message_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM wormhole_messages w
         JOIN bridge_transfers b ON b.id = w.transfer_id
         WHERE b.observed_tx_hash = $1",
    )
    .bind(HASH)
    .fetch_one(pool)
    .await
    .expect("count messages");

    assert_eq!(transfer_rows, 2, "redelivery duplicated a transfer row");
    assert_eq!(message_rows, 2, "redelivery duplicated a message row");
}
