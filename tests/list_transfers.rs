//! `GET /api/v2/transfers` against a real Postgres 18 container: keyset paging over
//! duplicate `created_at` values, every filter, cursor/limit validation and the empty
//! page. Needs Docker (or `TEST_DATABASE_URL`).

mod common;

use bridge::chains::Snapshot;
use bridge::chains::db::load_snapshot;
use bridge::domain::api::TransferListResponse;
use bridge::error::AppError;
use bridge::protocols::MessageId;
use bridge::services::transfers::{Cursor, ListQuery, list_transfers};
use chrono::{DateTime, Utc};
use common::test_db;
use sqlx::PgPool;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + seconds, 0).expect("valid timestamp")
}

#[allow(clippy::too_many_arguments)]
async fn insert_row(
    pool: &PgPool,
    seq: i64,
    direction: &str,
    status: &str,
    source_chain: Option<i16>,
    source_ref: &str,
    destination_chain: Option<i16>,
    destination_ref: &str,
    token: Option<&str>,
    created_at: DateTime<Utc>,
) {
    let (transfer_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO transactions (hash, chain, status, slot, timestamp, fee_lamports, signer)
        VALUES ($1, 1, 'Success', 1, NULL, 5000, 'signer')
        RETURNING id
        "#,
    )
    .bind(format!("list-tx-{seq}"))
    .fetch_one(pool)
    .await
    .expect("insert transaction");

    let (transfer_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO bridge_transfers (
            transaction_id, source_tx_hash,
            source_ref_protocol, source_ref_id, source_chain_id,
            destination_ref_protocol, destination_ref_id, destination_chain_id,
            token, token_ref_protocol, token_ref_id, token_chain_id,
            protocol, message_key, direction, event_index, observed_tx_hash,
            status, amount, created_at, updated_at
        )
        VALUES (
            $1, $2, 'wormhole', $3, $4, 'wormhole', $5, $6,
            $7, 'wormhole', '1', 1,
            'wormhole', $8, $9, 0, $2, $10, 1000000, $11, $11
        )
        RETURNING id
        "#,
    )
    .bind(transfer_id)
    .bind(format!("list-tx-{seq}"))
    .bind(source_ref)
    .bind(source_chain)
    .bind(destination_ref)
    .bind(destination_chain)
    .bind(token)
    .bind(format!("1/emitter-{seq}/{seq}"))
    .bind(direction)
    .bind(status)
    .bind(created_at)
    .fetch_one(pool)
    .await
    .expect("insert transfer");

    sqlx::query(
        "INSERT INTO wormhole_messages (transfer_id, emitter_chain, emitter_address, sequence)
         VALUES ($1, 1, $2, $3)",
    )
    .bind(transfer_id)
    .bind(format!("emitter-{seq}"))
    .bind(seq)
    .execute(pool)
    .await
    .expect("insert wormhole message");
}

/// A CCTP row has its own message detail table instead of `wormhole_messages`.
async fn insert_cctp_row(pool: &PgPool, seq: i64, created_at: DateTime<Utc>) {
    let tx_hash = format!("list-cctp-tx-{seq}");
    let nonce = format!("{seq:064x}");
    let (transaction_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO transactions (hash, chain, status, slot, timestamp, fee_lamports, signer)
        VALUES ($1, 1, 'Success', 1, NULL, 5000, 'signer')
        RETURNING id
        "#,
    )
    .bind(&tx_hash)
    .fetch_one(pool)
    .await
    .expect("insert transaction");

    let (transfer_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO bridge_transfers (
            transaction_id, source_tx_hash,
            source_ref_protocol, source_ref_id,
            destination_ref_protocol, destination_ref_id,
            protocol, message_key, direction, event_index, observed_tx_hash,
            status, amount, created_at, updated_at
        )
        VALUES (
            $1, $2, 'cctp', '5', 'cctp', '0',
            'cctp', $3, 'Outbound', 0, $2, 'Completed', 1000000, $4, $4
        )
        RETURNING id
        "#,
    )
    .bind(transaction_id)
    .bind(&tx_hash)
    .bind(format!("5/{nonce}"))
    .bind(created_at)
    .fetch_one(pool)
    .await
    .expect("insert cctp transfer");

    sqlx::query(
        "INSERT INTO cctp_messages (transfer_id, source_domain, nonce, version)
         VALUES ($1, 5, $2, 1)",
    )
    .bind(transfer_id)
    .bind(&nonce)
    .execute(pool)
    .await
    .expect("insert cctp message");
}

/// A deBridge row keeps its order id in `message_key` and has no detail table.
async fn insert_debridge_row(pool: &PgPool, seq: i64, created_at: DateTime<Utc>) {
    let tx_hash = format!("list-debridge-tx-{seq}");
    let order_id = format!("{seq:064x}");
    let (transaction_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO transactions (hash, chain, status, slot, timestamp, fee_lamports, signer)
        VALUES ($1, 1, 'Success', 1, NULL, 5000, 'signer')
        RETURNING id
        "#,
    )
    .bind(&tx_hash)
    .fetch_one(pool)
    .await
    .expect("insert transaction");

    sqlx::query(
        r#"
        INSERT INTO bridge_transfers (
            transaction_id, source_tx_hash,
            source_ref_protocol, source_ref_id,
            destination_ref_protocol, destination_ref_id,
            protocol, message_key, direction, event_index, observed_tx_hash,
            status, amount, created_at, updated_at
        )
        VALUES (
            $1, $2, 'debridge', '7565164', 'debridge', '1',
            'debridge', $3, 'Outbound', 0, $2, 'Pending', 1000000, $4, $4
        )
        "#,
    )
    .bind(transaction_id)
    .bind(&tx_hash)
    .bind(&order_id)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("insert debridge transfer");
}

fn sequences(response: &TransferListResponse) -> Vec<i64> {
    response
        .items
        .iter()
        .map(|item| match &item.message_id {
            MessageId::Wormhole { sequence, .. } => i64::try_from(*sequence).expect("sequence"),
            other => panic!("unexpected message id: {other:?}"),
        })
        .collect()
}

async fn list(
    pool: &PgPool,
    snapshot: &bridge::chains::Snapshot,
    query: ListQuery,
) -> TransferListResponse {
    list_transfers(pool, snapshot, query)
        .await
        .expect("list query")
}

fn query(limit: &str) -> ListQuery {
    ListQuery {
        limit: Some(limit.to_string()),
        ..ListQuery::default()
    }
}

#[tokio::test]
async fn pagination_visits_every_row_once_including_duplicate_timestamps() {
    let db = test_db().await;
    for seq in 1..=23_i64 {
        insert_row(
            &db.pool,
            seq,
            "Outbound",
            "Completed",
            Some(1),
            "1",
            Some(2),
            "2",
            None,
            at(seq / 3),
        )
        .await;
    }
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    let mut seen = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let response = list(
            &db.pool,
            &snapshot,
            ListQuery {
                cursor: cursor.clone(),
                ..query("5")
            },
        )
        .await;
        pages += 1;
        assert!(pages <= 10, "pagination did not terminate");
        assert!(response.items.len() <= 5);
        seen.extend(sequences(&response));
        match response.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let mut expected: Vec<i64> = (1..=23).collect();
    // Newest first: larger created_at, and the id breaks ties within a group.
    expected.sort_by_key(|seq| (std::cmp::Reverse(seq / 3), std::cmp::Reverse(*seq)));

    assert_eq!(seen, expected, "one row exactly once, in keyset order");
    assert_eq!(pages, 5, "23 rows at 5 per page");
}

async fn assert_filter(
    pool: &PgPool,
    snapshot: &Snapshot,
    query: ListQuery,
    expected: &[i64],
    label: &str,
) {
    let response = list(pool, snapshot, query).await;
    assert_eq!(sequences(&response), expected, "{label}");
    assert_eq!(response.next_cursor, None, "{label}");
}

#[tokio::test]
async fn filters_select_the_expected_rows() {
    let db = test_db().await;
    // (seq, direction, status, source chain/ref, destination chain/ref, token, created_at offset)
    let rows = [
        (
            1,
            "Outbound",
            "Completed",
            Some(1),
            "1",
            Some(2),
            "2",
            Some("USDC"),
            0,
        ),
        (
            2,
            "Inbound",
            "Completed",
            Some(2),
            "2",
            Some(1),
            "1",
            None,
            1,
        ),
        (
            3,
            "Outbound",
            "Failed",
            Some(1),
            "1",
            Some(3),
            "4",
            Some("USDC"),
            2,
        ),
        (
            4,
            "Outbound",
            "Pending",
            Some(1),
            "1",
            Some(2),
            "2",
            Some("USDT"),
            3,
        ),
        (
            5,
            "Inbound",
            "Completed",
            Some(3),
            "4",
            Some(1),
            "1",
            Some("USDC"),
            4,
        ),
        (
            6,
            "Outbound",
            "Completed",
            Some(1),
            "1",
            Some(3),
            "4",
            None,
            5,
        ),
    ];
    for (seq, direction, status, src_chain, src_ref, dst_chain, dst_ref, token, offset) in rows {
        insert_row(
            &db.pool,
            seq,
            direction,
            status,
            src_chain,
            src_ref,
            dst_chain,
            dst_ref,
            token,
            at(offset),
        )
        .await;
    }
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            direction: Some("Inbound".to_string()),
            ..query("50")
        },
        &[5, 2],
        "direction",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            status: Some("Failed".to_string()),
            ..query("50")
        },
        &[3],
        "status",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            chain: Some("ethereum".to_string()),
            ..query("50")
        },
        &[4, 2, 1],
        "chain matches source or destination",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            chain: Some("bsc".to_string()),
            ..query("50")
        },
        &[6, 5, 3],
        "chain matches the other side",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            token: Some("USDC".to_string()),
            ..query("50")
        },
        &[5, 3, 1],
        "token",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            from: Some("2026-10-01T00:00:00Z".to_string()),
            to: Some("2026-10-31T00:00:00Z".to_string()),
            ..query("50")
        },
        &[],
        "an empty range is an empty page",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            direction: Some("Outbound".to_string()),
            status: Some("Completed".to_string()),
            chain: Some("bsc".to_string()),
            ..query("50")
        },
        &[6],
        "filter combination",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            protocol: Some("cctp".to_string()),
            ..query("50")
        },
        &[],
        "a known protocol with no rows is an empty page",
    )
    .await;
    assert_filter(
        &db.pool,
        &snapshot,
        ListQuery {
            protocol: Some("debridge".to_string()),
            ..query("50")
        },
        &[],
        "the deBridge filter is accepted with no rows",
    )
    .await;
}

#[tokio::test]
async fn the_protocol_filter_selects_only_that_protocol() {
    let db = test_db().await;
    insert_row(
        &db.pool,
        1,
        "Outbound",
        "Completed",
        Some(1),
        "1",
        Some(2),
        "2",
        Some("USDC"),
        at(0),
    )
    .await;
    insert_cctp_row(&db.pool, 2, at(1)).await;
    insert_debridge_row(&db.pool, 3, at(2)).await;
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    for protocol in ["wormhole", "cctp", "debridge"] {
        let response = list(
            &db.pool,
            &snapshot,
            ListQuery {
                protocol: Some(protocol.to_string()),
                ..query("50")
            },
        )
        .await;
        assert_eq!(response.items.len(), 1, "{protocol}");
        assert_eq!(response.items[0].protocol, protocol);
        assert_eq!(response.next_cursor, None);
    }

    let combined = list(
        &db.pool,
        &snapshot,
        ListQuery {
            protocol: Some("wormhole".to_string()),
            direction: Some("Inbound".to_string()),
            ..query("50")
        },
    )
    .await;
    assert!(combined.items.is_empty(), "protocol combines with filters");
}

#[tokio::test]
async fn every_row_carries_its_protocol_display_name() {
    let db = test_db().await;
    insert_row(
        &db.pool,
        1,
        "Outbound",
        "Completed",
        Some(1),
        "1",
        Some(2),
        "2",
        None,
        at(0),
    )
    .await;
    insert_cctp_row(&db.pool, 2, at(1)).await;
    insert_debridge_row(&db.pool, 3, at(2)).await;
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    let response = list(&db.pool, &snapshot, query("50")).await;
    let names: Vec<(&str, &str)> = response
        .items
        .iter()
        .map(|item| (item.protocol.as_str(), item.protocol_name.as_str()))
        .collect();
    assert_eq!(
        names,
        vec![
            ("debridge", "deBridge"),
            ("cctp", "CCTP"),
            ("wormhole", "Portal"),
        ],
        "newest first"
    );
}

#[tokio::test]
async fn the_time_range_is_inclusive_from_and_exclusive_to() {
    let db = test_db().await;
    for seq in 1..=3 {
        insert_row(
            &db.pool,
            seq,
            "Outbound",
            "Completed",
            Some(1),
            "1",
            Some(2),
            "2",
            None,
            at(seq),
        )
        .await;
    }
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    let from = at(1).to_rfc3339();
    let to = at(3).to_rfc3339();
    let response = list(
        &db.pool,
        &snapshot,
        ListQuery {
            from: Some(from),
            to: Some(to.clone()),
            ..query("50")
        },
    )
    .await;
    // Row 1 is included (from is inclusive), row 3 is excluded (to is exclusive).
    assert_eq!(sequences(&response), vec![2, 1]);

    // The cursor combines with the range as an additional bound.
    let first = list(
        &db.pool,
        &snapshot,
        ListQuery {
            from: Some(at(1).to_rfc3339()),
            to: Some(to),
            ..query("1")
        },
    )
    .await;
    assert_eq!(sequences(&first), vec![2]);
    let next = first.next_cursor.expect("a second row is in range");
    let second = list(
        &db.pool,
        &snapshot,
        ListQuery {
            cursor: Some(next),
            from: Some(at(1).to_rfc3339()),
            to: Some(at(3).to_rfc3339()),
            ..query("1")
        },
    )
    .await;
    assert_eq!(sequences(&second), vec![1]);
    assert_eq!(second.next_cursor, None);
}

#[tokio::test]
async fn validation_errors_are_400s_and_the_last_page_has_no_cursor() {
    let db = test_db().await;
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    for (raw, expected) in [("0", "limit"), ("201", "limit"), ("abc", "limit")] {
        let error = list_transfers(
            &db.pool,
            &snapshot,
            ListQuery {
                limit: Some(raw.to_string()),
                ..ListQuery::default()
            },
        )
        .await
        .expect_err("invalid limit");
        assert!(
            matches!(&error, AppError::BadRequest(message) if message.contains(expected)),
            "{raw}: {error:?}"
        );
    }

    let error = list_transfers(
        &db.pool,
        &snapshot,
        ListQuery {
            cursor: Some("not-a-cursor".to_string()),
            ..ListQuery::default()
        },
    )
    .await
    .expect_err("invalid cursor");
    assert!(matches!(&error, AppError::BadRequest(message) if message == "invalid cursor"));

    let error = list_transfers(
        &db.pool,
        &snapshot,
        ListQuery {
            chain: Some("atlantis".to_string()),
            ..ListQuery::default()
        },
    )
    .await
    .expect_err("unknown chain");
    assert!(
        matches!(&error, AppError::BadRequest(message) if message == "unknown chain: atlantis")
    );

    let error = list_transfers(
        &db.pool,
        &snapshot,
        ListQuery {
            protocol: Some("tokens".to_string()),
            ..ListQuery::default()
        },
    )
    .await
    .expect_err("unknown protocol");
    assert!(
        matches!(&error, AppError::BadRequest(message) if message == "protocol must be wormhole, cctp, debridge or mayan")
    );

    let error = list_transfers(
        &db.pool,
        &snapshot,
        ListQuery {
            from: Some("2026-10-31T00:00:00Z".to_string()),
            to: Some("2026-10-01T00:00:00Z".to_string()),
            ..ListQuery::default()
        },
    )
    .await
    .expect_err("from after to");
    assert!(matches!(&error, AppError::BadRequest(message) if message == "from must be before to"));

    // An empty database is an empty last page, not an error.
    let empty = list(&db.pool, &snapshot, query("50")).await;
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);

    // A cursor strictly after the only row returns an empty page whose cursor is null.
    insert_row(
        &db.pool,
        1,
        "Outbound",
        "Completed",
        Some(1),
        "1",
        Some(2),
        "2",
        None,
        at(0),
    )
    .await;
    let only = list(&db.pool, &snapshot, query("50")).await;
    assert_eq!(sequences(&only), vec![1]);
    assert_eq!(only.next_cursor, None);
    let after = list(
        &db.pool,
        &snapshot,
        ListQuery {
            cursor: Some(
                Cursor {
                    created_at: at(-1),
                    id: i64::MAX,
                }
                .encode(),
            ),
            ..query("50")
        },
    )
    .await;
    assert!(after.items.is_empty());
    assert_eq!(after.next_cursor, None);
}

#[tokio::test]
async fn a_full_page_without_more_rows_has_no_next_cursor() {
    let db = test_db().await;
    for seq in 1..=5 {
        insert_row(
            &db.pool,
            seq,
            "Outbound",
            "Completed",
            Some(1),
            "1",
            Some(2),
            "2",
            None,
            at(seq),
        )
        .await;
    }
    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");

    let response = list(&db.pool, &snapshot, query("5")).await;
    assert_eq!(sequences(&response), vec![5, 4, 3, 2, 1]);
    assert_eq!(response.next_cursor, None);
}

#[tokio::test]
async fn an_nft_bridge_row_is_served_with_its_program() {
    let db = test_db().await;
    let (transaction_id,): (i64,) = sqlx::query_as(
        "INSERT INTO transactions (hash, chain, status, slot, timestamp, fee_lamports, signer) \
         VALUES ('list-nft-tx', 1, 'Success', 1, NULL, 5000, 'signer') RETURNING id",
    )
    .fetch_one(&db.pool)
    .await
    .expect("insert transaction");
    let (transfer_id,): (i64,) = sqlx::query_as(
        r#"
        INSERT INTO bridge_transfers (
            transaction_id, source_tx_hash,
            source_ref_protocol, source_ref_id, source_chain_id,
            destination_ref_protocol, destination_ref_id, destination_chain_id,
            protocol, program, message_key, direction, event_index, observed_tx_hash,
            token, token_symbol, status, amount, created_at, updated_at
        )
        VALUES (
            $1, 'list-nft-tx', 'wormhole', '1', 1, 'wormhole', '2', 2,
            'wormhole', 'nft_bridge', '1/ee/1317', 'Outbound', 0, 'list-nft-tx',
            '0xcollection', 'CAC', 'Pending', 1, now(), now()
        )
        RETURNING id
        "#,
    )
    .bind(transaction_id)
    .fetch_one(&db.pool)
    .await
    .expect("insert nft transfer");
    sqlx::query(
        "INSERT INTO wormhole_messages (transfer_id, emitter_chain, emitter_address, sequence) \
         VALUES ($1, 1, 'ee', 1317)",
    )
    .bind(transfer_id)
    .execute(&db.pool)
    .await
    .expect("insert wormhole message");

    let snapshot = load_snapshot(&db.pool).await.expect("snapshot");
    let response = list(&db.pool, &snapshot, query("50")).await;
    assert_eq!(response.items.len(), 1);
    assert_eq!(response.items[0].program, "nft_bridge");
    assert_eq!(response.items[0].token_symbol.as_deref(), Some("CAC"));
}
