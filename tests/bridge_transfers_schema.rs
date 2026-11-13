//! The A2 schema: protocol-agnostic `bridge_transfers`, `wormhole_messages`, raw chain
//! references with optional resolution, and the migration of rows that already exist.

mod common;

use bridge::chains::{ChainRegistry, RawChainRef};
use bridge::db::repository::{
    list_missing_origin_transfers, list_open_transfers, persist_transaction, record_origin_attempt,
};
use bridge::domain::bridge_transfer::{BridgeStatus, ChainId};
use bridge::domain::transaction::{Chain, TxStatus};
use sqlx::PgPool;

/// The A2 migration. Everything older is the schema existing rows were written under.
const A2: i64 = 20261018120000;
/// The A7 repair of redeems that were stored like sends.
const A7: i64 = 20261024120000;
/// The repair of inbound rows that stored the Solana redeemer as `source_wallet`.
const REPAIR_INBOUND_SOURCE_WALLET: i64 = 20261030120000;

async fn sql_resolve(pool: &PgPool, protocol: &str, id: &str) -> Option<i16> {
    sqlx::query_scalar("SELECT resolve_chain($1, $2)")
        .bind(protocol)
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn existing_rows_survive_the_migration() {
    let db = common::empty_db().await;
    let pool = &db.pool;
    common::migrate_before(pool, A2).await;

    // Rows as the old code wrote them: Wormhole IDs in SMALLINT columns, TEXT amounts.
    // (hash, sequence, destination chain, amount, status)
    let legacy: [(&str, i64, i16, Option<&str>, &str); 4] = [
        (
            "legacy-eth",
            1,
            2,
            Some("340282366920938463463374607431768211455"),
            "Pending",
        ),
        ("legacy-unknown", 2, 9999, Some("1.5"), "Pending"),
        ("legacy-detected", 3, 30, None, "Detected"),
        ("legacy-done", 4, 4, Some("42"), "Completed"),
    ];
    for (hash, sequence, destination, amount, status) in legacy {
        let transaction_id: i64 = sqlx::query_scalar(
            "INSERT INTO transactions (hash, chain, status, slot, fee_lamports)
             VALUES ($1, 1, 'Success', 1, 5000) RETURNING id",
        )
        .bind(hash)
        .fetch_one(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO bridge_transfers
                 (transaction_id, source_tx_hash, source_chain, emitter_chain, emitter_address,
                  sequence, destination_chain, amount, status, analysis)
             VALUES ($1, $2, 1, 1, 'aa', $3::numeric, $4, $5, $6, '{\"cached\": true}')",
        )
        .bind(transaction_id)
        .bind(hash)
        .bind(sequence.to_string())
        .bind(destination)
        .bind(amount)
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }

    common::migrate_all(pool).await;

    type Row = (
        String,
        String,
        String,
        String,
        String,
        Option<i16>,
        Option<i16>,
        Option<String>,
        String,
        serde_json::Value,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT protocol, message_key, direction, observed_tx_hash, destination_ref_id,
                source_chain_id, destination_chain_id, amount::text, status, analysis
         FROM bridge_transfers ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4, "no row may be lost");

    let eth = sql_resolve(pool, "wormhole", "2").await;
    let solana = sql_resolve(pool, "wormhole", "1").await;
    assert!(eth.is_some() && solana.is_some());

    for (row, (hash, _, _, _, _)) in rows.iter().zip(legacy) {
        assert_eq!(row.0, "wormhole");
        assert_eq!(row.2, "Outbound");
        assert_eq!(row.3, hash, "observed tx is the old source tx");
        assert_eq!(row.5, solana, "source chain resolved");
        assert_eq!(row.9, serde_json::json!({"cached": true}), "analysis kept");
    }

    // The message key is what MessageId::key() builds.
    assert_eq!(rows[0].1, "1/aa/1");
    assert_eq!(rows[3].1, "1/aa/4");

    // Rows that predate the discriminator are backfilled to the Token Bridge.
    let programs: Vec<String> =
        sqlx::query_scalar("SELECT program FROM bridge_transfers ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(programs, vec!["token_bridge"; 4]);

    // Known destination resolves; an unknown one keeps its raw reference.
    assert_eq!(rows[0].6, eth);
    assert_eq!(rows[1].4, "9999");
    assert_eq!(rows[1].6, None, "unknown chain stays unresolved");

    // u128::MAX survives as NUMERIC. A non-integer becomes NULL, a missing one stays NULL.
    assert_eq!(
        rows[0].7.as_deref(),
        Some("340282366920938463463374607431768211455")
    );
    assert_eq!(rows[1].7, None);
    assert_eq!(rows[2].7, None);
    assert_eq!(rows[3].7.as_deref(), Some("42"));

    assert_eq!(rows[3].8, "Completed");

    // The Wormhole fields moved to the detail table, one row per transfer.
    let detail: Vec<(i32, String, String)> = sqlx::query_as(
        "SELECT emitter_chain, emitter_address, sequence::text FROM wormhole_messages ORDER BY sequence",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(detail.len(), 4);
    assert_eq!(detail[0], (1, "aa".to_string(), "1".to_string()));

    // The Wormhole-shaped columns are gone.
    let leftover: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = 'bridge_transfers'
           AND column_name IN ('emitter_chain', 'emitter_address', 'sequence',
                               'source_chain', 'destination_chain')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(leftover, 0);
}

#[tokio::test]
async fn persisting_writes_the_protocol_agnostic_shape() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let tx = common::sample_tx("sig-known", 42, ChainId::Ethereum, Some("1000000"));
    persist_transaction(pool, &tx).await.unwrap();

    type Row = (
        String,
        String,
        String,
        String,
        Option<i16>,
        Option<i16>,
        String,
    );
    let row: Row = sqlx::query_as(
        "SELECT protocol, message_key, direction, destination_ref_id,
                source_chain_id, destination_chain_id, amount::text
         FROM bridge_transfers WHERE observed_tx_hash = 'sig-known'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(row.0, "wormhole");
    assert_eq!(row.1, "1/emitter-addr/42");
    assert_eq!(row.2, "Outbound");
    assert_eq!(row.3, "2");
    assert_eq!(row.4, sql_resolve(pool, "wormhole", "1").await);
    assert_eq!(row.5, sql_resolve(pool, "wormhole", "2").await);
    assert_eq!(row.6, "1000000");

    let (emitter_chain, emitter, sequence): (i32, String, String) = sqlx::query_as(
        "SELECT emitter_chain, emitter_address, sequence::text FROM wormhole_messages",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        (emitter_chain, emitter.as_str(), sequence.as_str()),
        (1, "emitter-addr", "42")
    );
}

#[tokio::test]
async fn the_program_discriminator_is_persisted() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let mut tx = common::sample_tx("sig-ntt", 43, ChainId::Ethereum, Some("7"));
    tx.bridge_transfer.as_mut().unwrap().program = "ntt".to_string();
    persist_transaction(pool, &tx).await.unwrap();

    let program: String = sqlx::query_scalar(
        "SELECT program FROM bridge_transfers WHERE observed_tx_hash = 'sig-ntt'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(program, "ntt");
}

#[tokio::test]
async fn an_unknown_chain_is_stored_as_unresolved_not_dropped() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let tx = common::sample_tx("sig-unknown", 7, ChainId::Unknown(9999), Some("5"));
    persist_transaction(pool, &tx).await.unwrap();

    let (reference, resolved): (String, Option<i16>) = sqlx::query_as(
        "SELECT destination_ref_id, destination_chain_id FROM bridge_transfers
         WHERE observed_tx_hash = 'sig-unknown'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(reference, "9999");
    assert_eq!(resolved, None);
}

#[tokio::test]
async fn a_non_integer_amount_is_stored_as_null_and_does_not_fail_the_write() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let tx = common::sample_tx("sig-decimal", 8, ChainId::Ethereum, Some("1.5"));
    persist_transaction(pool, &tx).await.unwrap();

    let amount: Option<String> = sqlx::query_scalar(
        "SELECT amount::text FROM bridge_transfers WHERE observed_tx_hash = 'sig-decimal'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(amount, None);
}

#[tokio::test]
async fn several_transfers_can_share_one_observed_tx() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_transaction(
        pool,
        &common::sample_tx("sig-shared", 1, ChainId::Ethereum, None),
    )
    .await
    .unwrap();
    // A second transfer from the same tx: another message, next event index.
    let transaction_id: i64 =
        sqlx::query_scalar("SELECT id FROM transactions WHERE hash = 'sig-shared'")
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query(
        "INSERT INTO bridge_transfers
             (transaction_id, protocol, message_key, direction, event_index, observed_tx_hash,
              source_ref_protocol, source_ref_id, destination_ref_protocol, destination_ref_id,
              status)
         VALUES ($1, 'wormhole', '1/emitter-addr/2', 'Outbound', 1, 'sig-shared',
                 'wormhole', '1', 'wormhole', '4', 'Detected')",
    )
    .bind(transaction_id)
    .execute(pool)
    .await
    .expect("two transfers per observed tx must be allowed");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM bridge_transfers WHERE observed_tx_hash = 'sig-shared'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count, 2);

    // The same message twice is still rejected.
    let duplicate = sqlx::query(
        "INSERT INTO bridge_transfers
             (transaction_id, protocol, message_key, direction, observed_tx_hash,
              source_ref_protocol, source_ref_id, destination_ref_protocol, destination_ref_id,
              status)
         VALUES ($1, 'wormhole', '1/emitter-addr/2', 'Outbound', 'other-sig',
                 'wormhole', '1', 'wormhole', '4', 'Detected')",
    )
    .bind(transaction_id)
    .execute(pool)
    .await;
    assert!(duplicate.is_err(), "(protocol, message_key) must be unique");
}

#[tokio::test]
async fn the_database_rejects_bad_direction_and_status_values() {
    let db = common::test_db().await;
    let pool = &db.pool;
    persist_transaction(
        pool,
        &common::sample_tx("sig-check", 1, ChainId::Ethereum, None),
    )
    .await
    .unwrap();

    for (column, value) in [("direction", "Sideways"), ("status", "Lost")] {
        let result = sqlx::query(&format!(
            "UPDATE bridge_transfers SET {column} = $1 WHERE observed_tx_hash = 'sig-check'"
        ))
        .bind(value)
        .execute(pool)
        .await;
        assert!(result.is_err(), "{column} = {value} should be rejected");
    }
}

#[tokio::test]
async fn sql_resolution_follows_the_same_rule_as_the_registry() {
    let db = common::test_db().await;
    let pool = &db.pool;

    // A proposal that is not active never resolves, in SQL or in Rust.
    sqlx::query(
        "INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, status, origin)
         SELECT 'wormhole', '9999', id, 'pending_review', 'llm' FROM chains WHERE slug = 'base'",
    )
    .execute(pool)
    .await
    .unwrap();

    // An active mapping onto a chain that is not active does not resolve either.
    sqlx::query(
        "INSERT INTO chains (slug, name, kind, status) VALUES ('ghost', 'Ghost', 'other', 'pending_review')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id)
         SELECT 'wormhole', '8888', id FROM chains WHERE slug = 'ghost'",
    )
    .execute(pool)
    .await
    .unwrap();

    let registry = ChainRegistry::load(pool).await.unwrap();
    for (protocol, id) in [
        ("wormhole", "1"),
        ("wormhole", "30"),
        ("wormhole", "9999"),
        ("wormhole", "8888"),
        ("wormhole", "424242"),
        ("evm", "8453"),
        ("evm", "1"),
        ("cctp", "0"),
    ] {
        let rust = registry
            .resolve(&RawChainRef::new(protocol, id))
            .key()
            .map(|k| k.0);
        assert_eq!(
            sql_resolve(pool, protocol, id).await,
            rust,
            "{protocol}:{id}"
        );
    }
}

#[tokio::test]
async fn activating_a_mapping_resolves_the_rows_stored_as_unresolved() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_transaction(
        pool,
        &common::sample_tx("sig-late", 1, ChainId::Unknown(9999), None),
    )
    .await
    .unwrap();

    let base: i16 = sqlx::query_scalar("SELECT id FROM chains WHERE slug = 'base'")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id)
         VALUES ('wormhole', '9999', $1)",
    )
    .bind(base)
    .execute(pool)
    .await
    .unwrap();

    let updated = sqlx::query(
        "UPDATE bridge_transfers
         SET destination_chain_id = resolve_chain(destination_ref_protocol, destination_ref_id)
         WHERE destination_chain_id IS NULL
           AND resolve_chain(destination_ref_protocol, destination_ref_id) IS NOT NULL",
    )
    .execute(pool)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(updated, 1);

    let resolved: Option<i16> = sqlx::query_scalar(
        "SELECT destination_chain_id FROM bridge_transfers WHERE observed_tx_hash = 'sig-late'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(resolved, Some(base));
}

#[tokio::test]
async fn a_later_analysis_without_enrichment_does_not_downgrade_the_status() {
    let db = common::test_db().await;
    let pool = &db.pool;

    // Pending: WormholeScan had the VAA.
    let pending = common::sample_tx("sig-down", 1, ChainId::Ethereum, Some("1"));
    persist_transaction(pool, &pending).await.unwrap();

    // Re-analysed while WormholeScan was unavailable: on-chain data only, Detected.
    let mut detected = pending.clone();
    detected.bridge_transfer.as_mut().unwrap().status = BridgeStatus::Detected;
    persist_transaction(pool, &detected).await.unwrap();

    let (status, analysis): (String, serde_json::Value) = sqlx::query_as(
        "SELECT status, analysis FROM bridge_transfers WHERE observed_tx_hash = 'sig-down'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(status, "Pending");
    assert_eq!(
        analysis["bridge_transfer"]["status"], "Pending",
        "the cached response must not describe an older status than the row"
    );

    // Detected can still be raised.
    let mut completed = pending.clone();
    completed.bridge_transfer.as_mut().unwrap().status = BridgeStatus::Completed;
    persist_transaction(pool, &completed).await.unwrap();
    let status: String = sqlx::query_scalar(
        "SELECT status FROM bridge_transfers WHERE observed_tx_hash = 'sig-down'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(status, "Completed");
}

#[tokio::test]
async fn an_inbound_redeem_is_stored_as_the_destination_side() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let tx = common::sample_redeem_tx("sig-redeem", 7, ChainId::Ethereum, None);
    persist_transaction(pool, &tx).await.unwrap();

    type Row = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<i16>,
        Option<i16>,
    );
    let row: Row = sqlx::query_as(
        "SELECT direction, observed_tx_hash, message_key, source_tx_hash, destination_tx_hash,
                status, source_chain_id, destination_chain_id
         FROM bridge_transfers WHERE observed_tx_hash = 'sig-redeem'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(row.0, "Inbound");
    assert_eq!(row.1, "sig-redeem");
    assert_eq!(row.2, "2/emitter-addr/7");
    assert_eq!(row.3, None, "the origin tx is unknown until enrichment");
    assert_eq!(row.4.as_deref(), Some("sig-redeem"));
    assert_eq!(row.5, "Completed");
    assert_eq!(row.6, sql_resolve(pool, "wormhole", "2").await);
    assert_eq!(row.7, sql_resolve(pool, "wormhole", "1").await);

    let emitter_chain: i32 = sqlx::query_scalar("SELECT emitter_chain FROM wormhole_messages")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(emitter_chain, 2);
}

#[tokio::test]
async fn enrichment_fills_the_origin_tx_of_a_redeem_and_it_sticks() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-fill", 8, ChainId::Ethereum, None),
    )
    .await
    .unwrap();

    // Enrichment learned the origin tx.
    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-fill", 8, ChainId::Ethereum, Some("0xorigin")),
    )
    .await
    .unwrap();

    // A later analysis that could not reach WormholeScan must not erase it.
    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-fill", 8, ChainId::Ethereum, None),
    )
    .await
    .unwrap();

    let (count, source, status): (i64, Option<String>, String) =
        sqlx::query_as("SELECT COUNT(*) OVER (), source_tx_hash, status FROM bridge_transfers")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(count, 1, "the redeem is one row, however often it is seen");
    assert_eq!(source.as_deref(), Some("0xorigin"));
    assert_eq!(status, "Completed");
}

#[tokio::test]
async fn a_completed_inbound_row_without_an_origin_tx_is_due_for_origin_backfill() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-origin-missing", 9, ChainId::Ethereum, None),
    )
    .await
    .unwrap();

    let due = list_missing_origin_transfers(pool, 0, 0, 10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].observed_tx_hash, "sig-origin-missing");

    // The attempt was just now, so the retry window keeps it out of the next pass even
    // though the attempt itself wrote nothing but the marker.
    record_origin_attempt(pool, due[0].id).await.unwrap();
    let due = list_missing_origin_transfers(pool, 0, 3600, 10)
        .await
        .unwrap();
    assert!(due.is_empty());

    // Once the window passes the row is due again: an unresolvable origin is retried,
    // not dropped.
    let due = list_missing_origin_transfers(pool, 0, 0, 10).await.unwrap();
    assert_eq!(due.len(), 1);
    record_origin_attempt(pool, due[0].id).await.unwrap();

    // Once enrichment knows the origin tx the row leaves the queue.
    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-origin-missing", 9, ChainId::Ethereum, Some("0xorigin")),
    )
    .await
    .unwrap();
    let due = list_missing_origin_transfers(pool, 0, 0, 10).await.unwrap();
    assert!(due.is_empty());

    // An outbound send is an open transfer, never an origin-backfill row.
    persist_transaction(
        pool,
        &common::sample_tx("sig-send", 10, ChainId::Ethereum, Some("1000")),
    )
    .await
    .unwrap();
    let open = list_open_transfers(pool, 0, 3600, 0, 10).await.unwrap();
    assert!(open.iter().any(|row| row.observed_tx_hash == "sig-send"));
    let due = list_missing_origin_transfers(pool, 0, 0, 10).await.unwrap();
    assert!(due.is_empty());
}

#[tokio::test]
async fn successive_passes_cover_every_missing_origin_row() {
    let db = common::test_db().await;
    let pool = &db.pool;

    for index in 0..7u64 {
        persist_transaction(
            pool,
            &common::sample_redeem_tx(
                &format!("sig-origin-{index}"),
                100 + index,
                ChainId::Ethereum,
                None,
            ),
        )
        .await
        .unwrap();
    }

    // More due rows than the batch: each pass attempts the oldest-attempted rows first
    // and records the attempt, so the next pass moves on instead of re-selecting them.
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..3 {
        let due = list_missing_origin_transfers(pool, 0, 3600, 3)
            .await
            .unwrap();
        assert!(due.len() <= 3, "the batch limit is respected");
        for row in due {
            assert!(
                seen.insert(row.observed_tx_hash.clone()),
                "{} was selected twice inside the retry window",
                row.observed_tx_hash
            );
            record_origin_attempt(pool, row.id).await.unwrap();
        }
    }

    assert_eq!(seen.len(), 7, "three passes of three covered every row");
    let due = list_missing_origin_transfers(pool, 0, 3600, 3)
        .await
        .unwrap();
    assert!(due.is_empty(), "the retry window holds every attempted row");
}

#[tokio::test]
async fn a_repaired_row_without_its_cached_analysis_is_rebuilt_for_backfill() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_transaction(
        pool,
        &common::sample_redeem_tx("sig-repaired", 11, ChainId::Ethereum, None),
    )
    .await
    .unwrap();
    // What the A7 repair migrations left behind: the cached analysis was dropped.
    sqlx::query("UPDATE bridge_transfers SET analysis = NULL")
        .execute(pool)
        .await
        .unwrap();

    let due = list_missing_origin_transfers(pool, 0, 0, 10).await.unwrap();
    assert_eq!(due.len(), 1, "a row without analysis is still due");
    let row = due.into_iter().next().unwrap();
    let tx = row
        .transaction()
        .expect("the transaction is rebuilt from the stored row");

    assert_eq!(tx.hash, "sig-repaired");
    assert!(matches!(tx.chain, Chain::Solana));
    assert!(matches!(tx.status, TxStatus::Success));
    let transfer = tx.bridge_transfer.expect("the transfer is rebuilt");
    assert_eq!(transfer.source_chain, ChainId::Ethereum);
    assert_eq!(transfer.destination_chain, ChainId::Solana);
    assert_eq!(transfer.source_tx_hash, None);
    assert_eq!(transfer.message_id.emitter_chain, 2);
    assert_eq!(transfer.message_id.emitter_address, "emitter-addr");
    assert_eq!(transfer.message_id.sequence, 11);
    assert!(matches!(transfer.status, BridgeStatus::Completed));
}

#[tokio::test]
async fn redeems_stored_like_sends_are_turned_around_by_the_repair() {
    let db = common::empty_db().await;
    let pool = &db.pool;
    common::migrate_before(pool, A7).await;

    // (hash, emitter chain, destination chain id): as the old WormholeScan-driven path
    // wrote them, always 'Outbound' with the observed tx as source_tx_hash.
    let rows: [(&str, i32, &str); 3] = [
        ("old-redeem", 2, "1"),    // from Ethereum into Solana: a redeem
        ("old-send", 1, "2"),      // a send out of Solana: untouched
        ("old-elsewhere", 2, "4"), // emitted on Ethereum, observed elsewhere: untouched
    ];
    for (i, (hash, emitter_chain, destination)) in rows.iter().enumerate() {
        let transaction_id: i64 = sqlx::query_scalar(
            "INSERT INTO transactions (hash, chain, status, slot, fee_lamports)
             VALUES ($1, 1, 'Success', 1, 5000) RETURNING id",
        )
        .bind(hash)
        .fetch_one(pool)
        .await
        .unwrap();

        let transfer_id: i64 = sqlx::query_scalar(
            "INSERT INTO bridge_transfers
                 (transaction_id, protocol, message_key, direction, observed_tx_hash,
                  source_tx_hash, source_ref_protocol, source_ref_id,
                  destination_ref_protocol, destination_ref_id, destination_tx_hash,
                  status, analysis)
             VALUES ($1, 'wormhole', $2, 'Outbound', $3, $3, 'wormhole', $4,
                     'wormhole', $5, NULL, 'Pending', '{\"cached\": true}')
             RETURNING id",
        )
        .bind(transaction_id)
        .bind(format!("{emitter_chain}/aa/{i}"))
        .bind(hash)
        .bind(emitter_chain.to_string())
        .bind(destination)
        .fetch_one(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO wormhole_messages (transfer_id, emitter_chain, emitter_address, sequence)
             VALUES ($1, $2, 'aa', $3::numeric)",
        )
        .bind(transfer_id)
        .bind(emitter_chain)
        .bind(i as i64)
        .execute(pool)
        .await
        .unwrap();
    }

    common::migrate_all(pool).await;

    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<serde_json::Value>,
    );
    let read = |hash: &'static str| async move {
        let row: Row = sqlx::query_as(
            "SELECT direction, observed_tx_hash, source_tx_hash, destination_tx_hash,
                    status, analysis
             FROM bridge_transfers WHERE observed_tx_hash = $1",
        )
        .bind(hash)
        .fetch_one(pool)
        .await
        .unwrap();
        row
    };

    let redeem = read("old-redeem").await;
    assert_eq!(redeem.0, "Inbound");
    assert_eq!(redeem.2, None, "the observed tx is not a source tx");
    assert_eq!(redeem.3.as_deref(), Some("old-redeem"));
    assert_eq!(redeem.4, "Completed");
    assert_eq!(redeem.5, None, "the stale cached analysis is dropped");

    for hash in ["old-send", "old-elsewhere"] {
        let row = read(hash).await;
        assert_eq!(row.0, "Outbound", "{hash}");
        assert_eq!(row.2.as_deref(), Some(hash), "{hash}");
        assert_eq!(row.3, None, "{hash}");
        assert_eq!(row.4, "Pending", "{hash}");
        assert!(row.5.is_some(), "{hash}: analysis untouched");
    }
}

/// (observed tx, direction, source wallet, analysis, updated_at) for every row.
type Snapshot = Vec<(
    String,
    String,
    Option<String>,
    Option<serde_json::Value>,
    String,
)>;

async fn snapshot(pool: &PgPool) -> Snapshot {
    sqlx::query_as(
        "SELECT observed_tx_hash, direction, source_wallet, analysis, updated_at::text
         FROM bridge_transfers ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn the_source_wallet_repair_clears_inbound_rows_only_and_is_idempotent() {
    let db = common::empty_db().await;
    let pool = &db.pool;
    common::migrate_before(pool, REPAIR_INBOUND_SOURCE_WALLET).await;

    // (hash, direction, source tx, source wallet): as the WormholeScan fallback wrote them,
    // a redeem stored with the Solana signer of the redeem tx as its source wallet.
    let rows: [(&str, &str, Option<&str>, Option<&str>); 3] = [
        ("redeem-bad", "Inbound", None, Some("SolanaRedeemer")),
        (
            "send-good",
            "Outbound",
            Some("send-good"),
            Some("SolanaSender"),
        ),
        ("redeem-clean", "Inbound", None, None),
    ];
    for (i, (hash, direction, source_tx, source_wallet)) in rows.iter().enumerate() {
        let transaction_id: i64 = sqlx::query_scalar(
            "INSERT INTO transactions (hash, chain, status, slot, fee_lamports)
             VALUES ($1, 1, 'Success', 1, 5000) RETURNING id",
        )
        .bind(hash)
        .fetch_one(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO bridge_transfers
                 (transaction_id, protocol, message_key, direction, observed_tx_hash,
                  source_tx_hash, source_wallet, source_ref_protocol, source_ref_id,
                  destination_ref_protocol, destination_ref_id, status, analysis)
             VALUES ($1, 'wormhole', $2, $3, $4, $5, $6, 'wormhole', '2',
                     'wormhole', '1', 'Completed', '{\"cached\": true}')",
        )
        .bind(transaction_id)
        .bind(format!("2/aa/{i}"))
        .bind(direction)
        .bind(hash)
        .bind(source_tx)
        .bind(source_wallet)
        .execute(pool)
        .await
        .unwrap();
    }

    common::migrate_all(pool).await;

    let after_first = snapshot(pool).await;
    assert_eq!(after_first.len(), 3, "no row may be lost");

    let bad = &after_first[0];
    assert_eq!(bad.0, "redeem-bad");
    assert_eq!(bad.1, "Inbound");
    assert_eq!(bad.2, None, "the Solana redeemer is not an origin sender");
    assert_eq!(bad.3, None, "the stale cached analysis is dropped");

    let send = &after_first[1];
    assert_eq!(send.1, "Outbound");
    assert_eq!(
        send.2.as_deref(),
        Some("SolanaSender"),
        "an outbound row keeps its source wallet"
    );
    assert!(send.3.is_some(), "an outbound row keeps its analysis");

    let clean = &after_first[2];
    assert_eq!(clean.1, "Inbound");
    assert_eq!(clean.2, None);
    assert!(
        clean.3.is_some(),
        "an inbound row that is already NULL is untouched"
    );

    // The second run matches no row: same values, and no row's updated_at moves.
    sqlx::query(include_str!(
        "../migrations/20261030120000_repair_inbound_source_wallet.sql"
    ))
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(snapshot(pool).await, after_first, "a second run is a no-op");
}
