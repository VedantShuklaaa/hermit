//! The reconcile precision pass must not write. Like `replay`, it calls
//! `analyse_tx`, which fetches, decodes and enriches but never persists; every
//! write in the pipeline lives in the Redis consumer and the read-through writer.
//!
//! This seeds persisted transfers, snapshots every row of the tables the pipeline
//! writes, runs the precision pass against the fake upstream, and asserts the
//! snapshot is byte-for-byte unchanged. The full-row snapshot covers the row
//! counts and the `created_at`, `updated_at` and `analysis` columns on the seeded
//! rows, and would catch an insert just as well.

mod common;

use bridge::chain_adapters::setup::build_registry;
use bridge::chains::ChainRegistry;
use bridge::config::{AppConfig, IngestConfig, Lane, ReenrichConfig, WorkerConfig};
use bridge::db::repository;
use bridge::domain::bridge_transfer::ChainId;
use bridge::services::precision::precision_pass;
use bridge::state::AppState;
use reqwest::Client;
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;

async fn get_transactions(base_url: &str) -> u64 {
    let body: Value = Client::new()
        .get(format!("{base_url}/stats"))
        .send()
        .await
        .expect("stats request")
        .json()
        .await
        .expect("stats body");
    body["calls"]["helius:getTransaction"].as_u64().unwrap_or(0)
}

async fn table_rows(pool: &PgPool, table: &str, order_by: &str) -> Value {
    sqlx::query_scalar(&format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.{order_by}), '[]'::jsonb)
         FROM {table} t"
    ))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("snapshot {table}: {error}"))
}

/// Every row of every table the pipeline writes, in a stable order.
async fn snapshot(pool: &PgPool) -> Value {
    serde_json::json!({
        "transactions": table_rows(pool, "transactions", "id").await,
        "bridge_transfers": table_rows(pool, "bridge_transfers", "id").await,
        "wormhole_messages": table_rows(pool, "wormhole_messages", "transfer_id").await,
        "indexer_state": table_rows(pool, "indexer_state", "key").await,
    })
}

fn test_config(database_url: &str, upstream_url: &str) -> AppConfig {
    AppConfig {
        role: bridge::config::Role::All,
        pubsub_channel: "bridge:tx_events".to_string(),
        health_port: 0,
        database_url: database_url.to_string(),
        // The analysis path never opens Redis; a value here is only a reminder that
        // nothing in the pass should need it.
        redis_url: "redis://127.0.0.1:1".to_string(),
        live_feed: bridge::config::LiveFeedConfig {
            enabled: false,
            max: 1000,
        },
        helius_api_url: upstream_url.to_string(),
        helius_stream_url: upstream_url.to_string(),
        helius_reconcile_url: upstream_url.to_string(),
        wormhole_url: upstream_url.to_string(),
        helius_api_rps: 20,
        helius_live_rps: 20,
        helius_live_token_wait: Duration::from_secs(20),
        reconcile_rps: 20,
        wormhole_rps: 20,
        upstream_permits: 4,
        failed_cache_ttl_secs: 10,
        port: 0,
        solana_token_bridge_program: bridge::protocols::wormhole::TOKEN_BRIDGE.to_string(),
        solana_bridge_programs: vec![(
            bridge::protocols::Protocol::Wormhole,
            bridge::protocols::wormhole::TOKEN_BRIDGE.to_string(),
        )],
        solana_nft_bridge_program: None,
        solana_ntt_programs: Vec::new(),
        solana_cctp_message_transmitter_program: None,
        solana_cctp_token_messenger_minter_program: None,
        solana_ws_url: "ws://127.0.0.1:0".to_string(),
        allowed_origins: Vec::new(),
        api_rate_limit: bridge::config::ApiRateLimitConfig {
            rps: 10,
            burst: 20,
            trust_forwarded_for: false,
        },
        ingest: IngestConfig {
            enabled: false,
            backfill: false,
            backfill_max: 0,
            backfill_max_age_secs: 0,
            backfill_pace_ms: 0,
            rewind_slots: 20,
            dedupe_ttl_secs: 0,
            ping_secs: 30,
            connect_stagger_ms: 0,
            max_ws_connections: None,
            watchdog_secs: 90,
            poll_secs: 300,
            poll_overlap_secs: 600,
            poll_offset_secs: 180,
            poll_max_publish: 500,
        },
        reenrich: ReenrichConfig {
            enabled: false,
            interval_secs: 1,
            batch: 1,
            min_age_secs: 0,
            retry_secs: 0,
            max_age_secs: 1,
            origin_share_percent: 40,
        },
        worker: WorkerConfig {
            max_deliveries: 10,
            reclaim_idle_ms: 300_000,
            reclaim_interval_secs: 30,
            worker_count: 5,
            concurrency: 2,
            read_count: 4,
        },
        evm: bridge::config::EvmConfig {
            enabled: false,
            rpc: Default::default(),
            poll_secs: 12,
            join_max_age_secs: 86_400,
        },
        chain_registry_refresh_secs: 60,
        health_max_ingest_age_secs: 3600,
    }
}

#[tokio::test]
async fn the_precision_pass_leaves_every_table_untouched() {
    let fake = common::start_fake_upstream().await;
    let hashes = common::transfer_signatures(&fake.base_url, 3).await;

    let db = common::test_db().await;

    // Three persisted transactions in the reconciliation window.
    for (index, hash) in hashes.iter().enumerate() {
        let tx = common::sample_tx(hash, 9000 + index as u64, ChainId::Ethereum, Some("1000"));
        repository::persist_analysis(&db.pool, &tx, &[])
            .await
            .expect("seed a persisted transfer");
    }

    let before = snapshot(&db.pool).await;
    assert_eq!(before["transactions"].as_array().unwrap().len(), 3);
    assert_eq!(before["bridge_transfers"].as_array().unwrap().len(), 3);
    assert_eq!(before["wormhole_messages"].as_array().unwrap().len(), 3);
    let calls_before = get_transactions(&fake.base_url).await;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("http client");
    // The state is handed the pool directly; the analysis path never reads
    // `database_url`. Nothing in the pass may need it, which is the point.
    let config = test_config("postgres://unused", &fake.base_url);
    let registry = build_registry(&config, &client, Lane::Api);
    let chains = ChainRegistry::load(&db.pool).await.expect("chain registry");
    let state =
        AppState::new(db.pool.clone(), config, client, registry, chains).expect("app state");

    let sample: Vec<&str> = hashes.iter().map(String::as_str).collect();
    let report = precision_pass(&state, &sample).await;

    // The pass really ran against the fake upstream and produced transfers: this is
    // the shape that would have been written if the pass persisted.
    assert_eq!(report.sampled, hashes.len());
    assert!(
        report.summary.transfers() > 0,
        "no transfer was re-analysed, so the test would not prove a write-free transfer path"
    );
    let calls_after = get_transactions(&fake.base_url).await;
    assert!(
        calls_after >= calls_before + hashes.len() as u64,
        "expected one getTransaction per sampled signature: {calls_before} -> {calls_after}"
    );

    let after = snapshot(&db.pool).await;
    assert_eq!(
        after, before,
        "the precision pass changed the database: {before} -> {after}"
    );
}
