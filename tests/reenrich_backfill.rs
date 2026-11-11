//! The origin backfill shares every pass with the open rows but keeps its own slice of
//! the batch. A backlog of open transfers must not starve it, and the retry window must
//! rotate its queue instead of re-selecting the same rows forever.

mod common;

use bridge::chain_adapters::setup::build_registry;
use bridge::chains::ChainRegistry;
use bridge::config::{
    AppConfig, IngestConfig, IngestSourceKind, Lane, ReenrichConfig, WorkerConfig,
};
use bridge::db::repository::persist_transaction;
use bridge::domain::bridge_transfer::ChainId;
use bridge::enrichment::reenrich::run_pass;
use bridge::state::AppState;
use reqwest::Client;
use sqlx::PgPool;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn test_config(upstream_url: &str, batch: i64, origin_share_percent: u8) -> AppConfig {
    AppConfig {
        role: bridge::config::Role::All,
        pubsub_channel: "bridge:tx_events".to_string(),
        health_port: 0,
        database_url: "postgres://unused".to_string(),
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
        helius_live_token_wait: Duration::from_secs(5),
        reconcile_rps: 20,
        wormhole_rps: 20,
        upstream_permits: 4,
        failed_cache_ttl_secs: 10,
        port: 0,
        solana_token_bridge_program: "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth".to_string(),
        solana_ws_url: "ws://127.0.0.1:0".to_string(),
        allowed_origins: Vec::new(),
        ingest: IngestConfig {
            enabled: false,
            source: IngestSourceKind::LogsSubscribe,
            laserstream_endpoint: "https://laserstream-devnet-ewr.helius-rpc.com".to_string(),
            backfill: false,
            backfill_max: 0,
            backfill_pace_ms: 0,
            rewind_slots: 20,
            dedupe_ttl_secs: 0,
            ping_secs: 30,
            watchdog_secs: 90,
            poll_secs: 300,
            poll_overlap_secs: 600,
            poll_offset_secs: 180,
            poll_max_publish: 500,
        },
        reenrich: ReenrichConfig {
            enabled: true,
            interval_secs: 1,
            batch,
            min_age_secs: 0,
            retry_secs: 3600,
            // Wide enough that the seeded open rows, aged two hours for the retry window,
            // are still inside it.
            max_age_secs: 24 * 3600,
            origin_share_percent,
        },
        worker: WorkerConfig {
            max_deliveries: 10,
            reclaim_idle_ms: 300_000,
            reclaim_interval_secs: 30,
            worker_count: 5,
            read_count: 10,
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

async fn state(pool: &PgPool, config: AppConfig) -> AppState {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("http client");
    let chains = ChainRegistry::load(pool).await.expect("chain registry");
    let registry = build_registry(&config, &client, Lane::Api);
    AppState::new(pool.clone(), config, client, registry, chains).expect("app state")
}

/// How many backfill rows have been attempted. Only the origin backfill writes the
/// marker, so open rows never count here.
async fn attempted_origins(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM bridge_transfers WHERE last_enrich_attempt_at IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .expect("count attempted origin rows")
}

#[tokio::test]
async fn a_backlog_of_open_rows_does_not_starve_the_origin_backfill() {
    let fake = common::start_fake_upstream().await;
    let db = common::test_db().await;

    // Twenty open transfers are due, more than several batches.
    for index in 0..20u64 {
        persist_transaction(
            &db.pool,
            &common::sample_tx(
                &format!("sig-open-{index}"),
                1000 + index,
                ChainId::Ethereum,
                Some("1000"),
            ),
        )
        .await
        .unwrap();
    }
    for index in 0..5u64 {
        persist_transaction(
            &db.pool,
            &common::sample_redeem_tx(
                &format!("sig-origin-{index}"),
                2000 + index,
                ChainId::Ethereum,
                None,
            ),
        )
        .await
        .unwrap();
    }

    // Open rows use `updated_at` as their attempt marker, so they need to be old enough
    // to be past the retry window, like the rows a production backlog is made of.
    sqlx::query(
        "UPDATE bridge_transfers
         SET created_at = NOW() - INTERVAL '2 hours', updated_at = NOW() - INTERVAL '2 hours'
         WHERE direction = 'Outbound'",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    // Half of a six-row batch is reserved for the backfill: three open rows and three
    // backfill rows, even though twenty open rows are due.
    let state = state(&db.pool, test_config(&fake.base_url, 6, 50)).await;
    let cfg = state.config.reenrich.clone();

    let stats = run_pass(&state, &cfg, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        attempted_origins(&db.pool).await,
        3,
        "the backfill kept three of the six slots"
    );
    assert_eq!(
        stats.unchanged, 6,
        "three open and three backfill rows were answered"
    );

    // The retry window holds the rows just attempted, so the next pass takes the rest of
    // the backfill instead of revisiting the same three.
    let stats = run_pass(&state, &cfg, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        attempted_origins(&db.pool).await,
        5,
        "the rest of the backfill is reached on the next pass"
    );
    assert!(stats.unchanged >= 2);
}
