//! How the Redis consumer treats failures. A transient upstream failure must leave
//! the message pending so recovery can redeliver it, a permanent one must dead-letter
//! it, and nothing may be redelivered forever.
//!
//! These tests drive the real consumer against a real Redis (testcontainers), a real
//! Postgres and the fake upstream, so they exercise the same code path production uses.

mod common;

use bridge::chain_adapters::setup::build_registry;
use bridge::chains::ChainRegistry;
use bridge::config::{AppConfig, IngestConfig, Lane, ReenrichConfig, WorkerConfig};
use bridge::redis::consumer::RedisConsumer;
use bridge::state::AppState;
use redis::aio::MultiplexedConnection;
use reqwest::Client;
use sqlx::PgPool;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const STREAM: &str = "bridge:transactions";
const GROUP: &str = "bridge-workers";
const FAILED: &str = "bridge:transactions:failed";

/// The state is handed the pool directly; nothing on the consumer path reads this.
const UNUSED_DB_URL: &str = "postgres://unused";

/// How long a test waits for the pipeline to settle before failing.
const SETTLE: Duration = Duration::from_secs(120);

fn test_config(
    database_url: &str,
    redis_url: &str,
    upstream_url: &str,
    max_deliveries: u32,
    reclaim_interval_secs: u64,
) -> AppConfig {
    AppConfig {
        role: bridge::config::Role::All,
        pubsub_channel: "bridge:tx_events".to_string(),
        health_port: 0,
        database_url: database_url.to_string(),
        redis_url: redis_url.to_string(),
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
        solana_token_bridge_program: bridge::protocols::wormhole::TOKEN_BRIDGE.to_string(),
        solana_bridge_programs: vec![(
            bridge::protocols::Protocol::Wormhole,
            bridge::protocols::wormhole::TOKEN_BRIDGE.to_string(),
        )],
        solana_nft_bridge_program: None,
        solana_cctp_message_transmitter_program: None,
        solana_cctp_token_messenger_minter_program: None,
        solana_ws_url: "ws://127.0.0.1:0".to_string(),
        allowed_origins: Vec::new(),
        ingest: IngestConfig {
            enabled: false,
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
            enabled: false,
            interval_secs: 1,
            batch: 1,
            min_age_secs: 0,
            retry_secs: 0,
            max_age_secs: 1,
            origin_share_percent: 40,
        },
        worker: WorkerConfig {
            max_deliveries,
            reclaim_idle_ms: 50,
            reclaim_interval_secs,
            worker_count: 1,
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

/// The state the consumer workers run with, wired like `main` wires it.
async fn live_state(pool: &PgPool, config: AppConfig) -> AppState {
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("http client");
    let chains = ChainRegistry::load(pool).await.expect("chain registry");
    let registry = build_registry(&config, &client, Lane::Api);
    let base = AppState::new(
        pool.clone(),
        config.clone(),
        client.clone(),
        registry,
        chains,
    )
    .expect("app state");

    let live_registry = build_registry(&config, &client, Lane::Ingest);
    base.for_ingest(live_registry)
}

fn spawn_consumer(
    url: &str,
    state: AppState,
    name: &str,
    shutdown: CancellationToken,
    recovery: bool,
) -> JoinHandle<()> {
    let name = name.to_string();
    let consumer = RedisConsumer::new(url, state, name.clone(), shutdown)
        .expect("consumer needs a valid redis url");
    tokio::spawn(async move {
        let result = if recovery {
            consumer.run_recovery().await
        } else {
            consumer.run().await
        };
        if let Err(error) = result {
            eprintln!("{name} stopped: {error:?}");
        }
    })
}

async fn connect(url: &str) -> MultiplexedConnection {
    redis::Client::open(url)
        .expect("redis url")
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection")
}

async fn create_group(connection: &mut MultiplexedConnection) {
    let result: redis::RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(STREAM)
        .arg(GROUP)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(connection)
        .await;

    if let Err(error) = result
        && !error.to_string().contains("BUSYGROUP")
    {
        panic!("xgroup create: {error}");
    }
}

async fn publish(connection: &mut MultiplexedConnection, chain: &str, tx_hash: &str, slot: u64) {
    let _: String = redis::cmd("XADD")
        .arg(STREAM)
        .arg("*")
        .arg("chain")
        .arg(chain)
        .arg("tx_hash")
        .arg(tx_hash)
        .arg("slot")
        .arg(slot)
        .query_async(connection)
        .await
        .expect("xadd");
}

/// Reads a message as a consumer that then "crashes": the message is pending and
/// owned by nobody running, which is the state recovery exists to clean up.
async fn pre_deliver(connection: &mut MultiplexedConnection, count: usize) {
    let response: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(GROUP)
        .arg("crashed-worker")
        .arg("COUNT")
        .arg(count)
        .arg("STREAMS")
        .arg(STREAM)
        .arg(">")
        .query_async(connection)
        .await
        .expect("xreadgroup");

    assert!(messages_in(&response) > 0, "nothing was delivered to crash");
}

fn messages_in(response: &redis::Value) -> usize {
    let redis::Value::Array(streams) = response else {
        return 0;
    };
    streams
        .iter()
        .filter_map(|stream| match stream {
            redis::Value::Array(parts) if parts.len() == 2 => match &parts[1] {
                redis::Value::Array(messages) => Some(messages.len()),
                _ => None,
            },
            _ => None,
        })
        .sum()
}

/// `(id, deliveries)` for every pending message, from the extended XPENDING reply.
async fn pending(connection: &mut MultiplexedConnection) -> Vec<(String, u64)> {
    let reply: redis::Value = redis::cmd("XPENDING")
        .arg(STREAM)
        .arg(GROUP)
        .arg("-")
        .arg("+")
        .arg(100)
        .query_async(connection)
        .await
        .expect("xpending");

    let redis::Value::Array(rows) = reply else {
        return Vec::new();
    };

    rows.iter()
        .filter_map(|row| match row {
            redis::Value::Array(entry) if entry.len() >= 4 => match (&entry[0], &entry[3]) {
                (redis::Value::BulkString(id), redis::Value::Int(deliveries)) => Some((
                    String::from_utf8(id.clone()).ok()?,
                    u64::try_from(*deliveries).ok()?,
                )),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// `(tx_hash, error)` for each dead-lettered message.
async fn dead_letters(connection: &mut MultiplexedConnection) -> Vec<(String, String)> {
    let reply: redis::Value = redis::cmd("XRANGE")
        .arg(FAILED)
        .arg("-")
        .arg("+")
        .query_async(connection)
        .await
        .expect("xrange failed stream");

    let redis::Value::Array(entries) = reply else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let redis::Value::Array(parts) = entry else {
                return None;
            };
            let redis::Value::Array(fields) = parts.get(1)? else {
                return None;
            };
            let mut tx_hash = None;
            let mut error = None;
            for pair in fields.chunks(2) {
                if pair.len() != 2 {
                    continue;
                }
                let (redis::Value::BulkString(key), redis::Value::BulkString(value)) =
                    (&pair[0], &pair[1])
                else {
                    continue;
                };
                match String::from_utf8(key.clone()).ok()?.as_str() {
                    "tx_hash" => tx_hash = String::from_utf8(value.clone()).ok(),
                    "error" => error = String::from_utf8(value.clone()).ok(),
                    _ => {}
                }
            }
            Some((tx_hash?, error.unwrap_or_default()))
        })
        .collect()
}

async fn persisted(pool: &PgPool, hashes: &[String]) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM transactions WHERE hash = ANY($1)")
        .bind(hashes.to_vec())
        .fetch_one(pool)
        .await
        .expect("count persisted transactions")
}

async fn fake_stats(base_url: &str) -> serde_json::Value {
    Client::new()
        .get(format!("{base_url}/stats"))
        .send()
        .await
        .expect("stats request")
        .json()
        .await
        .expect("stats body")
}

async fn fake_calls_to(base_url: &str, method: &str) -> u64 {
    fake_stats(base_url).await["calls"][method]
        .as_u64()
        .unwrap_or(0)
}

async fn fake_injected_429(base_url: &str, method: &str) -> u64 {
    fake_stats(base_url).await["injected_429"][method]
        .as_u64()
        .unwrap_or(0)
}

/// A burst through a limiter that is much slower than the burst must not lose any
/// message: upstream 429s are retried, and none of them reach the dead-letter stream.
#[tokio::test]
async fn a_burst_through_a_slow_limiter_is_fully_persisted() {
    let fake =
        common::start_fake_upstream_with(&[("FAKE_LATENCY_MS", "10"), ("FAKE_429_RATE", "0.2")])
            .await;
    let hashes = common::transfer_signatures(&fake.base_url, 6).await;

    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let mut connection = connect(&redis.url).await;
    create_group(&mut connection).await;

    for (index, hash) in hashes.iter().enumerate() {
        publish(&mut connection, "solana", hash, 100 + index as u64).await;
    }

    // Two live calls a second against a burst that needs about twice as many tokens.
    let mut config = test_config(UNUSED_DB_URL, &redis.url, &fake.base_url, 10, 2);
    config.helius_live_rps = 2;
    let state = live_state(&db.pool, config).await;

    let shutdown = CancellationToken::new();
    let consumer = spawn_consumer(
        &redis.url,
        state.clone(),
        "burst-worker",
        shutdown.clone(),
        false,
    );
    let recovery = spawn_consumer(&redis.url, state, "recovery-worker", shutdown.clone(), true);

    let deadline = Instant::now() + SETTLE;
    loop {
        if persisted(&db.pool, &hashes).await == hashes.len() as i64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {} of {} messages were persisted",
            persisted(&db.pool, &hashes).await,
            hashes.len()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Every message is acknowledged once it is persisted, so nothing is left pending.
    let deadline = Instant::now() + SETTLE;
    while !pending(&mut connection).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "messages still pending: {:?}",
            pending(&mut connection).await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        dead_letters(&mut connection).await.is_empty(),
        "a transient failure was dead-lettered: {:?}",
        dead_letters(&mut connection).await
    );
    assert_eq!(persisted(&db.pool, &hashes).await, hashes.len() as i64);

    shutdown.cancel();
    let _ = consumer.await;
    let _ = recovery.await;
}

/// A WormholeScan 429 is transient like a Helius one: the client retries it and the
/// message is persisted instead of dead-lettered. An attest is rejected by every
/// adapter, so this message takes the WormholeScan-driven fallback path, where the
/// operation fetch is what the transfer depends on.
#[tokio::test]
async fn a_wormholescan_429_on_the_fallback_path_is_retried_and_persisted() {
    // The first two calls to each WormholeScan route answer 429; the client gets one
    // more within its retry budget.
    let fake = common::start_fake_upstream_with(&[("FAKE_WORMHOLE_429_FIRST", "2")]).await;
    let hash = common::signature_for_fixture(&fake.base_url, "admin_call").await;

    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let mut connection = connect(&redis.url).await;
    create_group(&mut connection).await;
    publish(&mut connection, "solana", &hash, 1).await;

    let config = test_config(UNUSED_DB_URL, &redis.url, &fake.base_url, 10, 60);
    let state = live_state(&db.pool, config).await;

    let shutdown = CancellationToken::new();
    let consumer = spawn_consumer(
        &redis.url,
        state,
        "wormhole-429-worker",
        shutdown.clone(),
        false,
    );

    let hashes = [hash.clone()];
    let deadline = Instant::now() + SETTLE;
    while persisted(&db.pool, &hashes).await == 0 {
        assert!(
            Instant::now() < deadline,
            "a transient WormholeScan 429 was never retried to success"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(persisted(&db.pool, &hashes).await, 1);
    assert_eq!(
        fake_injected_429(&fake.base_url, "wormhole:operations").await,
        2,
        "the fallback path did not receive the injected 429s"
    );
    assert!(
        dead_letters(&mut connection).await.is_empty(),
        "a transient 429 was dead-lettered: {:?}",
        dead_letters(&mut connection).await
    );

    // A persisted message is acknowledged, so nothing is left pending.
    let deadline = Instant::now() + SETTLE;
    while !pending(&mut connection).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "messages still pending: {:?}",
            pending(&mut connection).await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    shutdown.cancel();
    let _ = consumer.await;
}

/// The recovery worker runs the same handler as first delivery, so a reclaimed
/// message that fails with `Overloaded` stays pending instead of dead-lettering.
#[tokio::test]
async fn a_reclaimed_message_failing_with_overloaded_stays_pending() {
    // Every Helius call answers 429, which the client retries and then reports as a
    // transient rate limit, exactly like a real Overloaded.
    let fake = common::start_fake_upstream_with(&[("FAKE_429_RATE", "1.0")]).await;

    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let mut connection = connect(&redis.url).await;
    create_group(&mut connection).await;
    publish(&mut connection, "solana", "transient-reclaim", 1).await;
    pre_deliver(&mut connection, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A long interval keeps recovery to one sweep, so the message is reclaimed once.
    let config = test_config(UNUSED_DB_URL, &redis.url, &fake.base_url, 10, 60);
    let state = live_state(&db.pool, config).await;

    let shutdown = CancellationToken::new();
    let recovery = spawn_consumer(&redis.url, state, "recovery-worker", shutdown.clone(), true);

    // Reclaiming increments the delivery count before the message is handled, so
    // wait for the handler to finish: it retries a handful of times with backoff.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let calls = fake_calls_to(&fake.base_url, "helius:getTransaction").await;
    assert!(calls > 0, "the reclaimed message was never analysed");

    let pending = pending(&mut connection).await;
    assert_eq!(
        pending.len(),
        1,
        "the message is no longer pending: {pending:?}"
    );
    assert!(
        pending[0].1 >= 2,
        "the recovered message was not delivered again: {pending:?}"
    );
    assert!(
        dead_letters(&mut connection).await.is_empty(),
        "a transient failure was dead-lettered: {:?}",
        dead_letters(&mut connection).await
    );
    assert_eq!(
        persisted(&db.pool, &["transient-reclaim".to_string()]).await,
        0
    );

    shutdown.cancel();
    let _ = recovery.await;
}

/// A message that keeps failing is dead-lettered once it is past its delivery
/// budget, with the reason recorded, instead of being redelivered forever.
#[tokio::test]
async fn a_message_past_its_delivery_budget_is_dead_lettered() {
    let fake = common::start_fake_upstream().await;

    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let mut connection = connect(&redis.url).await;
    create_group(&mut connection).await;
    publish(&mut connection, "solana", "over-budget", 1).await;
    pre_deliver(&mut connection, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // One delivery is all a message gets; the reclaim already is the second one.
    let config = test_config(UNUSED_DB_URL, &redis.url, &fake.base_url, 1, 60);
    let state = live_state(&db.pool, config).await;

    let shutdown = CancellationToken::new();
    let recovery = spawn_consumer(&redis.url, state, "recovery-worker", shutdown.clone(), true);

    let deadline = Instant::now() + SETTLE;
    while dead_letters(&mut connection).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the over-budget message was never dead-lettered"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let letters = dead_letters(&mut connection).await;
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].0, "over-budget");
    assert!(
        letters[0].1.contains("exceeded max deliveries (2)"),
        "unexpected dead-letter reason: {}",
        letters[0].1
    );
    assert!(
        pending(&mut connection).await.is_empty(),
        "a dead-lettered message is still pending"
    );

    shutdown.cancel();
    let _ = recovery.await;
}

/// A failure that cannot fix itself is dead-lettered on its first delivery rather
/// than burning the message's whole delivery budget.
#[tokio::test]
async fn a_permanent_failure_is_dead_lettered_immediately() {
    let fake = common::start_fake_upstream().await;

    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let mut connection = connect(&redis.url).await;
    create_group(&mut connection).await;
    // No adapter handles this chain, so analysis fails deterministically.
    publish(&mut connection, "ethereum", "permanent-failure", 1).await;

    let config = test_config(UNUSED_DB_URL, &redis.url, &fake.base_url, 10, 60);
    let state = live_state(&db.pool, config).await;

    let shutdown = CancellationToken::new();
    let consumer = spawn_consumer(
        &redis.url,
        state,
        "permanent-worker",
        shutdown.clone(),
        false,
    );

    let deadline = Instant::now() + SETTLE;
    while dead_letters(&mut connection).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "the permanent failure was never dead-lettered"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let letters = dead_letters(&mut connection).await;
    assert_eq!(letters.len(), 1);
    assert!(
        letters[0].1.contains("unsupported chain: ethereum"),
        "unexpected dead-letter reason: {}",
        letters[0].1
    );
    assert!(
        pending(&mut connection).await.is_empty(),
        "a dead-lettered message is still pending"
    );

    shutdown.cancel();
    let _ = consumer.await;
}
