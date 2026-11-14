//! One subscription and one checkpoint per configured program. A program whose
//! subscription is rejected or whose connection dies must never stop the others, so
//! the tests drive the real ingester against a fake Solana WebSocket server.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bridge::config::IngestConfig;
use bridge::db::repository::{get_last_ingested_slot, ingest_checkpoint_subject};
use bridge::ingestion::solana::SolanaIngester;
use bridge::redis::producer::RedisProducer;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const PROGRAM_A: &str = "wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb";
const PROGRAM_B: &str = "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth";
/// The NFT Bridge program, a second Wormhole program with its own subscription.
const NFT_PROGRAM: &str = "WnFt12ZrnzZrFZkt2xsNsaNWoQribnuQ5B5FrDbwDhD";

struct FakeSolana {
    url: String,
    subscriptions: Arc<Mutex<Vec<String>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for FakeSolana {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// A WebSocket server that answers `logsSubscribe` for any program. Programs in
/// `reject` get a JSON-RPC error instead of a confirmation; every other program is
/// confirmed and, when `slots` holds an entry for it, receives one `logsNotification`
/// at that slot. Each connection is served on its own task, so one failing program
/// cannot block another's.
async fn fake_solana(reject: &[&str], slots: &[(&str, u64)]) -> FakeSolana {
    let reject: HashSet<String> = reject.iter().map(|p| p.to_string()).collect();
    let slots: HashMap<String, u64> = slots
        .iter()
        .map(|(program, slot)| (program.to_string(), *slot))
        .collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    let subscriptions = Arc::new(Mutex::new(Vec::new()));
    let recorded = subscriptions.clone();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let recorded = recorded.clone();
            let reject = reject.clone();
            let slots = slots.clone();

            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };

                // The ingester's only request is the `logsSubscribe`.
                let Some(Ok(Message::Text(text))) = ws.next().await else {
                    return;
                };
                let request: Value = serde_json::from_str(&text).expect("subscribe JSON");
                let id = request["id"].clone();
                let program = request["params"][0]["mentions"][0]
                    .as_str()
                    .expect("mentioned program")
                    .to_string();
                recorded.lock().unwrap().push(program.clone());

                if reject.contains(&program) {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "jsonrpc": "2.0",
                                "error": {"code": -32602, "message": "Invalid params"},
                                "id": id
                            })
                            .to_string(),
                        ))
                        .await;
                    return;
                }

                let _ = ws
                    .send(Message::Text(
                        json!({"jsonrpc": "2.0", "result": 1, "id": id}).to_string(),
                    ))
                    .await;

                if let Some(slot) = slots.get(&program) {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "jsonrpc": "2.0",
                                "method": "logsNotification",
                                "params": {
                                    "subscription": 1,
                                    "result": {
                                        "context": {"slot": slot},
                                        "value": {"signature": signature(&program), "err": null}
                                    }
                                }
                            })
                            .to_string(),
                        ))
                        .await;
                }

                // Hold the subscription open and answer keepalive pings until the
                // ingester closes the connection.
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Ping(payload) = message {
                        let _ = ws.send(Message::Pong(payload)).await;
                    }
                }
            });
        }
    });

    FakeSolana {
        url: format!("ws://{addr}"),
        subscriptions,
        handle,
    }
}

fn signature(program: &str) -> String {
    format!("sig-{program}")
}

fn ingest_config() -> IngestConfig {
    IngestConfig {
        enabled: true,
        // The fake server has no HTTP RPC, and this test is about the live path.
        backfill: false,
        backfill_max: 0,
        backfill_pace_ms: 0,
        rewind_slots: 0,
        dedupe_ttl_secs: 60,
        ping_secs: 30,
        // The tests want every program connected up front.
        connect_stagger_ms: 0,
        max_ws_connections: None,
        watchdog_secs: 90,
        poll_secs: 60,
        poll_overlap_secs: 0,
        poll_offset_secs: 0,
        poll_max_publish: 0,
    }
}

/// Waits for the per-program checkpoint to reach `expected`, so the assertions do not
/// race the WebSocket handlers.
async fn wait_for_slot(pool: &PgPool, program: &str, expected: u64) {
    for _ in 0..100 {
        let slot = get_last_ingested_slot(pool, &ingest_checkpoint_subject(program))
            .await
            .expect("read checkpoint");
        if slot == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let slot = get_last_ingested_slot(pool, &ingest_checkpoint_subject(program))
        .await
        .expect("read checkpoint");
    panic!("{program} checkpoint stayed at {slot}, expected {expected}");
}

async fn start_ingester(
    fake: &FakeSolana,
    db: &common::TestDb,
    redis_url: &str,
    programs: &[&str],
) -> (
    CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let producer = RedisProducer::new(redis_url).await.expect("redis producer");
    let ingester = SolanaIngester::new(
        fake.url.clone(),
        programs.iter().map(|program| program.to_string()).collect(),
        producer,
        db.pool.clone(),
        reqwest::Client::new(),
        // Backfill is off, so this URL is never called.
        "http://127.0.0.1:1".to_string(),
        ingest_config(),
    );
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    let task = tokio::spawn(async move { ingester.run(task_stop).await });

    (stop, task)
}

async fn stop_ingester(stop: CancellationToken, task: tokio::task::JoinHandle<anyhow::Result<()>>) {
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("ingester stops after cancellation")
        .expect("ingester task did not panic")
        .expect("ingester run returned an error");
}

#[tokio::test]
async fn each_program_is_subscribed_and_checkpoints_independently() {
    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let fake = fake_solana(&[], &[(PROGRAM_A, 111), (PROGRAM_B, 222)]).await;

    let (stop, task) = start_ingester(&fake, &db, &redis.url, &[PROGRAM_A, PROGRAM_B]).await;

    wait_for_slot(&db.pool, PROGRAM_A, 111).await;
    wait_for_slot(&db.pool, PROGRAM_B, 222).await;

    // One subscription each, never one shared subscription.
    let subscriptions = fake.subscriptions.lock().unwrap().clone();
    assert!(
        subscriptions.contains(&PROGRAM_A.to_string()),
        "program A was not subscribed: {subscriptions:?}"
    );
    assert!(
        subscriptions.contains(&PROGRAM_B.to_string()),
        "program B was not subscribed: {subscriptions:?}"
    );

    // Each program's own transaction reached the store: the checkpoint is written
    // only after the notification was published, and the dedupe marker proves it.
    let mut client = redis::Client::open(redis.url.as_str())
        .expect("redis client")
        .get_connection_manager()
        .await
        .expect("redis connection");
    for program in [PROGRAM_A, PROGRAM_B] {
        let seen: i64 = redis::cmd("EXISTS")
            .arg(format!("bridge:seen:solana:{}", signature(program)))
            .query_async(&mut client)
            .await
            .expect("EXISTS");
        assert_eq!(seen, 1, "the notification for {program} was not published");
    }

    stop_ingester(stop, task).await;
}

#[tokio::test]
async fn the_token_bridge_and_nft_bridge_ingest_side_by_side() {
    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let fake = fake_solana(&[], &[(PROGRAM_A, 444), (NFT_PROGRAM, 555)]).await;

    let (stop, task) = start_ingester(&fake, &db, &redis.url, &[PROGRAM_A, NFT_PROGRAM]).await;

    wait_for_slot(&db.pool, PROGRAM_A, 444).await;
    wait_for_slot(&db.pool, NFT_PROGRAM, 555).await;

    let subscriptions = fake.subscriptions.lock().unwrap().clone();
    assert!(
        subscriptions.contains(&PROGRAM_A.to_string()),
        "the Token Bridge was not subscribed: {subscriptions:?}"
    );
    assert!(
        subscriptions.contains(&NFT_PROGRAM.to_string()),
        "the NFT Bridge was not subscribed: {subscriptions:?}"
    );

    let mut client = redis::Client::open(redis.url.as_str())
        .expect("redis client")
        .get_connection_manager()
        .await
        .expect("redis connection");
    for program in [PROGRAM_A, NFT_PROGRAM] {
        let seen: i64 = redis::cmd("EXISTS")
            .arg(format!("bridge:seen:solana:{}", signature(program)))
            .query_async(&mut client)
            .await
            .expect("EXISTS");
        assert_eq!(seen, 1, "the notification for {program} was not published");
    }

    stop_ingester(stop, task).await;
}

#[tokio::test]
async fn a_rejected_program_does_not_stop_the_other() {
    let db = common::test_db().await;
    let redis = common::test_redis().await;
    // B's subscription is rejected; A must keep ingesting on its own task.
    let fake = fake_solana(&[PROGRAM_B], &[(PROGRAM_A, 333)]).await;

    let (stop, task) = start_ingester(&fake, &db, &redis.url, &[PROGRAM_A, PROGRAM_B]).await;

    wait_for_slot(&db.pool, PROGRAM_A, 333).await;

    assert_eq!(
        get_last_ingested_slot(&db.pool, &ingest_checkpoint_subject(PROGRAM_B))
            .await
            .expect("read checkpoint"),
        0,
        "the rejected program must not advance a checkpoint"
    );

    let subscriptions = fake.subscriptions.lock().unwrap().clone();
    assert!(
        subscriptions.contains(&PROGRAM_A.to_string()),
        "program A was not subscribed: {subscriptions:?}"
    );
    assert!(
        subscriptions.contains(&PROGRAM_B.to_string()),
        "program B subscription was never attempted: {subscriptions:?}"
    );

    stop_ingester(stop, task).await;
}
