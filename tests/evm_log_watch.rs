//! EVM log watching: fixture decoding, the watcher's cursor/finality/range behaviour,
//! and the joins that fill a Wormhole transfer's tx hashes without WormholeScan.
//!
//! Everything runs against a loopback JSON-RPC stub and a throwaway Postgres. No test
//! calls a live provider.

mod common;

use axum::{
    Json, Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use bridge::chains::ChainRegistry;
use bridge::clients::limiter::Limiter;
use bridge::domain::analysis::DecodedTransfer;
use bridge::domain::bridge_transfer::{BridgeStatus, ChainId};
use bridge::evm::client::EvmClient;
use bridge::evm::db as evm_db;
use bridge::evm::decode::{self, EventKind};
use bridge::evm::lookup::EvmEventLookup;
use bridge::evm::watcher::EvmWatcher;
use bridge::protocols::amount::RawAmount;
use bridge::protocols::{DetectedTransfer, Direction, MessageId, TokenRef};
use bridge::services::detect;
use governor::{Quota, RateLimiter};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::num::NonZeroU32;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const CORE: &str = "0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B";
const TOKEN: &str = "0x3ee18B2214AFF97000D974cf647E7C347E8fa585";
const ETH_TOKEN_EMITTER: &str = "0000000000000000000000003ee18b2214aff97000d974cf647e7c347e8fa585";
const SEND_EMITTER: &str = "abababababababababababababababababababababababababababababababab";
const SEND_TX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const REDEEM_TX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SEQUENCE: u64 = 1234;
const CURSOR_BLOCK: u64 = 17_999_999;
const TARGET_BLOCK: u64 = 18_000_001;

type EventDump = (
    String,
    i32,
    String,
    String,
    Option<chrono::DateTime<chrono::Utc>>,
);
type RowDump = (
    String,
    String,
    Option<i16>,
    Option<i16>,
    Option<String>,
    Option<String>,
);

fn fixture(name: &str) -> Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json")
}

fn hash_for(block: u64) -> String {
    format!("0x{block:064x}")
}

fn expected_wormhole_id() -> u16 {
    2
}

fn limiter() -> Arc<Limiter> {
    Arc::new(RateLimiter::direct(Quota::per_second(
        NonZeroU32::new(1_000).unwrap(),
    )))
}

/// A loopback JSON-RPC node. It serves a finalized head, canonical block hashes and the
/// two captured logs, and records the block span of every `eth_getLogs` request.
#[derive(Default)]
struct StubState {
    logs: Vec<Value>,
    spans: Mutex<Vec<u64>>,
    /// Finalized head. `None` uses [`TARGET_BLOCK`].
    target_block: Option<u64>,
    /// Reject `eth_getLogs` windows wider than this as range-too-large.
    max_ok_range: Option<u64>,
    /// Answer the first `eth_getLogs` request with HTTP 429.
    fail_first_429: AtomicBool,
}

impl StubState {
    fn target(&self) -> u64 {
        self.target_block.unwrap_or(TARGET_BLOCK)
    }
}

async fn rpc(State(state): State<Arc<StubState>>, Json(body): Json<Value>) -> Response {
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = body
        .get("params")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let target = state.target();

    match method {
        "eth_blockNumber" => Json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": format!("0x{:x}", target + 64)
        }))
        .into_response(),
        "eth_getBlockByNumber" => {
            let tag = params.first().and_then(Value::as_str).unwrap_or_default();
            let block = if tag == "finalized" {
                target
            } else {
                u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap_or_default()
            };
            Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {
                    "number": format!("0x{block:x}"),
                    "hash": hash_for(block),
                    "timestamp": format!("0x{:x}", 1_700_000_000 + block)
                }
            }))
            .into_response()
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            let from = filter
                .get("fromBlock")
                .and_then(Value::as_str)
                .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
                .unwrap_or_default();
            let to = filter
                .get("toBlock")
                .and_then(Value::as_str)
                .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
                .unwrap_or_default();
            let span = to.saturating_sub(from) + 1;
            state.spans.lock().unwrap().push(span);

            if state.fail_first_429.swap(false, Ordering::SeqCst) {
                return (axum::http::StatusCode::TOO_MANY_REQUESTS, "slow down").into_response();
            }
            if state.max_ok_range.is_some_and(|max| span > max) {
                return Json(json!({
                    "jsonrpc": "2.0", "id": 1,
                    "error": {
                        "code": -32602,
                        "message": "invalid argument 0: eth_getLogs is limited to a 2,000 block range"
                    }
                }))
                .into_response();
            }

            let matching: Vec<Value> = state
                .logs
                .iter()
                .filter(|log| {
                    log.get("blockNumber")
                        .and_then(Value::as_str)
                        .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
                        .is_some_and(|block| block >= from && block <= to)
                })
                .cloned()
                .collect();
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": matching})).into_response()
        }
        _ => Json(json!({
            "jsonrpc": "2.0", "id": 1,
            "error": {"code": -32601, "message": "method not found"}
        }))
        .into_response(),
    }
}

async fn stub(state: Arc<StubState>) -> String {
    let app = Router::new().route("/", post(rpc)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("stub address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve stub");
    });
    format!("http://{addr}")
}

fn captured_logs() -> Vec<Value> {
    let mut logs = fixture("evm_ethereum_log_message_published.json")["result"]
        .as_array()
        .cloned()
        .expect("send logs");
    logs.extend(
        fixture("evm_ethereum_transfer_redeemed.json")["result"]
            .as_array()
            .cloned()
            .expect("redeem logs"),
    );
    logs
}

#[tokio::test]
async fn captured_fixtures_decode_to_the_message_identity() {
    let state = Arc::new(StubState {
        logs: captured_logs(),
        ..Default::default()
    });
    let url = stub(state).await;
    let client = EvmClient::new(reqwest::Client::new(), url, limiter());

    let logs = client
        .logs(
            18_000_000,
            18_000_001,
            &[CORE, TOKEN],
            &[
                decode::LOG_MESSAGE_PUBLISHED_TOPIC,
                decode::TRANSFER_REDEEMED_TOPIC,
            ],
        )
        .await
        .expect("logs");
    assert_eq!(logs.len(), 2);

    let send =
        decode::decode_log(&logs[0], expected_wormhole_id(), CORE, TOKEN).expect("send decodes");
    assert_eq!(send.event_kind, EventKind::LogMessagePublished);
    assert_eq!(send.emitter_chain, 2);
    assert_eq!(send.emitter_address, ETH_TOKEN_EMITTER);
    assert_eq!(send.sequence, SEQUENCE);

    let redeem =
        decode::decode_log(&logs[1], expected_wormhole_id(), CORE, TOKEN).expect("redeem decodes");
    assert_eq!(redeem.event_kind, EventKind::TransferRedeemed);
    assert_eq!(redeem.emitter_chain, 1);
    assert_eq!(redeem.emitter_address, SEND_EMITTER);
    assert_eq!(redeem.sequence, SEQUENCE);
}

fn real_logs() -> Vec<Value> {
    fixture("evm_ethereum_real_1439615.json")["result"]
        .as_array()
        .cloned()
        .expect("real logs")
}

/// Mainnet capture, blocks 26385278..=26385287. The Token Bridge redeem for
/// sequence 1439615 (tx 0xb5c9306b...f861f9) must decode, and every field is
/// checked against the raw topics rather than hardcoded values.
#[tokio::test]
async fn real_mainnet_redeem_decodes() {
    const REDEEM_TOPIC: &str = "0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169";

    let all = real_logs();
    assert_eq!(all.len(), 9, "capture should hold 8 sends and 1 redeem");

    let redeem_raw: Vec<Value> = all
        .iter()
        .filter(|l| l["topics"][0].as_str() == Some(REDEEM_TOPIC))
        .cloned()
        .collect();
    assert_eq!(redeem_raw.len(), 1);

    let topics = redeem_raw[0]["topics"].as_array().expect("topics");
    let t1 = topics[1].as_str().expect("emitter chain topic");
    let t2 = topics[2].as_str().expect("emitter address topic");
    let want_chain = u16::from_str_radix(&t1[t1.len() - 4..], 16).expect("chain");
    let want_emitter = t2.trim_start_matches("0x").to_lowercase();

    let state = Arc::new(StubState {
        logs: redeem_raw,
        ..Default::default()
    });
    let url = stub(state).await;
    let client = EvmClient::new(reqwest::Client::new(), url, limiter());
    let logs = client
        .logs(
            26_385_278,
            26_385_287,
            &[CORE, TOKEN],
            &[
                decode::LOG_MESSAGE_PUBLISHED_TOPIC,
                decode::TRANSFER_REDEEMED_TOPIC,
            ],
        )
        .await
        .expect("logs");
    assert_eq!(logs.len(), 1);

    let redeem =
        decode::decode_log(&logs[0], expected_wormhole_id(), CORE, TOKEN).expect("redeem decodes");
    assert_eq!(redeem.event_kind, EventKind::TransferRedeemed);
    assert_eq!(redeem.sequence, 1_439_615);
    assert_eq!(redeem.emitter_chain, want_chain);
    assert_eq!(redeem.emitter_address, want_emitter);
}

fn decoded(
    direction: Direction,
    emitter_chain: u16,
    emitter_address: &str,
    observed_tx: &str,
    source: u16,
    destination: u16,
    registry: &ChainRegistry,
) -> DecodedTransfer {
    let detected = DetectedTransfer {
        message: MessageId::Wormhole {
            emitter_chain,
            emitter_address: emitter_address.to_string(),
            sequence: SEQUENCE,
        },
        direction,
        observed_tx: observed_tx.to_string(),
        event_index: 0,
        source: bridge::chains::RawChainRef::wormhole(source),
        destination: bridge::chains::RawChainRef::wormhole(destination),
        source_wallet: Some("Wallet".to_string()),
        destination_wallet: None,
        token: Some(TokenRef {
            chain: bridge::chains::RawChainRef::wormhole(1),
            address: "TokenMint".to_string(),
        }),
        amount_raw: RawAmount::parse("1000000"),
        via: None,
    };
    let bridge =
        detect::to_bridge_transfer(&detected, None, &registry.snapshot()).expect("transfer");
    DecodedTransfer { detected, bridge }
}

async fn ethereum_chain_id(pool: &PgPool) -> i16 {
    sqlx::query_scalar("SELECT id FROM chains WHERE slug = 'ethereum'")
        .fetch_one(pool)
        .await
        .expect("ethereum chain")
}

async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn watcher(url: String, pool: PgPool, chain_id: i16, max_block_range: u64) -> EvmWatcher {
    EvmWatcher::new(
        evm_db::WatchTarget {
            chain_id,
            slug: "ethereum".to_string(),
            wormhole_chain_id: expected_wormhole_id(),
            core_bridge_address: CORE.to_string(),
            token_bridge_address: TOKEN.to_string(),
            finality_depth: 64,
            rpc_url: url,
        },
        reqwest::Client::new(),
        limiter(),
        pool,
        86_400,
        60,
        max_block_range,
    )
}

#[tokio::test]
async fn the_watcher_stores_logs_and_joins_both_sides() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    let registry = ChainRegistry::load(pool).await.expect("chain registry");

    let outbound = decoded(
        Direction::Outbound,
        1,
        SEND_EMITTER,
        "solana-send",
        1,
        2,
        &registry,
    );
    let inbound = decoded(
        Direction::Inbound,
        expected_wormhole_id(),
        ETH_TOKEN_EMITTER,
        "solana-redeem",
        2,
        1,
        &registry,
    );
    let transfers = vec![outbound, inbound];
    let mut tx = common::sample_tx("solana-send", SEQUENCE, ChainId::Ethereum, Some("1000000"));
    tx.bridge_transfer = Some(transfers[0].bridge.clone());
    bridge::db::repository::persist_analysis(pool, &tx, &transfers)
        .await
        .expect("persist transfers");

    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState {
        logs: captured_logs(),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(watcher(url, pool.clone(), chain_id, 10).run(shutdown.clone()));

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let outbound_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM bridge_transfers WHERE message_key = $1")
                .bind(format!("1/{SEND_EMITTER}/{SEQUENCE}"))
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        let inbound_source: Option<String> = sqlx::query_scalar(
            "SELECT source_tx_hash FROM bridge_transfers WHERE message_key = $1",
        )
        .bind(format!("2/{ETH_TOKEN_EMITTER}/{SEQUENCE}"))
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if outbound_status.as_deref() == Some("Completed")
            && inbound_source.as_deref() == Some(SEND_TX)
        {
            break;
        }
        if Instant::now() >= deadline {
            let events: Vec<EventDump> = sqlx::query_as(
                "SELECT event_kind, emitter_chain, emitter_address, sequence::text, applied_at
                     FROM evm_log_events ORDER BY id",
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            let cursor: Option<(i64, String)> = sqlx::query_as(
                "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
            )
            .bind(chain_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
            let rows: Vec<RowDump> = sqlx::query_as(
                "SELECT message_key, status, source_chain_id, destination_chain_id,
                            source_tx_hash, destination_tx_hash
                     FROM bridge_transfers ORDER BY id",
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            panic!(
                "transfers not joined; events={events:?} cursor={cursor:?} rows={rows:?} spans={:?}",
                state.spans.lock().unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let outbound_row: (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT status, destination_tx_hash, destination_explorer_url
         FROM bridge_transfers WHERE message_key = $1",
    )
    .bind(format!("1/{SEND_EMITTER}/{SEQUENCE}"))
    .fetch_one(pool)
    .await
    .expect("outbound row");
    assert_eq!(outbound_row.0, "Completed");
    assert_eq!(outbound_row.1.as_deref(), Some(REDEEM_TX));
    assert_eq!(
        outbound_row.2.as_deref(),
        Some(format!("https://etherscan.io/tx/{REDEEM_TX}").as_str())
    );

    let inbound_row: (String, Option<String>) = sqlx::query_as(
        "SELECT status, source_tx_hash FROM bridge_transfers WHERE message_key = $1",
    )
    .bind(format!("2/{ETH_TOKEN_EMITTER}/{SEQUENCE}"))
    .fetch_one(pool)
    .await
    .expect("inbound row");
    assert_eq!(inbound_row.0, "Completed");
    assert_eq!(inbound_row.1.as_deref(), Some(SEND_TX));

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_log_events")
        .fetch_one(pool)
        .await
        .expect("event count");
    assert_eq!(events, 2);

    let cursor: (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .expect("cursor");
    assert_eq!(cursor.0, TARGET_BLOCK as i64);
    assert_eq!(cursor.1, hash_for(TARGET_BLOCK));

    // The cached analysis is patched too, so a cached read does not look stale.
    let analysis: Option<String> = sqlx::query_scalar(
        "SELECT analysis #>> '{bridge_transfer,destination_tx_hash}'
         FROM bridge_transfers WHERE message_key = $1",
    )
    .bind(format!("1/{SEND_EMITTER}/{SEQUENCE}"))
    .fetch_one(pool)
    .await
    .expect("analysis");
    assert_eq!(analysis.as_deref(), Some(REDEEM_TX));

    // A later plain re-analysis of the send (no watcher data) must not downgrade the
    // row or erase the cached hash the event join wrote.
    let plain = decoded(
        Direction::Outbound,
        1,
        SEND_EMITTER,
        "solana-send",
        1,
        2,
        &registry,
    );
    let mut plain_tx =
        common::sample_tx("solana-send", SEQUENCE, ChainId::Ethereum, Some("1000000"));
    plain_tx.bridge_transfer = Some(plain.bridge.clone());
    bridge::db::repository::persist_analysis(pool, &plain_tx, std::slice::from_ref(&plain))
        .await
        .expect("re-persist");

    let (status, hash, cached): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT status, destination_tx_hash,
                analysis #>> '{bridge_transfer,destination_tx_hash}'
         FROM bridge_transfers WHERE message_key = $1",
    )
    .bind(format!("1/{SEND_EMITTER}/{SEQUENCE}"))
    .fetch_one(pool)
    .await
    .expect("row after re-analysis");
    assert_eq!(status, "Completed");
    assert_eq!(hash.as_deref(), Some(REDEEM_TX));
    assert_eq!(cached.as_deref(), Some(REDEEM_TX));
}

#[tokio::test]
async fn only_a_range_too_large_answer_halves_the_window() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState {
        max_ok_range: Some(4),
        target_block: Some(CURSOR_BLOCK + 30),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(watcher(url, pool.clone(), chain_id, 10).run(shutdown.clone()));
    wait_until("the cursor to reach the head", || {
        let pool = pool.clone();
        async move {
            let block: Option<i64> = sqlx::query_scalar(
                "SELECT block_number FROM evm_watch_cursors WHERE chain_id = $1",
            )
            .bind(chain_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
            block == Some((CURSOR_BLOCK + 30) as i64)
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let spans = state.spans.lock().unwrap().clone();
    assert_eq!(
        spans.first().copied(),
        Some(10),
        "first window is the max range"
    );
    assert!(spans.contains(&5), "10 halved to 5: {spans:?}");
    assert!(spans.contains(&2), "5 halved to 2: {spans:?}");
    assert!(spans.iter().all(|span| *span <= 10));
}

#[tokio::test]
async fn a_429_keeps_the_window_and_retries() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState {
        fail_first_429: AtomicBool::new(true),
        target_block: Some(CURSOR_BLOCK + 30),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(watcher(url, pool.clone(), chain_id, 10).run(shutdown.clone()));

    wait_until("the cursor to reach the head", || {
        let pool = pool.clone();
        async move {
            let block: Option<i64> = sqlx::query_scalar(
                "SELECT block_number FROM evm_watch_cursors WHERE chain_id = $1",
            )
            .bind(chain_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
            block == Some((CURSOR_BLOCK + 30) as i64)
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let spans = state.spans.lock().unwrap().clone();
    assert_eq!(
        spans.first().copied(),
        Some(10),
        "the first request uses the configured range"
    );
    assert_eq!(
        spans.get(1).copied(),
        Some(10),
        "the retry after a 429 keeps the range: {spans:?}"
    );
}

#[tokio::test]
async fn a_reorg_drops_events_and_rewinds_the_cursor() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;

    // The stored cursor hash is not the canonical hash the node now reports.
    evm_db::set_cursor(
        pool,
        chain_id,
        CURSOR_BLOCK,
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    .await
    .expect("cursor");
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[evm_db::NewEvent {
            event_kind: EventKind::LogMessagePublished,
            emitter_chain: 2,
            emitter_address: ETH_TOKEN_EMITTER.to_string(),
            sequence: SEQUENCE,
            tx_hash: SEND_TX.to_string(),
            log_index: 0,
            block_number: CURSOR_BLOCK,
            block_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                .to_string(),
            block_timestamp: None,
        }],
        CURSOR_BLOCK,
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    )
    .await
    .expect("stale event");

    let state = Arc::new(StubState::default());
    let url = stub(state).await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(watcher(url, pool.clone(), chain_id, 10).run(shutdown.clone()));

    wait_until("the cursor to rewind", || {
        let pool = pool.clone();
        async move {
            let block: Option<i64> = sqlx::query_scalar(
                "SELECT block_number FROM evm_watch_cursors WHERE chain_id = $1",
            )
            .bind(chain_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
            block == Some(CURSOR_BLOCK as i64 - 1)
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_log_events")
        .fetch_one(pool)
        .await
        .expect("event count");
    assert_eq!(events, 0, "stale events are dropped");
}

#[tokio::test]
async fn the_transfer_side_lookup_fills_both_directions() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    let registry = ChainRegistry::load(pool).await.expect("chain registry");
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[
            evm_db::NewEvent {
                event_kind: EventKind::LogMessagePublished,
                emitter_chain: 2,
                emitter_address: ETH_TOKEN_EMITTER.to_string(),
                sequence: SEQUENCE,
                tx_hash: SEND_TX.to_string(),
                log_index: 0,
                block_number: 1,
                block_hash: hash_for(1),
                block_timestamp: None,
            },
            evm_db::NewEvent {
                event_kind: EventKind::TransferRedeemed,
                emitter_chain: 1,
                emitter_address: SEND_EMITTER.to_string(),
                sequence: SEQUENCE,
                tx_hash: REDEEM_TX.to_string(),
                log_index: 0,
                block_number: 2,
                block_hash: hash_for(2),
                block_timestamp: None,
            },
        ],
        2,
        &hash_for(2),
    )
    .await
    .expect("events");

    let lookup = EvmEventLookup::new(pool.clone(), registry.clone());

    let mut outbound = decoded(
        Direction::Outbound,
        1,
        SEND_EMITTER,
        "solana-send",
        1,
        2,
        &registry,
    )
    .bridge;
    assert!(lookup.apply(&mut outbound).await.expect("lookup"));
    assert_eq!(outbound.destination_tx_hash.as_deref(), Some(REDEEM_TX));
    assert!(matches!(outbound.status, BridgeStatus::Completed));

    let mut inbound = decoded(
        Direction::Inbound,
        expected_wormhole_id(),
        ETH_TOKEN_EMITTER,
        "solana-redeem",
        2,
        1,
        &registry,
    )
    .bridge;
    assert!(lookup.apply(&mut inbound).await.expect("lookup"));
    assert_eq!(inbound.source_tx_hash.as_deref(), Some(SEND_TX));

    // The lookup has no age limit: an event from an old block still answers.
    let found = lookup
        .destination_tx(&outbound.message_id)
        .await
        .expect("destination lookup");
    assert_eq!(found.as_deref(), Some(REDEEM_TX));
}
