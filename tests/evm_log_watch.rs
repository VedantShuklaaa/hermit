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
use bridge::error::AppError;
use bridge::evm::client::EvmClient;
use bridge::evm::db::{self as evm_db, FinalityMode};
use bridge::evm::decode::{self, EventKind};
use bridge::evm::lookup::EvmEventLookup;
use bridge::evm::watcher::{EvmWatcher, WatcherEndpoint};
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
/// The captured fixtures' first block: their two logs sit at 18,000,000 and
/// 18,000,001 (TARGET_BLOCK).
const FORK_ONE_BLOCK: u64 = 18_000_000;

/// Alchemy's current free-tier `eth_getLogs` rejection. It arrives with HTTP 400,
/// which is why the capture script's `curl -f` failed on it.
const ALCHEMY_RANGE_ERROR_MESSAGE: &str =
    "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range";

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

/// The canonical hash after a reorg replaced `block` and every later block.
fn reorged_hash_for(block: u64) -> String {
    format!("0x{:064x}", block | 0x8000_0000_0000_0000)
}

fn expected_wormhole_id() -> u16 {
    2
}

fn limiter() -> Arc<Limiter> {
    Arc::new(RateLimiter::direct(Quota::per_second(
        NonZeroU32::new(1_000).unwrap(),
    )))
}

/// How the stub rejects an over-wide `eth_getLogs` window.
#[derive(Default, Clone, Copy)]
enum RangeError {
    /// HTTP 200 with the older free-tier wording.
    #[default]
    Http200,
    /// HTTP 400 with Alchemy's current free-tier wording.
    AlchemyHttp400,
}

/// A loopback JSON-RPC node. It serves a finalized head, canonical block hashes and the
/// two captured logs, and records the block span of every `eth_getLogs` request.
#[derive(Default)]
struct StubState {
    logs: Vec<Value>,
    spans: Mutex<Vec<u64>>,
    /// Every JSON-RPC method this endpoint was asked for, so a test can see which
    /// endpoint the watcher picked and in what order.
    requests: Mutex<Vec<String>>,
    /// Finalized head. `None` uses [`TARGET_BLOCK`].
    target_block: Option<u64>,
    /// Reject `eth_getLogs` windows wider than this as range-too-large.
    max_ok_range: Option<u64>,
    /// The status and body used for that rejection.
    range_error: RangeError,
    /// Answer the first `eth_getLogs` request with HTTP 429.
    fail_first_429: AtomicBool,
    /// Answer the first `eth_getLogs` request with an unrelated HTTP 400.
    fail_first_unrelated_400: AtomicBool,
    /// Answer every `eth_getLogs` request with HTTP 503.
    fail_get_logs: AtomicBool,
    /// Answer every request with HTTP 503, so the endpoint looks down.
    fail_all: AtomicBool,
    /// Delay every `eth_getLogs` answer, to exercise a client timeout.
    get_logs_delay: Option<Duration>,
    /// Canonical hashes for this block and every later one differ from `hash_for`,
    /// modelling a reorg that replaced that suffix of the chain.
    fork_block: Option<u64>,
    /// The block tag or number of every `eth_getBlockByNumber` request, in order.
    block_tags: Mutex<Vec<String>>,
}

impl StubState {
    fn target(&self) -> u64 {
        self.target_block.unwrap_or(TARGET_BLOCK)
    }

    fn canonical_hash(&self, block: u64) -> String {
        match self.fork_block {
            Some(fork) if block >= fork => reorged_hash_for(block),
            _ => hash_for(block),
        }
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

    state.requests.lock().unwrap().push(method.to_string());
    if state.fail_all.load(Ordering::SeqCst) {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    match method {
        "eth_chainId" => Json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": "0x1"
        }))
        .into_response(),
        "eth_blockNumber" => Json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": format!("0x{:x}", target + 64)
        }))
        .into_response(),
        "eth_getBlockByNumber" => {
            let tag = params.first().and_then(Value::as_str).unwrap_or_default();
            state.block_tags.lock().unwrap().push(tag.to_string());
            let block = if tag == "finalized" {
                target
            } else {
                u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap_or_default()
            };
            Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {
                    "number": format!("0x{block:x}"),
                    "hash": state.canonical_hash(block),
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

            if let Some(delay) = state.get_logs_delay {
                tokio::time::sleep(delay).await;
            }
            if state.fail_first_429.swap(false, Ordering::SeqCst) {
                return (axum::http::StatusCode::TOO_MANY_REQUESTS, "slow down").into_response();
            }
            if state.fail_first_unrelated_400.swap(false, Ordering::SeqCst) {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": {"code": -32602, "message": "invalid argument 0: cannot unmarshal"}
                    })),
                )
                    .into_response();
            }
            if state.max_ok_range.is_some_and(|max| span > max) {
                return match state.range_error {
                    RangeError::Http200 => Json(json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": {
                            "code": -32602,
                            "message": "invalid argument 0: eth_getLogs is limited to a 2,000 block range"
                        }
                    }))
                    .into_response(),
                    RangeError::AlchemyHttp400 => (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(json!({
                            "jsonrpc": "2.0", "id": 1,
                            "error": {
                                "code": -32600,
                                "message": ALCHEMY_RANGE_ERROR_MESSAGE
                            }
                        })),
                    )
                        .into_response(),
                };
            }
            if state.fail_get_logs.load(Ordering::SeqCst) {
                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
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

/// A writer that keeps everything the test's thread logs, so a skip can be asserted.
/// `set_default` is thread-local, so concurrent tests do not share it.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("logs").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn capture_logs() -> (CapturedLogs, tracing::subscriber::DefaultGuard) {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (logs, guard)
}

fn log_text(logs: &CapturedLogs) -> String {
    String::from_utf8(logs.0.lock().expect("logs").clone()).expect("utf-8 logs")
}

/// The four chains enabled together in the resilience tests. `ethereum` is seeded by
/// the migrations; the others get a target here.
const WATCHED_CHAINS: [&str; 4] = ["arbitrum", "avalanche", "base", "ethereum"];

async fn add_watch_targets(pool: &PgPool) {
    for slug in ["base", "arbitrum", "avalanche"] {
        sqlx::query(
            r#"
            INSERT INTO evm_watch_targets (
                chain_id, core_bridge_address, token_bridge_address, finality_depth
            )
            SELECT id, $1, $2, 64 FROM chains WHERE slug = $3
            "#,
        )
        .bind(CORE)
        .bind(TOKEN)
        .bind(slug)
        .execute(pool)
        .await
        .expect("insert watch target");
    }
}

async fn set_finality(pool: &PgPool, slug: &str, mode: &str, depth: i64) {
    sqlx::query(
        "UPDATE evm_watch_targets
         SET finality_mode = $1, finality_depth = $2
         WHERE chain_id = (SELECT id FROM chains WHERE slug = $3)",
    )
    .bind(mode)
    .bind(depth)
    .bind(slug)
    .execute(pool)
    .await
    .expect("set finality");
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

fn endpoint(url: String, max_block_range: u64) -> WatcherEndpoint {
    WatcherEndpoint {
        url,
        limiter: limiter(),
        max_block_range,
    }
}

fn watcher(url: String, pool: PgPool, chain_id: i16, max_block_range: u64) -> EvmWatcher {
    watcher_with_endpoints(vec![endpoint(url, max_block_range)], pool, chain_id)
}

fn watcher_with_endpoints(
    endpoints: Vec<WatcherEndpoint>,
    pool: PgPool,
    chain_id: i16,
) -> EvmWatcher {
    watcher_with_finality(endpoints, pool, chain_id, FinalityMode::Finalized, 64, 60)
}

fn watcher_with_finality(
    endpoints: Vec<WatcherEndpoint>,
    pool: PgPool,
    chain_id: i16,
    finality_mode: FinalityMode,
    finality_depth: u64,
    poll_secs: u64,
) -> EvmWatcher {
    EvmWatcher::new(
        evm_db::WatchTarget {
            chain_id,
            slug: "ethereum".to_string(),
            wormhole_chain_id: expected_wormhole_id(),
            core_bridge_address: CORE.to_string(),
            token_bridge_address: TOKEN.to_string(),
            finality_mode,
            finality_depth,
            evm_chain_id: Some(1),
        },
        reqwest::Client::new(),
        endpoints,
        pool,
        86_400,
        poll_secs,
    )
}

/// A stale event left by a chain that was later reorged away.
fn stale_event(block: u64, tx_hash: &str) -> evm_db::NewEvent {
    evm_db::NewEvent {
        event_kind: EventKind::TransferRedeemed,
        emitter_chain: 1,
        emitter_address: SEND_EMITTER.to_string(),
        sequence: 99_999,
        tx_hash: tx_hash.to_string(),
        log_index: 0,
        block_number: block,
        block_hash: hash_for(block),
        block_timestamp: None,
    }
}

async fn cursor_block(pool: &PgPool, chain_id: i16) -> Option<i64> {
    sqlx::query_scalar("SELECT block_number FROM evm_watch_cursors WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn event_tx_hashes(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT tx_hash FROM evm_log_events ORDER BY block_number, log_index")
        .fetch_all(pool)
        .await
        .unwrap_or_default()
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
async fn an_alchemy_http_400_range_error_halves_the_window() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState {
        max_ok_range: Some(4),
        range_error: RangeError::AlchemyHttp400,
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
    assert!(
        spans.contains(&5),
        "10 halved to 5 despite HTTP 400: {spans:?}"
    );
    assert!(spans.contains(&2), "5 halved to 2: {spans:?}");
    assert!(spans.iter().all(|span| *span <= 10));
}

#[tokio::test]
async fn an_unrelated_http_400_is_not_a_range_error() {
    let state = Arc::new(StubState {
        fail_first_unrelated_400: AtomicBool::new(true),
        ..Default::default()
    });
    let url = stub(state).await;
    let client = EvmClient::new(reqwest::Client::new(), url, limiter());

    let error = client
        .logs(
            1,
            10,
            &[CORE, TOKEN],
            &[
                decode::LOG_MESSAGE_PUBLISHED_TOPIC,
                decode::TRANSFER_REDEEMED_TOPIC,
            ],
        )
        .await
        .expect_err("an unrelated 400 must not become a range error");
    assert!(
        matches!(error, AppError::UpstreamProvider { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_timeout_is_not_a_range_error() {
    let state = Arc::new(StubState {
        get_logs_delay: Some(Duration::from_millis(500)),
        ..Default::default()
    });
    let url = stub(state).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(50))
        .build()
        .expect("http client");
    let client = EvmClient::new(http, url, limiter());

    let error = client
        .logs(
            1,
            10,
            &[CORE, TOKEN],
            &[
                decode::LOG_MESSAGE_PUBLISHED_TOPIC,
                decode::TRANSFER_REDEEMED_TOPIC,
            ],
        )
        .await
        .expect_err("a timeout must not become a range error");
    assert!(matches!(error, AppError::UpstreamTimeout(_)), "{error:?}");
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

/// A dead primary must not stop the chain: the watcher moves to the fallback and
/// the cursor advances from the fallback's logs.
#[tokio::test]
async fn a_failing_primary_fails_over_to_the_fallback() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let primary = Arc::new(StubState {
        fail_all: AtomicBool::new(true),
        ..Default::default()
    });
    let fallback = Arc::new(StubState {
        logs: captured_logs(),
        ..Default::default()
    });
    let primary_url = stub(primary.clone()).await;
    let fallback_url = stub(fallback.clone()).await;

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(
        watcher_with_endpoints(
            vec![endpoint(primary_url, 10), endpoint(fallback_url, 10)],
            pool.clone(),
            chain_id,
        )
        .run(shutdown.clone()),
    );

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
            block == Some(TARGET_BLOCK as i64)
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;

    let primary_requests = primary.requests.lock().unwrap().clone();
    assert!(
        primary_requests
            .iter()
            .any(|method| method == "eth_getBlockByNumber"),
        "the primary is tried first: {primary_requests:?}"
    );
    assert!(
        !fallback.requests.lock().unwrap().is_empty(),
        "the fallback served the chain"
    );

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_log_events")
        .fetch_one(pool)
        .await
        .expect("event count");
    assert_eq!(events, 2);
}

/// A range rejection is a fact about one provider, not the chain: the primary
/// halves its own window, then fails over at the smaller window, and the fallback
/// still asks for its full configured window.
#[tokio::test]
async fn a_range_rejection_shrinks_only_the_endpoint_that_rejected_it() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let rejecting = Arc::new(StubState {
        max_ok_range: Some(5),
        fail_get_logs: AtomicBool::new(true),
        target_block: Some(CURSOR_BLOCK + 30),
        ..Default::default()
    });
    let healthy = Arc::new(StubState {
        logs: captured_logs(),
        target_block: Some(CURSOR_BLOCK + 30),
        ..Default::default()
    });
    let rejecting_url = stub(rejecting.clone()).await;
    let healthy_url = stub(healthy.clone()).await;

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(
        watcher_with_endpoints(
            vec![endpoint(rejecting_url, 10), endpoint(healthy_url, 10)],
            pool.clone(),
            chain_id,
        )
        .run(shutdown.clone()),
    );

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
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;

    let rejected_spans = rejecting.spans.lock().unwrap().clone();
    assert_eq!(
        rejected_spans.first().copied(),
        Some(10),
        "the rejecting endpoint starts at its configured window"
    );
    assert!(
        rejected_spans.contains(&5),
        "its window halves to 5: {rejected_spans:?}"
    );
    assert!(rejected_spans.iter().all(|span| *span <= 10));

    let healthy_spans = healthy.spans.lock().unwrap().clone();
    assert_eq!(
        healthy_spans.first().copied(),
        Some(10),
        "the fallback keeps its own window: {healthy_spans:?}"
    );
    assert!(
        healthy_spans.iter().all(|span| *span <= 10),
        "the fallback's window never grows past its configuration: {healthy_spans:?}"
    );
}

/// Every endpoint down is a failed poll: nothing is stored and the cursor is not
/// moved, so the next poll re-reads the same window.
#[tokio::test]
async fn every_endpoint_down_leaves_the_cursor_alone() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let primary = Arc::new(StubState {
        fail_all: AtomicBool::new(true),
        ..Default::default()
    });
    let fallback = Arc::new(StubState {
        fail_all: AtomicBool::new(true),
        ..Default::default()
    });
    let primary_url = stub(primary.clone()).await;
    let fallback_url = stub(fallback.clone()).await;

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(
        watcher_with_endpoints(
            vec![endpoint(primary_url, 10), endpoint(fallback_url, 10)],
            pool.clone(),
            chain_id,
        )
        .run(shutdown.clone()),
    );

    wait_until("both endpoints to be tried", || {
        let primary = primary.clone();
        let fallback = fallback.clone();
        async move {
            let tried = |state: &StubState| {
                state
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|method| method == "eth_getBlockByNumber")
            };
            tried(&primary) && tried(&fallback)
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;

    let cursor: (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .expect("cursor");
    assert_eq!(cursor.0, CURSOR_BLOCK as i64, "the cursor did not move");
    assert_eq!(cursor.1, hash_for(CURSOR_BLOCK));

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM evm_log_events")
        .fetch_one(pool)
        .await
        .expect("event count");
    assert_eq!(events, 0);
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

/// `finalized` mode keeps asking for the tag, as Ethereum did before E2.
#[tokio::test]
async fn finalized_mode_reads_the_finalized_tag() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState::default());
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(watcher(url, pool.clone(), chain_id, 10).run(shutdown.clone()));

    wait_until("the cursor to reach the finalized target", || {
        let pool = pool.clone();
        async move { cursor_block(&pool, chain_id).await == Some(TARGET_BLOCK as i64) }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let tags = state.block_tags.lock().unwrap().clone();
    assert!(
        tags.iter().any(|tag| tag == "finalized"),
        "finalized mode reads the tag: {tags:?}"
    );
}

/// `depth` mode never asks for the tag and targets `head - finality_depth`.
#[tokio::test]
async fn depth_mode_never_reads_the_finalized_tag() {
    const DEPTH: u64 = 16;

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    evm_db::set_cursor(pool, chain_id, CURSOR_BLOCK, &hash_for(CURSOR_BLOCK))
        .await
        .expect("cursor");

    let state = Arc::new(StubState::default());
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 10)],
        pool.clone(),
        chain_id,
        FinalityMode::Depth,
        DEPTH,
        60,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    let expected = TARGET_BLOCK + 64 - DEPTH;
    wait_until("the cursor to reach head - finality_depth", || {
        let pool = pool.clone();
        async move { cursor_block(&pool, chain_id).await == Some(expected as i64) }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let tags = state.block_tags.lock().unwrap().clone();
    assert!(
        tags.iter().all(|tag| tag != "finalized"),
        "depth mode never reads the finalized tag: {tags:?}"
    );
    assert!(
        tags.iter().all(|tag| tag.starts_with("0x")),
        "depth mode reads only numeric block numbers: {tags:?}"
    );
}

/// A reorg two blocks deep: both stored blocks are stale, so both must be deleted
/// and the cursor must land below the fork before the new chain is read.
#[tokio::test]
async fn a_two_block_reorg_drops_both_stale_blocks_and_reingests_the_new_chain() {
    const FORK: u64 = 18_000_000;
    const STALE_BELOW_FORK: &str = "stale-below-fork";
    const STALE_AT_CURSOR: &str = "stale-at-cursor";

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;

    // The old chain: events at the fork block and at the cursor, whose hash differs
    // from the node's current canonical hash at both heights.
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[
            stale_event(FORK, STALE_BELOW_FORK),
            stale_event(TARGET_BLOCK, STALE_AT_CURSOR),
        ],
        TARGET_BLOCK,
        &hash_for(TARGET_BLOCK),
    )
    .await
    .expect("old-chain events");

    let state = Arc::new(StubState {
        logs: captured_logs(),
        target_block: Some(TARGET_BLOCK + 1),
        fork_block: Some(FORK),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 10)],
        pool.clone(),
        chain_id,
        FinalityMode::Finalized,
        64,
        1,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    wait_until("the reorged range to re-ingest", || {
        let pool = pool.clone();
        async move {
            let hashes = event_tx_hashes(&pool).await;
            cursor_block(&pool, chain_id).await == Some((TARGET_BLOCK + 1) as i64)
                && hashes.contains(&SEND_TX.to_string())
                && hashes.contains(&REDEEM_TX.to_string())
                && !hashes.contains(&STALE_BELOW_FORK.to_string())
                && !hashes.contains(&STALE_AT_CURSOR.to_string())
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let events: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT block_number, tx_hash, block_hash FROM evm_log_events
         ORDER BY block_number, log_index",
    )
    .fetch_all(pool)
    .await
    .expect("events");
    assert_eq!(events.len(), 2, "{events:?}");
    // The events came from the fixtures' captured logs, so their block hashes are
    // the fixture hashes: the re-read is the new chain, not the stored stale rows.
    let fixture = captured_logs();
    assert_eq!(events[0].0, FORK as i64);
    assert_eq!(events[0].1, SEND_TX);
    assert_eq!(events[0].2, fixture[0]["blockHash"].as_str().unwrap());
    assert_eq!(events[1].0, TARGET_BLOCK as i64);
    assert_eq!(events[1].1, REDEEM_TX);
    assert_eq!(events[1].2, fixture[1]["blockHash"].as_str().unwrap());
}

/// A one-block reorg deletes only the stale cursor block; the matching block below
/// it stays and its event is not re-read.
#[tokio::test]
async fn a_one_block_reorg_still_rewinds_one_block() {
    const STALE_KEPT: &str = "stale-kept";
    const STALE_CURSOR: &str = "stale-cursor";

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;

    // The block below the cursor matches the chain; only the cursor block was
    // replaced.
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[
            stale_event(FORK_ONE_BLOCK, STALE_KEPT),
            stale_event(TARGET_BLOCK, STALE_CURSOR),
        ],
        TARGET_BLOCK,
        &hash_for(TARGET_BLOCK),
    )
    .await
    .expect("old-chain events");

    let state = Arc::new(StubState {
        logs: captured_logs(),
        target_block: Some(TARGET_BLOCK + 1),
        fork_block: Some(TARGET_BLOCK),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 10)],
        pool.clone(),
        chain_id,
        FinalityMode::Finalized,
        64,
        1,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    wait_until("the replaced cursor block to be re-read", || {
        let pool = pool.clone();
        async move {
            let hashes = event_tx_hashes(&pool).await;
            cursor_block(&pool, chain_id).await == Some((TARGET_BLOCK + 1) as i64)
                && hashes.contains(&REDEEM_TX.to_string())
                && hashes.contains(&STALE_KEPT.to_string())
                && !hashes.contains(&STALE_CURSOR.to_string())
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, tx_hash FROM evm_log_events ORDER BY block_number, log_index",
    )
    .fetch_all(pool)
    .await
    .expect("events");
    assert_eq!(
        events,
        vec![
            (FORK_ONE_BLOCK as i64, STALE_KEPT.to_string()),
            (TARGET_BLOCK as i64, REDEEM_TX.to_string())
        ]
    );
}

/// A reorg with no stored hashes between the real fork and the lowest stale event
/// block: the walk must land on the last matching checkpoint, so the canonical events
/// that sit in the unhashed gap are re-read instead of being skipped forever.
#[tokio::test]
async fn a_reorg_recovers_canonical_events_from_blocks_with_no_stored_hash() {
    const FORK: u64 = 18_000_000;
    const MATCHING: u64 = 17_999_990;
    const LOWEST_STALE: u64 = 18_000_010;
    const CURSOR: u64 = 18_000_020;
    const TARGET: u64 = 18_000_021;
    const STALE_LOW: &str = "stale-lowest";
    const STALE_CURSOR: &str = "stale-cursor";
    const KEPT: &str = "kept-before-fork";

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;

    // The old chain: an event before the fork (still canonical), the lowest stale
    // event block, and one at the cursor. Nothing is stored between the fork and
    // `LOWEST_STALE`, so that range has no stored hashes at all.
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[
            stale_event(MATCHING, KEPT),
            stale_event(LOWEST_STALE, STALE_LOW),
            stale_event(CURSOR, STALE_CURSOR),
        ],
        CURSOR,
        &hash_for(CURSOR),
    )
    .await
    .expect("old-chain events");

    let state = Arc::new(StubState {
        logs: captured_logs(),
        target_block: Some(TARGET),
        fork_block: Some(FORK),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 100)],
        pool.clone(),
        chain_id,
        FinalityMode::Finalized,
        64,
        1,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    wait_until("the unhashed gap to be re-read", || {
        let pool = pool.clone();
        async move {
            let hashes = event_tx_hashes(&pool).await;
            cursor_block(&pool, chain_id).await == Some(TARGET as i64)
                && hashes.contains(&KEPT.to_string())
                && hashes.contains(&SEND_TX.to_string())
                && hashes.contains(&REDEEM_TX.to_string())
                && !hashes.contains(&STALE_LOW.to_string())
                && !hashes.contains(&STALE_CURSOR.to_string())
        }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    // The canonical fixtures at FORK and FORK+1 live inside the gap, so old code
    // that rewound only to `LOWEST_STALE` would have missed them.
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, tx_hash FROM evm_log_events ORDER BY block_number, log_index",
    )
    .fetch_all(pool)
    .await
    .expect("events");
    assert_eq!(
        events,
        vec![
            (MATCHING as i64, KEPT.to_string()),
            (FORK as i64, SEND_TX.to_string()),
            ((FORK + 1) as i64, REDEEM_TX.to_string()),
        ]
    );

    // Re-reading the region must not duplicate an event already stored.
    let duplicates: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) - COUNT(DISTINCT (chain_id, tx_hash, log_index)) FROM evm_log_events",
    )
    .fetch_one(pool)
    .await
    .expect("duplicate count");
    assert_eq!(duplicates, 0);
}

/// A rewind never goes deeper than the finality bound: with no matching checkpoint
/// the cursor stops at the floor, not below it.
#[tokio::test]
async fn a_rewind_stops_at_the_finality_bound() {
    const DEPTH: u64 = 16;
    const STALE: u64 = CURSOR_BLOCK - 2;

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    let floor = CURSOR_BLOCK - DEPTH;

    // The only stored checkpoint is inside the window and stale; there is no
    // matching checkpoint to land on.
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[stale_event(STALE, "stale-in-window")],
        CURSOR_BLOCK,
        &hash_for(CURSOR_BLOCK),
    )
    .await
    .expect("old-chain events");

    let state = Arc::new(StubState {
        fork_block: Some(STALE),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 10)],
        pool.clone(),
        chain_id,
        FinalityMode::Depth,
        DEPTH,
        60,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    wait_until("the rewind to stop at the window floor", || {
        let pool = pool.clone();
        async move { cursor_block(&pool, chain_id).await == Some((floor - 1) as i64) }
    })
    .await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let cursor: (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .expect("cursor");
    assert_eq!(cursor.0, (floor - 1) as i64);
    assert_eq!(cursor.1, hash_for(floor - 1));

    // Events at and above the floor are gone; the re-read starts at the floor.
    let events: Vec<(i64, String)> =
        sqlx::query_as("SELECT block_number, tx_hash FROM evm_log_events ORDER BY block_number")
            .fetch_all(pool)
            .await
            .expect("events");
    assert!(events.is_empty(), "{events:?}");
}

/// A reorg deeper than the bound cannot be resolved with the stored checkpoints:
/// the poll must fail without deleting events or moving the cursor, so canonical
/// events below the floor are never silently skipped.
#[tokio::test]
async fn a_reorg_deeper_than_the_bound_leaves_the_cursor_and_events_alone() {
    const DEPTH: u64 = 16;
    const STALE_FLOOR_HASH: &str =
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;
    let floor = CURSOR_BLOCK - DEPTH;

    let mut checkpoint = stale_event(floor, "stale-at-floor");
    checkpoint.block_hash = STALE_FLOOR_HASH.to_string();
    evm_db::insert_events_and_advance(
        pool,
        chain_id,
        &[checkpoint],
        CURSOR_BLOCK,
        &hash_for(CURSOR_BLOCK),
    )
    .await
    .expect("old-chain events");

    // The fork is at the window floor, so the stored checkpoint at the floor no
    // longer matches and the fork may reach below the bound.
    let state = Arc::new(StubState {
        fork_block: Some(floor),
        ..Default::default()
    });
    let url = stub(state.clone()).await;
    let shutdown = CancellationToken::new();
    let watcher = watcher_with_finality(
        vec![endpoint(url, 10)],
        pool.clone(),
        chain_id,
        FinalityMode::Depth,
        DEPTH,
        1,
    );
    let handle = tokio::spawn(watcher.run(shutdown.clone()));

    wait_until("the walk to inspect the floor checkpoint", || {
        let state = state.clone();
        async move {
            state
                .block_tags
                .lock()
                .unwrap()
                .iter()
                .any(|tag| tag == &format!("0x{floor:x}"))
        }
    })
    .await;
    // Give any wrongly-accepted rewind time to delete events and move the cursor.
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let cursor: (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .expect("cursor");
    assert_eq!(cursor.0, CURSOR_BLOCK as i64, "the cursor did not move");
    assert_eq!(cursor.1, hash_for(CURSOR_BLOCK));

    let events: Vec<(i64, String)> =
        sqlx::query_as("SELECT block_number, tx_hash FROM evm_log_events ORDER BY block_number")
            .fetch_all(pool)
            .await
            .expect("events");
    assert_eq!(
        events,
        vec![(floor as i64, "stale-at-floor".to_string())],
        "no event was deleted"
    );
    assert!(
        state.spans.lock().unwrap().is_empty(),
        "an unresolved reorg must not read logs past it"
    );
}

/// A `depth` target with no depth has no usable finality target, so loading the
/// targets (watcher startup) skips it instead of silently reading the head. The skip
/// must not fail the whole load, even when it is the only row.
#[tokio::test]
async fn a_depth_target_without_a_positive_depth_is_skipped_at_startup() {
    let db = common::test_db().await;
    let pool = &db.pool;
    let chain_id = ethereum_chain_id(pool).await;

    sqlx::query(
        "UPDATE evm_watch_targets SET finality_mode = 'depth', finality_depth = 12
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .execute(pool)
    .await
    .expect("valid depth target");

    let targets = evm_db::load_targets(pool)
        .await
        .expect("valid targets load");
    let target = targets
        .iter()
        .find(|target| target.chain_id == chain_id)
        .expect("ethereum target");
    assert_eq!(target.finality_mode, FinalityMode::Depth);
    assert_eq!(target.finality_depth, 12);

    sqlx::query("UPDATE evm_watch_targets SET finality_depth = 0 WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await
        .expect("invalid depth target");

    let targets = evm_db::load_targets(pool)
        .await
        .expect("one bad row must not fail the load");
    assert!(
        targets.iter().all(|target| target.chain_id != chain_id),
        "the depth-0 target must be skipped"
    );
}

/// Four targets, one malformed: the other three still load, and the skipped row is
/// logged with its chain and the reason.
#[tokio::test]
async fn one_malformed_target_is_skipped_and_the_rest_still_load() {
    let db = common::test_db().await;
    let pool = &db.pool;
    add_watch_targets(pool).await;
    set_finality(pool, "base", "depth", 0).await;

    let (logs, _guard) = capture_logs();
    let targets = evm_db::load_targets(pool)
        .await
        .expect("the bad row must not fail the load");

    let slugs: Vec<&str> = targets.iter().map(|target| target.slug.as_str()).collect();
    assert_eq!(targets.len(), 3, "three valid targets: {slugs:?}");
    assert!(
        !slugs.contains(&"base"),
        "the malformed row must be skipped"
    );
    for slug in ["arbitrum", "avalanche", "ethereum"] {
        assert!(slugs.contains(&slug), "{slug} must still load: {slugs:?}");
    }

    let logs = log_text(&logs);
    assert!(logs.contains("base"), "the log must name the chain: {logs}");
    assert!(
        logs.contains("finality_depth"),
        "the log must carry the reason: {logs}"
    );
}

/// The same result whether the malformed row sorts first (`arbitrum`) or last
/// (`ethereum`): the other three chains still load.
#[tokio::test]
async fn a_malformed_target_is_skipped_first_or_last() {
    let db = common::test_db().await;
    let pool = &db.pool;
    add_watch_targets(pool).await;

    // `arbitrum` is first in the loader's slug order.
    set_finality(pool, "arbitrum", "depth", 0).await;
    let first = evm_db::load_targets(pool).await.expect("load targets");
    assert_eq!(first.len(), 3, "three targets with the first row bad");
    assert!(!first.iter().any(|target| target.slug == "arbitrum"));

    // Move the fault to `ethereum`, which is last in the slug order.
    set_finality(pool, "arbitrum", "finalized", 64).await;
    set_finality(pool, "ethereum", "depth", 0).await;
    let last = evm_db::load_targets(pool).await.expect("load targets");
    assert_eq!(last.len(), 3, "three targets with the last row bad");
    assert!(!last.iter().any(|target| target.slug == "ethereum"));
    assert!(last.iter().any(|target| target.slug == "base"));
}

/// Every row malformed: no target loads, the load does not abort or panic, and the
/// fully broken configuration is logged as an error.
#[tokio::test]
async fn every_malformed_target_starts_no_watcher() {
    let db = common::test_db().await;
    let pool = &db.pool;
    add_watch_targets(pool).await;
    for slug in WATCHED_CHAINS {
        set_finality(pool, slug, "depth", 0).await;
    }

    let (logs, _guard) = capture_logs();
    let targets = evm_db::load_targets(pool)
        .await
        .expect("an all-invalid set must not fail the load");
    assert!(targets.is_empty(), "no watcher may start");

    let logs = log_text(&logs);
    assert!(
        logs.contains("no EVM watcher will start"),
        "the all-invalid set must be logged: {logs}"
    );
}

/// Valid rows behave exactly as before: all four load with their own finality.
#[tokio::test]
async fn valid_targets_load_unchanged() {
    let db = common::test_db().await;
    let pool = &db.pool;
    add_watch_targets(pool).await;

    let targets = evm_db::load_targets(pool).await.expect("load targets");
    let mut slugs: Vec<&str> = targets.iter().map(|target| target.slug.as_str()).collect();
    slugs.sort_unstable();
    assert_eq!(slugs, WATCHED_CHAINS);
    assert!(
        targets
            .iter()
            .all(|target| target.finality_mode == FinalityMode::Finalized)
    );
    assert!(targets.iter().all(|target| target.finality_depth == 64));
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
