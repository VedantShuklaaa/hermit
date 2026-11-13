//! A fake Helius + WormholeScan upstream for local runs and benchmarks.
//!
//!   cargo run --release --bin fake_upstream
//!   HELIUS_URL=http://127.0.0.1:9090 WORMHOLE_URL=http://127.0.0.1:9090 cargo run --release
//!
//! It replays `tests/fixtures` (override with `--fixtures DIR`). Any signature maps to
//! one fixture pair by a hash of the signature, so unique signatures still get a
//! deterministic answer, and a fixture can be replayed under a different signature.
//!
//! Served routes:
//!   POST /                                  Helius JSON-RPC (getTransaction,
//!                                           getMultipleAccounts, getSignaturesForAddress,
//!                                           getAsset)
//!   GET  /api/v1/operations?txHash=...      WormholeScan operations
//!   GET  /api/v1/vaas/:chain/:emitter/:sequence
//!   GET  /api/v1/global-tx/:chain/:emitter/:sequence
//!   GET  /stats                             per-method calls, injected 429s, in-flight high-water
//!   POST /stats/reset                       zero the counters
//!   GET  /debug/fixture/:signature          which fixture a signature maps to and its status
//!
//! Env: FAKE_PORT (9090), FAKE_LATENCY_MS (50), FAKE_JITTER_MS (0), FAKE_429_RATE (0.0,
//! clamped to 0..=1), FAKE_WORMHOLE_429_FIRST (0: the first N calls to each WormholeScan
//! route answer 429, then the route succeeds). No API key and no network access are needed.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::Snapshot;
use bridge::domain::bridge_transfer::{BridgeMessageId, BridgeStatus};
use bridge::protocols::registry::default_registry;
use bridge::protocols::{AccountData, SolanaTx};
use bridge::services::detect;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Config {
    port: u16,
    latency: Duration,
    jitter_ms: u64,
    rate_429: f64,
    wormhole_429_first: u64,
    fixtures_dir: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        fn number<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        let mut fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--fixtures" => {
                    let dir = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--fixtures needs a directory"))?;
                    fixtures_dir = PathBuf::from(dir);
                }
                "--help" | "-h" => {
                    println!(
                        "usage: fake_upstream [--fixtures DIR]\n\n\
                         env: FAKE_PORT, FAKE_LATENCY_MS, FAKE_JITTER_MS, FAKE_429_RATE, \
                         FAKE_WORMHOLE_429_FIRST"
                    );
                    std::process::exit(0);
                }
                other => anyhow::bail!("unknown argument {other}"),
            }
        }

        Ok(Self {
            port: number("FAKE_PORT", 9090u16),
            latency: Duration::from_millis(number("FAKE_LATENCY_MS", 50u64)),
            jitter_ms: number("FAKE_JITTER_MS", 0u64),
            rate_429: number("FAKE_429_RATE", 0.0f64).clamp(0.0, 1.0),
            wormhole_429_first: number("FAKE_WORMHOLE_429_FIRST", 0u64),
            fixtures_dir,
        })
    }
}

/// The status the app's pipeline resolves a replayed fixture to. The chain-first
/// detection and the fixture's own WormholeScan operation are the same inputs the app
/// uses, so this answers the cache question directly: only `Completed` results live in
/// the 600 s completed cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureStatus {
    Completed,
    Pending,
    Detected,
    NoTransfer,
    Malformed,
}

impl FixtureStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "Completed",
            Self::Pending => "Pending",
            Self::Detected => "Detected",
            Self::NoTransfer => "NoTransfer",
            Self::Malformed => "Malformed",
        }
    }

    fn completed(self) -> bool {
        self == Self::Completed
    }
}

/// One captured pair (`helius_<name>.json` + `wormhole_<name>.json`), plus the posted
/// VAA account when the fixture is an inbound redeem.
struct Fixture {
    name: String,
    helius: Value,
    wormhole: Option<Value>,
    account: Option<Value>,
    status: FixtureStatus,
}

fn read_json(path: &FsPath) -> anyhow::Result<Value> {
    let text =
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("{}: invalid json: {e}", path.display()))
}

fn load_fixtures(dir: &FsPath) -> anyhow::Result<Vec<Fixture>> {
    let mut fixtures = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|e| anyhow::anyhow!("{}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        let Some(file) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(name) = file
            .strip_prefix("helius_")
            .and_then(|n| n.strip_suffix(".json"))
        else {
            continue;
        };
        let wormhole = dir.join(format!("wormhole_{name}.json"));
        let account = dir.join(format!("account_{name}_vaa.json"));
        fixtures.push(Fixture {
            name: name.to_string(),
            helius: read_json(&path)?,
            wormhole: wormhole
                .exists()
                .then(|| read_json(&wormhole))
                .transpose()?,
            account: account.exists().then(|| read_json(&account)).transpose()?,
            status: FixtureStatus::Malformed,
        });
    }
    fixtures.sort_by(|a, b| a.name.cmp(&b.name));
    anyhow::ensure!(
        !fixtures.is_empty(),
        "no helius_*.json fixtures in {}",
        dir.display()
    );

    let statuses: Vec<FixtureStatus> = fixtures
        .iter()
        .map(|fixture| fixture_status(fixture, &fixtures))
        .collect();
    for (fixture, status) in fixtures.iter_mut().zip(statuses) {
        fixture.status = status;
    }
    Ok(fixtures)
}

/// Rebuilds the tx the app analyses: the captured transaction plus the posted VAA
/// account that `getMultipleAccounts` supplies for inbound redeems.
fn fixture_tx(fixture: &Fixture) -> Option<SolanaTx> {
    let mut tx = SolanaTx::from_helius(&fixture.helius, &fixture.name).ok()?;
    let Some(account) = fixture.account.as_ref() else {
        return Some(tx);
    };
    let value = account.get("value")?;
    let owner = value.get("owner")?.as_str()?;
    let data = value.get("data")?.get(0)?.as_str()?;
    let data = STANDARD.decode(data).ok()?;
    if let [key] = default_registry().required_accounts(&tx).as_slice() {
        tx.accounts.insert(
            key.clone(),
            AccountData {
                owner: owner.to_string(),
                data,
            },
        );
    }
    Some(tx)
}

/// The captured WormholeScan operation for a message id, if a fixture has one.
fn operation_for<'a>(
    fixtures: &'a [Fixture],
    chain: Option<u64>,
    emitter: &str,
    sequence: Option<u64>,
) -> Option<&'a Value> {
    fixtures
        .iter()
        .filter_map(|fixture| fixture.wormhole.as_ref())
        .find(|operation| {
            operation.get("emitterChain").and_then(Value::as_u64) == chain
                && operation
                    .get("emitterAddress")
                    .and_then(Value::as_str)
                    .is_some_and(|address| address.eq_ignore_ascii_case(emitter))
                && operation.get("sequence").and_then(Value::as_u64) == sequence
        })
}

/// The destination tx the fake's `/global-tx` route would return for this message.
fn global_destination_tx(fixtures: &[Fixture], id: &BridgeMessageId) -> Option<String> {
    operation_for(
        fixtures,
        Some(u64::from(id.emitter_chain)),
        &id.emitter_address,
        Some(id.sequence),
    )
    .and_then(|operation| operation.pointer("/targetChain/transaction/txHash"))
    .and_then(Value::as_str)
    .map(String::from)
}

/// Mirrors the app's status resolution: detection, then the captured WormholeScan
/// operation, then the `/global-tx` fallback the enrichment provider would make.
fn fixture_status(fixture: &Fixture, fixtures: &[Fixture]) -> FixtureStatus {
    let Some(tx) = fixture_tx(fixture) else {
        return FixtureStatus::Malformed;
    };
    let detected = detect::detect(&default_registry(), &tx);
    let Some(first) = detected.first() else {
        return FixtureStatus::NoTransfer;
    };
    let chains = Snapshot::default();
    let Ok(mut bridge) = detect::to_bridge_transfer(first, None, &chains) else {
        return FixtureStatus::Malformed;
    };
    if let Some(operation) = &fixture.wormhole {
        detect::apply_wormholescan(&mut bridge, operation, &chains);
        if matches!(bridge.status, BridgeStatus::Pending)
            && let Some(hash) = global_destination_tx(fixtures, &bridge.message_id)
        {
            detect::apply_destination_tx(&mut bridge, &hash, &chains);
        }
    }
    match bridge.status {
        BridgeStatus::Completed => FixtureStatus::Completed,
        BridgeStatus::Pending => FixtureStatus::Pending,
        BridgeStatus::Detected => FixtureStatus::Detected,
    }
}

/// Stable 64-bit FNV-1a: a signature always maps to the same fixture, across runs.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// The account each fixture's adapters would preload, keyed by pubkey. The fake derives
/// this from the captured tx itself, exactly as the app's fetch step would.
fn build_account_index(fixtures: &[Fixture]) -> BTreeMap<String, Value> {
    let mut index = BTreeMap::new();
    for fixture in fixtures {
        let Some(account) = &fixture.account else {
            continue;
        };
        let Ok(tx) = SolanaTx::from_helius(&fixture.helius, "fixture") else {
            continue;
        };
        let keys = default_registry().required_accounts(&tx);
        if let [key] = keys.as_slice() {
            index.insert(key.clone(), account["value"].clone());
        }
    }
    index
}

#[derive(Default)]
struct Stats {
    calls: BTreeMap<String, u64>,
    injected_429: BTreeMap<String, u64>,
    in_flight: usize,
    in_flight_high_water: usize,
}

struct FakeState {
    config: Config,
    fixtures: Vec<Fixture>,
    accounts: BTreeMap<String, Value>,
    stats: Arc<Mutex<Stats>>,
}

impl FakeState {
    fn fixture_for(&self, signature: &str) -> Option<&Fixture> {
        let index = fnv1a(signature.as_bytes()) % self.fixtures.len() as u64;
        self.fixtures.get(index as usize)
    }

    fn begin_call(&self, method: &str) -> InFlight {
        let mut stats = self.stats.lock().expect("stats mutex poisoned");
        *stats.calls.entry(method.to_string()).or_default() += 1;
        stats.in_flight += 1;
        stats.in_flight_high_water = stats.in_flight_high_water.max(stats.in_flight);
        InFlight(self.stats.clone())
    }

    async fn delay(&self) {
        let jitter = if self.config.jitter_ms == 0 {
            0
        } else {
            rand::random::<u64>() % (self.config.jitter_ms + 1)
        };
        tokio::time::sleep(self.config.latency + Duration::from_millis(jitter)).await;
    }

    fn rate_limited(&self, method: &str) -> bool {
        let mut stats = self.stats.lock().expect("stats mutex poisoned");
        // `begin_call` counted this request before the check, so `calls` is the
        // 1-based attempt number on this route.
        let forced = method.starts_with("wormhole:")
            && stats.calls.get(method).copied().unwrap_or(0) <= self.config.wormhole_429_first;
        if !forced && (self.config.rate_429 <= 0.0 || rand::random::<f64>() >= self.config.rate_429)
        {
            return false;
        }
        *stats.injected_429.entry(method.to_string()).or_default() += 1;
        true
    }
}

/// Counts the request as in flight for the whole response, including the simulated
/// latency, so `/stats` exposes the concurrency the app actually reached.
struct InFlight(Arc<Mutex<Stats>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        if let Ok(mut stats) = self.0.lock() {
            stats.in_flight = stats.in_flight.saturating_sub(1);
        }
    }
}

fn rpc_result(id: Value, result: Value) -> Response {
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

fn rpc_error(id: Value, code: i64, message: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    }))
    .into_response()
}

/// Helius answers a plan-level rate limit with HTTP 429 and a JSON-RPC error body; the
/// app's clients branch on the status, the body only has to stay parseable JSON.
fn helius_rate_limited(id: Value) -> Response {
    let mut response = Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": -32429, "message": "Too many requests for a specific RPC call"},
    }))
    .into_response();
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn wormhole_rate_limited() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "error": "Too Many Requests",
            "message": "Too many requests, please try again later.",
            "status": 429,
        })),
    )
        .into_response()
}

async fn json_rpc(State(state): State<Arc<FakeState>>, Json(body): Json<Value>) -> Response {
    let method = body
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let _in_flight = state.begin_call(&format!("helius:{method}"));
    state.delay().await;

    if state.rate_limited(&format!("helius:{method}")) {
        return helius_rate_limited(id);
    }

    let params = body.get("params").and_then(Value::as_array);
    match method.as_str() {
        "getTransaction" => {
            let signature = params
                .and_then(|p| p.first())
                .and_then(Value::as_str)
                .unwrap_or_default();
            match state.fixture_for(signature) {
                Some(fixture) => rpc_result(id, fixture.helius.clone()),
                None => rpc_result(id, Value::Null),
            }
        }
        "getMultipleAccounts" => {
            let values: Vec<Value> = params
                .and_then(|p| p.first())
                .and_then(Value::as_array)
                .map(|keys| {
                    keys.iter()
                        .map(|key| {
                            key.as_str()
                                .and_then(|k| state.accounts.get(k))
                                .cloned()
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .unwrap_or_default();
            rpc_result(
                id,
                json!({"context": {"slot": 1, "apiVersion": "fake"}, "value": values}),
            )
        }
        "getSignaturesForAddress" => rpc_result(id, json!([])),
        // Token metadata. The values are derived from the mint so every run answers
        // the same way, and the shape matches `token_info` in Helius's response.
        "getAsset" => {
            let mint = body
                .get("params")
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let hash = fnv1a(mint.as_bytes());
            let decimals = if hash.is_multiple_of(2) { 6u64 } else { 9u64 };
            let symbol = format!("T{:04X}", (hash >> 16) as u16);
            rpc_result(
                id,
                json!({"token_info": {"decimals": decimals, "symbol": symbol}}),
            )
        }
        other => rpc_error(id, -32601, &format!("Method not found: {other}")),
    }
}

#[derive(Deserialize)]
struct OperationsQuery {
    #[serde(rename = "txHash")]
    tx_hash: Option<String>,
}

async fn operations(
    State(state): State<Arc<FakeState>>,
    Query(query): Query<OperationsQuery>,
) -> Response {
    let _in_flight = state.begin_call("wormhole:operations");
    state.delay().await;
    if state.rate_limited("wormhole:operations") {
        return wormhole_rate_limited();
    }

    let operation = query
        .tx_hash
        .as_deref()
        .and_then(|hash| state.fixture_for(hash))
        .and_then(|fixture| fixture.wormhole.clone());
    match operation {
        Some(operation) => Json(json!({"operations": [operation]})).into_response(),
        None => Json(json!({"operations": []})).into_response(),
    }
}

async fn global_tx(
    State(state): State<Arc<FakeState>>,
    Path((chain, emitter, sequence)): Path<(String, String, String)>,
) -> Response {
    let _in_flight = state.begin_call("wormhole:global-tx");
    state.delay().await;
    if state.rate_limited("wormhole:global-tx") {
        return wormhole_rate_limited();
    }

    let chain: Option<u64> = chain.parse().ok();
    let sequence: Option<u64> = sequence.parse().ok();
    let destination = operation_for(&state.fixtures, chain, &emitter, sequence)
        .and_then(|operation| operation.pointer("/targetChain/transaction/txHash"))
        .and_then(Value::as_str);

    match destination {
        Some(hash) => Json(json!({"destinationTx": {"txHash": hash}})).into_response(),
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

/// The message-key route the origin-tx fallback uses. Like the real API, it returns the
/// source hash as bare hex (no `0x`); the app adds the prefix from the chain registry.
async fn vaas(
    State(state): State<Arc<FakeState>>,
    Path((chain, emitter, sequence)): Path<(String, String, String)>,
) -> Response {
    let _in_flight = state.begin_call("wormhole:vaas");
    state.delay().await;
    if state.rate_limited("wormhole:vaas") {
        return wormhole_rate_limited();
    }

    let chain: Option<u64> = chain.parse().ok();
    let sequence: Option<u64> = sequence.parse().ok();
    let source = operation_for(&state.fixtures, chain, &emitter, sequence)
        .and_then(|operation| operation.pointer("/sourceChain/transaction/txHash"))
        .and_then(Value::as_str)
        .map(|hash| hash.strip_prefix("0x").unwrap_or(hash));

    match source {
        Some(hash) => Json(json!({"data": {"txHash": hash}})).into_response(),
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

async fn stats(State(state): State<Arc<FakeState>>) -> Response {
    let stats = state.stats.lock().expect("stats mutex poisoned");
    Json(json!({
        "calls": stats.calls,
        "injected_429": stats.injected_429,
        "in_flight": stats.in_flight,
        "in_flight_high_water": stats.in_flight_high_water,
    }))
    .into_response()
}

async fn reset_stats(State(state): State<Arc<FakeState>>) -> Response {
    let mut stats = state.stats.lock().expect("stats mutex poisoned");
    stats.calls.clear();
    stats.injected_429.clear();
    stats.in_flight_high_water = stats.in_flight;
    Json(json!({"reset": true})).into_response()
}

/// Benchmark helper: which fixture the fake will replay for this signature, and the
/// status the app resolves it to. The hot set uses only Completed fixtures so its keys
/// stay in the 600 s completed cache instead of expiring from the 15 s open cache.
async fn debug_fixture(
    State(state): State<Arc<FakeState>>,
    Path(signature): Path<String>,
) -> Response {
    match state.fixture_for(&signature) {
        Some(fixture) => Json(json!({
            "signature": signature,
            "fixture": fixture.name,
            "status": fixture.status.as_str(),
            "completed": fixture.status.completed(),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no fixtures loaded"})),
        )
            .into_response(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let port = config.port;
    let fixtures = load_fixtures(&config.fixtures_dir)?;
    let accounts = build_account_index(&fixtures);

    let names: Vec<String> = fixtures.iter().map(|f| f.name.clone()).collect();
    let state = Arc::new(FakeState {
        config,
        fixtures,
        accounts,
        stats: Arc::new(Mutex::new(Stats::default())),
    });

    let app = Router::new()
        .route("/", post(json_rpc))
        .route("/api/v1/operations", get(operations))
        .route("/api/v1/vaas/:chain/:emitter/:sequence", get(vaas))
        .route(
            "/api/v1/global-tx/:chain/:emitter/:sequence",
            get(global_tx),
        )
        .route("/stats", get(stats))
        .route("/stats/reset", post(reset_stats))
        .route("/debug/fixture/:signature", get(debug_fixture))
        .with_state(state.clone());

    let address = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&address).await?;
    eprintln!(
        "fake upstream on http://{address} ({} fixtures: {}, {} accounts, latency {} ms, 429 rate {})",
        names.len(),
        names.join(", "),
        state.accounts.len(),
        state.config.latency.as_millis(),
        state.config.rate_429,
    );
    axum::serve(listener, app).await?;
    Ok(())
}
