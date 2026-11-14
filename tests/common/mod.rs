// Each test binary compiles its own copy of this module and uses a different slice of
// it, so an item unused here is still used somewhere.
#![allow(dead_code)]

use bridge::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use bridge::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use reqwest::Client;
use serde_json::Value;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::borrow::Cow;
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::time::Duration;
use testcontainers_modules::{
    postgres::Postgres,
    redis::Redis,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

/// The production database is Postgres 18, so tests run against 18 and not older.
const POSTGRES_TAG: &str = "18";

pub struct TestDb {
    pub pool: PgPool,
    /// The same database as a URL, for tests that spawn the binary.
    pub url: String,
    // Held so the container lives as long as the test.
    _container: Option<ContainerAsync<Postgres>>,
}

/// A migrated database with no test data.
pub async fn test_db() -> TestDb {
    let db = empty_db().await;
    sqlx::migrate!("./migrations")
        .run(&db.pool)
        .await
        .expect("failed to run migrations");
    db
}

/// A database with no migrations applied, for tests that stage old data first.
pub async fn empty_db() -> TestDb {
    let (pool, url, container) = match std::env::var("TEST_DATABASE_URL") {
        Ok(url) => {
            let (pool, fresh_url) = fresh_database(&url).await;
            (pool, fresh_url, None)
        }
        Err(_) => {
            let container = Postgres::default()
                .with_tag(POSTGRES_TAG)
                .start()
                .await
                .expect("failed to start postgres container");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("failed to get mapped port");
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            let pool = PgPool::connect(&url)
                .await
                .expect("failed to connect to test postgres");
            (pool, url, Some(container))
        }
    };

    TestDb {
        pool,
        url,
        _container: container,
    }
}

/// Applies only the migrations older than `version`. Running the full set afterwards
/// applies the rest, which is how a migration is tested against existing rows.
pub async fn migrate_before(pool: &PgPool, version: i64) {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.migrations = Cow::Owned(
        migrator
            .migrations
            .iter()
            .filter(|m| m.version < version)
            .cloned()
            .collect(),
    );
    migrator
        .run(pool)
        .await
        .expect("failed to run earlier migrations");
}

pub async fn migrate_all(pool: &PgPool) {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .expect("failed to run migrations");
}

/// A Solana tx holding one Wormhole send to `destination`.
pub fn sample_tx(
    hash: &str,
    sequence: u64,
    destination: ChainId,
    amount: Option<&str>,
) -> NormalisedTransaction {
    NormalisedTransaction {
        hash: hash.to_string(),
        chain: Chain::Solana,
        status: TxStatus::Success,
        slot: 123,
        timestamp: Some(1_700_000_000),
        fee_lamports: 5000,
        signer: Some("SomeSignerPubkey".to_string()),
        bridge_event: None,
        bridge_transfer: Some(BridgeTransfer {
            program: bridge::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain: ChainId::Solana,
            source_tx_hash: Some(hash.to_string()),
            source_wallet: Some("SomeSignerPubkey".to_string()),
            source_explorer_url: None,
            destination_chain: destination,
            destination_wallet: None,
            destination_tx_hash: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: Some("W".to_string()),
            amount: amount.map(String::from),
            amount_formatted: Some("1.0".to_string()),
            message_id: BridgeMessageId {
                emitter_chain: 1,
                emitter_address: "emitter-addr".to_string(),
                sequence,
            },
            status: BridgeStatus::Pending,
        }),
    }
}

/// A Solana tx holding one Wormhole redeem of a message sent from `source`. The transfer
/// is the destination side: the observed tx is `hash`, the origin tx is `source_tx`.
pub fn sample_redeem_tx(
    hash: &str,
    sequence: u64,
    source: ChainId,
    source_tx: Option<&str>,
) -> NormalisedTransaction {
    let mut tx = sample_tx(hash, sequence, ChainId::Solana, Some("1000000"));
    let wormhole_id = source.wormhole_id();
    let transfer = tx.bridge_transfer.as_mut().unwrap();
    transfer.source_chain = source;
    transfer.source_tx_hash = source_tx.map(String::from);
    transfer.source_wallet = None;
    transfer.destination_tx_hash = Some(hash.to_string());
    transfer.status = BridgeStatus::Completed;
    transfer.message_id.emitter_chain = wormhole_id;
    tx
}

async fn fresh_database(base_url: &str) -> (PgPool, String) {
    let admin_options = PgConnectOptions::from_str(base_url).expect("invalid TEST_DATABASE_URL");
    let admin = PgPool::connect_with(admin_options.clone())
        .await
        .expect("failed to connect to TEST_DATABASE_URL");

    let name = format!("bridge_test_{:016x}", rand::random::<u64>());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("failed to create test database");

    let pool = PgPool::connect_with(admin_options.database(&name))
        .await
        .expect("failed to connect to test database");
    (pool, with_database(base_url, &name))
}

/// Replaces the database segment of a Postgres URL without losing credentials or
/// query parameters (sqlx's connect options do not implement `Display`).
fn with_database(base_url: &str, database: &str) -> String {
    let (base, query) = match base_url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (base_url, None),
    };
    let prefix = base
        .rsplit_once('/')
        .map(|(prefix, _)| prefix)
        .unwrap_or(base);

    let mut url = format!("{prefix}/{database}");
    if let Some(query) = query {
        url.push('?');
        url.push_str(query);
    }
    url
}

/// `XAUTOCLAIM`, which the recovery worker relies on, needs Redis 6.2+.
const REDIS_TAG: &str = "7";

pub struct TestRedis {
    pub url: String,
    // Held so the container lives as long as the test.
    _container: Option<ContainerAsync<Redis>>,
}

/// A Redis with no streams, on its own container (or `TEST_REDIS_URL` when set).
pub async fn test_redis() -> TestRedis {
    if let Ok(url) = std::env::var("TEST_REDIS_URL") {
        return TestRedis {
            url,
            _container: None,
        };
    }

    let container = Redis::default()
        .with_tag(REDIS_TAG)
        .start()
        .await
        .expect("failed to start redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get mapped port");

    TestRedis {
        url: format!("redis://127.0.0.1:{port}"),
        _container: Some(container),
    }
}

/// Kills the fake upstream when the test ends, including on panic.
pub struct FakeUpstream {
    child: Child,
    pub base_url: String,
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_fake_upstream(
    port: u16,
    envs: &[(&str, &str)],
    fixtures: Option<&std::path::Path>,
) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fake_upstream"));
    command
        .env("FAKE_PORT", port.to_string())
        .env("FAKE_LATENCY_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    if let Some(dir) = fixtures {
        command.arg("--fixtures").arg(dir);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    command.spawn().expect("spawn fake_upstream")
}

pub async fn start_fake_upstream() -> FakeUpstream {
    start_fake_upstream_with(&[]).await
}

/// Starts the fake upstream with extra environment, for tests that need injected
/// latency or 429s. The defaults keep it instant and always answering.
pub async fn start_fake_upstream_with(envs: &[(&str, &str)]) -> FakeUpstream {
    start_fake_upstream_opts(envs, None).await
}

/// Starts the fake upstream over a fixture directory other than `tests/fixtures`, so
/// a test can replay a minimal captured pair without changing the shared set.
pub async fn start_fake_upstream_with_fixtures(fixtures: &std::path::Path) -> FakeUpstream {
    start_fake_upstream_opts(&[], Some(fixtures)).await
}

async fn start_fake_upstream_opts(
    envs: &[(&str, &str)],
    fixtures: Option<&std::path::Path>,
) -> FakeUpstream {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a free port")
        .local_addr()
        .expect("local address")
        .port();
    // The guard owns the child from here, so the panic below still kills it.
    let fake = FakeUpstream {
        child: spawn_fake_upstream(port, envs, fixtures),
        base_url: format!("http://127.0.0.1:{port}"),
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .expect("health-check client");
    for _ in 0..100 {
        if client
            .get(format!("{}/stats", fake.base_url))
            .send()
            .await
            .is_ok()
        {
            return fake;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("fake_upstream did not answer on {}", fake.base_url);
}

/// A signature the fake upstream replays as `fixture`. Any signature maps to a
/// fixture by hash, so probing finds one for the fixture a test needs.
pub async fn signature_for_fixture(base_url: &str, fixture: &str) -> String {
    let client = Client::new();
    for i in 0..500 {
        let hash = format!("fixture-lookup-{i}");
        let body: Value = client
            .get(format!("{base_url}/debug/fixture/{hash}"))
            .send()
            .await
            .expect("debug fixture request")
            .json()
            .await
            .expect("debug fixture body");
        if body["fixture"].as_str() == Some(fixture) {
            return hash;
        }
    }
    panic!("no signature maps to fixture {fixture}");
}

/// Signatures whose fixture makes the classifier produce a transfer, so a test
/// exercises the path that decodes and persists rather than a no-transfer tx.
pub async fn transfer_signatures(base_url: &str, count: usize) -> Vec<String> {
    let client = Client::new();
    let mut found = Vec::new();
    for i in 0..500 {
        let hash = format!("precision-read-only-{i}");
        let body: Value = client
            .get(format!("{base_url}/debug/fixture/{hash}"))
            .send()
            .await
            .expect("debug fixture request")
            .json()
            .await
            .expect("debug fixture body");
        let status = body["status"].as_str().unwrap_or_default();
        if status != "NoTransfer" && status != "Malformed" {
            found.push(hash);
            if found.len() == count {
                return found;
            }
        }
    }
    panic!(
        "the fixtures only hold {} transfer transactions",
        found.len()
    );
}
