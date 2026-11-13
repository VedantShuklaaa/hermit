//! The split deployments. `ROLE=api` serves reads and forwards the stream role's
//! broadcast events; `ROLE=stream` runs the pipeline and publishes to Redis. These
//! tests cover role parsing, the pub/sub bridge and the api role's process wiring.

mod common;

use bridge::config::Role;
use bridge::redis::broadcast::{BroadcastSink, forward};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const PROGRAM: &str = bridge::protocols::wormhole::TOKEN_BRIDGE;
const STREAM: &str = "bridge:transactions";

fn bridge_command(role: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bridge"));
    command
        .env("ROLE", role)
        .env("DATABASE_URL", "postgres://unused")
        .env("REDIS_URL", "redis://127.0.0.1:1")
        .env("HELIUS_URL", "http://127.0.0.1:1")
        .env("SOLANA_WS_URL", "ws://127.0.0.1:1")
        .env("SOLANA_TOKEN_BRIDGE_PROGRAM", PROGRAM)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

#[test]
fn an_invalid_role_fails_startup_with_a_clear_error() {
    let output = bridge_command("worker").output().expect("run bridge");

    assert!(!output.status.success(), "invalid ROLE must not start");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid ROLE"), "{stderr}");
    assert!(stderr.contains("worker"), "{stderr}");
    assert!(stderr.contains("all, api, stream"), "{stderr}");
}

#[test]
fn role_parsing_accepts_the_three_deployments() {
    assert_eq!(Role::parse("all").unwrap(), Role::All);
    assert_eq!(Role::parse("api").unwrap(), Role::Api);
    assert_eq!(Role::parse("stream").unwrap(), Role::Stream);
}

#[tokio::test]
async fn stream_publishes_reach_the_api_forwarder() {
    let redis = common::test_redis().await;
    let channel = format!("bridge:tx_events:test:{}", rand::random::<u64>());

    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let shutdown = CancellationToken::new();
    let forwarder = tokio::spawn(forward(
        redis.url.clone(),
        channel.clone(),
        tx,
        shutdown.clone(),
    ));

    // The forwarder subscribes asynchronously, and Redis pub/sub drops messages
    // published before SUBSCRIBE, so publish until one comes through.
    let sink = BroadcastSink::redis(&redis.url, channel)
        .await
        .expect("stream-side Redis sink");
    let payload = "{\"hash\":\"bridged\"}".to_string();
    let deadline = Instant::now() + Duration::from_secs(15);

    loop {
        sink.publish(payload.clone()).await;
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(received)) if received == payload => break,
            _ if Instant::now() >= deadline => {
                panic!("the api forwarder never delivered the stream event")
            }
            _ => {}
        }
    }

    shutdown.cancel();
    forwarder
        .await
        .expect("forwarder task")
        .expect("forwarder stopped cleanly");
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a free port")
        .local_addr()
        .expect("local address")
        .port()
}

async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("the api role did not start on port {port}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A consumer group is created by the first worker that starts. The api role must
/// never create one, otherwise ingestion and analysis are running behind the read API.
#[tokio::test]
async fn api_mode_starts_no_consumers() {
    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let port = free_port();

    let mut command = bridge_command("api");
    command
        .env("DATABASE_URL", &db.url)
        .env("REDIS_URL", &redis.url)
        .env("PORT", port.to_string());
    let child = ChildGuard(command.spawn().expect("start the api role"));

    wait_for_port(port).await;
    // Give a wrongly started consumer time to reach `XGROUP CREATE`.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut connection = redis::Client::open(redis.url.as_str())
        .expect("redis url")
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    let groups: redis::RedisResult<redis::Value> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(STREAM)
        .query_async(&mut connection)
        .await;

    // No stream (and so no group) is the expected answer; if another test on a
    // shared Redis created it, it must still not contain our consumer group.
    if let Ok(value) = groups {
        let rendered = format!("{value:?}");
        assert!(
            !rendered.contains("bridge-workers"),
            "ROLE=api started a consumer group: {rendered}"
        );
    }

    drop(child);
}
