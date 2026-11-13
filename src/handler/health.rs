//! `GET /health`: Postgres and Redis reachability plus pipeline freshness.
//!
//! Redis is billed per command, so the whole report is cached in memory for
//! [`CACHE_TTL`]. A monitor polling every few seconds therefore costs at most one
//! PING, one XINFO GROUPS and one XLEN per process per 15 s, even under concurrency:
//! the cache lock is held across the measurement, so simultaneous misses coalesce.
//!
//! Nothing here calls Helius or WormholeScan.

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use redis::Value;
use redis::aio::ConnectionManager;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::state::AppState;

const BRIDGE_TX_STREAM: &str = "bridge:transactions";
const FAILED_TX_STREAM: &str = "bridge:transactions:failed";
const CONSUMER_GROUP: &str = "bridge-workers";

// `/health` measures the Token Bridge's checkpoint only: the service is "advancing"
// when the Token Bridge is, never because some other configured program is.

const CACHE_TTL: Duration = Duration::from_secs(15);
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// A health answer held for [`CACHE_TTL`].
#[derive(Clone)]
struct Cached {
    report: HealthReport,
    http: StatusCode,
}

impl Cached {
    fn response(&self) -> Response {
        (self.http, Json(self.report.clone())).into_response()
    }
}

static CACHE: LazyLock<Mutex<Option<(Instant, Cached)>>> = LazyLock::new(|| Mutex::new(None));

/// One connection for the whole process. `ConnectionManager` reconnects on demand.
static REDIS: LazyLock<Mutex<Option<(String, ConnectionManager)>>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Ok,
    Degraded,
}

impl HealthStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
        }
    }
}

/// What the probes observed. Every stream field is `None` when Redis did not answer.
#[derive(Debug, Clone, Copy)]
pub struct Measurements {
    pub postgres_ok: bool,
    pub redis_ok: bool,
    /// Seconds since the ingester last handled a notification; `None` when the
    /// checkpoint row does not exist (a fresh database or ingestion never started).
    pub ingest_age_secs: Option<i64>,
    pub stream_lag: Option<i64>,
    pub stream_pending: Option<i64>,
    pub dead_letter_len: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub max_ingest_age_secs: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub status: HealthStatus,
    pub http: StatusCode,
}

/// The whole decision. 503 only when Postgres or Redis is down, or the heartbeat
/// is past the threshold. A *missing* heartbeat is reported `degraded` with 200:
/// there may be nothing wrong with the service, just no checkpoint yet.
pub fn decide(measurements: &Measurements, thresholds: Thresholds) -> Decision {
    let ingest_stale = measurements
        .ingest_age_secs
        .is_none_or(|age| age > thresholds.max_ingest_age_secs);
    let unavailable = !measurements.postgres_ok
        || !measurements.redis_ok
        || measurements
            .ingest_age_secs
            .is_some_and(|age| age > thresholds.max_ingest_age_secs);

    Decision {
        status: if unavailable || ingest_stale {
            HealthStatus::Degraded
        } else {
            HealthStatus::Ok
        },
        http: if unavailable {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        },
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    status: &'static str,
    postgres_ok: bool,
    redis_ok: bool,
    ingest_age_secs: Option<i64>,
    stream_lag: Option<i64>,
    stream_pending: Option<i64>,
    dead_letter_len: Option<i64>,
}

pub async fn health_handler(State(state): State<AppState>) -> Response {
    let mut cache = CACHE.lock().await;

    if let Some((stored_at, cached)) = cache.as_ref()
        && stored_at.elapsed() < CACHE_TTL
    {
        publish_gauges(&cached.report);
        return cached.response();
    }

    let measurements = measure(&state).await;
    let decision = decide(
        &measurements,
        Thresholds {
            max_ingest_age_secs: state.config.health_max_ingest_age_secs,
        },
    );

    let cached = Cached {
        report: HealthReport {
            status: decision.status.as_str(),
            postgres_ok: measurements.postgres_ok,
            redis_ok: measurements.redis_ok,
            ingest_age_secs: measurements.ingest_age_secs,
            stream_lag: measurements.stream_lag,
            stream_pending: measurements.stream_pending,
            dead_letter_len: measurements.dead_letter_len,
        },
        http: decision.http,
    };
    *cache = Some((Instant::now(), cached.clone()));
    publish_gauges(&cached.report);

    cached.response()
}

/// Publishes the cached probe values as gauges; it never performs a probe of its
/// own. A field the probe could not measure stays at its previous value rather
/// than being reported as a false zero.
fn publish_gauges(report: &HealthReport) {
    if let Some(seconds) = report.ingest_age_secs {
        metrics::gauge!(crate::metrics::INGEST_AGE_SECONDS).set(seconds as f64);
    }
    if let Some(lag) = report.stream_lag {
        metrics::gauge!(crate::metrics::STREAM_LAG).set(lag as f64);
    }
    if let Some(pending) = report.stream_pending {
        metrics::gauge!(crate::metrics::STREAM_PENDING).set(pending as f64);
    }
    if let Some(depth) = report.dead_letter_len {
        metrics::gauge!(crate::metrics::DEAD_LETTER_DEPTH).set(depth as f64);
    }
}

async fn measure(state: &AppState) -> Measurements {
    let (postgres_ok, ingest_age_secs) =
        measure_postgres(&state.db, &state.config.solana_token_bridge_program).await;
    let (redis_ok, stream_lag, stream_pending, dead_letter_len) =
        measure_redis(&state.config.redis_url).await;

    Measurements {
        postgres_ok,
        redis_ok,
        ingest_age_secs,
        stream_lag,
        stream_pending,
        dead_letter_len,
    }
}

async fn measure_postgres(pool: &PgPool, token_bridge_program: &str) -> (bool, Option<i64>) {
    let ping = tokio::time::timeout(
        PING_TIMEOUT,
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool),
    )
    .await;

    if !matches!(ping, Ok(Ok(1))) {
        return (false, None);
    }

    // computed in the database, so app/DB clock skew cannot distort the age.
    let age = tokio::time::timeout(
        PING_TIMEOUT,
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT GREATEST(0, EXTRACT(EPOCH FROM (NOW() - updated_at))::bigint)
            FROM indexer_state
            WHERE key = $1
            "#,
        )
        .bind(crate::db::repository::ingest_checkpoint_key(
            token_bridge_program,
        ))
        .fetch_optional(pool),
    )
    .await;

    match age {
        Ok(Ok(age)) => (true, age),
        Ok(Err(error)) => {
            tracing::warn!(%error, "health: checkpoint query failed");
            (true, None)
        }
        Err(_) => {
            tracing::warn!("health: checkpoint query timed out");
            (true, None)
        }
    }
}

async fn measure_redis(redis_url: &str) -> (bool, Option<i64>, Option<i64>, Option<i64>) {
    let Some(mut connection) = redis_connection(redis_url).await else {
        return (false, None, None, None);
    };

    let ping = tokio::time::timeout(PING_TIMEOUT, async {
        let reply: String = redis::cmd("PING").query_async(&mut connection).await?;
        Ok::<_, redis::RedisError>(reply)
    })
    .await;

    if !matches!(ping, Ok(Ok(_))) {
        return (false, None, None, None);
    }

    let (stream_lag, stream_pending) = read_stream_lag(&mut connection).await;
    let dead_letter_len = read_dead_letter_len(&mut connection).await;

    (true, stream_lag, stream_pending, dead_letter_len)
}

async fn redis_connection(redis_url: &str) -> Option<ConnectionManager> {
    let mut cached = REDIS.lock().await;

    if let Some((url, manager)) = cached.as_ref()
        && url == redis_url
    {
        return Some(manager.clone());
    }

    let client = redis::Client::open(redis_url).ok()?;
    let manager = tokio::time::timeout(PING_TIMEOUT, ConnectionManager::new(client))
        .await
        .ok()?
        .ok()?;

    *cached = Some((redis_url.to_string(), manager.clone()));
    Some(manager)
}

async fn read_stream_lag(connection: &mut ConnectionManager) -> (Option<i64>, Option<i64>) {
    let groups = tokio::time::timeout(PING_TIMEOUT, async {
        let value: Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(BRIDGE_TX_STREAM)
            .query_async(connection)
            .await?;
        Ok::<_, redis::RedisError>(value)
    })
    .await;

    // A missing stream is not a Redis failure; the workers create it on first use.
    match groups {
        Ok(Ok(value)) => group_lag_pending(&value),
        _ => (None, None),
    }
}

async fn read_dead_letter_len(connection: &mut ConnectionManager) -> Option<i64> {
    tokio::time::timeout(PING_TIMEOUT, async {
        let len: i64 = redis::cmd("XLEN")
            .arg(FAILED_TX_STREAM)
            .query_async(connection)
            .await?;
        Ok::<_, redis::RedisError>(len)
    })
    .await
    .ok()?
    .ok()
}

/// `lag` and `pending` of our consumer group. Handles both RESP2 (a flat array of
/// alternating field/value) and RESP3 (a map). `lag` is nil when Redis cannot tell.
fn group_lag_pending(value: &Value) -> (Option<i64>, Option<i64>) {
    let Value::Array(groups) = value else {
        return (None, None);
    };

    for group in groups {
        let fields = field_map(group);
        let is_ours = fields.iter().any(|(name, value)| {
            name == "name"
                && matches!(value, Value::BulkString(name) if name == CONSUMER_GROUP.as_bytes())
        });
        if is_ours {
            return (int_field(&fields, "lag"), int_field(&fields, "pending"));
        }
    }

    (None, None)
}

/// Field/value pairs of one XINFO entry, whatever encoding Redis used.
fn field_map(value: &Value) -> Vec<(String, Value)> {
    match value {
        Value::Array(items) => items
            .chunks(2)
            .filter_map(|pair| match pair {
                [Value::BulkString(name), value] => {
                    Some((String::from_utf8_lossy(name).into_owned(), value.clone()))
                }
                _ => None,
            })
            .collect(),
        Value::Map(entries) => entries
            .iter()
            .filter_map(|(name, value)| match name {
                Value::BulkString(name) => {
                    Some((String::from_utf8_lossy(name).into_owned(), value.clone()))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn int_field(fields: &[(String, Value)], name: &str) -> Option<i64> {
    match fields
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)?
    {
        Value::Int(value) => Some(*value),
        Value::BulkString(value) => String::from_utf8_lossy(value).parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measurements() -> Measurements {
        Measurements {
            postgres_ok: true,
            redis_ok: true,
            ingest_age_secs: Some(10),
            stream_lag: Some(0),
            stream_pending: Some(0),
            dead_letter_len: Some(0),
        }
    }

    fn thresholds() -> Thresholds {
        Thresholds {
            max_ingest_age_secs: 3600,
        }
    }

    #[test]
    fn all_ok_is_200() {
        let decision = decide(&measurements(), thresholds());
        assert_eq!(decision.status, HealthStatus::Ok);
        assert_eq!(decision.http, StatusCode::OK);
    }

    #[test]
    fn postgres_down_is_503() {
        let decision = decide(
            &Measurements {
                postgres_ok: false,
                ..measurements()
            },
            thresholds(),
        );
        assert_eq!(decision.status, HealthStatus::Degraded);
        assert_eq!(decision.http, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn redis_down_is_503() {
        let decision = decide(
            &Measurements {
                redis_ok: false,
                ..measurements()
            },
            thresholds(),
        );
        assert_eq!(decision.status, HealthStatus::Degraded);
        assert_eq!(decision.http, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn stale_ingest_is_503() {
        let decision = decide(
            &Measurements {
                ingest_age_secs: Some(3601),
                ..measurements()
            },
            thresholds(),
        );
        assert_eq!(decision.status, HealthStatus::Degraded);
        assert_eq!(decision.http, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn exactly_at_the_threshold_is_200() {
        let decision = decide(
            &Measurements {
                ingest_age_secs: Some(3600),
                ..measurements()
            },
            thresholds(),
        );
        assert_eq!(decision.status, HealthStatus::Ok);
        assert_eq!(decision.http, StatusCode::OK);
    }

    #[test]
    fn missing_heartbeat_is_degraded_but_200() {
        let decision = decide(
            &Measurements {
                ingest_age_secs: None,
                ..measurements()
            },
            thresholds(),
        );
        assert_eq!(decision.status, HealthStatus::Degraded);
        assert_eq!(decision.http, StatusCode::OK);
    }

    /// RESP2 XINFO GROUPS is a flat array of alternating field/value pairs.
    #[test]
    fn parses_resp2_xinfo_groups() {
        let bulk = |value: &str| Value::BulkString(value.as_bytes().to_vec());
        let group = Value::Array(vec![
            bulk("name"),
            bulk(CONSUMER_GROUP),
            bulk("consumers"),
            Value::Int(5),
            bulk("pending"),
            Value::Int(2),
            bulk("lag"),
            Value::Int(0),
        ]);

        assert_eq!(
            group_lag_pending(&Value::Array(vec![group])),
            (Some(0), Some(2))
        );
    }

    /// Redis reports `lag` as nil when it cannot compute it; that is not an error.
    #[test]
    fn missing_lag_is_none() {
        let bulk = |value: &str| Value::BulkString(value.as_bytes().to_vec());
        let group = Value::Array(vec![
            bulk("name"),
            bulk(CONSUMER_GROUP),
            bulk("pending"),
            Value::Int(3),
            bulk("lag"),
            Value::Nil,
        ]);

        assert_eq!(
            group_lag_pending(&Value::Array(vec![group])),
            (None, Some(3))
        );
    }
}
