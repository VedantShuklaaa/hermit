//! Server-owned live feed: a bounded ring of recent transaction frames in Redis.
//!
//! Every frame that reaches the WebSocket is also written here, so any browser sees
//! the same recent transactions. The read path never touches Postgres; the only
//! database query is the one-time warm-up after Redis loses the ring.
//!
//! Ordering is by first sighting, not last update: the index is a sorted set whose
//! score is `first_seen_at_ms` and whose `ZADD` uses `NX`, so an enrichment update
//! replaces the frame without moving it. Eviction always removes the oldest scores.

use crate::config::LiveFeedConfig;
use crate::domain::transaction::NormalisedTransaction;
use crate::error::AppError;
use crate::state::AppState;
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;

pub const INDEX_KEY: &str = "bridge:live:index";
pub const FRAMES_KEY: &str = "bridge:live:frames";
pub const WARMUP_KEY: &str = "bridge:live:warmup";

pub const DEFAULT_LIMIT: u32 = 15;
pub const MAX_LIMIT: u32 = 100;

const WARMUP_LOCK_SECS: u64 = 30;
/// A Redis failure suppresses further attempts for this long. After it expires one
/// request probes Redis; success closes the breaker, failure opens it again.
const BREAKER_WINDOW_MS: i64 = 5_000;
/// How long the last successful cursorless page may be served while Redis is down.
const SNAPSHOT_TTL_MS: i64 = 10_000;
/// A Redis command must not hold up the WebSocket broadcast or an HTTP response.
const REDIS_CALL_TIMEOUT: Duration = Duration::from_millis(300);

/// Atomic upsert + trim. `ZADD NX` keeps the original first-seen score on updates.
/// The timestamps are appended to the already-serialised frame so the rest of the
/// payload, including field order, is left untouched.
const UPSERT_LUA: &str = r#"
local index = KEYS[1]
local frames = KEYS[2]
local tx_hash = ARGV[1]
local now_ms = tonumber(ARGV[2])
local payload = ARGV[3]
local max = tonumber(ARGV[4])

local first = redis.call('ZSCORE', index, tx_hash)
if first then
  first = tonumber(first)
else
  redis.call('ZADD', index, 'NX', now_ms, tx_hash)
  first = now_ms
end

local frame = string.sub(payload, 1, -2)
  .. ',"first_seen_at":' .. string.format('%.0f', first)
  .. ',"updated_at":' .. string.format('%.0f', now_ms) .. '}'
redis.call('HSET', frames, tx_hash, frame)

local n = redis.call('ZCARD', index)
if n > max then
  local evict = redis.call('ZRANGE', index, 0, n - max - 1)
  local i = 1
  while i <= #evict do
    local chunk = {}
    local last = math.min(i + 499, #evict)
    for j = i, last do
      chunk[#chunk + 1] = evict[j]
    end
    redis.call('ZREM', index, unpack(chunk))
    redis.call('HDEL', frames, unpack(chunk))
    i = last + 1
  end
end

return frame
"#;

static UPSERT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(UPSERT_LUA));

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// An opaque position in the feed. `score` is `first_seen_at_ms`; `tx_hash` breaks
/// ties at the same millisecond (Redis orders equal scores lexicographically).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub score: i64,
    pub tx_hash: String,
}

impl Cursor {
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("cursor serializes");
        URL_SAFE_NO_PAD.encode(json)
    }

    pub fn decode(raw: &str) -> Result<Self, AppError> {
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid_cursor())?;
        serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())
    }
}

fn invalid_cursor() -> AppError {
    AppError::BadRequest("invalid before cursor".to_string())
}

/// Parses `limit`: default 15, clamped to 1..100. A non-numeric value is a 400.
pub fn parse_limit(raw: Option<&str>) -> Result<u32, AppError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_LIMIT);
    };
    let value = raw.parse::<i64>().map_err(|_| {
        AppError::BadRequest(format!(
            "limit must be an integer between 1 and {MAX_LIMIT}"
        ))
    })?;
    Ok(value.clamp(1, i64::from(MAX_LIMIT)) as u32)
}

#[derive(Clone, Debug, Serialize)]
pub struct LiveFeedResponse {
    pub items: Vec<serde_json::Value>,
    pub next_cursor: Option<String>,
    pub total: i64,
    pub server_time: i64,
}

impl LiveFeedResponse {
    pub fn empty(now_ms: i64) -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
            total: 0,
            server_time: now_ms,
        }
    }
}

struct CachedSnapshot {
    at_ms: i64,
    limit: u32,
    response: LiveFeedResponse,
}

struct Breaker {
    /// 0 means closed. A future timestamp means open until then; after it expires a
    /// single probe is admitted by the compare-and-swap in [`Breaker::allow`].
    open_until_ms: AtomicU64,
}

impl Breaker {
    fn new() -> Self {
        Self {
            open_until_ms: AtomicU64::new(0),
        }
    }

    fn allow(&self, now_ms: i64) -> bool {
        let now = now_ms.max(0) as u64;
        let current = self.open_until_ms.load(Ordering::Acquire);
        if current == 0 {
            return true;
        }
        if now < current {
            return false;
        }
        // The window expired: admit exactly one caller to probe Redis. If Redis is
        // healthy that probe calls `success` and closes the breaker for everyone.
        self.open_until_ms
            .compare_exchange(
                current,
                now + BREAKER_WINDOW_MS as u64,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn success(&self) {
        self.open_until_ms.store(0, Ordering::Release);
    }

    fn failure(&self, now_ms: i64) {
        self.open_until_ms.store(
            now_ms.max(0) as u64 + BREAKER_WINDOW_MS as u64,
            Ordering::Release,
        );
    }
}

pub struct LiveFeed {
    config: LiveFeedConfig,
    redis_url: String,
    /// Connected lazily. A failure leaves it `None`, but `ConnectionManager` can be
    /// reused safely if only the server was briefly unavailable.
    connection: Mutex<Option<ConnectionManager>>,
    breaker: Breaker,
    cached: RwLock<Option<CachedSnapshot>>,
}

impl LiveFeed {
    pub fn new(config: LiveFeedConfig, redis_url: &str) -> Self {
        Self {
            config,
            redis_url: redis_url.to_string(),
            connection: Mutex::new(None),
            breaker: Breaker::new(),
            cached: RwLock::new(None),
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn max(&self) -> usize {
        self.config.max
    }

    /// Returns a connected manager. The caller has already passed the breaker and
    /// owns reporting success or failure.
    async fn connection(&self) -> Option<ConnectionManager> {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.as_ref() {
            return Some(connection.clone());
        }

        let client = match redis::Client::open(self.redis_url.as_str()) {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(%error, "live feed Redis client could not be built");
                return None;
            }
        };
        match client.get_connection_manager().await {
            Ok(connection) => {
                *guard = Some(connection.clone());
                Some(connection)
            }
            Err(error) => {
                tracing::warn!(%error, "live feed Redis connection failed");
                None
            }
        }
    }

    /// Writes one frame at a specific first-seen time. Production uses `now_ms`; the
    /// warm-up uses the stored row's `created_at`, and tests use fixed values.
    pub async fn upsert_at(&self, tx_hash: &str, payload: &str, at_ms: i64) -> Result<String> {
        if !self.config.enabled {
            anyhow::bail!("live feed is disabled");
        }
        if !self.breaker.allow(at_ms) {
            anyhow::bail!("live feed Redis circuit is open");
        }
        let Some(mut connection) = self.connection().await else {
            self.breaker.failure(at_ms);
            anyhow::bail!("live feed Redis is unavailable");
        };

        match self
            .upsert_with(&mut connection, tx_hash, at_ms, payload)
            .await
        {
            Ok(frame) => {
                self.breaker.success();
                Ok(frame)
            }
            Err(error) => {
                self.breaker.failure(at_ms);
                Err(error.into())
            }
        }
    }

    async fn upsert_with(
        &self,
        connection: &mut ConnectionManager,
        tx_hash: &str,
        at_ms: i64,
        payload: &str,
    ) -> redis::RedisResult<String> {
        UPSERT
            .key(INDEX_KEY)
            .key(FRAMES_KEY)
            .arg(tx_hash)
            .arg(at_ms)
            .arg(payload)
            .arg(self.config.max as i64)
            .invoke_async(connection)
            .await
    }

    /// One page straight from Redis, with no cache or warm-up. Exposed for tests and
    /// used by [`LiveFeed::snapshot`].
    pub async fn read_page(&self, limit: u32, before: Option<&Cursor>) -> Result<LiveFeedResponse> {
        let now = now_ms();
        if !self.breaker.allow(now) {
            anyhow::bail!("live feed Redis circuit is open");
        }
        let Some(mut connection) = self.connection().await else {
            self.breaker.failure(now);
            anyhow::bail!("live feed Redis is unavailable");
        };
        match self.read_page_timed(&mut connection, limit, before).await {
            Ok(page) => {
                self.breaker.success();
                Ok(page)
            }
            Err(error) => {
                self.breaker.failure(now);
                Err(error)
            }
        }
    }

    async fn read_page_timed(
        &self,
        connection: &mut ConnectionManager,
        limit: u32,
        before: Option<&Cursor>,
    ) -> Result<LiveFeedResponse> {
        match tokio::time::timeout(
            REDIS_CALL_TIMEOUT,
            self.read_page_with(connection, limit, before),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => anyhow::bail!("live feed Redis read timed out"),
        }
    }

    async fn read_page_with(
        &self,
        connection: &mut ConnectionManager,
        limit: u32,
        before: Option<&Cursor>,
    ) -> Result<LiveFeedResponse> {
        let limit = limit.clamp(1, MAX_LIMIT);
        let want = i64::from(limit) + 1;

        let (members, total) = match before {
            None => {
                let (flat, total): (Vec<String>, i64) = redis::pipe()
                    .cmd("ZREVRANGEBYSCORE")
                    .arg(INDEX_KEY)
                    .arg("+inf")
                    .arg("-inf")
                    .arg("WITHSCORES")
                    .arg("LIMIT")
                    .arg(0)
                    .arg(want)
                    .cmd("ZCARD")
                    .arg(INDEX_KEY)
                    .query_async(connection)
                    .await?;
                (pairs(flat), total)
            }
            Some(cursor) => {
                let exclusive = format!("({}", cursor.score);
                // Same-score members are visited first: they sort ahead of every
                // strictly lower score. Within the score, descending lexicographic
                // order is Redis' ZREVRANGE order, so "after the cursor" means the
                // member is smaller than the cursor's hash.
                let (lower_flat, same, total): (Vec<String>, Vec<String>, i64) = redis::pipe()
                    .cmd("ZREVRANGEBYSCORE")
                    .arg(INDEX_KEY)
                    .arg(exclusive)
                    .arg("-inf")
                    .arg("WITHSCORES")
                    .arg("LIMIT")
                    .arg(0)
                    .arg(want)
                    .cmd("ZRANGEBYSCORE")
                    .arg(INDEX_KEY)
                    .arg(cursor.score)
                    .arg(cursor.score)
                    .cmd("ZCARD")
                    .arg(INDEX_KEY)
                    .query_async(connection)
                    .await?;

                let mut members = Vec::new();
                for member in same.into_iter().rev() {
                    if member < cursor.tx_hash {
                        members.push((member, cursor.score));
                        if members.len() as i64 >= want {
                            break;
                        }
                    }
                }
                members.extend(pairs(lower_flat));
                (members, total)
            }
        };

        let has_more = members.len() as i64 > i64::from(limit);
        let selected: Vec<(String, i64)> = members.into_iter().take(limit as usize).collect();
        let frames = hmget(connection, &selected).await?;

        let mut items = Vec::with_capacity(selected.len());
        let mut last = None;
        for ((tx_hash, score), frame) in selected.into_iter().zip(frames) {
            let Some(raw) = frame else {
                tracing::warn!(%tx_hash, "live feed index entry has no frame");
                continue;
            };
            match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(value) => {
                    items.push(value);
                    last = Some(Cursor { score, tx_hash });
                }
                Err(error) => {
                    tracing::warn!(%error, %tx_hash, "live feed frame is not valid JSON");
                }
            }
        }

        Ok(LiveFeedResponse {
            items,
            next_cursor: if has_more {
                last.map(|cursor| cursor.encode())
            } else {
                None
            },
            total,
            server_time: now_ms(),
        })
    }

    /// The endpoint's entry point: one Redis read, the cold-start warm-up, and a
    /// short in-process fallback when Redis is down.
    pub async fn snapshot(
        &self,
        db: &PgPool,
        limit: u32,
        before: Option<&Cursor>,
    ) -> Result<LiveFeedResponse> {
        if !self.config.enabled {
            anyhow::bail!("live feed is disabled");
        }
        let limit = limit.clamp(1, MAX_LIMIT);
        let now = now_ms();
        if !self.breaker.allow(now) {
            return self.fallback(limit, before, now);
        }
        let Some(mut connection) = self.connection().await else {
            self.breaker.failure(now);
            return self.fallback(limit, before, now);
        };

        let mut page = match self.read_page_timed(&mut connection, limit, before).await {
            Ok(page) => {
                self.breaker.success();
                page
            }
            Err(error) => {
                self.breaker.failure(now);
                tracing::warn!(%error, "live feed read failed");
                return self.fallback(limit, before, now);
            }
        };

        if page.total == 0 && before.is_none() {
            match self.try_warmup_lock_with(&mut connection).await {
                Ok(true) => match self.warm_up_with(&mut connection, db).await {
                    Ok(()) => match self.read_page_timed(&mut connection, limit, before).await {
                        Ok(warmed) => page = warmed,
                        Err(error) => tracing::warn!(%error, "live feed read after warm-up failed"),
                    },
                    Err(error) => {
                        tracing::warn!(%error, "live feed warm-up query failed");
                    }
                },
                Ok(false) => {}
                Err(error) => {
                    self.breaker.failure(now);
                    tracing::warn!(%error, "live feed warm-up lock failed");
                    return self.fallback(limit, before, now);
                }
            }
        }

        if before.is_none() {
            self.store_cache(limit, &page, now);
        }
        Ok(page)
    }

    /// `SET NX EX` is the whole warm-up lock: one process queries Postgres and the
    /// rest return an empty page until it has populated the ring.
    async fn try_warmup_lock_with(
        &self,
        connection: &mut ConnectionManager,
    ) -> redis::RedisResult<bool> {
        let acquired: Option<String> = redis::cmd("SET")
            .arg(WARMUP_KEY)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(WARMUP_LOCK_SECS)
            .query_async(connection)
            .await?;
        Ok(acquired.is_some())
    }

    /// Public wrapper used by the warm-up test.
    pub async fn try_warmup_lock(&self) -> Result<bool> {
        let now = now_ms();
        if !self.breaker.allow(now) {
            anyhow::bail!("live feed Redis circuit is open");
        }
        let Some(mut connection) = self.connection().await else {
            self.breaker.failure(now);
            anyhow::bail!("live feed Redis is unavailable");
        };
        let result = self.try_warmup_lock_with(&mut connection).await;
        match result {
            Ok(acquired) => {
                self.breaker.success();
                Ok(acquired)
            }
            Err(error) => {
                self.breaker.failure(now);
                Err(error.into())
            }
        }
    }

    /// Fills the ring from the newest stored analyses. This is the only Postgres read
    /// on the live-feed path, and it happens only after a cold start or Redis reset.
    pub async fn warm_up(&self, db: &PgPool) -> Result<()> {
        let now = now_ms();
        if !self.breaker.allow(now) {
            anyhow::bail!("live feed Redis circuit is open");
        }
        let Some(mut connection) = self.connection().await else {
            self.breaker.failure(now);
            anyhow::bail!("live feed Redis is unavailable");
        };
        match self.warm_up_with(&mut connection, db).await {
            Ok(()) => {
                self.breaker.success();
                Ok(())
            }
            Err(error) => {
                self.breaker.failure(now);
                Err(error)
            }
        }
    }

    async fn warm_up_with(&self, connection: &mut ConnectionManager, db: &PgPool) -> Result<()> {
        // There is no `detected_at` column; `created_at` is when the indexer stored
        // the row and is covered by `idx_bt_created`.
        let rows: Vec<(serde_json::Value, chrono::DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT analysis, created_at
            FROM bridge_transfers
            WHERE analysis IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT $1
            "#,
        )
        .bind(self.config.max as i64)
        .fetch_all(db)
        .await?;

        let mut seen = HashSet::new();
        let mut frames = Vec::with_capacity(rows.len());
        for (analysis, created_at) in rows {
            let Ok(tx) = serde_json::from_value::<NormalisedTransaction>(analysis) else {
                continue;
            };
            // A transaction can hold several transfers, each with its own row. The
            // live frame is per transaction, so keep the newest row for its hash.
            if !seen.insert(tx.hash.clone()) {
                continue;
            }
            frames.push((
                tx.hash.clone(),
                created_at.timestamp_millis(),
                serde_json::to_string(&tx)?,
            ));
        }
        frames.sort_by_key(|(_, at_ms, _)| *at_ms);

        for (tx_hash, at_ms, payload) in frames {
            self.upsert_with(connection, &tx_hash, at_ms, &payload)
                .await?;
        }
        Ok(())
    }

    fn fallback(&self, limit: u32, before: Option<&Cursor>, now: i64) -> Result<LiveFeedResponse> {
        if before.is_none()
            && let Ok(cached) = self.cached.read()
            && let Some(cached) = cached.as_ref()
            && cached.limit == limit
            && now.saturating_sub(cached.at_ms) <= SNAPSHOT_TTL_MS
        {
            let mut response = cached.response.clone();
            response.server_time = now;
            return Ok(response);
        }
        anyhow::bail!("live feed Redis is unavailable")
    }

    fn store_cache(&self, limit: u32, response: &LiveFeedResponse, now: i64) {
        if let Ok(mut cached) = self.cached.write() {
            *cached = Some(CachedSnapshot {
                at_ms: now,
                limit,
                response: response.clone(),
            });
        }
    }
}

/// Turns `[member, score, member, score, ...]` into pairs.
fn pairs(flat: Vec<String>) -> Vec<(String, i64)> {
    let (chunks, _) = flat.as_chunks::<2>();
    chunks
        .iter()
        .filter_map(|pair| {
            pair[1]
                .parse::<i64>()
                .ok()
                .map(|score| (pair[0].clone(), score))
        })
        .collect()
}

async fn hmget(
    connection: &mut ConnectionManager,
    members: &[(String, i64)],
) -> redis::RedisResult<Vec<Option<String>>> {
    if members.is_empty() {
        return Ok(Vec::new());
    }
    let mut command = redis::cmd("HMGET");
    command.arg(FRAMES_KEY);
    for (member, _) in members {
        command.arg(member);
    }
    command.query_async(connection).await
}

/// The single publish hook. When the feed is enabled the Redis write supplies the
/// frame with its timestamps; if Redis fails, the original frame is still broadcast.
pub async fn publish(state: &AppState, tx: &NormalisedTransaction) {
    let payload = match serde_json::to_string(tx) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(?error, "failed to serialize transaction for broadcast");
            return;
        }
    };

    let payload = if state.live_feed.enabled() {
        match tokio::time::timeout(
            REDIS_CALL_TIMEOUT,
            state.live_feed.upsert_at(&tx.hash, &payload, now_ms()),
        )
        .await
        {
            Ok(Ok(frame)) => frame,
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    tx_hash = %tx.hash,
                    "live feed Redis write failed, broadcasting without feed timestamps"
                );
                payload
            }
            Err(_) => {
                tracing::warn!(
                    tx_hash = %tx.hash,
                    "live feed Redis write timed out, broadcasting without feed timestamps"
                );
                payload
            }
        }
    } else {
        payload
    };

    state.broadcast.publish(payload).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips() {
        let cursor = Cursor {
            score: 1_700_000_000_123,
            tx_hash: "abc".to_string(),
        };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn an_invalid_cursor_is_a_bad_request() {
        assert!(matches!(
            Cursor::decode("not base64!"),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn limit_defaults_and_clamps() {
        assert_eq!(parse_limit(None).unwrap(), DEFAULT_LIMIT);
        assert_eq!(parse_limit(Some("0")).unwrap(), 1);
        assert_eq!(parse_limit(Some("15")).unwrap(), 15);
        assert_eq!(parse_limit(Some("500")).unwrap(), MAX_LIMIT);
        assert_eq!(parse_limit(Some("-3")).unwrap(), 1);
        assert!(matches!(
            parse_limit(Some("nope")),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn with_scores_pairs_are_parsed() {
        let flat = vec![
            "b".to_string(),
            "200".to_string(),
            "a".to_string(),
            "100".to_string(),
        ];
        assert_eq!(
            pairs(flat),
            vec![("b".to_string(), 200), ("a".to_string(), 100)]
        );
    }

    #[test]
    fn the_breaker_admits_one_probe_per_window() {
        let breaker = Breaker::new();
        assert!(breaker.allow(1_000));
        breaker.failure(1_000);
        assert!(!breaker.allow(2_000));
        assert!(!breaker.allow(5_999));
        // The first caller after the window gets the probe, the next does not.
        assert!(breaker.allow(6_000));
        assert!(!breaker.allow(6_001));
        breaker.success();
        assert!(breaker.allow(6_002));
    }
}
