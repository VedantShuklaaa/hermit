use crate::domain::bridge_transfer::BridgeStatus;
use axum::body::Bytes;
use dashmap::{DashMap, DashSet};
use metrics::Counter;
use moka::future::Cache;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::db::repository::{
    get_analysis_any_age, get_transaction_with_transfers, persist_analysis,
};
use crate::domain::api::TransactionResponse;
use crate::domain::bridge::BridgeEvent;
use crate::domain::transaction::NormalisedTransaction;
use crate::error::AppError;
use crate::routes::v1::compat;
use crate::services::analyzer::{analyse_tx, analyse_tx_prepaid};
use crate::services::api_v2;
use crate::state::AppState;

const FRESH_SECS: f64 = 60.0;
const MAX_BACKGROUND_REFRESHES: usize = 8;

#[derive(Clone)]
enum Neg {
    NotFound,
    VaaNotAvailable,
    RateLimited,
    Timeout,
    Provider,
    Normalisation(String),
}

impl Neg {
    fn from_error(e: &AppError) -> Option<Self> {
        match e {
            AppError::TransactionNotFound(_) => Some(Neg::NotFound),
            AppError::VaaNotAvailable(_) => Some(Neg::VaaNotAvailable),
            AppError::UpstreamRateLimited(_) => Some(Neg::RateLimited),
            AppError::UpstreamTimeout(_) => Some(Neg::Timeout),
            AppError::UpstreamProvider { .. } => Some(Neg::Provider),
            AppError::Normalisation(message) => Some(Neg::Normalisation(message.clone())),
            _ => None,
        }
    }

    fn into_error(self, hash: &str) -> AppError {
        match self {
            Neg::NotFound => AppError::TransactionNotFound(hash.to_string()),
            Neg::VaaNotAvailable => AppError::VaaNotAvailable(hash.to_string()),
            Neg::RateLimited => AppError::UpstreamRateLimited("cached"),
            Neg::Timeout => AppError::UpstreamTimeout("cached"),
            Neg::Provider => AppError::UpstreamProvider {
                provider: "cached",
                message: "recent upstream failure".to_string(),
                retryable: true,
            },
            Neg::Normalisation(message) => AppError::Normalisation(message),
        }
    }
}

/// One key for every per-chain memo: the chain slug lowercased, so `Solana` and
/// `solana` share single-flight, the refresh cooldown and the negative caches.
fn normalised_key(chain: &str, hash: &str) -> String {
    format!("{}:{hash}", chain.to_ascii_lowercase())
}

/// A `/api/v2` response and the legacy `bridge_event` `/api/v1` still exposes. The
/// event is not part of the generalised model; keeping it here lets the compatibility
/// mapper reproduce the frozen v1 response exactly.
#[derive(Clone)]
pub struct V2Analysis {
    response: TransactionResponse,
    bridge_event: Option<BridgeEvent>,
    v1_body: Option<Bytes>,
    v2_body: Option<Bytes>,
}

impl V2Analysis {
    /// Fills the cache entry. The `/api/v1` body is serialized once here, so a hit
    /// clones a `Bytes` handle instead of remapping and re-serializing.
    // v2-only misses pay this v1 serialization once too; that cost is cold-path only.
    pub fn new(response: TransactionResponse, bridge_event: Option<BridgeEvent>) -> Self {
        let v1_body = compat::to_v1(&response, bridge_event.clone())
            .ok()
            .and_then(|tx| serde_json::to_vec(&tx).ok())
            .map(Bytes::from);
        // Same bytes axum's `Json` would produce per request; cold-path cost only.
        let v2_body = serde_json::to_vec(&response).ok().map(Bytes::from);
        Self {
            response,
            bridge_event,
            v1_body,
            v2_body,
        }
    }

    pub fn response(&self) -> &TransactionResponse {
        &self.response
    }

    pub fn bridge_event(&self) -> Option<&BridgeEvent> {
        self.bridge_event.as_ref()
    }

    pub fn v1_body(&self) -> Option<&Bytes> {
        self.v1_body.as_ref()
    }

    pub fn v2_body(&self) -> Option<&Bytes> {
        self.v2_body.as_ref()
    }
}

pub struct ReadThrough {
    inflight: DashMap<String, Arc<Mutex<()>>>,
    upstream_permits: Arc<Semaphore>,
    completed: Cache<String, NormalisedTransaction>,
    open: Cache<String, NormalisedTransaction>,
    v2_completed: Cache<String, Arc<V2Analysis>>,
    v2_open: Cache<String, Arc<V2Analysis>>,
    // negative caches
    not_found: Cache<String, Neg>, // tx doesn't exist: longer TTL
    failed: Cache<String, Neg>,    // transient or normalisation failure: short TTL
    // stale-while-revalidate
    refresh_permits: Arc<Semaphore>,
    /// Keys with a refresh currently running. Claimed atomically so concurrent hits
    /// for one key can only ever start one refresh; released by an RAII guard.
    refresh_inflight: Arc<DashSet<String>>,
    refresh_cooldown: Cache<String, ()>, // at most one refresh attempt per key per TTL
    refresh_stats: RefreshStats,
}

/// Counters for the background refresh admission decisions, so a benchmark can
/// show how many refreshes ran and why the others were skipped.
#[derive(Debug, Default)]
pub struct RefreshStats {
    spawned: AtomicU64,
    skipped_no_token: AtomicU64,
    skipped_inflight: AtomicU64,
}

impl RefreshStats {
    pub fn spawned(&self) -> u64 {
        self.spawned.load(Ordering::Relaxed)
    }

    pub fn skipped_no_token(&self) -> u64 {
        self.skipped_no_token.load(Ordering::Relaxed)
    }

    pub fn skipped_inflight(&self) -> u64 {
        self.skipped_inflight.load(Ordering::Relaxed)
    }
}

/// Cached metric handles. The label sets are fixed, so each handle is looked up
/// once instead of on every hot-path record.
struct CacheMetrics {
    memory: Counter,
    pg_fresh: Counter,
    pg_stale: Counter,
    cold: Counter,
    skipped_no_token: Counter,
    skipped_inflight: Counter,
}

impl CacheMetrics {
    fn get() -> &'static Self {
        static METRICS: OnceLock<CacheMetrics> = OnceLock::new();
        METRICS.get_or_init(|| Self {
            memory: metrics::counter!(
                crate::metrics::CACHE_LOOKUPS_TOTAL, "result" => "memory"
            ),
            pg_fresh: metrics::counter!(
                crate::metrics::CACHE_LOOKUPS_TOTAL, "result" => "pg_fresh"
            ),
            pg_stale: metrics::counter!(
                crate::metrics::CACHE_LOOKUPS_TOTAL, "result" => "pg_stale"
            ),
            cold: metrics::counter!(
                crate::metrics::CACHE_LOOKUPS_TOTAL, "result" => "cold"
            ),
            skipped_no_token: metrics::counter!(
                crate::metrics::CACHE_REFRESH_SKIPPED_TOTAL, "reason" => "no_token"
            ),
            skipped_inflight: metrics::counter!(
                crate::metrics::CACHE_REFRESH_SKIPPED_TOTAL, "reason" => "in_flight"
            ),
        })
    }
}

/// Removes the key from the in-flight set when the refresh finishes, fails or
/// panics.
struct RefreshGuard {
    inflight: Arc<DashSet<String>>,
    key: String,
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.inflight.remove(&self.key);
    }
}

/// Everything a background refresh holds while it runs: a background slot and a
/// shared upstream permit. Both are taken with `try_acquire`, so a refresh never
/// queues behind user requests.
struct RefreshReservation {
    _slot: OwnedSemaphorePermit,
    _upstream: OwnedSemaphorePermit,
}

impl ReadThrough {
    pub fn new(max_concurrent_upstream: usize, failed_ttl: Duration) -> Self {
        Self {
            inflight: DashMap::new(),
            upstream_permits: Arc::new(Semaphore::new(max_concurrent_upstream)),
            completed: Cache::builder()
                .max_capacity(50_000)
                .time_to_live(Duration::from_secs(600))
                .build(),
            open: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(15))
                .build(),
            v2_completed: Cache::builder()
                .max_capacity(50_000)
                .time_to_live(Duration::from_secs(600))
                .build(),
            v2_open: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(15))
                .build(),
            // lower this (e.g. 5-10s) if freshly sent txs must show up quickly
            not_found: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(30))
                .build(),
            failed: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(failed_ttl)
                .build(),
            refresh_permits: Arc::new(Semaphore::new(MAX_BACKGROUND_REFRESHES)),
            refresh_inflight: Arc::new(DashSet::new()),
            refresh_cooldown: Cache::builder()
                .max_capacity(50_000)
                .time_to_live(Duration::from_secs(15))
                .build(),
            refresh_stats: RefreshStats::default(),
        }
    }

    /// Atomically claims a key for a background refresh. `None` means another
    /// refresh for the same key is already running.
    fn begin_refresh(&self, key: &str) -> Option<RefreshGuard> {
        if !self.refresh_inflight.insert(key.to_string()) {
            return None;
        }
        Some(RefreshGuard {
            inflight: Arc::clone(&self.refresh_inflight),
            key: key.to_string(),
        })
    }

    /// Counters for spawned, skipped-without-token and skipped-already-in-flight
    /// refreshes.
    pub fn refresh_stats(&self) -> &RefreshStats {
        &self.refresh_stats
    }
}

/// DB lookup of any stored analysis. Returns (analysis, is_fresh).
async fn lookup(state: &AppState, hash: &str) -> Option<(NormalisedTransaction, bool)> {
    match get_analysis_any_age(&state.db, hash, FRESH_SECS).await {
        Ok(Some(c)) => serde_json::from_value(c.analysis)
            .map(|tx| (tx, c.fresh))
            .map_err(|e| tracing::warn!(error = %e, "cached analysis has stale shape, ignoring"))
            .ok(),
        Ok(None) => None,
        Err(e) => {
            tracing::warn!(error = %e, "analysis lookup failed");
            None
        }
    }
}

async fn remember(rt: &ReadThrough, hash: &str, tx: &NormalisedTransaction) {
    let done = matches!(
        tx.bridge_transfer.as_ref().map(|b| &b.status),
        Some(BridgeStatus::Completed)
    );
    let cache = if done { &rt.completed } else { &rt.open };
    cache.insert(hash.to_string(), tx.clone()).await;
}

/// Memory-only check: positive caches first, then negative caches.
async fn check_memory(
    rt: &ReadThrough,
    key: &str,
    hash: &str,
) -> Option<Result<NormalisedTransaction, AppError>> {
    if let Some(hit) = rt.completed.get(hash).await {
        CacheMetrics::get().memory.increment(1);
        return Some(Ok(hit));
    }
    if let Some(hit) = rt.open.get(hash).await {
        CacheMetrics::get().memory.increment(1);
        return Some(Ok(hit));
    }
    if let Some(n) = rt.not_found.get(key).await {
        CacheMetrics::get().memory.increment(1);
        return Some(Err(n.into_error(hash)));
    }
    if let Some(n) = rt.failed.get(key).await {
        CacheMetrics::get().memory.increment(1);
        return Some(Err(n.into_error(hash)));
    }
    None
}

/// Reserves the resources a refresh needs, without ever waiting: a background
/// slot, a shared upstream permit and one provider token each for Helius and
/// WormholeScan. `None` means something is busy right now.
fn try_reserve_refresh(state: &AppState) -> Option<RefreshReservation> {
    let rt = state.read_through();
    let slot = rt.refresh_permits.clone().try_acquire_owned().ok()?;
    let upstream = rt.upstream_permits.clone().try_acquire_owned().ok()?;
    // The tokens pay for the first upstream attempt of each call; a refresh that
    // cannot get one is skipped instead of queueing behind user traffic.
    state.helius_limiter.check().ok()?;
    state.wormhole_limiter.check().ok()?;
    Some(RefreshReservation {
        _slot: slot,
        _upstream: upstream,
    })
}

/// Runs the fetch under the admission guard and sets the cooldown on completion,
/// whether it succeeded or failed. The guard is released only after the cooldown
/// is set, so a concurrent hit cannot slip through and start a duplicate refresh.
async fn complete_refresh<T, F, Fut>(
    rt: &ReadThrough,
    key: &str,
    _guard: RefreshGuard,
    fetch: F,
) -> Result<T, AppError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, AppError>>,
{
    let result = fetch().await;
    rt.refresh_cooldown.insert(key.to_string(), ()).await;
    result
}

/// Background refresh of a stale entry. Failures are only logged: the stale
/// copy keeps being served and the cooldown spaces out the next attempt.
async fn refresh(
    state: AppState,
    key: String,
    chain: String,
    hash: String,
    guard: RefreshGuard,
    _reservation: RefreshReservation,
) {
    let rt = state.read_through();

    let fetched = complete_refresh(rt, &key, guard, || {
        // The reservation already paid the first Helius and WormholeScan token.
        analyse_tx_prepaid(&state, &chain, &hash)
    })
    .await;

    match fetched {
        Ok(analysed) if analysed.tx.bridge_transfer.is_some() || !analysed.transfers.is_empty() => {
            if let Err(e) = persist_analysis(&state.db, &analysed.tx, &analysed.transfers).await {
                tracing::warn!(error = %e, hash = %hash, "refresh write-back failed");
            }
            remember(rt, &hash, &analysed.tx).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, hash = %hash, "background refresh failed, still serving stale");
        }
    }
}

async fn maybe_spawn_refresh(state: &AppState, key: &str, chain: &str, hash: &str) {
    let rt = state.read_through();

    if rt.refresh_cooldown.contains_key(key) {
        return;
    }

    let Some(guard) = rt.begin_refresh(key) else {
        rt.refresh_stats
            .skipped_inflight
            .fetch_add(1, Ordering::Relaxed);
        CacheMetrics::get().skipped_inflight.increment(1);
        tracing::debug!(key = %key, "background refresh skipped: already in flight");
        return;
    };

    let Some(reservation) = try_reserve_refresh(state) else {
        rt.refresh_stats
            .skipped_no_token
            .fetch_add(1, Ordering::Relaxed);
        CacheMetrics::get().skipped_no_token.increment(1);
        tracing::debug!(key = %key, "background refresh skipped: no upstream capacity right now");
        // `guard` drops here, so a later hit can retry; no cooldown is set.
        return;
    };

    rt.refresh_stats.spawned.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(key = %key, "background refresh spawned");

    let state = state.clone();
    let key = key.to_string();
    let chain = chain.to_string();
    let hash = hash.to_string();
    tokio::spawn(async move {
        refresh(state, key, chain, hash, guard, reservation).await;
    });
}

fn is_valid_solana_signature(s: &str) -> bool {
    bs58::decode(s)
        .into_vec()
        .map(|v| v.len() == 64)
        .unwrap_or(false)
}

pub async fn analyse_cached(
    state: &AppState,
    chain: &str,
    hash: &str,
) -> Result<NormalisedTransaction, AppError> {
    if chain == "solana" && !is_valid_solana_signature(hash) {
        return Err(AppError::InvalidTransactionHash(hash.to_string()));
    }

    let rt = state.read_through();
    let key = normalised_key(chain, hash);

    // 1: fast path, memory only (hits and remembered failures)
    if let Some(r) = check_memory(rt, &key, hash).await {
        return r;
    }

    let lock = rt
        .inflight
        .entry(key.clone())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let _guard = lock.lock().await;

    let result = async {
        // someone ahead of us may have filled a cache (or recorded a failure)
        if let Some(r) = check_memory(rt, &key, hash).await {
            return r;
        }

        if let Some((hit, fresh)) = lookup(state, hash).await {
            if fresh {
                CacheMetrics::get().pg_fresh.increment(1);
            } else {
                CacheMetrics::get().pg_stale.increment(1);
            }
            remember(rt, hash, &hit).await;
            if !fresh {
                maybe_spawn_refresh(state, &key, chain, hash).await;
            }
            return Ok(hit);
        }

        // nothing stored anywhere: real cold fetch, bounded wait for a permit
        CacheMetrics::get().cold.increment(1);
        let fetched = async {
            let _permit =
                tokio::time::timeout(Duration::from_secs(2), rt.upstream_permits.acquire())
                    .await
                    .map_err(|_| AppError::Overloaded)?
                    .expect("semaphore closed");
            analyse_tx(state, chain, hash).await
        }
        .await;

        match &fetched {
            Ok(analysed) => {
                // A protocol with no v1 body (CCTP) still persists its decoded rows.
                if analysed.tx.bridge_transfer.is_some() || !analysed.transfers.is_empty() {
                    if let Err(e) =
                        persist_analysis(&state.db, &analysed.tx, &analysed.transfers).await
                    {
                        tracing::warn!(error = %e, hash, "write-back failed");
                    }
                    remember(rt, hash, &analysed.tx).await; // 3: fill after fetch
                }
            }
            Err(e) => {
                if let Some(neg) = Neg::from_error(e) {
                    let cache = if matches!(neg, Neg::NotFound) {
                        &rt.not_found
                    } else {
                        &rt.failed
                    };
                    cache.insert(key.clone(), neg).await;
                }
            }
        }
        // v1 only needs the transaction body here; every decoded transfer has been
        // written by `persist_analysis` above.
        fetched.map(|analysed| analysed.tx)
    }
    .await;

    // only remove the entry if it's still ours
    rt.inflight.remove_if(&key, |_, v| Arc::ptr_eq(v, &lock));
    result
}

/// The `/api/v2` read path. It reuses the v1 freshness, single-flight and negative-cache
/// machinery (so a cold analysis is done once), then serves every stored transfer of the
/// transaction from Postgres.
pub async fn analyse_cached_v2(
    state: &AppState,
    chain: &str,
    hash: &str,
) -> Result<Arc<V2Analysis>, AppError> {
    let rt = state.read_through();

    if let Some(hit) = rt.v2_completed.get(hash).await {
        CacheMetrics::get().memory.increment(1);
        return Ok(hit);
    }
    if let Some(hit) = rt.v2_open.get(hash).await {
        CacheMetrics::get().memory.increment(1);
        return Ok(hit);
    }

    // Ensures the transaction has been analysed and persisted, with the same caching,
    // single-flight and upstream protection as v1.
    let legacy = analyse_cached(state, chain, hash).await?;

    let snapshot = state.chains.snapshot();
    let response = match get_transaction_with_transfers(&state.db, hash).await {
        Ok(Some(stored)) => api_v2::assemble(stored, &snapshot)?,
        // A transaction with no stored transfer rows: the analysis is the only source.
        Ok(None) => api_v2::from_v1(&legacy, &snapshot),
        Err(error) => return Err(AppError::Internal(error)),
    };

    let entry = Arc::new(V2Analysis::new(response, legacy.bridge_event));
    let cache = if entry.response().is_complete() {
        &rt.v2_completed
    } else {
        &rt.v2_open
    };
    cache.insert(hash.to_string(), entry.clone()).await;
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_lowercases_the_chain_so_casing_cannot_split_the_caches() {
        assert_eq!(normalised_key("Solana", "sig"), "solana:sig");
        assert_eq!(
            normalised_key("SOLANA", "sig"),
            normalised_key("solana", "sig")
        );
        assert_ne!(
            normalised_key("solana", "sig"),
            normalised_key("ethereum", "sig")
        );
    }

    #[test]
    fn normalisation_failures_are_negative_cached_with_their_message() {
        let neg = Neg::from_error(&AppError::Normalisation("bad vaa".into()))
            .expect("normalisation errors are cached");
        match neg.into_error("sig") {
            AppError::Normalisation(message) => assert_eq!(message, "bad vaa"),
            other => panic!("expected a normalisation error, got {other:?}"),
        }
    }

    #[test]
    fn the_failed_cache_ttl_comes_from_the_constructor() {
        let rt = ReadThrough::new(4, Duration::from_secs(10));
        assert_eq!(
            rt.failed.policy().time_to_live(),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn concurrent_admissions_for_one_key_admit_exactly_one_refresh() {
        use std::sync::Barrier;

        let rt = Arc::new(ReadThrough::new(4, Duration::from_secs(10)));
        let key = "solana:sig";
        let threads = 8;
        let start = Arc::new(Barrier::new(threads));
        let attempted = Arc::new(Barrier::new(threads));

        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let rt = Arc::clone(&rt);
                let start = Arc::clone(&start);
                let attempted = Arc::clone(&attempted);
                std::thread::spawn(move || {
                    start.wait();
                    let guard = rt.begin_refresh(key);
                    // Keep the guard alive until every thread has tried, so an early
                    // drop cannot hand the key to a later caller and mask the race.
                    attempted.wait();
                    guard.is_some()
                })
            })
            .collect();

        let admitted: usize = handles
            .into_iter()
            .map(|handle| usize::from(handle.join().expect("admission thread")))
            .sum();
        assert_eq!(admitted, 1, "exactly one refresh may claim the key");
    }

    #[tokio::test]
    async fn an_exhausted_limiter_skips_the_refresh_and_leaves_the_permit_for_users() {
        let mut config = crate::config::AppConfig::test_config();
        config.helius_api_rps = 1;
        config.wormhole_rps = 1;
        config.upstream_permits = 1;
        let state = AppState::test_state(config);
        let key = "solana:sig";

        // Spend the single burst token of each provider.
        assert!(state.helius_limiter.check().is_ok());
        assert!(state.wormhole_limiter.check().is_ok());

        maybe_spawn_refresh(&state, key, "solana", "sig").await;

        let rt = state.read_through();
        assert_eq!(rt.refresh_stats().spawned(), 0);
        assert_eq!(rt.refresh_stats().skipped_no_token(), 1);
        assert_eq!(rt.refresh_stats().skipped_inflight(), 0);
        // A skip leaves no in-flight marker and no cooldown behind, so the next hit
        // may retry.
        assert!(rt.refresh_inflight.get(key).is_none());
        assert!(!rt.refresh_cooldown.contains_key(key));
        // The skipped refresh holds no upstream permit: the user cold path still
        // has the only one.
        assert!(
            rt.upstream_permits.clone().try_acquire_owned().is_ok(),
            "the user cold path must still get its upstream permit"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_releases_the_guard_and_sets_the_cooldown() {
        let rt = ReadThrough::new(4, Duration::from_secs(10));
        let key = "solana:sig";
        let guard = rt.begin_refresh(key).expect("the key is free");

        let result: Result<(), AppError> = complete_refresh(&rt, key, guard, || async {
            Err(AppError::UpstreamTimeout("test"))
        })
        .await;

        assert!(result.is_err());
        assert!(
            rt.refresh_inflight.get(key).is_none(),
            "the in-flight guard must be released after the error"
        );
        assert!(
            rt.refresh_cooldown.contains_key(key),
            "completion must set the cooldown"
        );
    }
}
