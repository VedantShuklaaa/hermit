use crate::error::AppError;
use governor::{
    RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use metrics::{counter, histogram};
use std::future::Future;
use std::time::{Duration, Instant};

pub type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

fn is_retryable(e: &AppError) -> bool {
    matches!(
        e,
        AppError::UpstreamRateLimited(_)
            | AppError::UpstreamTimeout(_)
            | AppError::UpstreamProvider {
                retryable: true,
                ..
            }
    )
}

/// How long an API request waits for a Helius token before it is shed.
pub const API_TOKEN_WAIT: Duration = Duration::from_secs(2);

/// The static label pair every limiter metric carries. Only fixed strings, so the
/// series count stays bounded no matter what a caller passes through.
#[derive(Clone, Copy, Debug)]
pub struct UpstreamLabels {
    provider: &'static str,
    lane: &'static str,
}

impl UpstreamLabels {
    pub const fn new(provider: &'static str, lane: &'static str) -> Self {
        Self { provider, lane }
    }

    /// A Helius call on `lane` (`"api"` or `"ingest"`).
    pub const fn helius(lane: &'static str) -> Self {
        Self::new("helius", lane)
    }

    /// A WormholeScan call. One limiter is shared by every role, so the lane is
    /// reported as `"shared"`.
    pub const fn wormhole() -> Self {
        Self::new("wormhole", "shared")
    }

    /// An EVM JSON-RPC call from a chain watcher.
    pub const fn evm() -> Self {
        Self::new("evm", "watcher")
    }
}

fn record_wait(started: Instant, labels: UpstreamLabels) {
    histogram!(
        crate::metrics::LIMITER_WAIT_SECONDS,
        "provider" => labels.provider,
        "lane" => labels.lane
    )
    .record(started.elapsed().as_secs_f64());
}

fn record_upstream_call<T>(result: &Result<T, AppError>, labels: UpstreamLabels) {
    let outcome = match result {
        Ok(_) => "ok",
        Err(AppError::UpstreamTimeout(_)) => "timeout",
        Err(_) => "err",
    };
    counter!(
        crate::metrics::UPSTREAM_CALLS_TOTAL,
        "provider" => labels.provider,
        "lane" => labels.lane,
        "outcome" => outcome
    )
    .increment(1);
}

/// Takes one token from the limiter, failing fast with `Overloaded` after the API
/// wait instead of queueing forever.
pub async fn acquire_token(limiter: &Limiter, labels: UpstreamLabels) -> Result<(), AppError> {
    acquire_token_within(limiter, API_TOKEN_WAIT, labels).await
}

/// Same, with the caller's own patience: the API sheds quickly, a background worker
/// can wait for its turn because its message stays in the stream meanwhile.
pub async fn acquire_token_within(
    limiter: &Limiter,
    max_wait: Duration,
    labels: UpstreamLabels,
) -> Result<(), AppError> {
    let started = Instant::now();
    let result = wait_for_token(limiter, max_wait).await;
    record_wait(started, labels);
    result
}

async fn wait_for_token(limiter: &Limiter, max_wait: Duration) -> Result<(), AppError> {
    tokio::time::timeout(max_wait, limiter.until_ready())
        .await
        .map_err(|_| AppError::Overloaded)?;
    Ok(())
}

/// Runs an upstream call behind a rate limiter, retrying transient failures
/// with exponential backoff + jitter. Every attempt (including retries) goes
/// through the limiter, so retries can't exceed the provider's rate limit.
pub async fn call_upstream<T, F, Fut>(
    limiter: &Limiter,
    labels: UpstreamLabels,
    f: F,
) -> Result<T, AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AppError>>,
{
    call_upstream_inner(limiter, labels, false, f).await
}

/// Like [`call_upstream`], but the first attempt runs without waiting on the
/// limiter: the caller already took its token with [`acquire_token`]. Retries
/// still go through the limiter.
pub async fn call_upstream_after_token<T, F, Fut>(
    limiter: &Limiter,
    labels: UpstreamLabels,
    f: F,
) -> Result<T, AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AppError>>,
{
    call_upstream_inner(limiter, labels, true, f).await
}

async fn call_upstream_inner<T, F, Fut>(
    limiter: &Limiter,
    labels: UpstreamLabels,
    first_attempt_paid: bool,
    mut f: F,
) -> Result<T, AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AppError>>,
{
    let mut delay = Duration::from_millis(200);
    let mut attempt = 0;
    loop {
        if !(first_attempt_paid && attempt == 0) {
            let started = Instant::now();
            let token = wait_for_token(limiter, API_TOKEN_WAIT).await;
            record_wait(started, labels);
            token?;
        }

        let result = f().await;
        record_upstream_call(&result, labels);

        match result {
            Err(e) if is_retryable(&e) && attempt < 3 => {
                let jitter = rand::random::<u64>() % (delay.as_millis() as u64 / 2 + 1);
                tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
                delay *= 2;
                attempt += 1;
            }
            other => return other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::Quota;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    const TEST_LABELS: UpstreamLabels = UpstreamLabels::new("helius", "api");

    /// One token every 10 s: after the burst is spent, the next token is well
    /// beyond the 2 s fail-fast window.
    fn slow_limiter() -> Limiter {
        RateLimiter::direct(Quota::with_period(Duration::from_secs(10)).unwrap())
    }

    #[tokio::test]
    async fn acquire_token_sheds_before_the_call_starts() {
        let limiter = slow_limiter();
        limiter.check().expect("the burst token is available");

        // The caller only starts the upstream call when the token was taken.
        let called = AtomicBool::new(false);
        let started = Instant::now();
        let token = acquire_token(&limiter, TEST_LABELS).await;
        if token.is_ok() {
            called.store(true, Ordering::SeqCst);
        }

        assert!(matches!(token, Err(AppError::Overloaded)));
        assert!(!called.load(Ordering::SeqCst));
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(2),
            "shed after only {waited:?}"
        );
        assert!(
            waited < Duration::from_secs(5),
            "shed after {waited:?}, much later than the 2 s timeout"
        );
    }

    #[tokio::test]
    async fn retry_after_paid_first_attempt_still_waits_on_the_limiter() {
        let limiter = slow_limiter();
        acquire_token(&limiter, TEST_LABELS)
            .await
            .expect("token is available");

        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let result = call_upstream_after_token(&limiter, TEST_LABELS, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(AppError::UpstreamTimeout("test")) }
        })
        .await;

        // The paid first attempt runs; the retry must take a token, none arrives
        // within 2 s, so it sheds. The elapsed time includes the backoff first.
        assert!(matches!(result, Err(AppError::Overloaded)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(2),
            "shed after only {waited:?}"
        );
        assert!(
            waited < Duration::from_secs(5),
            "shed after {waited:?}, much later than the 2 s timeout"
        );
    }

    #[tokio::test]
    async fn paid_first_attempt_does_not_wait_on_the_limiter() {
        let limiter = slow_limiter();
        acquire_token(&limiter, TEST_LABELS)
            .await
            .expect("token is available");

        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let result = call_upstream_after_token(&limiter, TEST_LABELS, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, AppError>("ok") }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the paid first attempt waited {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_longer_wait_gets_the_token_a_short_one_would_miss() {
        let limiter =
            RateLimiter::direct(governor::Quota::with_period(Duration::from_millis(300)).unwrap());
        limiter.check().expect("the burst token is free");

        let short = acquire_token_within(&limiter, Duration::from_millis(50), TEST_LABELS).await;
        assert!(matches!(short, Err(AppError::Overloaded)));

        acquire_token_within(&limiter, Duration::from_secs(2), TEST_LABELS)
            .await
            .expect("the next token arrives within the wait");
    }
}
