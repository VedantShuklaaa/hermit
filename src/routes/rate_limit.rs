//! In-process per-client rate limiting for the public HTTP API.
//!
//! A `governor` keyed limiter keeps one token bucket per client IP. The layer is
//! applied to the `/api` router only, so `/health` and the metrics listener (served
//! on its own port) are never limited. Counters live in this process: a multi-replica
//! deployment enforces the budget per replica, not across the fleet.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use governor::clock::{Clock, DefaultClock};
use governor::{DefaultKeyedRateLimiter, Quota};

use crate::config::ApiRateLimitConfig;
use crate::error::AppError;

/// Every this many checks, drop keys whose bucket is indistinguishable from fresh.
/// This amortizes cleanup instead of paying it on every request, and keeps the
/// keyed store bounded by the clients seen in roughly the last replenish window.
const PRUNE_EVERY: u64 = 1024;

const X_FORWARDED_FOR: &str = "x-forwarded-for";

/// The middleware state: the keyed limiter and how to identify a client.
#[derive(Clone)]
pub struct RateLimit {
    limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>,
    trust_forwarded_for: bool,
    checked: Arc<AtomicU64>,
}

impl RateLimit {
    pub fn new(config: &ApiRateLimitConfig) -> Self {
        let rps = NonZeroU32::new(config.rps.max(1)).expect("rps is at least 1");
        let burst = NonZeroU32::new(config.burst.max(1)).expect("burst is at least 1");
        Self {
            limiter: Arc::new(DefaultKeyedRateLimiter::keyed(
                Quota::per_second(rps).allow_burst(burst),
            )),
            trust_forwarded_for: config.trust_forwarded_for,
            checked: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The peer's IP, or the leftmost `X-Forwarded-For` entry when the deployment
    /// declares that header trustworthy. A listener without connect info shares one
    /// anonymous bucket rather than escaping the limit.
    fn client_key(&self, headers: &HeaderMap, request: &Request) -> IpAddr {
        if self.trust_forwarded_for
            && let Some(ip) = forwarded_ip(headers)
        {
            return ip;
        }

        request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|peer| peer.0.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    }

    fn check(&self, key: IpAddr) -> Result<(), AppError> {
        if self
            .checked
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(PRUNE_EVERY)
        {
            self.limiter.retain_recent();
        }

        self.limiter.check_key(&key).map_err(|not_until| {
            let wait = not_until.wait_time_from(DefaultClock::default().now());
            // Round up: Retry-After is whole seconds, and retrying early just earns
            // another 429.
            let retry_after_secs = (wait.as_nanos().div_ceil(1_000_000_000) as u64).max(1);
            AppError::RateLimited { retry_after_secs }
        })
    }
}

/// The leftmost entry is the original client when every proxy appends rather than
/// forwards. A malformed value falls back to the peer address.
fn forwarded_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(X_FORWARDED_FOR)?
        .to_str()
        .ok()?
        .split(',')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// Rejects the request with `429` + `Retry-After` before it reaches a handler.
pub async fn enforce(
    State(rate_limit): State<RateLimit>,
    request: Request,
    next: Next,
) -> Response {
    let key = rate_limit.client_key(request.headers(), &request);
    if let Err(error) = rate_limit.check(key) {
        return error.into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn the_leftmost_forwarded_entry_is_the_client() {
        assert_eq!(
            forwarded_ip(&headers("203.0.113.7, 10.0.0.2")),
            Some("203.0.113.7".parse().unwrap())
        );
        assert_eq!(
            forwarded_ip(&headers(" 2001:db8::1 , 10.0.0.2")),
            Some("2001:db8::1".parse().unwrap())
        );
    }

    #[test]
    fn a_malformed_forwarded_header_is_ignored() {
        assert_eq!(forwarded_ip(&headers("not-an-ip")), None);
        assert_eq!(forwarded_ip(&HeaderMap::new()), None);
    }
}
