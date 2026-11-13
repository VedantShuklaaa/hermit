mod rate_limit;
pub mod v1;
mod v2;

use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{HeaderValue, Method, Request};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::handler::health::health_handler;
use crate::state::AppState;

fn build_cors(allowed_origins: &[String]) -> CorsLayer {
    let origins: Vec<HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| o.parse::<HeaderValue>().ok())
        .collect();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(Any)
}

/// Records one request in `http_request_duration_seconds`. The route label is the
/// router's matched path pattern, never the raw URI, so path parameters cannot
/// blow up label cardinality; requests that match no route are `unmatched`.
async fn record_http_metrics(request: Request<Body>, next: Next) -> Response {
    let started = Instant::now();
    let matched = request.extensions().get::<MatchedPath>().cloned();

    let response = next.run(request).await;

    let route = matched
        .as_ref()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let status_class = match response.status().as_u16() / 100 {
        2 => "2xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    };

    metrics::histogram!(
        crate::metrics::HTTP_REQUEST_DURATION_SECONDS,
        "route" => route,
        "status_class" => status_class
    )
    .record(started.elapsed().as_secs_f64());

    response
}

pub fn build_router(state: AppState) -> Router {
    let cors = build_cors(&state.config.allowed_origins);
    let rate_limit = rate_limit::RateLimit::new(&state.config.api_rate_limit);

    // Only /api/** passes through the limiter. /health stays outside this router, and
    // Prometheus metrics are served by their own listener, so neither is limited.
    let api = Router::new()
        .nest("/v1", v1::router())
        .nest("/v2", v2::router(state.config.live_feed.enabled))
        .layer(middleware::from_fn_with_state(
            rate_limit,
            rate_limit::enforce,
        ));

    Router::new()
        .route("/health", get(health_handler))
        .nest("/api", api)
        .layer(cors)
        .layer(middleware::from_fn(record_http_metrics))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::http::{StatusCode, header::RETRY_AFTER};
    use std::net::SocketAddr;

    /// Serves the real router, with connect info so the limiter can key by peer IP.
    async fn serve(config: AppConfig) -> String {
        let app = build_router(AppState::test_state(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a test port");
        let addr = listener.local_addr().expect("test address");
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .expect("test server");
        });
        format!("http://{addr}")
    }

    /// One request at a time, so the second one must be rejected.
    fn tight_config() -> AppConfig {
        let mut config = AppConfig::test_config();
        config.api_rate_limit.rps = 1;
        config.api_rate_limit.burst = 1;
        config
    }

    /// An unsupported chain is rejected by the handler before any cache or database
    /// read, so the request is fast and never touches the network.
    fn analyse_url(base: &str) -> String {
        format!("{base}/api/v1/transactions/analyse?transaction_hash=x&chain=ethereum")
    }

    #[tokio::test]
    async fn the_public_api_answers_429_with_retry_after_once_the_burst_is_spent() {
        let base = serve(tight_config()).await;
        let client = reqwest::Client::new();
        let url = analyse_url(&base);

        let allowed = client.get(&url).send().await.expect("first request");
        assert_eq!(allowed.status(), StatusCode::BAD_REQUEST);

        let limited = client.get(&url).send().await.expect("second request");
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = limited
            .headers()
            .get(RETRY_AFTER)
            .expect("429 carries Retry-After")
            .to_str()
            .expect("Retry-After is ASCII")
            .parse::<u64>()
            .expect("Retry-After is whole seconds");
        assert!(retry_after >= 1, "Retry-After was {retry_after}");

        let body: serde_json::Value = limited.json().await.expect("JSON error body");
        assert_eq!(body["error"]["code"], "RATE_LIMITED");
    }

    #[tokio::test]
    async fn health_and_metrics_are_exempt_from_the_api_limit() {
        let base = serve(tight_config()).await;
        let client = reqwest::Client::new();
        let url = analyse_url(&base);

        let allowed = client.get(&url).send().await.expect("first request");
        assert_eq!(allowed.status(), StatusCode::BAD_REQUEST);
        let limited = client.get(&url).send().await.expect("second request");
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

        // /health is outside the limited router. With no database in this state it
        // answers 503, but it must never be the limiter's 429.
        let health = client
            .get(format!("{base}/health"))
            .send()
            .await
            .expect("/health");
        assert_ne!(health.status(), StatusCode::TOO_MANY_REQUESTS);

        // Metrics are served on their own listener, so this router has no such route.
        let metrics = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("/metrics");
        assert_eq!(metrics.status(), StatusCode::NOT_FOUND);
    }
}
