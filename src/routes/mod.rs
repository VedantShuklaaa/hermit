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

    Router::new()
        .route("/health", get(health_handler))
        .nest("/api/v1", v1::router())
        .nest("/api/v2", v2::router(state.config.live_feed.enabled))
        .layer(cors)
        .layer(middleware::from_fn(record_http_metrics))
        .with_state(state)
}
