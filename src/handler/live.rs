use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header::CACHE_CONTROL};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::live_feed::{self, Cursor, LiveFeedResponse};
use crate::state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct LiveQuery {
    pub limit: Option<String>,
    pub before: Option<String>,
}

/// `GET /api/v2/transfers/live`: one page from the Redis ring, newest first.
/// Postgres is never on this path; a Redis outage serves the last in-process page
/// for at most ten seconds, then an empty 503 body.
pub async fn live_transfers_handler(
    State(state): State<AppState>,
    Query(raw): Query<LiveQuery>,
) -> Response {
    let limit = match live_feed::parse_limit(raw.limit.as_deref()) {
        Ok(limit) => limit,
        Err(error) => return error.into_response(),
    };
    let before = match raw.before.as_deref() {
        None => None,
        Some(raw) => match Cursor::decode(raw) {
            Ok(cursor) => Some(cursor),
            Err(error) => return error.into_response(),
        },
    };

    match state
        .live_feed
        .snapshot(&state.db, limit, before.as_ref())
        .await
    {
        Ok(body) => with_cache_control(
            Json(body).into_response(),
            "public, s-maxage=2, stale-while-revalidate=10",
        ),
        Err(error) => {
            tracing::warn!(%error, "live feed snapshot unavailable");
            let body = LiveFeedResponse::empty(live_feed::now_ms());
            with_cache_control(
                (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response(),
                "no-store",
            )
        }
    }
}

fn with_cache_control(mut response: Response, value: &'static str) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(value));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;

    #[test]
    fn the_limit_is_clamped_before_reaching_the_feed() {
        assert_eq!(live_feed::parse_limit(Some("500")).unwrap(), 100);
    }

    #[test]
    fn a_bad_before_cursor_is_rejected() {
        assert!(matches!(
            Cursor::decode("%%%"),
            Err(AppError::BadRequest(_))
        ));
    }
}
