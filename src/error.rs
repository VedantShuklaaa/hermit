use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("invalid transaction hash: {0}")]
    InvalidTransactionHash(String),

    #[error("transaction not found: {0}")]
    TransactionNotFound(String),

    #[error("VAA not available for transaction: {0}")]
    VaaNotAvailable(String),

    /// A provider answered, but not usefully. `retryable` separates failures a retry
    /// can plausibly fix (5xx, transport, malformed response body) from answers that
    /// cannot change (invalid params, method not found, data that does not parse).
    #[error("upstream provider error ({provider}): {message}")]
    UpstreamProvider {
        provider: &'static str,
        message: String,
        retryable: bool,
    },

    #[error("upstream provider rate limited: {0}")]
    UpstreamRateLimited(&'static str),

    #[error("upstream provider timed out: {0}")]
    UpstreamTimeout(&'static str),

    /// A provider refused an `eth_getLogs` window as too large. The watcher halves
    /// its range and retries; it must not be treated as a transient retry, or the
    /// same oversized request would be sent again.
    #[error("upstream provider block range too large: {0}")]
    UpstreamRangeTooLarge(&'static str),

    #[error("failed to normalise transaction data: {0}")]
    Normalisation(String),

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("service overloaded, retry shortly")]
    Overloaded,

    /// The public API's per-client limiter rejected the request before it reached a
    /// handler. `retry_after_secs` is what the limiter says is left to wait.
    #[error("rate limit exceeded, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    #[error("internal error")]
    Internal(#[from] anyhow::Error),
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
}

impl AppError {
    /// The stable error code, as the API reports it. Also the error kind in the replay tool.
    pub fn code(&self) -> &'static str {
        self.status_and_code().1
    }

    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            AppError::InvalidTransactionHash(_) => (StatusCode::BAD_REQUEST, "INVALID_TX_HASH"),
            AppError::TransactionNotFound(_) => (StatusCode::NOT_FOUND, "TX_NOT_FOUND"),
            AppError::VaaNotAvailable(_) => (StatusCode::NOT_FOUND, "VAA_NOT_AVAILABLE"),
            AppError::UpstreamProvider { .. } => (StatusCode::BAD_GATEWAY, "UPSTREAM_ERROR"),
            AppError::UpstreamRateLimited(_) => {
                (StatusCode::TOO_MANY_REQUESTS, "UPSTREAM_RATE_LIMITED")
            }
            AppError::UpstreamTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "UPSTREAM_TIMEOUT"),
            AppError::UpstreamRangeTooLarge(_) => {
                (StatusCode::BAD_GATEWAY, "UPSTREAM_RANGE_TOO_LARGE")
            }
            AppError::Normalisation(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "NORMALISATION_FAILED")
            }
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            AppError::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "OVERLOADED"),
            AppError::RateLimited { .. } => (StatusCode::TOO_MANY_REQUESTS, "RATE_LIMITED"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        }
    }

    /// Whether retrying later can plausibly succeed. A transient error must never
    /// move a stream message to the dead-letter stream: it stays pending and the
    /// recovery worker redelivers it after its idle time.
    ///
    /// `VaaNotAvailable` is permanent here even though the VAA may simply not exist
    /// yet: the consumer already waits up to 90 s for it before giving up.
    pub fn is_transient(&self) -> bool {
        match self {
            AppError::Overloaded
            | AppError::RateLimited { .. }
            | AppError::UpstreamRateLimited(_)
            | AppError::UpstreamTimeout(_)
            | AppError::TransactionNotFound(_) => true,
            AppError::UpstreamProvider { retryable, .. } => *retryable,
            AppError::InvalidTransactionHash(_)
            | AppError::VaaNotAvailable(_)
            | AppError::Normalisation(_)
            | AppError::BadRequest(_)
            | AppError::UpstreamRangeTooLarge(_)
            | AppError::Internal(_) => false,
        }
    }
}

/// Whether a persistence failure can be retried later. SQLx surfaces outages as
/// pool/connection/IO errors; those stay pending. Constraint violations, bad data
/// and malformed rows cannot be fixed by waiting, so they dead-letter.
pub fn persistence_error_is_transient(error: &anyhow::Error) -> bool {
    let Some(sqlx::Error::Database(db)) = error.downcast_ref::<sqlx::Error>() else {
        return match error.downcast_ref::<sqlx::Error>() {
            Some(
                sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::Io(_)
                | sqlx::Error::Protocol(_)
                | sqlx::Error::WorkerCrashed,
            ) => true,
            // Configuration, decode, row/column shape and migration errors are
            // deterministic, and anything that is not an SQLx error is unexpected.
            Some(_) | None => false,
        };
    };

    // Constraint and data exceptions do not heal on their own.
    if db.is_unique_violation() || db.is_foreign_key_violation() || db.is_check_violation() {
        return false;
    }

    // Postgres SQLSTATE class: connection, transaction rollback, resources and
    // operator intervention are worth another delivery; data, integrity, syntax
    // and access-rule classes are not.
    let Some(code) = db.code() else {
        return false;
    };
    matches!(
        code.get(..2).unwrap_or(""),
        "08" | "40" | "53" | "57" | "58"
    )
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();

        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        } else {
            tracing::warn!(error = %self, "request rejected");
        }

        let message = match &self {
            AppError::Internal(_) => "internal server error".to_string(),
            other => other.to_string(),
        };

        let mut resp = (
            status,
            Json(ErrorBody {
                error: ErrorDetail { code, message },
            }),
        )
            .into_response();

        match &self {
            AppError::RateLimited { retry_after_secs } => {
                if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
                    resp.headers_mut().insert(RETRY_AFTER, value);
                }
            }
            AppError::Overloaded | AppError::UpstreamRateLimited(_) => {
                resp.headers_mut()
                    .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            }
            _ => {}
        }
        resp
    }
}

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            AppError::UpstreamTimeout("unknown")
        } else if err.status().map(|s| s.as_u16()) == Some(429) {
            AppError::UpstreamRateLimited("unknown")
        } else {
            // Connect, request, body and decode failures: the transport failed, the
            // provider never gave a usable answer, so another attempt may work.
            AppError::UpstreamProvider {
                provider: "unknown",
                message: err.to_string(),
                retryable: true,
            }
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::Normalisation(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_and_transport_failures_are_transient() {
        for error in [
            AppError::Overloaded,
            AppError::RateLimited {
                retry_after_secs: 1,
            },
            AppError::UpstreamRateLimited("helius"),
            AppError::UpstreamTimeout("helius"),
            AppError::TransactionNotFound("sig".into()),
        ] {
            assert!(error.is_transient(), "{error:?} should be transient");
        }
    }

    #[test]
    fn provider_answers_are_transient_only_when_they_can_change() {
        let retryable = AppError::UpstreamProvider {
            provider: "helius",
            message: "status 503 Service Unavailable".into(),
            retryable: true,
        };
        let permanent = AppError::UpstreamProvider {
            provider: "helius",
            message: "{\"code\":-32602,\"message\":\"Invalid params\"}".into(),
            retryable: false,
        };

        assert!(retryable.is_transient());
        assert!(!permanent.is_transient());
    }

    #[test]
    fn data_and_request_failures_are_permanent() {
        for error in [
            AppError::InvalidTransactionHash("".into()),
            AppError::VaaNotAvailable("sig".into()),
            AppError::Normalisation("bad payload".into()),
            AppError::BadRequest("nope".into()),
            AppError::Internal(anyhow::anyhow!("bug")),
        ] {
            assert!(!error.is_transient(), "{error:?} should be permanent");
        }
    }

    #[test]
    fn json_rpc_error_codes_classify_requests_and_server_failures() {
        let code = |n: i64| serde_json::json!({ "code": n, "message": "m" });

        for permanent in [-32700, -32600, -32601, -32602, -32015] {
            assert!(
                !crate::clients::helius::json_rpc_error_is_retryable(&code(permanent)),
                "code {permanent} should be permanent"
            );
        }
        for transient in [-32000, -32005, -32429] {
            assert!(
                crate::clients::helius::json_rpc_error_is_retryable(&code(transient)),
                "code {transient} should be retryable"
            );
        }
        // An error without a code is the safest thing to retry.
        assert!(crate::clients::helius::json_rpc_error_is_retryable(
            &serde_json::json!({ "message": "unknown" })
        ));
    }

    #[test]
    fn pool_and_io_persistence_failures_are_transient() {
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::Io(std::io::Error::other("connection reset")),
            sqlx::Error::WorkerCrashed,
        ] {
            let error = anyhow::Error::new(error);
            assert!(
                persistence_error_is_transient(&error),
                "{error:?} should be transient"
            );
        }
    }

    #[test]
    fn deterministic_persistence_and_unexpected_failures_are_permanent() {
        for error in [
            sqlx::Error::RowNotFound,
            sqlx::Error::ColumnNotFound("missing".into()),
            sqlx::Error::ColumnIndexOutOfBounds { index: 3, len: 2 },
        ] {
            let error = anyhow::Error::new(error);
            assert!(
                !persistence_error_is_transient(&error),
                "{error:?} should be permanent"
            );
        }

        let not_sqlx = anyhow::anyhow!("something else");
        assert!(!persistence_error_is_transient(&not_sqlx));
    }
}
