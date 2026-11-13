//! Prometheus metrics.
//!
//! [`init`] installs the global recorder and serves `/metrics` on the metrics port.
//! The metric names and recorded labels are documented in the README "Metrics"
//! section; the constants here are the single source of truth for names.

use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

/// Metrics port when `METRICS_PORT` is unset.
pub const DEFAULT_PORT: u16 = 9100;

pub const CACHE_LOOKUPS_TOTAL: &str = "cache_lookups_total";
pub const CACHE_REFRESH_SKIPPED_TOTAL: &str = "cache_refresh_skipped_total";
pub const LIMITER_WAIT_SECONDS: &str = "limiter_wait_seconds";
pub const UPSTREAM_CALLS_TOTAL: &str = "upstream_calls_total";
pub const HTTP_REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";
pub const STREAM_LAG: &str = "stream_lag";
pub const STREAM_PENDING: &str = "stream_pending";
pub const DEAD_LETTER_DEPTH: &str = "dead_letter_depth";
pub const INGEST_AGE_SECONDS: &str = "ingest_age_seconds";
pub const INGEST_RECONNECTS_TOTAL: &str = "ingest_reconnects_total";
pub const INGEST_BACKFILL_INFLIGHT: &str = "ingest_backfill_inflight";
pub const INGEST_BACKFILL_DURATION_SECONDS: &str = "ingest_backfill_duration_seconds";
pub const INGEST_BACKFILL_PUBLISHED_TOTAL: &str = "ingest_backfill_published_total";
pub const POLL_CYCLES_TOTAL: &str = "poll_cycles_total";
pub const POLL_MISSED_TOTAL: &str = "poll_missed_total";
pub const POLL_LAST_SUCCESS_TIMESTAMP_SECONDS: &str = "poll_last_success_timestamp_seconds";
pub const EVM_BLOCKS_TOTAL: &str = "evm_blocks_total";
pub const EVM_ENDPOINT_FAILOVERS_TOTAL: &str = "evm_endpoint_failovers_total";
pub const EVM_EVENTS_TOTAL: &str = "evm_events_total";
pub const EVM_JOIN_MATCHES_TOTAL: &str = "evm_join_matches_total";
pub const EVM_REORGS_TOTAL: &str = "evm_reorgs_total";
pub const EVM_RANGE_HALVINGS_TOTAL: &str = "evm_range_halvings_total";
pub const EVM_WATCH_CURSOR_BLOCK: &str = "evm_watch_cursor_block";
pub const EVM_WATCH_TARGET_BLOCK: &str = "evm_watch_target_block";

/// Installs the Prometheus recorder and starts its HTTP listener on
/// `0.0.0.0:$METRICS_PORT` (default 9100). Every role calls this once at startup.
pub fn init() -> anyhow::Result<()> {
    let port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    describe();
    tracing::info!(%addr, "prometheus metrics listening");
    Ok(())
}

/// Registers HELP text for every metric this process can emit.
fn describe() {
    metrics::describe_counter!(
        CACHE_LOOKUPS_TOTAL,
        "Read-through cache lookups by result (memory, pg_fresh, pg_stale, cold)"
    );
    metrics::describe_counter!(
        CACHE_REFRESH_SKIPPED_TOTAL,
        "Stale-while-revalidate refreshes skipped, by reason (no_token, in_flight)"
    );
    metrics::describe_histogram!(
        LIMITER_WAIT_SECONDS,
        metrics::Unit::Seconds,
        "Time spent waiting for a provider rate-limiter token, by provider and lane"
    );
    metrics::describe_counter!(
        UPSTREAM_CALLS_TOTAL,
        "Upstream provider calls by outcome (ok, err, timeout), by provider and lane"
    );
    metrics::describe_histogram!(
        HTTP_REQUEST_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "HTTP request duration by matched route and status class"
    );
    metrics::describe_gauge!(
        STREAM_LAG,
        "Bridge stream consumer-group lag, from the health probe"
    );
    metrics::describe_gauge!(
        STREAM_PENDING,
        "Bridge stream consumer-group pending entries, from the health probe"
    );
    metrics::describe_gauge!(
        DEAD_LETTER_DEPTH,
        "Entries in the failed-transaction stream, from the health probe"
    );
    metrics::describe_gauge!(
        INGEST_AGE_SECONDS,
        "Seconds since the ingester last handled a notification, from the health probe"
    );
    metrics::describe_counter!(
        INGEST_RECONNECTS_TOTAL,
        "Solana WebSocket reconnects by program and reason (watchdog, subscribe_failed, closed, error)"
    );
    metrics::describe_gauge!(
        INGEST_BACKFILL_INFLIGHT,
        "Backfill tasks currently running next to a live ingestion connection, by program"
    );
    metrics::describe_histogram!(
        INGEST_BACKFILL_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of one backfill run for a connection, including paced publishes and cancellation, by program"
    );
    metrics::describe_counter!(
        INGEST_BACKFILL_PUBLISHED_TOTAL,
        "Signatures a backfill published to the bridge stream, dedupe suppressions excluded, by program"
    );
    metrics::describe_counter!(
        POLL_CYCLES_TOTAL,
        "Reconciliation-poll cycles by program and result (ok, error)"
    );
    metrics::describe_counter!(
        POLL_MISSED_TOTAL,
        "Signatures the reconciliation poll had to publish because no row and no recent publish existed, by program"
    );
    metrics::describe_gauge!(
        POLL_LAST_SUCCESS_TIMESTAMP_SECONDS,
        "Unix time of the last successful reconciliation-poll cycle, by program"
    );
    metrics::describe_counter!(
        EVM_BLOCKS_TOTAL,
        "Finalized EVM blocks read by a chain watcher, by chain and endpoint host"
    );
    metrics::describe_counter!(
        EVM_ENDPOINT_FAILOVERS_TOTAL,
        "Times a chain watcher moved off a failing EVM RPC endpoint, by chain and endpoint host"
    );
    metrics::describe_counter!(
        EVM_EVENTS_TOTAL,
        "Bridge events decoded from EVM logs, by chain and event kind"
    );
    metrics::describe_counter!(
        EVM_JOIN_MATCHES_TOTAL,
        "Stored EVM events joined to a bridge transfer, by chain"
    );
    metrics::describe_counter!(
        EVM_REORGS_TOTAL,
        "Reorgs detected by a chain watcher (cursor block hash no longer canonical), by chain"
    );
    metrics::describe_counter!(
        EVM_RANGE_HALVINGS_TOTAL,
        "eth_getLogs calls rejected as too large whose block range was halved, by chain"
    );
    metrics::describe_gauge!(
        EVM_WATCH_CURSOR_BLOCK,
        "Last block a chain watcher has stored events for, by chain"
    );
    metrics::describe_gauge!(
        EVM_WATCH_TARGET_BLOCK,
        "Finalized block a chain watcher is reading towards, by chain"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The metrics macros are no-ops without a recorder: a module that records
    /// before (or without) `init` must not panic.
    #[test]
    fn recording_without_an_installed_recorder_does_not_panic() {
        metrics::counter!(CACHE_LOOKUPS_TOTAL, "result" => "memory").increment(1);
        metrics::histogram!(
            LIMITER_WAIT_SECONDS,
            "provider" => "helius",
            "lane" => "api"
        )
        .record(0.001);
    }
}
