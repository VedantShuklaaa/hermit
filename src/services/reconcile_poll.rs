//! The periodic reconciliation poll (4f) and the paged scan it shares with the
//! `reconcile` bin.
//!
//! The WebSocket ingester is the low-latency path; this poll is the completeness
//! safety net. Every cycle, for each registered program, it asks
//! `getSignaturesForAddress` what touched the program in a window, subtracts the
//! signatures `bridge_transfers` already holds, and republishes the rest as normal
//! stream candidates. The consumers classify and persist them exactly as they do a
//! live notification; the Redis dedupe and idempotent upserts make a repeat safe.
//!
//! Its Helius calls use the reconcile lane (`HELIUS_RECONCILE_URL`, `RECONCILE_RPS`)
//! and report the `reconcile` metric label, so a poll cycle cannot starve the
//! ingester or the API. Per-cycle errors are logged and retried on the next cycle;
//! the task only returns on shutdown.

use anyhow::Result;
use chrono::Utc;
use governor::{Quota, RateLimiter};
use reqwest::Client;
use sqlx::PgPool;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::clients::helius;
use crate::clients::limiter::{Limiter, UpstreamLabels, call_upstream};
use crate::config::IngestConfig;
use crate::db::repository;
use crate::ingestion::types::CandidateTransaction;
use crate::protocols::Protocol;
use crate::protocols::registry::ProtocolRegistry;
use crate::redis::producer::RedisProducer;
use crate::services::reconcile::{
    PersistedTransaction, SignatureScan, Window, checkpoint_after_cycle, missing_signatures,
    poll_window,
};

/// Pages `getSignaturesForAddress` from newest to oldest until the window's start.
///
/// Shared with the `reconcile` bin: the two callers differ only in the endpoint,
/// limiter and metric lane they pass in.
pub async fn scan_signatures(
    client: &Client,
    helius_url: &str,
    limiter: &Limiter,
    lane: &'static str,
    program: &str,
    window: Window,
) -> Result<SignatureScan> {
    let mut scan = SignatureScan::default();
    let mut before: Option<String> = None;

    loop {
        let page = call_upstream(limiter, UpstreamLabels::helius(lane), || {
            helius::get_signatures_for_address(client, helius_url, program, before.clone())
        })
        .await?;

        if page.is_empty() {
            scan.push_page(&[], window);
            break;
        }

        let last = page
            .last()
            .and_then(|entry| entry["signature"].as_str())
            .map(str::to_string);
        let done = scan.push_page(&page, window);
        if done {
            break;
        }

        // `before` is exclusive. A repeated cursor would loop forever.
        match last {
            Some(last) if before.as_deref() != Some(last.as_str()) => before = Some(last),
            _ => break,
        }
    }

    Ok(scan)
}

/// The rows already persisted for `protocols` in the window. Shared with the
/// `reconcile` bin.
pub async fn persisted_rows(
    pool: &PgPool,
    protocols: &[Protocol],
    window: Window,
) -> Result<Vec<PersistedTransaction>> {
    let mut rows = Vec::new();
    for protocol in protocols {
        let found = repository::persisted_transfers_in_window(
            pool,
            protocol.as_str(),
            window.start,
            window.end,
        )
        .await?;
        rows.extend(found.into_iter().map(|row| PersistedTransaction {
            observed_tx_hash: row.observed_tx_hash,
            slot: row.slot,
            block_time: row.block_time,
            stored_at: row.stored_at,
        }));
    }
    Ok(rows)
}

/// What one poll cycle did, for the log line and the metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollCycle {
    pub window: Window,
    /// Successful signatures in the window.
    pub scanned: usize,
    /// Signatures with no persisted row, before dedupe.
    pub missing: usize,
    /// Signatures actually added to the stream (not suppressed by the Redis dedupe).
    pub published: usize,
    /// Signatures missing from the store but already published recently.
    pub skipped: usize,
    /// `INGEST_POLL_MAX_PUBLISH` stopped the cycle with signatures left over.
    pub cap_hit: bool,
}

/// The supervised periodic poll. It owns its own Helius limiter, so it never spends
/// the ingest or API lane's quota.
#[derive(Clone)]
pub struct ReconcilePoller {
    programs: Arc<ProtocolRegistry>,
    db: PgPool,
    redis: RedisProducer,
    client: Client,
    helius_url: String,
    limiter: Arc<Limiter>,
    config: IngestConfig,
}

impl ReconcilePoller {
    pub fn new(
        programs: Arc<ProtocolRegistry>,
        db: PgPool,
        redis: RedisProducer,
        client: Client,
        helius_url: String,
        reconcile_rps: u32,
        config: IngestConfig,
    ) -> Self {
        let per_second = NonZeroU32::new(reconcile_rps).unwrap_or(NonZeroU32::MIN);
        let limiter = Arc::new(RateLimiter::direct(Quota::per_second(per_second)));

        Self {
            programs,
            db,
            redis,
            client,
            helius_url,
            limiter,
            config,
        }
    }

    /// Runs one cycle immediately, then every `INGEST_POLL_SECS`, until `stop` is
    /// cancelled. A failed cycle or program is logged and retried on the next cycle;
    /// nothing here panics.
    pub async fn run(self, stop: CancellationToken) -> Result<()> {
        let programs = self.programs.ingestion_programs();
        if programs.is_empty() {
            warn!("no registered programs, the reconciliation poll has nothing to scan");
            return Ok(());
        }

        info!(
            programs = programs.len(),
            interval_secs = self.config.poll_secs,
            overlap_secs = self.config.poll_overlap_secs,
            offset_secs = self.config.poll_offset_secs,
            "starting reconciliation poll"
        );

        let interval = Duration::from_secs(self.config.poll_secs);

        loop {
            let started = Instant::now();

            for program in &programs {
                if stop.is_cancelled() {
                    return Ok(());
                }

                let now = Utc::now().timestamp();
                match self.run_cycle(program, now).await {
                    Ok(cycle) => self.record_success(program, now, &cycle),
                    Err(error) => {
                        metrics::counter!(
                            crate::metrics::POLL_CYCLES_TOTAL,
                            "program" => program.clone(),
                            "result" => "error"
                        )
                        .increment(1);
                        error!(
                            program,
                            %error,
                            "reconciliation poll cycle failed, retrying next cycle"
                        );
                    }
                }
            }

            let remaining = interval.saturating_sub(started.elapsed());
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = sleep(remaining) => {}
            }
        }
    }

    /// One program's cycle: plan the window, scan it, drop what is already stored,
    /// publish the rest, and advance the checkpoint.
    async fn run_cycle(&self, program: &str, now: i64) -> Result<PollCycle> {
        let last_poll_end = repository::get_last_poll_end(&self.db, program).await?;
        let window = poll_window(
            now,
            last_poll_end,
            self.config.poll_overlap_secs,
            self.config.poll_offset_secs,
        );

        let scan = scan_signatures(
            &self.client,
            &self.helius_url,
            &self.limiter,
            "reconcile",
            program,
            window,
        )
        .await?;

        let protocols = self.programs.protocols_for(program);
        let persisted = persisted_rows(&self.db, &protocols, window).await?;
        let missing = missing_signatures(&scan.signatures, &persisted);

        let mut published = 0usize;
        let mut skipped = 0usize;
        let mut cap_hit = false;

        for signature in &missing {
            if published >= self.config.poll_max_publish {
                cap_hit = true;
                break;
            }

            let candidate = CandidateTransaction {
                chain: "solana".to_string(),
                tx_hash: signature.signature.clone(),
                slot: signature.slot,
            };

            if self
                .redis
                .publish_transaction(&candidate, self.config.dedupe_ttl_secs)
                .await?
            {
                published += 1;
            } else {
                skipped += 1;
            }
        }

        // The checkpoint only moves forward, and the SQL upsert enforces the same
        // rule for two cycles racing each other. A capped cycle keeps the checkpoint
        // so the signatures it could not publish are covered again next time.
        if let Some(checkpoint) = checkpoint_after_cycle(last_poll_end, window, cap_hit) {
            repository::set_last_poll_end(&self.db, program, checkpoint).await?;
        }

        Ok(PollCycle {
            window,
            scanned: scan.signatures.len(),
            missing: missing.len(),
            published,
            skipped,
            cap_hit,
        })
    }

    fn record_success(&self, program: &str, now: i64, cycle: &PollCycle) {
        metrics::counter!(
            crate::metrics::POLL_CYCLES_TOTAL,
            "program" => program.to_string(),
            "result" => "ok"
        )
        .increment(1);
        metrics::gauge!(
            crate::metrics::POLL_LAST_SUCCESS_TIMESTAMP_SECONDS,
            "program" => program.to_string()
        )
        .set(now as f64);

        if cycle.published > 0 {
            metrics::counter!(
                crate::metrics::POLL_MISSED_TOTAL,
                "program" => program.to_string()
            )
            .increment(cycle.published as u64);

            warn!(
                program,
                missed = cycle.published,
                window_start = cycle.window.start,
                window_end = cycle.window.end,
                "reconciliation poll found signatures missing from the store and republished them"
            );
        }

        if cycle.cap_hit {
            warn!(
                program,
                cap = self.config.poll_max_publish,
                window_start = cycle.window.start,
                window_end = cycle.window.end,
                "reconciliation poll hit INGEST_POLL_MAX_PUBLISH, the rest waits for the next cycle"
            );
        }

        info!(
            program,
            window_start = cycle.window.start,
            window_end = cycle.window.end,
            scanned = cycle.scanned,
            missing = cycle.missing,
            published = cycle.published,
            skipped = cycle.skipped,
            cap_hit = cycle.cap_hit,
            "reconciliation poll cycle completed"
        );
    }
}
