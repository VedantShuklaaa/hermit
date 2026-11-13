//! Background re-enrichment. A transfer is almost always `Detected` or `Pending` when it
//! is first seen, because the VAA or the destination tx does not exist yet. Without this
//! pass only a user calling `/analyse` moves it forward, so rows nobody asks about would
//! stay stale. Each pass picks open rows that are old enough, asks the enrichment
//! provider again and persists the result.
//!
//! The same pass also revisits completed inbound rows whose origin tx is still unknown.
//! A redeem is stored `Completed` at detection; WormholeScan's matched operation fills
//! `source_tx_hash` when it learns it, and that can be after the first analysis. Those
//! rows get no `max_age_secs`: the origin tx may become known at any time, and without
//! the backfill the row would stay incomplete forever. The backfill keeps a guaranteed
//! share of the batch (`REENRICH_ORIGIN_SHARE`) so open rows cannot starve it.
//!
//! `last_enrich_attempt_at` is the backfill's last-attempt marker. It is written for
//! every attempt, including an unreachable provider and an unchanged outcome, so the
//! row moves out of the retry window and the queue rotates. It starts NULL, so rows
//! whose cached analysis a repair migration dropped are tried first; the transaction is
//! rebuilt from the stored row before enrichment. Open rows still use `updated_at`,
//! which every answered attempt bumps by persisting. The upsert never moves a status
//! backwards.

use super::{EnrichInput, EnrichOutcome, Enriched, Prefetch};
use crate::config::ReenrichConfig;
use crate::db::repository::{
    DueTransfer, list_missing_origin_transfers, list_open_transfers, persist_transaction,
    record_origin_attempt,
};
use crate::domain::bridge_transfer::{BridgeStatus, BridgeTransfer};
use crate::domain::transaction::NormalisedTransaction;
use crate::protocols::Direction;
use crate::state::AppState;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Stop a pass early after this many provider outages in a row: it is down or
/// rate-limited, and asking for more rows only burns the limiter.
const MAX_UNAVAILABLE_STREAK: u32 = 3;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassStats {
    pub advanced: u32,
    /// Inbound rows that learned their origin tx, counted in addition to
    /// `advanced`/`unchanged`.
    pub origin_filled: u32,
    pub unchanged: u32,
    pub unavailable: u32,
    pub failed: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum Step {
    Advanced,
    Unchanged,
    Unavailable,
}

fn rank(status: &BridgeStatus) -> u8 {
    match status {
        BridgeStatus::Detected => 0,
        BridgeStatus::Pending => 1,
        BridgeStatus::Completed => 2,
    }
}

/// Puts the enriched transfer (and event) back into the transaction. Returns what the
/// attempt achieved. Pure, so the persistence decision is testable without a database.
fn settle(
    tx: &mut NormalisedTransaction,
    transfer: BridgeTransfer,
    status_before: u8,
    enriched: Enriched,
) -> Step {
    let step = if enriched.outcome == EnrichOutcome::Unavailable {
        Step::Unavailable
    } else if rank(&transfer.status) > status_before {
        Step::Advanced
    } else {
        Step::Unchanged
    };
    if step != Step::Unavailable
        && let Some(event) = enriched.bridge_event
    {
        tx.bridge_event = Some(event);
    }
    tx.bridge_transfer = Some(transfer);
    step
}

/// One attempt. Returns the step and whether the attempt filled a missing origin tx.
async fn reenrich_one(state: &AppState, row: DueTransfer) -> anyhow::Result<(Step, bool)> {
    let observed_tx_hash = row.observed_tx_hash.clone();
    let Some(mut tx) = row.transaction() else {
        anyhow::bail!("no usable analysis to re-enrich");
    };
    let Some(mut transfer) = tx.bridge_transfer.take() else {
        return Ok((Step::Unchanged, false));
    };

    let origin_missing =
        transfer.direction() == Direction::Inbound && transfer.source_tx_hash.is_none();
    let status_before = rank(&transfer.status);
    let enriched = state
        .enrichment
        .enrich(
            EnrichInput {
                source_tx_hash: &observed_tx_hash,
                prefetch: Prefetch::None,
            },
            &mut transfer,
        )
        .await;

    let origin_filled = origin_missing && transfer.source_tx_hash.is_some();
    let step = settle(&mut tx, transfer, status_before, enriched);
    if step != Step::Unavailable {
        persist_transaction(&state.db, &tx).await?;
        // Status advances and origin-tx resolution are update frames. They only
        // reach the WebSocket and the Redis ring when the live feed is enabled, so
        // flag-off behaviour stays exactly as before.
        let changed = step == Step::Advanced || origin_filled;
        if changed && state.live_feed.enabled() {
            crate::live_feed::publish(state, &tx).await;
        }
    }
    Ok((step, origin_filled))
}

/// Attempts every row in `rows`, updating `stats` and the provider-outage streak. Stops
/// on shutdown or after too many outages in a row. When `record_attempt` is set, the
/// origin backfill's marker is written whatever the outcome, including an unreachable
/// provider, so the row leaves the front of the queue.
async fn attempt_rows(
    state: &AppState,
    rows: Vec<DueTransfer>,
    shutdown: &CancellationToken,
    stats: &mut PassStats,
    unavailable_streak: &mut u32,
    record_attempt: bool,
) {
    for row in rows {
        if shutdown.is_cancelled() {
            return;
        }
        let transfer_id = row.id;
        let observed_tx_hash = row.observed_tx_hash.clone();
        let result = reenrich_one(state, row).await;
        if record_attempt && let Err(error) = record_origin_attempt(&state.db, transfer_id).await {
            tracing::warn!(transfer_id, %error, "could not record origin attempt");
        }

        match result {
            Ok((step, origin_filled)) => {
                match step {
                    Step::Advanced => {
                        stats.advanced += 1;
                        *unavailable_streak = 0;
                    }
                    Step::Unchanged => {
                        stats.unchanged += 1;
                        *unavailable_streak = 0;
                    }
                    Step::Unavailable => {
                        stats.unavailable += 1;
                        *unavailable_streak += 1;
                        if *unavailable_streak >= MAX_UNAVAILABLE_STREAK {
                            tracing::warn!(
                                provider = state.enrichment.name(),
                                "provider unavailable, ending the pass early"
                            );
                            return;
                        }
                        continue;
                    }
                }
                if origin_filled {
                    stats.origin_filled += 1;
                }
            }
            Err(error) => {
                stats.failed += 1;
                tracing::warn!(signature = %observed_tx_hash, %error, "re-enrichment failed");
            }
        }
    }
}

/// One pass: open transfers first, so a backlog of missing origin txs cannot delay the
/// status convergence that existing rows need, then completed inbound rows whose origin
/// tx is still unknown. The batch is split up front, so the backfill keeps a guaranteed
/// share even when open rows are backlogged (`REENRICH_ORIGIN_SHARE`).
pub async fn run_pass(
    state: &AppState,
    cfg: &ReenrichConfig,
    shutdown: &CancellationToken,
) -> anyhow::Result<PassStats> {
    let (open_batch, origin_batch) = cfg.batch_split(cfg.batch);
    let rows = list_open_transfers(
        &state.db,
        cfg.min_age_secs,
        cfg.max_age_secs,
        cfg.retry_secs,
        open_batch,
    )
    .await?;

    let mut stats = PassStats::default();
    let mut unavailable_streak = 0;

    attempt_rows(
        state,
        rows,
        shutdown,
        &mut stats,
        &mut unavailable_streak,
        false,
    )
    .await;
    if shutdown.is_cancelled() || origin_batch == 0 {
        return Ok(stats);
    }

    let rows =
        list_missing_origin_transfers(&state.db, cfg.min_age_secs, cfg.retry_secs, origin_batch)
            .await?;
    attempt_rows(
        state,
        rows,
        shutdown,
        &mut stats,
        &mut unavailable_streak,
        true,
    )
    .await;
    Ok(stats)
}

/// Runs a pass every `interval_secs` until shutdown. A database error ends one pass,
/// not the worker.
pub async fn run(state: AppState, shutdown: CancellationToken) -> anyhow::Result<()> {
    let cfg = state.config.reenrich.clone();
    tracing::info!(?cfg, "re-enrichment worker started");

    let mut ticker = tokio::time::interval(Duration::from_secs(cfg.interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }

        match run_pass(&state, &cfg, &shutdown).await {
            Ok(stats) if stats != PassStats::default() => {
                tracing::info!(
                    advanced = stats.advanced,
                    origin_filled = stats.origin_filled,
                    unchanged = stats.unchanged,
                    unavailable = stats.unavailable,
                    failed = stats.failed,
                    "re-enrichment pass"
                );
            }
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "re-enrichment pass failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::bridge_transfer::{BridgeMessageId, ChainId};
    use crate::domain::transaction::{Chain, TxStatus};

    fn tx() -> NormalisedTransaction {
        NormalisedTransaction {
            hash: "sig".into(),
            chain: Chain::Solana,
            status: TxStatus::Success,
            slot: 1,
            timestamp: None,
            fee_lamports: 0,
            signer: None,
            bridge_event: None,
            bridge_transfer: None,
        }
    }

    fn transfer(status: BridgeStatus) -> BridgeTransfer {
        BridgeTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain: ChainId::Solana,
            source_tx_hash: Some("sig".into()),
            source_wallet: None,
            source_explorer_url: None,
            destination_chain: ChainId::Ethereum,
            destination_wallet: None,
            destination_tx_hash: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount: None,
            amount_formatted: None,
            message_id: BridgeMessageId {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 42,
            },
            status,
        }
    }

    #[test]
    fn a_status_that_rose_is_advanced_and_stored() {
        let mut t = tx();
        let step = settle(
            &mut t,
            transfer(BridgeStatus::Completed),
            rank(&BridgeStatus::Pending),
            Enriched::of(EnrichOutcome::Applied),
        );
        assert_eq!(step, Step::Advanced);
        assert!(matches!(
            t.bridge_transfer.unwrap().status,
            BridgeStatus::Completed
        ));
    }

    #[test]
    fn an_answered_attempt_without_progress_is_unchanged_but_still_persisted() {
        for outcome in [
            EnrichOutcome::NoVaa,
            EnrichOutcome::Mismatch,
            EnrichOutcome::Applied,
        ] {
            let mut t = tx();
            let step = settle(
                &mut t,
                transfer(BridgeStatus::Pending),
                rank(&BridgeStatus::Pending),
                Enriched::of(outcome),
            );
            assert_eq!(step, Step::Unchanged);
            assert!(t.bridge_transfer.is_some());
        }
    }

    #[test]
    fn an_unreachable_provider_is_not_an_answer() {
        let mut t = tx();
        let step = settle(
            &mut t,
            transfer(BridgeStatus::Detected),
            rank(&BridgeStatus::Detected),
            Enriched::of(EnrichOutcome::Unavailable),
        );
        assert_eq!(step, Step::Unavailable);
    }

    #[test]
    fn ranks_follow_the_status_order() {
        assert!(rank(&BridgeStatus::Detected) < rank(&BridgeStatus::Pending));
        assert!(rank(&BridgeStatus::Pending) < rank(&BridgeStatus::Completed));
    }
}
