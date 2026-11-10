//! One EVM chain watcher: advances a persisted block cursor, reads the two Wormhole
//! Token Bridge logs through `eth_getLogs`, stores them and runs the event-side join.
//!
//! Finality: the target is the node's `finalized` block when it answers the tag, else
//! `head - finality_depth`. Only finalized blocks are read, so a reorg below the target
//! is not expected. The cursor's block hash is still checked every poll; a mismatch
//! drops the events at and after the cursor, rewinds one block and re-reads.

use super::client::EvmClient;
use super::db::{self, Cursor, NewEvent, WatchTarget};
use super::decode::{self, EventKind};
use crate::clients::limiter::Limiter;
use crate::error::AppError;
use chrono::Utc;
use metrics::{counter, gauge};
use reqwest::Client;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct EvmWatcher {
    target: WatchTarget,
    client: EvmClient,
    pool: PgPool,
    join_max_age_secs: i64,
    poll_secs: u64,
    max_block_range: u64,
    /// Set false once the node rejects the `finalized` tag; the watcher then uses
    /// `head - finality_depth` for the rest of its life.
    use_finalized: bool,
}

impl EvmWatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: WatchTarget,
        http_client: Client,
        limiter: Arc<Limiter>,
        pool: PgPool,
        join_max_age_secs: i64,
        poll_secs: u64,
        max_block_range: u64,
    ) -> Self {
        let client = EvmClient::new(http_client, target.rpc_url.clone(), limiter);
        Self {
            target,
            client,
            pool,
            join_max_age_secs,
            poll_secs,
            max_block_range,
            use_finalized: true,
        }
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let mut range = self.max_block_range.max(1);
        let mut delay = Duration::from_secs(self.poll_secs.max(1));
        let base_delay = delay;

        tracing::info!(
            chain = %self.target.slug,
            address = %self.target.core_bridge_address,
            finality_depth = self.target.finality_depth,
            "EVM watcher started"
        );

        loop {
            match self.tick(&mut range).await {
                Ok(()) => delay = base_delay,
                Err(error) => {
                    // 429s, timeouts and unknown errors keep the current range. The
                    // sleep and the call limiter's own retry backoff are the backoff.
                    tracing::warn!(
                        chain = %self.target.slug,
                        range,
                        %error,
                        "EVM poll failed, backing off"
                    );
                    delay = (delay * 2).min(base_delay * 10);
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn tick(&mut self, range: &mut u64) -> Result<(), AppError> {
        let target = self.target_block().await?;
        gauge!(crate::metrics::EVM_WATCH_TARGET_BLOCK, "chain" => self.target.slug.clone())
            .set(target.number as f64);

        let Some(mut cursor) = db::load_cursor(&self.pool, self.target.chain_id).await? else {
            // First run: start at the finalized head. Backfill is deliberately not
            // done; the row-level lookup and WormholeScan cover events before this.
            db::set_cursor(
                &self.pool,
                self.target.chain_id,
                target.number,
                &target.hash,
            )
            .await?;
            gauge!(crate::metrics::EVM_WATCH_CURSOR_BLOCK, "chain" => self.target.slug.clone())
                .set(target.number as f64);
            tracing::info!(
                chain = %self.target.slug,
                block = target.number,
                "EVM watcher initialised its cursor at the finalized head"
            );
            return Ok(());
        };
        gauge!(crate::metrics::EVM_WATCH_CURSOR_BLOCK, "chain" => self.target.slug.clone())
            .set(cursor.block_number as f64);

        if cursor.block_number >= target.number {
            return Ok(());
        }

        // Reorg check: the cursor block must still be canonical. A mismatch means
        // logs at and after it may be stale, so they are dropped and the cursor
        // rewinds one block. The next poll re-reads the affected range.
        let canonical = self.client.block_by_number(cursor.block_number).await?;
        if canonical.hash != cursor.block_hash {
            let removed =
                db::delete_events_from(&self.pool, self.target.chain_id, cursor.block_number)
                    .await?;
            let previous_number = cursor.block_number.saturating_sub(1);
            let previous = self.client.block_by_number(previous_number).await?;
            db::set_cursor(
                &self.pool,
                self.target.chain_id,
                previous.number,
                &previous.hash,
            )
            .await?;
            counter!(crate::metrics::EVM_REORGS_TOTAL, "chain" => self.target.slug.clone())
                .increment(1);
            tracing::warn!(
                chain = %self.target.slug,
                block = cursor.block_number,
                removed,
                rewound_to = previous.number,
                "EVM reorg detected, dropped events after the cursor and rewound"
            );
            return Ok(());
        }

        loop {
            let start = cursor.block_number + 1;
            if start > target.number {
                break;
            }
            let end = start
                .saturating_add(*range)
                .saturating_sub(1)
                .min(target.number);

            let logs = match self
                .client
                .logs(
                    start,
                    end,
                    &[
                        self.target.core_bridge_address.as_str(),
                        self.target.token_bridge_address.as_str(),
                    ],
                    &[
                        decode::LOG_MESSAGE_PUBLISHED_TOPIC,
                        decode::TRANSFER_REDEEMED_TOPIC,
                    ],
                )
                .await
            {
                Ok(logs) => logs,
                Err(AppError::UpstreamRangeTooLarge(_)) => {
                    if *range <= 1 {
                        return Err(AppError::UpstreamRangeTooLarge("evm"));
                    }
                    *range /= 2;
                    counter!(
                        crate::metrics::EVM_RANGE_HALVINGS_TOTAL,
                        "chain" => self.target.slug.clone()
                    )
                    .increment(1);
                    tracing::warn!(
                        chain = %self.target.slug,
                        range = *range,
                        "EVM getLogs range rejected as too large, halving"
                    );
                    continue;
                }
                // 429s, timeouts and unknown errors keep the current range.
                Err(error) => return Err(error),
            };

            let timestamps = self.block_timestamps(&logs).await;
            let events: Vec<NewEvent> = logs
                .iter()
                .filter_map(|log| {
                    let decoded = decode::decode_log(
                        log,
                        self.target.wormhole_chain_id,
                        &self.target.core_bridge_address,
                        &self.target.token_bridge_address,
                    )?;
                    Some(NewEvent {
                        event_kind: decoded.event_kind,
                        emitter_chain: decoded.emitter_chain,
                        emitter_address: decoded.emitter_address,
                        sequence: decoded.sequence,
                        tx_hash: log.tx_hash.clone(),
                        log_index: log.log_index,
                        block_number: log.block_number,
                        block_hash: log.block_hash.clone(),
                        block_timestamp: timestamps.get(&log.block_number).copied().flatten(),
                    })
                })
                .collect();

            let end_block = self.client.block_by_number(end).await?;
            db::insert_events_and_advance(
                &self.pool,
                self.target.chain_id,
                &events,
                end,
                &end_block.hash,
            )
            .await?;

            cursor = Cursor {
                block_number: end,
                block_hash: end_block.hash,
            };

            counter!(
                crate::metrics::EVM_BLOCKS_TOTAL,
                "chain" => self.target.slug.clone()
            )
            .increment(end - start + 1);
            for kind in [EventKind::LogMessagePublished, EventKind::TransferRedeemed] {
                let count = events
                    .iter()
                    .filter(|event| event.event_kind == kind)
                    .count();
                if count > 0 {
                    counter!(
                        crate::metrics::EVM_EVENTS_TOTAL,
                        "chain" => self.target.slug.clone(),
                        "kind" => kind.as_str()
                    )
                    .increment(count as u64);
                }
            }
            gauge!(crate::metrics::EVM_WATCH_CURSOR_BLOCK, "chain" => self.target.slug.clone())
                .set(end as f64);
        }

        let joined =
            db::join_recent(&self.pool, self.target.chain_id, self.join_max_age_secs).await?;
        if joined.matched > 0 {
            counter!(
                crate::metrics::EVM_JOIN_MATCHES_TOTAL,
                "chain" => self.target.slug.clone()
            )
            .increment(joined.matched);
            tracing::info!(
                chain = %self.target.slug,
                matched = joined.matched,
                "EVM events joined to transfers"
            );
        }
        Ok(())
    }

    async fn target_block(&mut self) -> Result<super::client::Block, AppError> {
        if self.use_finalized {
            match self.client.finalized_block().await {
                Ok(block) => return Ok(block),
                Err(error) if finalized_unsupported(&error) => {
                    self.use_finalized = false;
                    tracing::warn!(
                        chain = %self.target.slug,
                        %error,
                        "node rejected the finalized tag, falling back to head - finality_depth"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        let head = self.client.block_number().await?;
        let number = head.saturating_sub(self.target.finality_depth);
        self.client.block_by_number(number).await
    }

    /// One `eth_getBlockByNumber` per block that carried a log. A failed timestamp
    /// fetch leaves the event's timestamp NULL; the log itself is still stored.
    async fn block_timestamps(
        &self,
        logs: &[super::client::Log],
    ) -> HashMap<u64, Option<chrono::DateTime<Utc>>> {
        let mut numbers: Vec<u64> = logs.iter().map(|log| log.block_number).collect();
        numbers.sort_unstable();
        numbers.dedup();

        let mut timestamps = HashMap::with_capacity(numbers.len());
        for number in numbers {
            match self.client.block_by_number(number).await {
                Ok(block) => {
                    timestamps.insert(number, block.timestamp);
                }
                Err(error) => {
                    tracing::debug!(
                        chain = %self.target.slug,
                        block = number,
                        %error,
                        "could not read EVM block timestamp"
                    );
                    timestamps.insert(number, None);
                }
            }
        }
        timestamps
    }
}

/// A node that does not understand the `finalized` tag answers with an invalid-params
/// or method/block error. A rate limit or timeout is not a reason to stop using it.
fn finalized_unsupported(error: &AppError) -> bool {
    let AppError::UpstreamProvider {
        provider,
        message,
        retryable,
    } = error
    else {
        return false;
    };
    if *provider != super::client::PROVIDER || *retryable {
        return false;
    }
    let message = message.to_ascii_lowercase();
    [
        "finalized",
        "invalid block",
        "unknown block",
        "not supported",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_permanent_finalized_error_switches_the_mode() {
        let permanent = AppError::UpstreamProvider {
            provider: "evm",
            message: "{\"code\":-32602,\"message\":\"invalid block tag finalized\"}".into(),
            retryable: false,
        };
        assert!(finalized_unsupported(&permanent));

        let transient = AppError::UpstreamProvider {
            provider: "evm",
            message: "{\"message\":\"finalized timeout\"}".into(),
            retryable: true,
        };
        assert!(!finalized_unsupported(&transient));

        assert!(!finalized_unsupported(&AppError::UpstreamRateLimited(
            "evm"
        )));
    }
}
