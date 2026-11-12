//! One EVM chain watcher: advances a persisted block cursor, reads the two Wormhole
//! Token Bridge logs through `eth_getLogs`, stores them and runs the event-side join.
//!
//! Endpoints: a chain runs on a primary and an optional fallback RPC. Each endpoint
//! has its own limiter and its own live `eth_getLogs` window, because provider quotas
//! and range caps are per endpoint. A 429, timeout or 5xx moves the watcher to the
//! next endpoint; only the endpoint that rejected a range has its window halved. A
//! failed chunk never stores events or advances the cursor.
//!
//! Finality: a target in `finalized` mode reads the node's `finalized` block when it
//! answers the tag, else `head - finality_depth`. A target in `depth` mode always reads
//! `head - finality_depth` and never asks for the tag. Only blocks at or below the
//! target are read, so a reorg below it is not expected. The cursor's block hash is
//! still checked every poll. A mismatch walks the stored event block hashes down from
//! the cursor to the target's rewind bound and looks for the newest checkpoint whose
//! hash still matches the chain. That checkpoint proves every block at or below it is
//! canonical, so the cursor rewinds to it and every event above it is deleted in one
//! transaction. Rewinding to a matching checkpoint, rather than to the lowest stale
//! event block, re-reads the whole region that may have changed, including blocks that
//! stored no hash. When no stored checkpoint within the bound matches, the window floor
//! (the existing depth/128-block bound) is the conservative rewind point; if the floor
//! itself is a stale checkpoint, the reorg is deeper than the bound and the poll fails
//! without touching the cursor or the stored events.

use super::client::{Block, EvmClient, Log};
use super::db::{self, Cursor, FinalityMode, NewEvent, WatchTarget};
use super::decode::{self, EventKind};
use crate::clients::limiter::Limiter;
use crate::error::AppError;
use chrono::Utc;
use metrics::{counter, gauge};
use reqwest::Client;
use sqlx::PgPool;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// One configured RPC endpoint: its URL, its own request budget, and the initial
/// `eth_getLogs` window. Every endpoint keeps its window separately at runtime.
#[derive(Clone)]
pub struct WatcherEndpoint {
    pub url: String,
    pub limiter: Arc<Limiter>,
    pub max_block_range: u64,
}

#[derive(Clone)]
struct EndpointState {
    client: EvmClient,
    /// This endpoint's live window. Only a range-too-large answer from this endpoint
    /// shrinks it.
    range: u64,
    /// Set false once this endpoint rejects the `finalized` tag.
    use_finalized: bool,
}

type EndpointCall<'a, T> = Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;

/// How far back a reorg walk may go in `finalized` mode. Finalized blocks should not
/// reorg at all; this is a safety bound, not an expected depth. `depth` mode uses the
/// target's own `finality_depth`.
const FINALIZED_REORG_WALK_BLOCKS: u64 = 128;

struct Rewind {
    delete_from: u64,
    rewound_to: u64,
    removed: u64,
    /// The stored checkpoint the cursor landed on, when one still matched the chain.
    matched: Option<u64>,
    /// Set when no stored checkpoint matched and the window floor was used as the
    /// conservative rewind point.
    used_floor: bool,
}

#[derive(Clone)]
pub struct EvmWatcher {
    target: WatchTarget,
    endpoints: Vec<EndpointState>,
    /// Tried first. Moves forward when an endpoint fails and stays there, so a dead
    /// primary is not retried on every chunk.
    preferred: usize,
    pool: PgPool,
    join_max_age_secs: i64,
    poll_secs: u64,
}

impl EvmWatcher {
    /// `endpoints` must be non-empty; the primary comes first.
    pub fn new(
        target: WatchTarget,
        http_client: Client,
        endpoints: Vec<WatcherEndpoint>,
        pool: PgPool,
        join_max_age_secs: i64,
        poll_secs: u64,
    ) -> Self {
        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| EndpointState {
                client: EvmClient::new(http_client.clone(), endpoint.url, endpoint.limiter),
                range: endpoint.max_block_range.max(1),
                use_finalized: true,
            })
            .collect();
        Self {
            target,
            endpoints,
            preferred: 0,
            pool,
            join_max_age_secs,
            poll_secs,
        }
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let mut delay = Duration::from_secs(self.poll_secs.max(1));
        let base_delay = delay;

        if self.endpoints.is_empty() {
            anyhow::bail!("EVM watcher for {} has no RPC endpoint", self.target.slug);
        }

        self.verify_endpoints().await;
        if self.endpoints.is_empty() {
            tracing::error!(
                chain = %self.target.slug,
                "every configured EVM RPC endpoint serves another chain, watching is off"
            );
            // Idle rather than return: supervised restarts would only repeat the
            // same check. Fix the URL and restart the process.
            shutdown.cancelled().await;
            return Ok(());
        }

        tracing::info!(
            chain = %self.target.slug,
            endpoints = self.endpoints.len(),
            primary = %self.endpoints[0].client.host(),
            address = %self.target.core_bridge_address,
            finality_mode = self.target.finality_mode.as_str(),
            finality_depth = self.target.finality_depth,
            "EVM watcher started"
        );

        loop {
            match self.tick().await {
                Ok(()) => delay = base_delay,
                Err(error) => {
                    // 429s, timeouts and unknown errors keep every window. The sleep
                    // and the call limiter's own retry backoff are the backoff.
                    tracing::warn!(
                        chain = %self.target.slug,
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

    /// `eth_chainId` on every endpoint when the target names its EVM chain. A
    /// mismatch is a pasted-wrong-URL bug, so that endpoint is dropped. A failed
    /// check (429, timeout) is not proof of a wrong chain, so the endpoint is kept.
    async fn verify_endpoints(&mut self) {
        let Some(expected) = self.target.evm_chain_id else {
            return;
        };
        let chain = self.target.slug.clone();

        let mut kept = Vec::with_capacity(self.endpoints.len());
        for endpoint in std::mem::take(&mut self.endpoints) {
            match endpoint.client.chain_id().await {
                Ok(actual) if actual == expected => kept.push(endpoint),
                Ok(actual) => tracing::error!(
                    chain = %chain,
                    endpoint = %endpoint.client.host(),
                    expected,
                    actual,
                    "EVM RPC endpoint serves a different chain, not using it"
                ),
                Err(error) => {
                    tracing::warn!(
                        chain = %chain,
                        endpoint = %endpoint.client.host(),
                        %error,
                        "could not verify the EVM RPC endpoint's chain id, using it anyway"
                    );
                    kept.push(endpoint);
                }
            }
        }
        self.endpoints = kept;
        self.preferred = 0;
    }

    fn active_host(&self) -> String {
        self.endpoints[self.preferred].client.host().to_string()
    }

    fn labels(&self) -> (String, String) {
        (self.target.slug.clone(), self.active_host())
    }

    /// Moves the preferred endpoint to the one after `failed` and records it. Chain
    /// and host only, never the URL.
    fn note_failover(&mut self, failed: usize) {
        let next = (failed + 1) % self.endpoints.len();
        let from = self.endpoints[failed].client.host().to_string();
        let to = self.endpoints[next].client.host().to_string();
        self.preferred = next;
        counter!(
            crate::metrics::EVM_ENDPOINT_FAILOVERS_TOTAL,
            "chain" => self.target.slug.clone(),
            "endpoint" => from.clone()
        )
        .increment(1);
        tracing::warn!(
            chain = %self.target.slug,
            endpoint = %from,
            next = %to,
            "EVM RPC endpoint failed, failing over"
        );
    }

    /// Runs one call on each endpoint in turn until one answers. Endpoint health
    /// failures move on; a permanent error is returned as-is.
    async fn call_endpoints<T, F>(&mut self, call: F) -> Result<T, AppError>
    where
        F: for<'a> Fn(&'a EvmClient) -> EndpointCall<'a, T>,
    {
        let count = self.endpoints.len();
        let base = self.preferred;
        let mut last_error = None;

        for step in 0..count {
            let index = (base + step) % count;
            let result = call(&self.endpoints[index].client).await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if is_failover_error(&error) => {
                    if step + 1 < count {
                        self.note_failover(index);
                    }
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| AppError::UpstreamProvider {
            provider: super::client::PROVIDER,
            message: "no EVM RPC endpoint was tried".to_string(),
            retryable: true,
        }))
    }

    async fn tick(&mut self) -> Result<(), AppError> {
        let target = self.target_block().await?;
        let (chain, endpoint) = self.labels();
        gauge!(
            crate::metrics::EVM_WATCH_TARGET_BLOCK,
            "chain" => chain.clone(),
            "endpoint" => endpoint.clone()
        )
        .set(target.number as f64);

        let Some(mut cursor) = db::load_cursor(&self.pool, self.target.chain_id).await? else {
            // First run: start at the finality target. Backfill is deliberately not
            // done; the row-level lookup and WormholeScan cover events before this.
            db::set_cursor(
                &self.pool,
                self.target.chain_id,
                target.number,
                &target.hash,
            )
            .await?;
            gauge!(
                crate::metrics::EVM_WATCH_CURSOR_BLOCK,
                "chain" => chain.clone(),
                "endpoint" => endpoint.clone()
            )
            .set(target.number as f64);
            tracing::info!(
                chain = %chain,
                block = target.number,
                endpoint = %endpoint,
                "EVM watcher initialised its cursor at the finality target"
            );
            return Ok(());
        };
        gauge!(
            crate::metrics::EVM_WATCH_CURSOR_BLOCK,
            "chain" => chain.clone(),
            "endpoint" => endpoint.clone()
        )
        .set(cursor.block_number as f64);

        if cursor.block_number >= target.number {
            return Ok(());
        }

        // Reorg check: the cursor block must still be canonical. A mismatch means
        // logs at and after it may be stale. Walk the stored event block hashes back
        // to the deepest block that no longer matches the chain, drop every event at
        // or after it, and rewind the cursor below it in one transaction. The next
        // poll re-reads the abandoned range from the new canonical chain.
        let cursor_block = cursor.block_number;
        let canonical = self
            .call_endpoints(|client| Box::pin(client.block_by_number(cursor_block)))
            .await?;
        if canonical.hash != cursor.block_hash {
            let rewind = self.rewind_after_reorg(&cursor).await?;
            counter!(
                crate::metrics::EVM_REORGS_TOTAL,
                "chain" => chain.clone(),
                "endpoint" => self.active_host()
            )
            .increment(1);
            tracing::warn!(
                chain = %chain,
                block = cursor.block_number,
                delete_from = rewind.delete_from,
                removed = rewind.removed,
                rewound_to = rewind.rewound_to,
                matched = ?rewind.matched,
                used_floor = rewind.used_floor,
                "EVM reorg detected, dropped stale events and rewound to the last matching checkpoint"
            );
            return Ok(());
        }

        loop {
            let start = cursor.block_number + 1;
            if start > target.number {
                break;
            }

            let (logs, end) = self.logs_chunk(start, target.number).await?;
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

            let end_block = self
                .call_endpoints(|client| Box::pin(client.block_by_number(end)))
                .await?;
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

            let (chain, endpoint) = self.labels();
            counter!(
                crate::metrics::EVM_BLOCKS_TOTAL,
                "chain" => chain.clone(),
                "endpoint" => endpoint.clone()
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
                        "chain" => chain.clone(),
                        "endpoint" => endpoint.clone(),
                        "kind" => kind.as_str()
                    )
                    .increment(count as u64);
                }
            }
            gauge!(
                crate::metrics::EVM_WATCH_CURSOR_BLOCK,
                "chain" => chain.clone(),
                "endpoint" => endpoint.clone()
            )
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

    /// Finds a rewind point that re-reads the whole region a reorg may have changed.
    ///
    /// The cursor block itself is a known mismatch. Walking the stored event block
    /// hashes newest first, the first checkpoint whose hash still matches proves the
    /// fork is above it: a chain's ancestors are unchanged once a block matches, so
    /// every block at or below that checkpoint is canonical. Rewinding there, instead
    /// of to the lowest stale event block, re-reads the gap between the real fork and
    /// that stale block, including blocks that never stored a hash.
    ///
    /// When no stored checkpoint inside the bound still matches, the window floor is
    /// used as a conservative margin. If the floor itself is a stored checkpoint that
    /// no longer matches, the fork is at or below the floor and the bound cannot
    /// establish a safe rewind point: this returns an error and leaves the cursor and
    /// the stored events untouched, so a deeper reorg is never silently skipped.
    async fn rewind_after_reorg(&mut self, cursor: &Cursor) -> Result<Rewind, AppError> {
        let bound = match self.target.finality_mode {
            FinalityMode::Depth => self.target.finality_depth.max(1),
            FinalityMode::Finalized => FINALIZED_REORG_WALK_BLOCKS,
        };
        let floor = cursor.block_number.saturating_sub(bound);
        let checkpoints = db::event_block_hashes(
            &self.pool,
            self.target.chain_id,
            floor,
            cursor.block_number,
            bound.saturating_add(1),
        )
        .await?;

        let mut matched = None;
        let mut lowest_mismatch = cursor.block_number;
        let mut checked = 0usize;

        for (number, hash) in checkpoints {
            checked += 1;
            let canonical = self
                .call_endpoints(|client| Box::pin(client.block_by_number(number)))
                .await?;
            if canonical.hash == hash {
                matched = Some((number, canonical.hash));
                break;
            }
            lowest_mismatch = number;
        }

        let (delete_from, rewound_to, rewound_hash, matched_block, used_floor) = match matched {
            Some((number, hash)) => (number + 1, number, hash, Some(number), false),
            None if checked == 0 => {
                // Nothing below the cursor stored a hash, so the cursor block is the
                // only known stale block. Re-reading it is the whole affected region.
                if cursor.block_number == 0 {
                    tracing::error!(
                        chain = %self.target.slug,
                        "EVM reorg reported at genesis with no stored checkpoint to rewind to"
                    );
                    return Err(AppError::Internal(anyhow::anyhow!(
                        "EVM reorg on chain {} was reported at genesis with no stored checkpoint to rewind to",
                        self.target.slug
                    )));
                }
                let previous = self
                    .call_endpoints(|client| {
                        Box::pin(client.block_by_number(cursor.block_number - 1))
                    })
                    .await?;
                (
                    cursor.block_number,
                    cursor.block_number - 1,
                    previous.hash,
                    None,
                    false,
                )
            }
            None => {
                // No stored checkpoint inside the bound still matches. If the floor
                // is itself stale, the fork is at or below it and a rewind above the
                // floor would silently skip canonical events below the fork.
                if floor > 0 && lowest_mismatch <= floor {
                    tracing::error!(
                        chain = %self.target.slug,
                        cursor = cursor.block_number,
                        lowest_mismatch,
                        floor,
                        bound,
                        "EVM reorg is deeper than the rewind bound, refusing to move the cursor"
                    );
                    return Err(AppError::Internal(anyhow::anyhow!(
                        "EVM reorg on chain {} is deeper than the {}-block rewind bound: \
                         the stored block at {} no longer matches and the cursor stays at {}",
                        self.target.slug,
                        bound,
                        lowest_mismatch,
                        cursor.block_number
                    )));
                }
                // Conservative margin: rewind to the floor of the window and re-read
                // it. Genesis cannot be replaced, so at floor 0 it is the safe point.
                if floor == 0 {
                    let genesis = self
                        .call_endpoints(|client| Box::pin(client.block_by_number(0)))
                        .await?;
                    (1, 0, genesis.hash, None, true)
                } else {
                    let previous = self
                        .call_endpoints(|client| Box::pin(client.block_by_number(floor - 1)))
                        .await?;
                    (floor, floor - 1, previous.hash, None, true)
                }
            }
        };

        let removed = db::rewind_after_reorg(
            &self.pool,
            self.target.chain_id,
            delete_from,
            rewound_to,
            &rewound_hash,
        )
        .await?;

        Ok(Rewind {
            delete_from,
            rewound_to,
            removed,
            matched: matched_block,
            used_floor,
        })
    }

    /// One `eth_getLogs` chunk starting at `start`, on the preferred endpoint or the
    /// next one. The window is this endpoint's own, so a fallback can serve a range
    /// the primary rejected. The returned end is the block the chunk actually read.
    async fn logs_chunk(&mut self, start: u64, target: u64) -> Result<(Vec<Log>, u64), AppError> {
        let count = self.endpoints.len();
        let base = self.preferred;
        let mut last_error = None;
        let mut step = 0;

        while step < count {
            let index = (base + step) % count;
            let end = start
                .saturating_add(self.endpoints[index].range)
                .saturating_sub(1)
                .min(target);

            let result = self.endpoints[index]
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
                .await;

            match result {
                Ok(logs) => return Ok((logs, end)),
                Err(AppError::UpstreamRangeTooLarge(_)) => {
                    let range = self.endpoints[index].range;
                    if range <= 1 {
                        return Err(AppError::UpstreamRangeTooLarge(super::client::PROVIDER));
                    }
                    let shrunk = (range / 2).max(1);
                    self.endpoints[index].range = shrunk;
                    counter!(
                        crate::metrics::EVM_RANGE_HALVINGS_TOTAL,
                        "chain" => self.target.slug.clone(),
                        "endpoint" => self.endpoints[index].client.host().to_string()
                    )
                    .increment(1);
                    tracing::warn!(
                        chain = %self.target.slug,
                        endpoint = %self.endpoints[index].client.host(),
                        range = shrunk,
                        "EVM getLogs range rejected as too large, halving this endpoint's window"
                    );
                    // Retry the same endpoint with the smaller window. Its rejection
                    // says nothing about the other endpoints.
                }
                Err(error) if is_failover_error(&error) => {
                    if step + 1 < count {
                        self.note_failover(index);
                    }
                    last_error = Some(error);
                    step += 1;
                }
                // A permanent error is returned as-is; the cursor stays put.
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| AppError::UpstreamProvider {
            provider: super::client::PROVIDER,
            message: "no EVM RPC endpoint was tried".to_string(),
            retryable: true,
        }))
    }

    /// The finality target, from the preferred endpoint. `finalized` support is
    /// tracked per endpoint: a fallback that rejects the tag still serves
    /// `head - finality_depth` while the primary keeps using `finalized`.
    async fn target_block(&mut self) -> Result<Block, AppError> {
        let count = self.endpoints.len();
        let base = self.preferred;
        let mut last_error = None;

        for step in 0..count {
            let index = (base + step) % count;
            match self.target_block_on(index).await {
                Ok(block) => return Ok(block),
                Err(error) if is_failover_error(&error) => {
                    if step + 1 < count {
                        self.note_failover(index);
                    }
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| AppError::UpstreamProvider {
            provider: super::client::PROVIDER,
            message: "no EVM RPC endpoint was tried".to_string(),
            retryable: true,
        }))
    }

    async fn target_block_on(&mut self, index: usize) -> Result<Block, AppError> {
        // `depth` mode never asks for the tag, even if the endpoint would answer it.
        if self.target.finality_mode == FinalityMode::Finalized
            && self.endpoints[index].use_finalized
        {
            match self.endpoints[index].client.finalized_block().await {
                Ok(block) => return Ok(block),
                Err(error) if finalized_unsupported(&error) => {
                    self.endpoints[index].use_finalized = false;
                    tracing::warn!(
                        chain = %self.target.slug,
                        endpoint = %self.endpoints[index].client.host(),
                        %error,
                        "node rejected the finalized tag, falling back to head - finality_depth"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        let head = self.endpoints[index].client.block_number().await?;
        let number = head.saturating_sub(self.target.finality_depth);
        self.endpoints[index].client.block_by_number(number).await
    }

    /// One `eth_getBlockByNumber` per block that carried a log. A failed timestamp
    /// fetch leaves the event's timestamp NULL; the log itself is still stored.
    async fn block_timestamps(
        &mut self,
        logs: &[Log],
    ) -> HashMap<u64, Option<chrono::DateTime<Utc>>> {
        let mut numbers: Vec<u64> = logs.iter().map(|log| log.block_number).collect();
        numbers.sort_unstable();
        numbers.dedup();

        let mut timestamps = HashMap::with_capacity(numbers.len());
        for number in numbers {
            match self
                .call_endpoints(|client| Box::pin(client.block_by_number(number)))
                .await
            {
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

/// 429, timeout and 5xx: the endpoint is unhealthy, so another may answer. A
/// permanent error (bad params, range too large) says nothing about the others.
fn is_failover_error(error: &AppError) -> bool {
    match error {
        AppError::UpstreamRateLimited(_) | AppError::UpstreamTimeout(_) => true,
        AppError::UpstreamProvider { retryable, .. } => *retryable,
        _ => false,
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

    #[test]
    fn only_unhealthy_endpoints_move_the_watcher_on() {
        for error in [
            AppError::UpstreamRateLimited("evm"),
            AppError::UpstreamTimeout("evm"),
            AppError::UpstreamProvider {
                provider: "evm",
                message: "status 503 Service Unavailable".into(),
                retryable: true,
            },
        ] {
            assert!(is_failover_error(&error), "{error:?}");
        }

        for error in [
            AppError::UpstreamRangeTooLarge("evm"),
            AppError::UpstreamProvider {
                provider: "evm",
                message: "invalid params".into(),
                retryable: false,
            },
            AppError::UpstreamProvider {
                provider: "evm",
                message: "eth_chainId: unexpected result shape".into(),
                retryable: false,
            },
        ] {
            assert!(!is_failover_error(&error), "{error:?}");
        }
    }
}
