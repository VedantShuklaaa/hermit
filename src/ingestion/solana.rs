use anyhow::{Context, Result};
use futures_util::{FutureExt, SinkExt, Stream, StreamExt};
use serde_json::Value;
use sqlx::PgPool;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    clients::helius::get_signatures_for_address,
    config::IngestConfig,
    db::repository::{
        get_last_ingested_slot, ingest_checkpoint_subject, set_last_ingested_slot,
        touch_last_ingested_slot,
    },
    ingestion::{
        backfill::{BackfillStats, plan_page, should_backfill},
        checkpoint::{CheckpointGate, CheckpointUpdate},
        types::{CandidateTransaction, LogsConfig, LogsFilter, SolanaRpcRequest},
    },
    redis::producer::RedisProducer,
};

/// How long the server has to acknowledge `logsSubscribe`. Data messages are not expected
/// before the acknowledgement, so silence past this means the connection is dead.
const SUBSCRIBE_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a connection ended. `Stopped` is the clean shutdown path; every other variant is
/// a reconnect and carries the static label reported in `ingest_reconnects_total`.
#[derive(Debug)]
enum ConnectionEnd {
    Stopped,
    Closed,
    Watchdog,
    SubscribeFailed,
    Error(anyhow::Error),
}

impl ConnectionEnd {
    fn is_reconnect(&self) -> bool {
        !matches!(self, Self::Stopped)
    }

    fn reason(&self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Closed => "closed",
            Self::Watchdog => "watchdog",
            Self::SubscribeFailed => "subscribe_failed",
            Self::Error(_) => "error",
        }
    }
}

/// No frame of any kind arrived before the watchdog deadline. It is an error so the
/// supervisor reconnects, and its own type so the reconnect metric can say why.
#[derive(Debug)]
struct WatchdogExpired(Duration);

impl std::fmt::Display for WatchdogExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no frame received for {:?}, watchdog forcing a reconnect",
            self.0
        )
    }
}

impl std::error::Error for WatchdogExpired {}

/// Ping and no-frame watchdog timing. Time is passed in rather than read from the clock,
/// so the whole thing can be driven from a test without a socket.
#[derive(Debug)]
struct Keepalive {
    ping_interval: Duration,
    watchdog: Duration,
    last_frame: Instant,
    next_ping: Instant,
}

#[derive(Debug, PartialEq, Eq)]
enum KeepaliveAction {
    Ping,
    Watchdog,
    Idle,
}

impl Keepalive {
    fn new(ping_interval: Duration, watchdog: Duration, now: Instant) -> Self {
        Self {
            ping_interval,
            watchdog,
            last_frame: now,
            next_ping: now + ping_interval,
        }
    }

    /// When the next ping or the watchdog is due, whichever comes first.
    fn next_deadline(&self) -> Instant {
        self.next_ping.min(self.last_frame + self.watchdog)
    }

    /// Any frame at all proves the connection is alive: data, a Ping, or a Pong.
    fn on_frame(&mut self, now: Instant) {
        self.last_frame = now;
    }

    /// Resolves the deadline from [`next_deadline`]. The watchdog is checked first, so a
    /// silent connection is dropped even when a ping is also due.
    fn on_deadline(&mut self, now: Instant) -> KeepaliveAction {
        if now >= self.last_frame + self.watchdog {
            KeepaliveAction::Watchdog
        } else if now >= self.next_ping {
            self.next_ping = now + self.ping_interval;
            KeepaliveAction::Ping
        } else {
            KeepaliveAction::Idle
        }
    }
}

/// The reply to our `logsSubscribe` request, as far as it concerns us.
#[derive(Debug, PartialEq, Eq)]
enum SubscribeReply {
    /// The server accepted the subscription. The payload is the subscription id, kept
    /// optional because only the absence of an error is contractually guaranteed.
    Confirmed { subscription: Option<u64> },
    /// The server rejected the subscription; the string is the JSON-RPC error payload.
    Rejected(String),
    /// Not a reply to this request (a notification, or a response to another id).
    Unrelated,
}

fn parse_subscribe_reply(text: &str, expected_id: u64) -> Result<SubscribeReply> {
    let message: Value = serde_json::from_str(text).context("invalid Solana WebSocket JSON")?;

    if message.get("id").and_then(Value::as_u64) != Some(expected_id) {
        return Ok(SubscribeReply::Unrelated);
    }

    if let Some(error) = message.get("error") {
        return Ok(SubscribeReply::Rejected(error.to_string()));
    }

    match message.get("result") {
        Some(result) if !result.is_null() => Ok(SubscribeReply::Confirmed {
            subscription: result.as_u64(),
        }),
        _ => Ok(SubscribeReply::Rejected(
            "logsSubscribe reply has no result".to_owned(),
        )),
    }
}

/// Waits for the server to accept (or reject) `logsSubscribe` before any message is handled.
///
/// Frames that are not the reply are ignored: a subscription is not active until it is
/// acknowledged, so there is nothing to process yet. A rejection, a closed socket and a
/// timeout are all connection failures, which the caller turns into a reconnect.
async fn wait_for_subscribe_reply<S>(
    frames: &mut S,
    expected_id: u64,
    timeout: Duration,
) -> Result<Option<u64>>
where
    S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
{
    let reply = tokio::time::timeout(timeout, async {
        loop {
            let frame = match frames.next().await {
                Some(frame) => frame.context("failed to read logsSubscribe reply")?,
                None => anyhow::bail!("Solana WebSocket ended before acknowledging logsSubscribe"),
            };

            match frame {
                Message::Text(text) => match parse_subscribe_reply(&text, expected_id)? {
                    SubscribeReply::Confirmed { subscription } => {
                        return Ok::<Option<u64>, anyhow::Error>(subscription);
                    }
                    SubscribeReply::Rejected(reason) => {
                        anyhow::bail!("logsSubscribe rejected: {reason}")
                    }
                    SubscribeReply::Unrelated => {}
                },
                Message::Close(frame) => anyhow::bail!(
                    "Solana WebSocket closed before acknowledging logsSubscribe: {frame:?}"
                ),
                _ => {}
            }
        }
    })
    .await;

    match reply {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "timed out after {timeout:?} waiting for logsSubscribe reply"
        )),
    }
}

/// Renders an error chain for a log line with the WebSocket URL's query string
/// removed. A connect error can echo the request URI, and the query carries the
/// API key.
fn redact_ws_url(text: &str, ws_url: &str) -> String {
    let Some((host, query)) = ws_url.split_once('?') else {
        return text.to_string();
    };

    let text = text.replace(ws_url, host);
    if query.is_empty() {
        text
    } else {
        text.replace(query, "[redacted]")
    }
}

/// Base wait before reconnecting a dropped WebSocket. Programs that dropped together
/// would otherwise reconnect in lockstep, so a jitter is added on top.
const RECONNECT_BASE: Duration = Duration::from_secs(5);

/// Reconnect delay: the base plus up to half of it, the jitter style the Helius retry
/// loop already uses.
fn reconnect_backoff(base: Duration) -> Duration {
    let jitter = rand::random::<u64>() % (base.as_millis() as u64 / 2 + 1);
    base + Duration::from_millis(jitter)
}

/// Splits the configured programs into the ones that get a live connection and the
/// ones `INGEST_MAX_WS_CONNECTIONS` skips, and gives each live one its startup delay.
/// Config order decides the cut, so the same programs are kept on every restart.
fn plan_connections(
    programs: &[String],
    max_connections: Option<usize>,
    stagger_ms: u64,
) -> (Vec<(&str, Duration)>, Vec<&str>) {
    let cut = max_connections.map_or(programs.len(), |max| max.min(programs.len()));
    let (live, skipped) = programs.split_at(cut);

    let live = live
        .iter()
        .enumerate()
        .map(|(index, program)| {
            let delay = Duration::from_millis(stagger_ms.saturating_mul(index as u64));
            (program.as_str(), delay)
        })
        .collect();

    (live, skipped.iter().map(String::as_str).collect())
}

#[derive(Clone)]
pub struct SolanaIngester {
    ws_url: String,
    /// One independent subscription, backfill and checkpoint per program.
    programs: Vec<String>,
    redis: RedisProducer,
    db: PgPool,
    client: reqwest::Client,
    helius_url: String,
    config: IngestConfig,
}

impl SolanaIngester {
    pub fn new(
        ws_url: String,
        programs: Vec<String>,
        redis: RedisProducer,
        db: PgPool,
        client: reqwest::Client,
        helius_url: String,
        config: IngestConfig,
    ) -> Self {
        Self {
            ws_url,
            programs,
            redis,
            db,
            client,
            helius_url,
            config,
        }
    }

    /// Runs one supervised connection loop per program until `stop` is cancelled.
    /// Each program has its own task, so a program that fails to subscribe or whose
    /// connection keeps dying never stops the others from ingesting.
    pub async fn run(&self, stop: CancellationToken) -> Result<()> {
        if self.programs.is_empty() {
            warn!("no Solana bridge programs configured, ingestion is idle");
            return Ok(());
        }

        let (live, skipped) = plan_connections(
            &self.programs,
            self.config.max_ws_connections,
            self.config.connect_stagger_ms,
        );

        for program in skipped {
            warn!(
                program,
                max = ?self.config.max_ws_connections,
                "no live WebSocket connection: INGEST_MAX_WS_CONNECTIONS was reached; the reconciliation poll still covers this program"
            );
        }

        for (program, delay) in live {
            let ingester = self.clone();
            let program = program.to_string();
            let program_stop = stop.clone();

            tokio::spawn(async move {
                // Spread the first connects: a provider can refuse the whole key when
                // every program connects inside the same instant.
                tokio::select! {
                    _ = program_stop.cancelled() => return,
                    _ = sleep(delay) => {}
                }

                loop {
                    let task_ingester = ingester.clone();
                    let task_program = program.clone();
                    let task_stop = program_stop.clone();
                    let outcome = tokio::spawn(async move {
                        task_ingester.run_program(&task_program, task_stop).await
                    })
                    .await;

                    match outcome {
                        Ok(Ok(())) => break,
                        Ok(Err(error)) => {
                            error!(program, %error, "Solana ingestion program failed, restarting")
                        }
                        Err(error) => {
                            error!(program, %error, "Solana ingestion program panicked, restarting")
                        }
                    }

                    tokio::select! {
                        _ = program_stop.cancelled() => break,
                        _ = sleep(Duration::from_secs(5)) => {}
                    }
                }
            });
        }

        stop.cancelled().await;
        Ok(())
    }

    /// Runs (and reconnects) one program's subscription until `stop` is cancelled.
    async fn run_program(&self, program: &str, stop: CancellationToken) -> Result<()> {
        loop {
            let end = match self.run_connection(program, &stop).await {
                Ok(end) => end,
                Err(error) => ConnectionEnd::Error(error),
            };

            if !end.is_reconnect() {
                return Ok(());
            }

            if let ConnectionEnd::Error(error) = &end {
                let error = redact_ws_url(&format!("{error:#}"), &self.ws_url);
                error!(program, %error, "Solana ingestion connection failed");
            } else {
                warn!(
                    program,
                    reason = end.reason(),
                    "Solana WebSocket connection ended"
                );
            }

            metrics::counter!(
                crate::metrics::INGEST_RECONNECTS_TOTAL,
                "reason" => end.reason(),
                "program" => program.to_string()
            )
            .increment(1);

            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = sleep(reconnect_backoff(RECONNECT_BASE)) => {}
            }
        }
    }

    async fn run_connection(
        &self,
        program: &str,
        stop: &CancellationToken,
    ) -> Result<ConnectionEnd> {
        let ws_host = self.ws_url.split('?').next().unwrap_or_default();
        info!(ws_host, program, "Connecting to Solana WebSocket");

        let (ws_stream, _) = tokio::select! {
            _ = stop.cancelled() => return Ok(ConnectionEnd::Stopped),
            result = connect_async(&self.ws_url) => {
                result.context("failed to connect to Solana WebSocket")?
            }
        };

        info!("Connected to Solana WebSocket");

        let (mut write, mut read) = ws_stream.split();

        let request = SolanaRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "logsSubscribe",
            params: (
                LogsFilter {
                    mentions: vec![program.to_string()],
                },
                LogsConfig {
                    commitment: "confirmed",
                },
            ),
        };

        write
            .send(Message::Text(serde_json::to_string(&request)?))
            .await?;

        let subscription =
            match wait_for_subscribe_reply(&mut read, request.id, SUBSCRIBE_REPLY_TIMEOUT).await {
                Ok(subscription) => subscription,
                Err(error) => {
                    error!(
                        %error,
                        program,
                        "logsSubscribe failed, closing the connection"
                    );
                    return Ok(ConnectionEnd::SubscribeFailed);
                }
            };

        info!(
            program,
            subscription = ?subscription,
            "Subscribed to Solana bridge logs"
        );

        // The backfill runs next to the read loop, not before it: a paced backfill can
        // take up to `INGEST_BACKFILL_MAX * INGEST_BACKFILL_PACE_MS`, and live
        // notifications must not queue behind it. It is a child of this connection, so
        // it is cancelled with it; the gate keeps the checkpoint from passing the gap.
        let (gate, backfill) = self.start_backfill(program, stop).await;

        let mut keepalive = Keepalive::new(
            Duration::from_secs(self.config.ping_secs),
            Duration::from_secs(self.config.watchdog_secs),
            Instant::now(),
        );

        let ended: Result<ConnectionEnd> = async {
            loop {
                let deadline = keepalive.next_deadline();

                tokio::select! {
                    _ = stop.cancelled() => {
                        info!("stop requested, closing Solana WebSocket");
                        let _ = write.send(Message::Close(None)).await;
                        return Ok(ConnectionEnd::Stopped);
                    }
                    message = read.next() => {
                        let Some(message) = message else {
                            return Ok(ConnectionEnd::Closed);
                        };
                        let message = message.context("failed to read Solana WebSocket message")?;

                        // Every frame counts, including a Ping from the server.
                        keepalive.on_frame(Instant::now());

                        match message {
                            Message::Text(text) => {
                                self.handle_message(&text, &gate, program).await?;
                            }

                            Message::Ping(payload) => {
                                info!("Received Solana WebSocket ping");
                                write.send(Message::Pong(payload)).await?;
                            }

                            Message::Close(_) => {
                                warn!("Solana WebSocket sent close frame");
                                return Ok(ConnectionEnd::Closed);
                            }

                            _ => {}
                        }
                    }
                    _ = sleep_until(deadline) => {
                        match keepalive.on_deadline(Instant::now()) {
                            KeepaliveAction::Ping => {
                                write.send(Message::Ping(Vec::new())).await?;
                            }
                            KeepaliveAction::Watchdog => {
                                return Err(WatchdogExpired(keepalive.watchdog).into());
                            }
                            KeepaliveAction::Idle => {}
                        }
                    }
                }
            }
        }
        .await;

        // The backfill belongs to this connection: cancel it and wait for its cleanup,
        // so a reconnect never runs two backfills and the inflight gauge drops to zero.
        if let Some(backfill) = backfill {
            backfill.shutdown().await;
        }

        match ended {
            Err(error) if error.downcast_ref::<WatchdogExpired>().is_some() => {
                warn!(%error, "Solana WebSocket watchdog expired");
                Ok(ConnectionEnd::Watchdog)
            }
            other => other,
        }
    }

    /// Reads the checkpoint and, when there is one to resume from, spawns the backfill
    /// for this connection. The returned gate is the only path through which live
    /// notifications may advance the persisted checkpoint.
    async fn start_backfill(
        &self,
        program: &str,
        stop: &CancellationToken,
    ) -> (Arc<Mutex<CheckpointGate>>, Option<BackfillTask>) {
        let gate_without_backfill = || Arc::new(Mutex::new(CheckpointGate::new(0, false)));

        if !self.config.backfill {
            return (gate_without_backfill(), None);
        }

        let start_checkpoint =
            match get_last_ingested_slot(&self.db, &ingest_checkpoint_subject(program)).await {
                Ok(slot) => slot,
                Err(error) => {
                    warn!(
                        %error,
                        "Could not read the ingestion checkpoint, skipping backfill"
                    );
                    return (gate_without_backfill(), None);
                }
            };

        if !should_backfill(start_checkpoint) {
            info!("No ingestion checkpoint yet, skipping backfill");
            return (gate_without_backfill(), None);
        }

        let gate = Arc::new(Mutex::new(CheckpointGate::new(start_checkpoint, true)));
        let cancel = stop.child_token();
        let task_cancel = cancel.clone();
        let task_gate = gate.clone();
        let ingester = self.clone();

        let program = program.to_string();
        let handle = tokio::spawn(async move {
            run_backfill_task(ingester, program, start_checkpoint, task_gate, task_cancel).await;
        });

        (gate, Some(BackfillTask { cancel, handle }))
    }

    /// Replays the signatures the checkpoint has not covered yet. The caller wraps this
    /// future in a `select!` against the connection's cancellation token, so a connection
    /// ending mid-run drops it at its next await point.
    async fn backfill_missed_transactions(
        &self,
        program: &str,
        start_checkpoint: u64,
    ) -> Result<BackfillStats> {
        info!(
            program,
            last_ingested_slot = start_checkpoint,
            rewind_slots = self.config.rewind_slots,
            max = self.config.backfill_max,
            "Starting Solana transaction backfill"
        );

        let pace = Duration::from_millis(self.config.backfill_pace_ms);
        let mut before: Option<String> = None;
        let mut stats = BackfillStats::default();

        loop {
            let signatures =
                get_signatures_for_address(&self.client, &self.helius_url, program, before.clone())
                    .await?;

            if signatures.is_empty() {
                break;
            }

            let remaining = self.config.backfill_max.saturating_sub(stats.published);
            let page = plan_page(
                &signatures,
                start_checkpoint,
                self.config.rewind_slots,
                remaining,
            );

            for candidate in &page.candidates {
                let fresh = self
                    .redis
                    .publish_transaction(candidate, self.config.dedupe_ttl_secs)
                    .await
                    .context("failed to publish backfilled transaction")?;

                if !fresh {
                    stats.skipped += 1;
                    continue;
                }

                stats.published += 1;
                metrics::counter!(
                    crate::metrics::INGEST_BACKFILL_PUBLISHED_TOTAL,
                    "program" => program.to_string()
                )
                .increment(1);

                info!(
                    tx_hash = %candidate.tx_hash,
                    slot = candidate.slot,
                    "Published backfilled transaction"
                );

                tokio::time::sleep(pace).await;
            }

            if page.done {
                if stats.published >= self.config.backfill_max {
                    warn!(
                        max = self.config.backfill_max,
                        "Backfill stopped at INGEST_BACKFILL_MAX, older signatures were not replayed"
                    );
                }
                break;
            }

            before = signatures
                .last()
                .and_then(|value| value.get("signature").and_then(Value::as_str))
                .map(str::to_string);
        }

        info!(
            program,
            published = stats.published,
            skipped = stats.skipped,
            last_ingested_slot = start_checkpoint,
            "Solana transaction backfill completed"
        );

        Ok(stats)
    }

    async fn handle_message(
        &self,
        text: &str,
        gate: &Mutex<CheckpointGate>,
        program: &str,
    ) -> Result<()> {
        let message: Value = serde_json::from_str(text).context("invalid Solana WebSocket JSON")?;
        // The `logsSubscribe` reply is consumed before this loop starts, so anything that is
        // not a notification here (keepalive frames, stray replies) is ignored.
        if message.get("method").and_then(Value::as_str) != Some("logsNotification") {
            return Ok(());
        }

        let params = match message.get("params") {
            Some(params) => params,
            None => return Ok(()),
        };

        let result = match params.get("result") {
            Some(result) => result,
            None => return Ok(()),
        };

        let context = match result.get("context") {
            Some(context) => context,
            None => return Ok(()),
        };

        let slot = context
            .get("slot")
            .and_then(Value::as_u64)
            .context("missing slot in Solana log notification")?;

        let value = match result.get("value") {
            Some(value) => value,
            None => return Ok(()),
        };

        let signature = match value.get("signature").and_then(Value::as_str) {
            Some(signature) => signature,
            None => return Ok(()),
        };

        let err = value.get("err");

        if err.is_some_and(|value| !value.is_null()) {
            return Ok(());
        }

        let candidate = CandidateTransaction {
            chain: "solana".into(),
            tx_hash: signature.to_owned(),
            slot,
        };

        info!(
            chain = candidate.chain,
            program,
            tx_hash = %candidate.tx_hash,
            slot = candidate.slot,
            "Detected Solana transaction involving bridge program"
        );

        let fresh = self
            .redis
            .publish_transaction(&candidate, self.config.dedupe_ttl_secs)
            .await
            .context("failed to publish transaction to Redis")?;

        if !fresh {
            info!(tx_hash = %candidate.tx_hash, "Signature already published recently, skipping");
        }

        let update = gate.lock().await.on_live_message(candidate.slot);

        match update {
            CheckpointUpdate::Persist(slot) => {
                set_last_ingested_slot(&self.db, &ingest_checkpoint_subject(program), slot)
                    .await
                    .context("failed to update Solana ingestion checkpoint")?;

                info!(
                    program,
                    tx_hash = %candidate.tx_hash,
                    slot = candidate.slot,
                    "Published transaction to Redis and updated ingestion checkpoint"
                );
            }
            CheckpointUpdate::Deferred => {
                // The backfill owns the checkpoint interval until it finishes. Refresh
                // only the heartbeat, so `/health` still sees live ingestion while the
                // checkpoint stays before the unreplayed gap.
                touch_last_ingested_slot(&self.db, &ingest_checkpoint_subject(program))
                    .await
                    .context("failed to refresh the Solana ingestion heartbeat")?;

                info!(
                    program,
                    tx_hash = %candidate.tx_hash,
                    slot = candidate.slot,
                    "Published transaction to Redis; checkpoint advance deferred to the backfill"
                );
            }
        }

        Ok(())
    }
}

/// The backfill task owned by one WebSocket connection.
struct BackfillTask {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl BackfillTask {
    /// Cancels the task and waits for it, so a connection's backfill cleanup completes
    /// before a reconnect starts a fresh one.
    async fn shutdown(self) {
        self.cancel.cancel();

        match self.handle.await {
            Ok(()) => {}
            Err(error) if error.is_panic() => {
                error!(%error, "Solana transaction backfill task panicked");
            }
            Err(error) => {
                warn!(
                    %error,
                    "Solana transaction backfill task ended without completing"
                );
            }
        }
    }
}

enum BackfillOutcome {
    Finished(BackfillStats),
    Failed(anyhow::Error),
    Cancelled,
}

/// Runs one connection's backfill and applies its result to the checkpoint gate.
///
/// It never propagates a failure to the live read loop. A failed or panicked backfill
/// leaves the gate closed, so the checkpoint stays at `start_checkpoint` and the next
/// reconnect retries the same gap. A successful completion opens the gate and persists
/// the newest live slot seen while it ran (the store keeps the maximum).
async fn run_backfill_task(
    ingester: SolanaIngester,
    program: String,
    start_checkpoint: u64,
    gate: Arc<Mutex<CheckpointGate>>,
    cancel: CancellationToken,
) {
    let _inflight = BackfillInflight::start(&program);
    let started = Instant::now();

    let outcome = AssertUnwindSafe(async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => BackfillOutcome::Cancelled,
            result = ingester.backfill_missed_transactions(&program, start_checkpoint) => {
                match result {
                    Ok(stats) => BackfillOutcome::Finished(stats),
                    Err(error) => BackfillOutcome::Failed(error),
                }
            }
        }
    })
    .catch_unwind()
    .await;

    let elapsed = started.elapsed();
    metrics::histogram!(
        crate::metrics::INGEST_BACKFILL_DURATION_SECONDS,
        "program" => program.clone()
    )
    .record(elapsed.as_secs_f64());

    match outcome {
        Ok(BackfillOutcome::Finished(stats)) => {
            let checkpoint = {
                let mut gate = gate.lock().await;
                gate.on_backfill_finished()
            };

            if let Some(slot) = checkpoint
                && let Err(error) =
                    set_last_ingested_slot(&ingester.db, &ingest_checkpoint_subject(&program), slot)
                        .await
            {
                error!(
                    %error,
                    program,
                    slot,
                    "Failed to persist the checkpoint released by the backfill; the next reconnect replays"
                );
            }

            info!(
                program,
                published = stats.published,
                skipped = stats.skipped,
                elapsed_ms = elapsed.as_millis(),
                "Solana transaction backfill finished"
            );
        }
        Ok(BackfillOutcome::Failed(error)) => {
            gate.lock().await.on_backfill_failed();
            error!(
                program,
                %error,
                "Solana transaction backfill failed, live ingestion continues and the next reconnect retries"
            );
        }
        Ok(BackfillOutcome::Cancelled) => {
            info!("Solana transaction backfill cancelled with its connection");
        }
        Err(panic) => {
            gate.lock().await.on_backfill_failed();
            error!(
                program,
                panic = panic_message(panic.as_ref()),
                "Solana transaction backfill panicked, live ingestion continues and the next reconnect retries"
            );
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Holds `ingest_backfill_inflight` at 1 for the lifetime of the task, including a panic
/// unwind, so the gauge cannot leak.
struct BackfillInflight {
    program: String,
}

impl BackfillInflight {
    fn start(program: &str) -> Self {
        metrics::gauge!(
            crate::metrics::INGEST_BACKFILL_INFLIGHT,
            "program" => program.to_string()
        )
        .increment(1.0);
        Self {
            program: program.to_string(),
        }
    }
}

impl Drop for BackfillInflight {
    fn drop(&mut self) {
        metrics::gauge!(
            crate::metrics::INGEST_BACKFILL_INFLIGHT,
            "program" => self.program.clone()
        )
        .decrement(1.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    const ID: u64 = 1;

    fn frame(text: &str) -> Message {
        Message::Text(text.to_owned())
    }

    fn programs(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn staggers_connections_by_config_order() {
        let programs = programs(&["a", "b", "c"]);

        let (live, skipped) = plan_connections(&programs, None, 500);

        assert!(skipped.is_empty());
        assert_eq!(
            live,
            vec![
                ("a", Duration::ZERO),
                ("b", Duration::from_millis(500)),
                ("c", Duration::from_millis(1000)),
            ]
        );
    }

    #[test]
    fn caps_live_connections_to_the_first_programs_in_config_order() {
        let programs = programs(&["a", "b", "c", "d"]);

        let (live, skipped) = plan_connections(&programs, Some(2), 500);

        assert_eq!(
            live,
            vec![("a", Duration::ZERO), ("b", Duration::from_millis(500))]
        );
        assert_eq!(skipped, vec!["c", "d"]);
    }

    #[test]
    fn a_cap_wider_than_the_list_skips_nothing() {
        let programs = programs(&["a", "b"]);

        let (live, skipped) = plan_connections(&programs, Some(10), 500);

        assert_eq!(live.len(), 2);
        assert!(skipped.is_empty());
    }

    #[test]
    fn reconnect_backoff_stays_inside_its_jitter_window() {
        let base = Duration::from_secs(5);

        for _ in 0..100 {
            let delay = reconnect_backoff(base);

            assert!(delay >= base, "{delay:?}");
            assert!(delay <= base + Duration::from_millis(2500), "{delay:?}");
        }
    }

    #[test]
    fn accepts_a_confirmed_subscription() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","result":23784,"id":1}"#, ID);

        assert_eq!(
            reply.unwrap(),
            SubscribeReply::Confirmed {
                subscription: Some(23784)
            }
        );
    }

    #[test]
    fn accepts_a_confirmation_without_a_numeric_subscription_id() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","result":true,"id":1}"#, ID);

        assert_eq!(
            reply.unwrap(),
            SubscribeReply::Confirmed { subscription: None }
        );
    }

    #[test]
    fn surfaces_a_json_rpc_error_object() {
        let reply = parse_subscribe_reply(
            r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid params"},"id":1}"#,
            ID,
        );

        match reply.unwrap() {
            SubscribeReply::Rejected(reason) => assert!(reason.contains("Invalid params")),
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_reply_without_a_result() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","id":1}"#, ID);

        match reply.unwrap() {
            SubscribeReply::Rejected(reason) => assert!(reason.contains("no result")),
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn ignores_a_reply_with_a_different_id() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","result":23784,"id":2}"#, ID);

        assert_eq!(reply.unwrap(), SubscribeReply::Unrelated);
    }

    #[test]
    fn ignores_a_reply_with_a_non_numeric_id() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","result":23784,"id":"1"}"#, ID);

        assert_eq!(reply.unwrap(), SubscribeReply::Unrelated);
    }

    #[test]
    fn ignores_a_reply_with_a_missing_id() {
        let reply = parse_subscribe_reply(r#"{"jsonrpc":"2.0","result":23784}"#, ID);

        assert_eq!(reply.unwrap(), SubscribeReply::Unrelated);
    }

    #[test]
    fn ignores_a_logs_notification() {
        let reply = parse_subscribe_reply(
            r#"{"jsonrpc":"2.0","method":"logsNotification","params":{"result":{"context":{"slot":1},"value":{"signature":"sig","err":null}}}}"#,
            ID,
        );

        assert_eq!(reply.unwrap(), SubscribeReply::Unrelated);
    }

    #[test]
    fn malformed_json_is_an_error() {
        assert!(parse_subscribe_reply("not json", ID).is_err());
    }

    #[test]
    fn redacts_the_query_string_from_a_connect_error_url() {
        let ws_url = "wss://mainnet.helius-rpc.com/?api-key=SECRET";
        let text = format!(
            "failed to connect to Solana WebSocket: URL error: Unable to connect to {ws_url}"
        );

        let redacted = redact_ws_url(&text, ws_url);

        assert!(
            redacted.contains("wss://mainnet.helius-rpc.com/"),
            "{redacted}"
        );
        assert!(redacted.contains("URL error"), "{redacted}");
        assert!(!redacted.contains("SECRET"), "{redacted}");
        assert!(!redacted.contains("api-key"), "{redacted}");
    }

    #[test]
    fn redacts_a_query_string_without_the_host() {
        let ws_url = "wss://mainnet.helius-rpc.com/?api-key=SECRET";

        let redacted = redact_ws_url("request to ?api-key=SECRET failed", ws_url);

        assert_eq!(redacted, "request to ?[redacted] failed");
    }

    #[test]
    fn leaves_errors_without_a_query_string_alone() {
        let text = "failed to connect to Solana WebSocket: HTTP error: 403 Forbidden";

        assert_eq!(redact_ws_url(text, "wss://mainnet.helius-rpc.com/"), text);
    }

    #[tokio::test]
    async fn waits_until_a_matching_confirmation_arrives() {
        let mut frames = stream::iter(vec![
            Ok::<_, tungstenite::Error>(frame(r#"{"jsonrpc":"2.0","result":1,"id":2}"#)),
            Ok::<_, tungstenite::Error>(frame(r#"{"jsonrpc":"2.0","result":23784,"id":1}"#)),
        ]);

        let subscription = wait_for_subscribe_reply(&mut frames, ID, Duration::from_secs(1)).await;

        assert_eq!(subscription.unwrap(), Some(23784));
    }

    #[tokio::test]
    async fn a_rejection_fails_the_wait() {
        let mut frames = stream::iter(vec![Ok::<_, tungstenite::Error>(frame(
            r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid params"},"id":1}"#,
        ))]);

        let error = wait_for_subscribe_reply(&mut frames, ID, Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("logsSubscribe rejected"));
    }

    #[tokio::test]
    async fn a_missing_reply_times_out() {
        let mut frames = stream::pending();

        let error = wait_for_subscribe_reply(&mut frames, ID, Duration::from_millis(20))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("timed out"));
    }

    fn keepalive(ping_secs: u64, watchdog_secs: u64) -> Keepalive {
        Keepalive::new(
            Duration::from_secs(ping_secs),
            Duration::from_secs(watchdog_secs),
            Instant::now(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_connection_is_dropped_at_the_watchdog() {
        let start = Instant::now();
        let mut keepalive = keepalive(30, 90);
        let mut pings = 0;

        loop {
            tokio::time::sleep_until(keepalive.next_deadline()).await;

            match keepalive.on_deadline(Instant::now()) {
                KeepaliveAction::Ping => pings += 1,
                KeepaliveAction::Watchdog => break,
                KeepaliveAction::Idle => {}
            }
        }

        assert_eq!(pings, 2, "pings at 30 s and 60 s");
        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_secs(90),
            "the watchdog fires once no frame arrived for its interval"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pongs_keep_a_quiet_connection_alive() {
        let mut keepalive = keepalive(30, 90);
        let mut pings = 0;

        for _ in 0..10 {
            tokio::time::sleep_until(keepalive.next_deadline()).await;

            match keepalive.on_deadline(Instant::now()) {
                KeepaliveAction::Ping => {
                    pings += 1;
                    // The server's Pong arrives and proves the connection is alive.
                    keepalive.on_frame(Instant::now());
                }
                KeepaliveAction::Watchdog => {
                    panic!("watchdog fired although a pong arrived after every ping")
                }
                KeepaliveAction::Idle => {}
            }
        }

        assert_eq!(pings, 10);
    }

    #[tokio::test(start_paused = true)]
    async fn a_data_frame_resets_the_watchdog() {
        let start = Instant::now();
        let mut keepalive = keepalive(30, 90);

        // A data frame at t=80 moves the watchdog from 90 to 170.
        tokio::time::sleep(Duration::from_secs(80)).await;
        keepalive.on_frame(Instant::now());

        loop {
            tokio::time::sleep_until(keepalive.next_deadline()).await;

            match keepalive.on_deadline(Instant::now()) {
                KeepaliveAction::Ping => {}
                KeepaliveAction::Watchdog => break,
                KeepaliveAction::Idle => {}
            }
        }

        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_secs(170),
            "the data frame pushed the watchdog out by 80 seconds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_ping_interval_fires_as_configured() {
        let start = Instant::now();
        let mut keepalive = keepalive(15, 90);

        assert_eq!(
            keepalive.next_deadline(),
            start + Duration::from_secs(15),
            "the first ping is due after one interval"
        );

        tokio::time::sleep_until(keepalive.next_deadline()).await;
        assert_eq!(keepalive.on_deadline(Instant::now()), KeepaliveAction::Ping);
        assert_eq!(
            keepalive.next_deadline(),
            start + Duration::from_secs(30),
            "the next ping is scheduled one interval after the last one"
        );
    }
}
