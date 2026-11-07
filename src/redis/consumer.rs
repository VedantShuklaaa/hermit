use crate::db::repository::persist_analysis;
use crate::domain::analysis::Analysis;
use crate::error::{AppError, persistence_error_is_transient};
use crate::services::analyzer::analyse_tx;
use crate::state::AppState;
use anyhow::{Context, Result};
use redis::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::types::CandidateTransaction; // moved from ingestion::solana

const BRIDGE_TX_STREAM: &str = "bridge:transactions";
const CONSUMER_GROUP: &str = "bridge-workers";
const FAILED_TX_STREAM: &str = "bridge:transactions:failed";

pub struct RedisConsumer {
    client: redis::Client,
    state: AppState,
    consumer_name: String,
    shutdown: CancellationToken,
}

impl RedisConsumer {
    pub fn new(
        redis_url: &str,
        state: AppState,
        consumer_name: String,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client,
            state,
            consumer_name,
            shutdown,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to Redis")?;

        self.create_consumer_group(&mut connection).await?;

        info!(
            stream = BRIDGE_TX_STREAM,
            group = CONSUMER_GROUP,
            consumer = %self.consumer_name,
            "Redis consumer started"
        );

        loop {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(CONSUMER_GROUP)
                .arg(&self.consumer_name)
                .arg("COUNT")
                .arg(self.state.config.worker.read_count as i64)
                .arg("BLOCK")
                .arg(5000)
                .arg("STREAMS")
                .arg(BRIDGE_TX_STREAM)
                .arg(">");

            let read_fut = cmd.query_async::<Value>(&mut connection);

            let response = tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!(consumer = %self.consumer_name, "shutdown requested, stopping consumer");
                    return Ok(());
                }
                result = read_fut => result?,
            };

            // A new message has been delivered once.
            self.process_messages(&mut connection, response, &HashMap::new())
                .await?;
        }
    }

    pub async fn run_recovery(&self) -> Result<()> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to Redis recovery worker")?;

        info!(
            stream = BRIDGE_TX_STREAM,
            group = CONSUMER_GROUP,
            consumer = %self.consumer_name,
            "Redis recovery worker started"
        );

        let interval = Duration::from_secs(self.state.config.worker.reclaim_interval_secs);

        loop {
            self.reclaim_pending_messages(&mut connection).await?;

            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("shutdown requested, stopping recovery worker");
                    return Ok(());
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }

    async fn create_consumer_group(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
    ) -> Result<()> {
        let result: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(BRIDGE_TX_STREAM)
            .arg(CONSUMER_GROUP)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(connection)
            .await;

        match result {
            Ok(_) => {
                info!(
                    stream = BRIDGE_TX_STREAM,
                    group = CONSUMER_GROUP,
                    "Created Redis consumer group"
                );
            }
            Err(error) if error.to_string().contains("BUSYGROUP") => {
                info!(
                    group = CONSUMER_GROUP,
                    "Redis consumer group already exists"
                );
            }
            Err(error) => return Err(error.into()),
        }

        Ok(())
    }

    /// Analyses a transaction, waiting out the cases that fix themselves.
    ///
    /// A fresh Solana transfer has no Wormhole VAA until the guardians have seen it
    /// finalised, which takes well over the few seconds the other transient errors
    /// get, so a missing VAA is retried on its own, longer schedule.
    async fn analyse_with_retry(&self, chain: &str, tx_hash: &str) -> Result<Analysis, AppError> {
        const MAX_TRANSIENT_ATTEMPTS: u32 = 5;
        const VAA_RETRY_INTERVAL: Duration = Duration::from_secs(5);
        const VAA_MAX_WAIT: Duration = Duration::from_secs(90);

        let started = Instant::now();
        let mut transient_attempts = 0u32;

        loop {
            match analyse_tx(&self.state, chain, tx_hash).await {
                Ok(tx) => return Ok(tx),

                Err(AppError::VaaNotAvailable(_)) if started.elapsed() < VAA_MAX_WAIT => {
                    info!(
                        tx_hash = %tx_hash,
                        waited_secs = started.elapsed().as_secs(),
                        "VAA not available yet, retrying"
                    );
                    tokio::time::sleep(VAA_RETRY_INTERVAL).await;
                }

                // Everything short of a missing VAA (matched above) that a retry can
                // plausibly fix: shed permits, timeouts, 5xx provider answers.
                Err(error)
                    if error.is_transient() && transient_attempts + 1 < MAX_TRANSIENT_ATTEMPTS =>
                {
                    transient_attempts += 1;
                    let delay_ms = 500 * 2_u64.pow(transient_attempts - 1);

                    tracing::warn!(
                        tx_hash = %tx_hash,
                        attempt = transient_attempts,
                        delay_ms,
                        "Transaction not available yet, retrying"
                    );

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                Err(error) => return Err(error),
            }
        }
    }

    async fn persist_with_retry(&self, analysed: &Analysis) -> Result<()> {
        const MAX_ATTEMPTS: u32 = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            match persist_analysis(&self.state.db, &analysed.tx, &analysed.transfers).await {
                Ok(()) => return Ok(()),

                Err(error) if attempt < MAX_ATTEMPTS => {
                    let delay_ms = 1000 * 2_u64.pow(attempt - 1);

                    tracing::warn!(
                        worker = %self.consumer_name,
                        attempt,
                        delay_ms,
                        ?error,
                        "Database persistence failed, retrying"
                    );

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                Err(error) => return Err(error),
            }
        }

        unreachable!()
    }

    async fn dead_letter(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
        message_id: &str,
        candidate: &CandidateTransaction,
        error: &str,
    ) -> Result<()> {
        let _: String = redis::cmd("XADD")
            .arg(FAILED_TX_STREAM)
            .arg("*")
            .arg("message_id")
            .arg(message_id)
            .arg("chain")
            .arg(&candidate.chain)
            .arg("tx_hash")
            .arg(&candidate.tx_hash)
            .arg("slot")
            .arg(candidate.slot)
            .arg("error")
            .arg(error)
            .query_async(&mut *connection)
            .await?;

        let _: i64 = redis::cmd("XACK")
            .arg(BRIDGE_TX_STREAM)
            .arg(CONSUMER_GROUP)
            .arg(message_id)
            .query_async(&mut *connection)
            .await?;

        tracing::error!(
            worker = %self.consumer_name,
            message_id = %message_id,
            tx_hash = %candidate.tx_hash,
            "Moved failed transaction to dead-letter stream"
        );

        Ok(())
    }

    async fn reclaim_pending_messages(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
    ) -> Result<()> {
        let response: Value = redis::cmd("XAUTOCLAIM")
            .arg(BRIDGE_TX_STREAM)
            .arg(CONSUMER_GROUP)
            .arg(&self.consumer_name)
            .arg(self.state.config.worker.reclaim_idle_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(10)
            .query_async(&mut *connection)
            .await?;

        let Value::Array(mut parts) = response else {
            return Ok(());
        };

        if parts.len() < 2 {
            return Ok(());
        }

        let messages = parts.remove(1);

        let Value::Array(messages) = messages else {
            return Ok(());
        };

        if messages.is_empty() {
            return Ok(());
        }

        info!(count = messages.len(), "Reclaimed pending Redis messages");

        // XAUTOCLAIM does not report how often a message has been delivered, and it
        // has already incremented the counter by now, so read it from the pending list.
        let message_ids: Vec<String> = messages
            .iter()
            .filter_map(|message| match message {
                Value::Array(parts) => match parts.first() {
                    Some(Value::BulkString(bytes)) => String::from_utf8(bytes.clone()).ok(),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        let deliveries = self.delivery_counts(connection, &message_ids).await?;

        let response = Value::Array(vec![Value::Array(vec![
            Value::BulkString(BRIDGE_TX_STREAM.as_bytes().to_vec()),
            Value::Array(messages),
        ])]);

        self.process_messages(connection, response, &deliveries)
            .await?;

        Ok(())
    }

    /// Times each message has been delivered, from the group's pending list. The
    /// extended `XPENDING` form answers `[id, consumer, idle, times-delivered]`.
    async fn delivery_counts(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
        message_ids: &[String],
    ) -> Result<HashMap<String, u64>> {
        let mut counts = HashMap::new();
        let (Some(first), Some(last)) = (message_ids.first(), message_ids.last()) else {
            return Ok(counts);
        };

        let rows: Value = redis::cmd("XPENDING")
            .arg(BRIDGE_TX_STREAM)
            .arg(CONSUMER_GROUP)
            .arg(first)
            .arg(last)
            .arg(message_ids.len())
            .query_async(&mut *connection)
            .await?;

        let Value::Array(rows) = rows else {
            return Ok(counts);
        };

        for row in rows {
            let Value::Array(entry) = row else {
                continue;
            };
            let (Some(Value::BulkString(id)), Some(Value::Int(deliveries))) =
                (entry.first(), entry.get(3))
            else {
                continue;
            };
            if let Ok(id) = String::from_utf8(id.clone()) {
                counts.insert(id, *deliveries as u64);
            }
        }

        Ok(counts)
    }

    async fn process_messages(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
        response: Value,
        deliveries: &HashMap<String, u64>,
    ) -> Result<()> {
        let Value::Array(streams) = response else {
            return Ok(());
        };

        let mut batch: Vec<(String, CandidateTransaction, u64)> = Vec::new();

        for stream in streams {
            let Value::Array(mut stream_parts) = stream else {
                continue;
            };

            if stream_parts.len() != 2 {
                continue;
            }

            let messages = stream_parts.pop().unwrap();

            let Value::Array(messages) = messages else {
                continue;
            };

            for message in messages {
                let Value::Array(mut message_parts) = message else {
                    continue;
                };

                if message_parts.len() != 2 {
                    continue;
                }

                let fields = message_parts.pop().unwrap();
                let message_id = message_parts.pop().unwrap();

                let message_id = match message_id {
                    Value::BulkString(bytes) => String::from_utf8(bytes)?,
                    _ => continue,
                };

                let Value::Array(fields) = fields else {
                    continue;
                };

                let mut chain = None;
                let mut tx_hash = None;
                let mut slot = None;

                for pair in fields.chunks(2) {
                    if pair.len() != 2 {
                        continue;
                    }

                    let Value::BulkString(key) = &pair[0] else {
                        continue;
                    };

                    let Value::BulkString(value) = &pair[1] else {
                        continue;
                    };

                    let key = String::from_utf8(key.clone())?;
                    let value = String::from_utf8(value.clone())?;

                    match key.as_str() {
                        "chain" => chain = Some(value),
                        "tx_hash" => tx_hash = Some(value),
                        "slot" => slot = Some(value.parse::<u64>()?),
                        _ => {}
                    }
                }

                let candidate = CandidateTransaction {
                    chain: chain.unwrap_or_else(|| "unknown".to_string()),
                    tx_hash: tx_hash.context("Redis message missing tx_hash")?,
                    slot: slot.context("Redis message missing slot")?,
                };

                info!(
                    message_id = %message_id,
                    chain = %candidate.chain,
                    tx_hash = %candidate.tx_hash,
                    slot = candidate.slot,
                    "Parsed transaction from Redis"
                );

                // A message read with `>` has been delivered once; a reclaimed one
                // carries how many times the group has handed it to a worker.
                let delivery_count = deliveries.get(&message_id).copied().unwrap_or(1);

                batch.push((message_id, candidate, delivery_count));
            }
        }

        // A transaction waiting for its VAA can take a while, so a batch is handled
        // concurrently instead of letting one slow transaction hold up the rest.
        let results = futures_util::future::join_all(batch.into_iter().map(
            |(message_id, candidate, delivery_count)| {
                let mut connection = connection.clone();
                async move {
                    self.process_candidate(&mut connection, &message_id, &candidate, delivery_count)
                        .await
                }
            },
        ))
        .await;

        for result in results {
            result?;
        }

        Ok(())
    }

    async fn process_candidate(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
        message_id: &str,
        candidate: &CandidateTransaction,
        delivery_count: u64,
    ) -> Result<()> {
        let max_deliveries = u64::from(self.state.config.worker.max_deliveries);

        // The retry budget is what keeps a message that keeps failing from being
        // redelivered forever. Everything below is either a success or a dead letter.
        if delivery_count > max_deliveries {
            tracing::error!(
                worker = %self.consumer_name,
                message_id = %message_id,
                tx_hash = %candidate.tx_hash,
                delivery_count,
                max_deliveries,
                "Transaction exceeded its delivery budget, dead-lettering it"
            );

            self.dead_letter(
                connection,
                message_id,
                candidate,
                &format!("exceeded max deliveries ({delivery_count}) without succeeding"),
            )
            .await?;

            return Ok(());
        }

        match self
            .analyse_with_retry(&candidate.chain, &candidate.tx_hash)
            .await
        {
            Ok(analysed) => {
                if let Err(error) = self.persist_with_retry(&analysed).await {
                    if persistence_error_is_transient(&error) {
                        // Do not ack and do not dead-letter: an outage is not the
                        // transaction's fault, and recovery will redeliver it.
                        tracing::warn!(
                            worker = %self.consumer_name,
                            message_id = %message_id,
                            tx_hash = %candidate.tx_hash,
                            delivery_count,
                            ?error,
                            "Transient persistence failure, leaving the message pending"
                        );
                        return Ok(());
                    }

                    tracing::error!(
                        worker = %self.consumer_name,
                        message_id = %message_id,
                        tx_hash = %candidate.tx_hash,
                        ?error,
                        "Failed to persist transaction after retries"
                    );

                    self.dead_letter(connection, message_id, candidate, &error.to_string())
                        .await?;

                    return Ok(());
                }

                tracing::info!(
                    worker = %self.consumer_name,
                    message_id = %message_id,
                    tx_hash = %candidate.tx_hash,
                    "Transaction persisted successfully"
                );

                // One hook for both consumers: the local channel in `ROLE=all`, Redis
                // pub/sub in `ROLE=stream`, and the server-owned live feed ring.
                crate::live_feed::publish(&self.state, &analysed.tx).await;

                let _: i64 = redis::cmd("XACK")
                    .arg(BRIDGE_TX_STREAM)
                    .arg(CONSUMER_GROUP)
                    .arg(message_id)
                    .query_async(&mut *connection)
                    .await?;

                tracing::info!(
                    worker = %self.consumer_name,
                    message_id = %message_id,
                    tx_hash = %candidate.tx_hash,
                    "Redis message acknowledged"
                );
            }

            Err(error) if error.is_transient() => {
                // No ack: the message stays pending and the recovery worker hands it
                // back after its idle time, up to the delivery budget.
                tracing::warn!(
                    worker = %self.consumer_name,
                    message_id = %message_id,
                    tx_hash = %candidate.tx_hash,
                    delivery_count,
                    ?error,
                    "Transient analysis failure, leaving the message pending"
                );
            }

            Err(error) => {
                tracing::error!(
                    worker = %self.consumer_name,
                    message_id = %message_id,
                    tx_hash = %candidate.tx_hash,
                    ?error,
                    "Failed to analyse transaction"
                );

                self.dead_letter(connection, message_id, candidate, &error.to_string())
                    .await?;
            }
        }

        Ok(())
    }
}
