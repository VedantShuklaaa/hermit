//! Experimental LaserStream (Helius gRPC) ingestion source.
//!
//! It reads the same transactions the `logsSubscribe` source sees (successful
//! transactions mentioning the bridge program, at confirmed commitment) and turns
//! them into the same [`CandidateTransaction`] published to the existing Redis
//! stream. Everything downstream is unchanged.
//!
//! Built only with `--features laserstream`; `logsSubscribe` stays the default.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use helius_laserstream::{
    LaserstreamConfig,
    grpc::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdate,
        subscribe_update::UpdateOneof,
    },
    subscribe,
};
use sqlx::PgPool;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    db::repository::set_last_ingested_slot, ingestion::types::CandidateTransaction,
    redis::producer::RedisProducer,
};

pub struct LaserStreamIngester {
    endpoint: String,
    api_key: String,
    bridge_program: String,
    redis: RedisProducer,
    db: PgPool,
    dedupe_ttl_secs: u64,
}

impl LaserStreamIngester {
    pub fn new(
        endpoint: String,
        api_key: String,
        bridge_program: String,
        redis: RedisProducer,
        db: PgPool,
        dedupe_ttl_secs: u64,
    ) -> Self {
        Self {
            endpoint,
            api_key,
            bridge_program,
            redis,
            db,
            dedupe_ttl_secs,
        }
    }

    /// Runs until `stop` is cancelled or the client gives up reconnecting.
    pub async fn run(&self, stop: CancellationToken) -> Result<()> {
        let config = LaserstreamConfig::new(self.endpoint.clone(), self.api_key.clone());

        let request = SubscribeRequest {
            transactions: HashMap::from([(
                "bridge".to_owned(),
                SubscribeRequestFilterTransactions {
                    account_include: vec![self.bridge_program.clone()],
                    vote: Some(false),
                    failed: Some(false),
                    ..Default::default()
                },
            )]),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        };

        info!(
            endpoint = %self.endpoint,
            program = %self.bridge_program,
            "Subscribing to LaserStream bridge transactions"
        );

        let (stream, _handle) = subscribe(config, request);
        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = stop.cancelled() => {
                    info!("stop requested, closing LaserStream");
                    return Ok(());
                }
                update = stream.next() => {
                    match update {
                        Some(Ok(update)) => self.handle_update(update).await?,
                        Some(Err(error)) => {
                            return Err(error).context("LaserStream subscription failed");
                        }
                        None => anyhow::bail!("LaserStream subscription ended"),
                    }
                }
            }
        }
    }

    async fn handle_update(&self, update: SubscribeUpdate) -> Result<()> {
        let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else {
            return Ok(());
        };

        let Some(transaction) = tx_update.transaction else {
            return Ok(());
        };

        if transaction.is_vote {
            return Ok(());
        }

        if transaction.signature.is_empty() {
            warn!(
                slot = tx_update.slot,
                "LaserStream transaction update has no signature"
            );
            return Ok(());
        }

        let candidate = CandidateTransaction {
            chain: "solana".into(),
            tx_hash: bs58::encode(&transaction.signature).into_string(),
            slot: tx_update.slot,
        };

        info!(
            chain = candidate.chain,
            tx_hash = %candidate.tx_hash,
            slot = candidate.slot,
            "Detected Solana transaction involving bridge program"
        );

        let fresh = self
            .redis
            .publish_transaction(&candidate, self.dedupe_ttl_secs)
            .await
            .context("failed to publish transaction to Redis")?;

        if !fresh {
            info!(tx_hash = %candidate.tx_hash, "Signature already published recently, skipping");
        }

        set_last_ingested_slot(&self.db, "solana", candidate.slot)
            .await
            .context("failed to update Solana ingestion checkpoint")?;

        Ok(())
    }
}
