use crate::chains::{NS_WORMHOLE, RawChainRef};
use crate::domain::analysis::{DecodedTransfer, RowValues};
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use crate::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use crate::error::AppError;
use crate::protocols::{MessageId, amount::RawAmount};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};

pub async fn persist_transaction(pool: &PgPool, tx: &NormalisedTransaction) -> anyhow::Result<()> {
    persist_analysis(pool, tx, &[]).await
}

/// Writes the transaction and every decoded transfer. `transfers` is empty on the
/// WormholeScan-driven path and for re-enrichment, where the v1 body already holds the
/// one transfer to persist.
pub async fn persist_analysis(
    pool: &PgPool,
    tx: &NormalisedTransaction,
    transfers: &[DecodedTransfer],
) -> anyhow::Result<()> {
    let mut db_tx: Transaction<'_, Postgres> = pool.begin().await?;
    let transaction_id = upsert_transaction(&mut db_tx, tx).await?;

    if transfers.is_empty() {
        if let Some(bridge) = &tx.bridge_transfer {
            let analysis = serde_json::to_value(tx)?;
            write_transfer(&mut db_tx, transaction_id, legacy_row(tx, bridge, analysis)).await?;
        }
    } else {
        for (position, decoded) in transfers.iter().enumerate() {
            // The cached v1 analysis follows the row it belongs to. Only the first
            // transfer carries the WormholeScan event `/api/v1` exposes.
            let mut row_tx = tx.clone();
            row_tx.bridge_transfer = decoded.bridge.clone();
            row_tx.bridge_event = if position == 0 {
                tx.bridge_event.clone()
            } else {
                None
            };
            let analysis = serde_json::to_value(&row_tx)?;

            let event_index = i16::try_from(decoded.detected.event_index).map_err(|_| {
                anyhow::anyhow!("event index too large: {}", decoded.detected.event_index)
            })?;
            write_transfer(
                &mut db_tx,
                transaction_id,
                TransferRow {
                    program: decoded.detected.program.clone(),
                    protocol: decoded.detected.message.protocol().as_str().to_string(),
                    direction: decoded.detected.direction.as_str().to_string(),
                    message: decoded.detected.message.clone(),
                    observed_tx_hash: decoded.detected.observed_tx.clone(),
                    event_index,
                    via: decoded.detected.via.as_ref().map(|via| via.name.clone()),
                    via_ref: decoded
                        .detected
                        .via
                        .as_ref()
                        .and_then(|via| via.reference.clone()),
                    source: decoded.detected.source.clone(),
                    destination: decoded.detected.destination.clone(),
                    token_ref: decoded
                        .detected
                        .token
                        .as_ref()
                        .map(|token| token.chain.clone()),
                    values: decoded.row.clone(),
                    analysis,
                },
            )
            .await?;
        }
    }

    db_tx.commit().await?;
    Ok(())
}

async fn upsert_transaction(
    db_tx: &mut Transaction<'_, Postgres>,
    tx: &NormalisedTransaction,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        r#"
        INSERT INTO transactions (
            hash,
            chain,
            status,
            slot,
            timestamp,
            fee_lamports,
            signer
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (hash)
        DO UPDATE SET
            status = EXCLUDED.status
        RETURNING id
        "#,
    )
    .bind(&tx.hash)
    .bind(tx.chain.wormhole_id() as i16)
    .bind(format!("{:?}", tx.status))
    .bind(tx.slot as i64)
    .bind(
        tx.timestamp
            .map(|ts| chrono::DateTime::from_timestamp(ts, 0)),
    )
    .bind(tx.fee_lamports as i64)
    .bind(&tx.signer)
    .fetch_one(&mut **db_tx)
    .await?)
}

/// One `bridge_transfers` row to write, generalised across protocols.
struct TransferRow {
    program: String,
    protocol: String,
    direction: String,
    message: MessageId,
    observed_tx_hash: String,
    event_index: i16,
    via: Option<String>,
    via_ref: Option<String>,
    source: RawChainRef,
    destination: RawChainRef,
    token_ref: Option<RawChainRef>,
    values: RowValues,
    analysis: serde_json::Value,
}

/// The single transfer the v1 body carries, for the WormholeScan-driven path and
/// re-enrichment. Raw chain references are the protocol's own (`wormhole`), and the
/// token chain is unknown to the v1 shape.
fn legacy_row(
    tx: &NormalisedTransaction,
    bridge: &BridgeTransfer,
    analysis: serde_json::Value,
) -> TransferRow {
    TransferRow {
        program: bridge.program.clone(),
        protocol: NS_WORMHOLE.to_string(),
        direction: bridge.direction().as_str().to_string(),
        message: MessageId::Wormhole {
            emitter_chain: bridge.message_id.emitter_chain,
            emitter_address: bridge.message_id.emitter_address.clone(),
            sequence: bridge.message_id.sequence,
        },
        observed_tx_hash: tx.hash.clone(),
        event_index: 0,
        via: None,
        via_ref: None,
        source: RawChainRef::wormhole(bridge.source_chain.wormhole_id()),
        destination: RawChainRef::wormhole(bridge.destination_chain.wormhole_id()),
        token_ref: None,
        values: crate::services::detect::row_values(bridge),
        analysis,
    }
}

async fn write_transfer(
    db_tx: &mut Transaction<'_, Postgres>,
    transaction_id: i64,
    row: TransferRow,
) -> anyhow::Result<()> {
    // An amount that is not a plain integer is stored as NULL. The analysis JSON keeps it.
    let amount = row
        .values
        .amount
        .as_deref()
        .and_then(RawAmount::parse)
        .map(|a| a.to_string());

    let transfer_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO bridge_transfers (
            transaction_id, protocol, message_key, direction, observed_tx_hash,
            event_index, source_tx_hash,
            source_ref_protocol, source_ref_id, source_chain_id,
            source_wallet, source_explorer_url,
            destination_ref_protocol, destination_ref_id, destination_chain_id,
            destination_wallet, destination_tx_hash, destination_explorer_url,
            token_ref_protocol, token_ref_id, token_chain_id,
            token, token_symbol, amount, amount_formatted, status, analysis,
            via, via_ref, program
        )
        VALUES (
            $1, $2, $3, $4, $5,
            $6, $7,
            $8, $9, resolve_chain($8, $9),
            $10, $11,
            $12, $13, resolve_chain($12, $13),
            $14, $15, $16,
            $17, $18, resolve_chain($17, $18),
            $19, $20, $21::numeric, $22, $23, $24,
            $25, $26, $27
        )
        ON CONFLICT (protocol, message_key)
        DO UPDATE SET
            -- an inbound redeem learns its origin tx later, from enrichment
            source_tx_hash = COALESCE(EXCLUDED.source_tx_hash, bridge_transfers.source_tx_hash),
            source_explorer_url = COALESCE(EXCLUDED.source_explorer_url, bridge_transfers.source_explorer_url),
            destination_tx_hash = COALESCE(EXCLUDED.destination_tx_hash, bridge_transfers.destination_tx_hash),
            destination_wallet = COALESCE(EXCLUDED.destination_wallet, bridge_transfers.destination_wallet),
            destination_explorer_url = COALESCE(EXCLUDED.destination_explorer_url, bridge_transfers.destination_explorer_url),
            token_symbol = COALESCE(EXCLUDED.token_symbol, bridge_transfers.token_symbol),
            amount_formatted = COALESCE(EXCLUDED.amount_formatted, bridge_transfers.amount_formatted),
            -- composition can fill the router and token chain on a later pass
            via = COALESCE(EXCLUDED.via, bridge_transfers.via),
            via_ref = COALESCE(EXCLUDED.via_ref, bridge_transfers.via_ref),
            token_ref_protocol = COALESCE(EXCLUDED.token_ref_protocol, bridge_transfers.token_ref_protocol),
            token_ref_id = COALESCE(EXCLUDED.token_ref_id, bridge_transfers.token_ref_id),
            token_chain_id = COALESCE(EXCLUDED.token_chain_id, bridge_transfers.token_chain_id),
            status = CASE WHEN bridge_transfers.status = 'Completed'
                            OR (bridge_transfers.status = 'Pending' AND EXCLUDED.status = 'Detected')
                          THEN bridge_transfers.status ELSE EXCLUDED.status END,
            -- only replace the cached response if it describes the same observed tx and
            -- does not lose a tx hash that the stored response already knew
            analysis = CASE WHEN bridge_transfers.observed_tx_hash = EXCLUDED.observed_tx_hash
                             AND NOT (bridge_transfers.status = 'Pending' AND EXCLUDED.status = 'Detected')
                             AND NOT (
                                 bridge_transfers.analysis #>> '{bridge_transfer,destination_tx_hash}' IS NOT NULL
                                 AND EXCLUDED.analysis #>> '{bridge_transfer,destination_tx_hash}' IS NULL
                             )
                             AND NOT (
                                 bridge_transfers.analysis #>> '{bridge_transfer,source_tx_hash}' IS NOT NULL
                                 AND EXCLUDED.analysis #>> '{bridge_transfer,source_tx_hash}' IS NULL
                             )
                            THEN EXCLUDED.analysis ELSE bridge_transfers.analysis END,
            updated_at = NOW()
        RETURNING id
        "#,
    )
    .bind(transaction_id)
    .bind(&row.protocol)
    .bind(row.message.key())
    .bind(&row.direction)
    .bind(&row.observed_tx_hash)
    .bind(row.event_index)
    .bind(&row.values.source_tx_hash)
    .bind(&row.source.protocol)
    .bind(&row.source.id)
    .bind(&row.values.source_wallet)
    .bind(&row.values.source_explorer_url)
    .bind(&row.destination.protocol)
    .bind(&row.destination.id)
    .bind(&row.values.destination_wallet)
    .bind(&row.values.destination_tx_hash)
    .bind(&row.values.destination_explorer_url)
    .bind(row.token_ref.as_ref().map(|r| r.protocol.as_str()))
    .bind(row.token_ref.as_ref().map(|r| r.id.as_str()))
    .bind(&row.values.token)
    .bind(&row.values.token_symbol)
    .bind(amount)
    .bind(&row.values.amount_formatted)
    .bind(format!("{:?}", row.values.status))
    .bind(row.analysis)
    .bind(row.via)
    .bind(row.via_ref)
    .bind(row.program)
    .fetch_one(&mut **db_tx)
    .await?;

    // One message identity per row, stored in the detail table for its protocol.
    match &row.message {
        MessageId::Wormhole {
            emitter_chain,
            emitter_address,
            sequence,
        } => {
            sqlx::query(
                r#"
                INSERT INTO wormhole_messages (transfer_id, emitter_chain, emitter_address, sequence)
                VALUES ($1, $2, $3, $4::numeric)
                ON CONFLICT (transfer_id) DO NOTHING
                "#,
            )
            .bind(transfer_id)
            .bind(i32::from(*emitter_chain))
            .bind(emitter_address)
            .bind(sequence.to_string())
            .execute(&mut **db_tx)
            .await?;
        }
        MessageId::Cctp {
            source_domain,
            nonce,
        } => {
            let source_domain = i32::try_from(*source_domain).map_err(|_| {
                AppError::Normalisation(format!("cctp source domain out of range: {source_domain}"))
            })?;
            sqlx::query(
                r#"
                INSERT INTO cctp_messages (transfer_id, source_domain, nonce, version)
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (transfer_id) DO NOTHING
                "#,
            )
            .bind(transfer_id)
            .bind(source_domain)
            .bind(nonce)
            .execute(&mut **db_tx)
            .await?;
        }
    }

    Ok(())
}

/// The checkpoint namespace for one program: `solana:<program>`. Checkpoint helpers
/// append `_last_ingested_slot` to it.
pub fn ingest_checkpoint_subject(program: &str) -> String {
    format!("solana:{program}")
}

/// The full `indexer_state` key for one program's ingest checkpoint.
pub fn ingest_checkpoint_key(program: &str) -> String {
    format!("{}_last_ingested_slot", ingest_checkpoint_subject(program))
}

pub async fn get_last_ingested_slot(pool: &PgPool, chain: &str) -> Result<u64> {
    let key = format!("{chain}_last_ingested_slot");
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM indexer_state WHERE key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;

    Ok(value.map(|v| v.parse()).transpose()?.unwrap_or(0))
}

/// Records the newest slot ingested for `chain`. The checkpoint only moves forward: log
/// notifications can arrive out of slot order, and rewinding would make the next backfill
/// replay (or skip past) work that was already done.
pub async fn set_last_ingested_slot(pool: &PgPool, chain: &str, slot: u64) -> Result<()> {
    let key = format!("{chain}_last_ingested_slot");
    sqlx::query(
        "INSERT INTO indexer_state (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE
         SET value = GREATEST(indexer_state.value::bigint, EXCLUDED.value::bigint)::text,
             -- the value only moves forward, but the heartbeat tracks every handled
             -- notification, including ones at or below the checkpoint (/health).
             updated_at = NOW()",
    )
    .bind(&key)
    .bind(slot.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Refreshes the ingest heartbeat (`updated_at`) without moving the checkpoint. Used
/// while a concurrent backfill defers checkpoint advances: live notifications are still
/// being handled, so `/health` must not go stale, but the value must not pass the
/// unreplayed gap. The row exists whenever a backfill is running, because a backfill
/// only starts from a non-zero checkpoint.
pub async fn touch_last_ingested_slot(pool: &PgPool, chain: &str) -> Result<()> {
    let key = format!("{chain}_last_ingested_slot");
    sqlx::query("UPDATE indexer_state SET updated_at = NOW() WHERE key = $1")
        .bind(&key)
        .execute(pool)
        .await?;
    Ok(())
}

/// Seeds a program's per-program checkpoint from the legacy `solana_last_ingested_slot`
/// row, once. Copies the value and `updated_at`, so the Token Bridge resumes at exactly
/// the slot the single-program ingester left off (nothing re-ingested or skipped) and
/// `/health` does not read a falsely young heartbeat. An existing per-program row is
/// never overwritten. Returns whether a row was created.
pub async fn migrate_legacy_ingest_checkpoint(pool: &PgPool, program: &str) -> Result<bool> {
    let inserted: Option<i64> = sqlx::query_scalar(
        r#"
        INSERT INTO indexer_state (key, value, updated_at)
        SELECT $1, value, updated_at
        FROM indexer_state
        WHERE key = 'solana_last_ingested_slot'
        ON CONFLICT (key) DO NOTHING
        RETURNING 1::bigint
        "#,
    )
    .bind(ingest_checkpoint_key(program))
    .fetch_optional(pool)
    .await?;

    Ok(inserted.is_some())
}

/// The end of the last successful reconciliation-poll window for `program`.
pub async fn get_last_poll_end(pool: &PgPool, program: &str) -> Result<Option<i64>> {
    let key = format!("reconcile_poll_{program}_last_end");
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM indexer_state WHERE key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;

    Ok(value.map(|v| v.parse()).transpose()?)
}

/// Records the end of a reconciliation-poll window. Like the ingest checkpoint, it
/// only moves forward: a cycle that began before the stored end, or a wall clock that
/// stepped back, must not rewind it and replay the same window.
pub async fn set_last_poll_end(pool: &PgPool, program: &str, ended_at: i64) -> Result<()> {
    let key = format!("reconcile_poll_{program}_last_end");
    sqlx::query(
        "INSERT INTO indexer_state (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE
         SET value = GREATEST(indexer_state.value::bigint, EXCLUDED.value::bigint)::text,
             updated_at = NOW()",
    )
    .bind(&key)
    .bind(ended_at.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_cached_analysis(pool: &PgPool, hash: &str) -> Result<Option<serde_json::Value>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT analysis FROM bridge_transfers
        WHERE observed_tx_hash = $1
          AND analysis IS NOT NULL
          AND (status = 'Completed' OR updated_at > NOW() - INTERVAL '30 seconds')
        ORDER BY event_index
        LIMIT 1
        "#,
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?)
}

/// A transfer due for another enrichment attempt.
pub struct DueTransfer {
    pub id: i64,
    pub observed_tx_hash: String,
    /// The cached v1 body, when the row still has one.
    pub analysis: Option<serde_json::Value>,
    /// The same body rebuilt from the stored row, when a repair migration dropped the
    /// cached analysis. `None` for every other due row.
    pub rebuilt: Option<NormalisedTransaction>,
}

impl DueTransfer {
    /// The transaction to re-enrich and persist, from the cached body or from the row.
    pub fn transaction(self) -> Option<NormalisedTransaction> {
        let cached = self
            .analysis
            .and_then(|value| serde_json::from_value(value).ok());
        cached.or(self.rebuilt)
    }
}

/// Open transfers that are old enough to leave to enrichment (`min_age_secs` since they
/// were created), young enough to still be worth asking about (`max_age_secs`) and not
/// attempted in the last `retry_secs`. Least recently attempted first. Served by
/// `idx_bt_open`.
pub async fn list_open_transfers(
    pool: &PgPool,
    min_age_secs: i64,
    max_age_secs: i64,
    retry_secs: i64,
    limit: i64,
) -> Result<Vec<DueTransfer>> {
    let rows: Vec<(i64, String, serde_json::Value)> = sqlx::query_as(
        r#"
        SELECT id, observed_tx_hash, analysis
        FROM bridge_transfers
        WHERE status IN ('Detected', 'Pending')
          AND analysis IS NOT NULL
          AND created_at < NOW() - ($1::double precision * INTERVAL '1 second')
          AND created_at > NOW() - ($2::double precision * INTERVAL '1 second')
          AND updated_at < NOW() - ($3::double precision * INTERVAL '1 second')
        ORDER BY updated_at ASC
        LIMIT $4
        "#,
    )
    .bind(min_age_secs)
    .bind(max_age_secs)
    .bind(retry_secs)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, observed_tx_hash, analysis)| DueTransfer {
            id,
            observed_tx_hash,
            analysis: Some(analysis),
            rebuilt: None,
        })
        .collect())
}

/// Completed inbound Wormhole rows whose origin tx is still unknown, due for another
/// attempt. A redeem is stored `Completed` before WormholeScan has matched its operation,
/// so the origin tx arrives later. Unlike open transfers there is no `max_age_secs`: the
/// origin tx can become known at any time, and the row is otherwise terminal.
///
/// The retry window uses `last_enrich_attempt_at`, which is written for every attempt
/// whatever its outcome, so a row that learns nothing still moves to the back of the
/// queue. Never-attempted rows come first. Rows whose cached analysis was dropped by a
/// repair are still returned: [`DueTransfer::transaction`] rebuilds the body from the
/// stored transaction, transfer and message columns. Served by `idx_bt_missing_origin`.
pub async fn list_missing_origin_transfers(
    pool: &PgPool,
    min_age_secs: i64,
    retry_secs: i64,
    limit: i64,
) -> Result<Vec<DueTransfer>> {
    let rows: Vec<MissingOriginRow> = sqlx::query_as(
        r#"
        SELECT b.id,
               b.observed_tx_hash,
               b.analysis,
               t.hash AS tx_hash,
               t.chain AS tx_chain,
               t.status AS tx_status,
               t.slot,
               t.timestamp,
               t.fee_lamports,
               t.signer,
               b.source_ref_id,
               b.source_tx_hash,
               b.source_wallet,
               b.source_explorer_url,
               b.destination_ref_id,
               b.destination_wallet,
               b.destination_tx_hash,
               b.destination_explorer_url,
               b.token,
               b.token_symbol,
               b.amount::text AS amount,
               b.amount_formatted,
               b.status AS transfer_status,
               w.emitter_chain,
               w.emitter_address,
               w.sequence::text AS sequence
        FROM bridge_transfers b
        JOIN transactions t ON t.id = b.transaction_id
        LEFT JOIN wormhole_messages w ON w.transfer_id = b.id
        WHERE b.protocol = 'wormhole'
          AND b.status = 'Completed'
          AND b.direction = 'Inbound'
          AND b.source_tx_hash IS NULL
          AND b.created_at < NOW() - ($1::double precision * INTERVAL '1 second')
          AND (b.last_enrich_attempt_at IS NULL
               OR b.last_enrich_attempt_at < NOW() - ($2::double precision * INTERVAL '1 second'))
        ORDER BY b.last_enrich_attempt_at ASC NULLS FIRST, b.updated_at ASC
        LIMIT $3
        "#,
    )
    .bind(min_age_secs)
    .bind(retry_secs)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let rebuilt = rebuild_missing_origin(&row);
            let MissingOriginRow {
                id,
                observed_tx_hash,
                analysis,
                ..
            } = row;
            DueTransfer {
                id,
                observed_tx_hash,
                analysis,
                rebuilt,
            }
        })
        .collect())
}

/// Records an origin-backfill attempt, whatever its outcome. The retry window and queue
/// rotation use this marker, so an attempt that writes nothing still counts.
pub async fn record_origin_attempt(pool: &PgPool, transfer_id: i64) -> Result<()> {
    sqlx::query("UPDATE bridge_transfers SET last_enrich_attempt_at = NOW() WHERE id = $1")
        .bind(transfer_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// The stored columns needed to rebuild a `NormalisedTransaction` when a repair dropped
/// the cached analysis.
#[derive(Debug, sqlx::FromRow)]
struct MissingOriginRow {
    id: i64,
    observed_tx_hash: String,
    analysis: Option<serde_json::Value>,
    tx_hash: String,
    tx_chain: i16,
    tx_status: String,
    slot: i64,
    timestamp: Option<DateTime<Utc>>,
    fee_lamports: i64,
    signer: Option<String>,
    source_ref_id: String,
    source_tx_hash: Option<String>,
    source_wallet: Option<String>,
    source_explorer_url: Option<String>,
    destination_ref_id: String,
    destination_wallet: Option<String>,
    destination_tx_hash: Option<String>,
    destination_explorer_url: Option<String>,
    token: Option<String>,
    token_symbol: Option<String>,
    amount: Option<String>,
    amount_formatted: Option<String>,
    transfer_status: String,
    emitter_chain: Option<i32>,
    emitter_address: Option<String>,
    sequence: Option<String>,
}

fn rebuild_missing_origin(row: &MissingOriginRow) -> Option<NormalisedTransaction> {
    let chain = Chain::from_wormhole_id(u16::try_from(row.tx_chain).ok()?)?;
    let tx_status = TxStatus::parse(&row.tx_status)?;
    let status = match row.transfer_status.as_str() {
        "Completed" => BridgeStatus::Completed,
        "Pending" => BridgeStatus::Pending,
        "Detected" => BridgeStatus::Detected,
        _ => return None,
    };
    let source_chain = ChainId::from_wormhole_id(row.source_ref_id.parse().ok()?);
    let destination_chain = ChainId::from_wormhole_id(row.destination_ref_id.parse().ok()?);

    Some(NormalisedTransaction {
        hash: row.tx_hash.clone(),
        chain,
        status: tx_status,
        slot: u64::try_from(row.slot).ok()?,
        timestamp: row.timestamp.map(|ts| ts.timestamp()),
        fee_lamports: u64::try_from(row.fee_lamports).ok()?,
        signer: row.signer.clone(),
        bridge_event: None,
        bridge_transfer: Some(BridgeTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain,
            source_tx_hash: row.source_tx_hash.clone(),
            source_wallet: row.source_wallet.clone(),
            source_explorer_url: row.source_explorer_url.clone(),
            destination_chain,
            destination_wallet: row.destination_wallet.clone(),
            destination_tx_hash: row.destination_tx_hash.clone(),
            destination_explorer_url: row.destination_explorer_url.clone(),
            token: row.token.clone(),
            token_symbol: row.token_symbol.clone(),
            amount: row.amount.clone(),
            amount_formatted: row.amount_formatted.clone(),
            message_id: BridgeMessageId {
                emitter_chain: u16::try_from(row.emitter_chain?).ok()?,
                emitter_address: row.emitter_address.clone()?,
                sequence: row.sequence.as_deref()?.parse().ok()?,
            },
            status,
        }),
    })
}

/// One persisted transfer in a block-time window, for the read-only `reconcile`
/// tool. `block_time` is the chain's `blockTime` (`transactions.timestamp`);
/// `stored_at` is when the `bridge_transfers` row was written.
#[derive(Debug, sqlx::FromRow)]
pub struct PersistedTransferRow {
    pub observed_tx_hash: String,
    pub slot: i64,
    pub block_time: i64,
    pub stored_at: i64,
}

/// Persisted transfers of one protocol whose transaction was in
/// `[start, end]` (Unix seconds, inclusive). Read-only.
pub async fn persisted_transfers_in_window(
    pool: &PgPool,
    protocol: &str,
    start: i64,
    end: i64,
) -> Result<Vec<PersistedTransferRow>> {
    let rows: Vec<PersistedTransferRow> = sqlx::query_as(
        r#"
        SELECT bt.observed_tx_hash,
               t.slot,
               EXTRACT(EPOCH FROM t.timestamp)::bigint AS block_time,
               EXTRACT(EPOCH FROM bt.created_at)::bigint AS stored_at
        FROM bridge_transfers bt
        JOIN transactions t ON t.id = bt.transaction_id
        WHERE bt.observed_tx_hash IS NOT NULL
          AND bt.protocol = $1
          AND t.timestamp >= to_timestamp($2::double precision)
          AND t.timestamp <= to_timestamp($3::double precision)
        ORDER BY t.timestamp, bt.event_index
        "#,
    )
    .bind(protocol)
    .bind(start as f64)
    .bind(end as f64)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// A stored analysis plus whether it is still fresh.
pub struct CachedAnalysis {
    pub analysis: serde_json::Value,
    pub fresh: bool,
}

/// Returns the stored analysis regardless of age, flagged fresh/stale.
/// Completed transfers are always fresh; anything else is fresh for `fresh_secs`
/// after its last update. Used for stale-while-revalidate.
pub async fn get_analysis_any_age(
    pool: &PgPool,
    hash: &str,
    fresh_secs: f64,
) -> Result<Option<CachedAnalysis>> {
    let row: Option<(serde_json::Value, bool)> = sqlx::query_as(
        r#"
        SELECT analysis,
               COALESCE(
                   status = 'Completed'
                   OR updated_at > NOW() - ($2::double precision * INTERVAL '1 second'),
                   FALSE
               ) AS fresh
        FROM bridge_transfers
        WHERE observed_tx_hash = $1
          AND analysis IS NOT NULL
        ORDER BY event_index
        LIMIT 1
        "#,
    )
    .bind(hash)
    .bind(fresh_secs)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(analysis, fresh)| CachedAnalysis { analysis, fresh }))
}

/// The transaction row for the generalised read path.
pub struct StoredTransaction {
    pub hash: String,
    /// Wormhole id of the chain the transaction was observed on.
    pub chain: i16,
    pub status: String,
    pub slot: i64,
    pub timestamp: Option<DateTime<Utc>>,
    pub fee_lamports: i64,
    pub signer: Option<String>,
}

/// One `bridge_transfers` row, with its protocol detail. A row can exist for a protocol
/// whose detail table is not `wormhole_messages`; the message columns are then `None`.
pub struct StoredTransfer {
    pub program: String,
    pub protocol: String,
    pub direction: String,
    pub observed_tx_hash: String,
    pub event_index: i16,
    pub via: Option<String>,
    pub via_ref: Option<String>,
    pub source_ref_protocol: String,
    pub source_ref_id: String,
    pub source_tx_hash: Option<String>,
    pub source_wallet: Option<String>,
    pub source_explorer_url: Option<String>,
    pub destination_ref_protocol: String,
    pub destination_ref_id: String,
    pub destination_wallet: Option<String>,
    pub destination_tx_hash: Option<String>,
    pub destination_explorer_url: Option<String>,
    pub token_ref_protocol: Option<String>,
    pub token_ref_id: Option<String>,
    pub token: Option<String>,
    pub token_symbol: Option<String>,
    pub amount: Option<String>,
    pub amount_formatted: Option<String>,
    pub status: String,
    pub emitter_chain: Option<i32>,
    pub emitter_address: Option<String>,
    pub sequence: Option<String>,
    pub cctp_source_domain: Option<i32>,
    pub cctp_nonce: Option<String>,
}

pub struct StoredTransactionWithTransfers {
    pub transaction: StoredTransaction,
    pub transfers: Vec<StoredTransfer>,
}

/// The transaction and every transfer decoded from it, ordered by `event_index`.
/// A transaction with no transfers comes back with an empty `transfers` list.
pub async fn get_transaction_with_transfers(
    pool: &PgPool,
    hash: &str,
) -> Result<Option<StoredTransactionWithTransfers>> {
    let rows = sqlx::query(
        r#"
        SELECT t.hash, t.chain, t.status AS tx_status, t.slot, t.timestamp,
               t.fee_lamports, t.signer,
               b.program, b.protocol, b.direction, b.observed_tx_hash, b.event_index,
               b.via, b.via_ref,
               b.source_ref_protocol, b.source_ref_id, b.source_tx_hash,
               b.source_wallet, b.source_explorer_url,
               b.destination_ref_protocol, b.destination_ref_id, b.destination_wallet,
               b.destination_tx_hash, b.destination_explorer_url,
               b.token_ref_protocol, b.token_ref_id, b.token, b.token_symbol,
               b.amount::text AS amount, b.amount_formatted,
               b.status AS transfer_status,
               w.emitter_chain, w.emitter_address, w.sequence::text AS sequence,
               c.source_domain AS cctp_source_domain, c.nonce AS cctp_nonce
        FROM transactions t
        LEFT JOIN bridge_transfers b ON b.transaction_id = t.id
        LEFT JOIN wormhole_messages w ON w.transfer_id = b.id
        LEFT JOIN cctp_messages c ON c.transfer_id = b.id
        WHERE t.hash = $1
        ORDER BY b.event_index
        "#,
    )
    .bind(hash)
    .fetch_all(pool)
    .await?;

    let Some(first) = rows.first() else {
        return Ok(None);
    };

    let transaction = StoredTransaction {
        hash: first.try_get("hash")?,
        chain: first.try_get("chain")?,
        status: first.try_get("tx_status")?,
        slot: first.try_get("slot")?,
        timestamp: first.try_get("timestamp")?,
        fee_lamports: first.try_get("fee_lamports")?,
        signer: first.try_get("signer")?,
    };

    let mut transfers = Vec::new();
    for row in &rows {
        let Some(protocol) = row.try_get::<Option<String>, _>("protocol")? else {
            continue;
        };
        transfers.push(StoredTransfer {
            program: row.try_get("program")?,
            protocol,
            direction: row.try_get("direction")?,
            observed_tx_hash: row.try_get("observed_tx_hash")?,
            event_index: row.try_get("event_index")?,
            via: row.try_get("via")?,
            via_ref: row.try_get("via_ref")?,
            source_ref_protocol: row.try_get("source_ref_protocol")?,
            source_ref_id: row.try_get("source_ref_id")?,
            source_tx_hash: row.try_get("source_tx_hash")?,
            source_wallet: row.try_get("source_wallet")?,
            source_explorer_url: row.try_get("source_explorer_url")?,
            destination_ref_protocol: row.try_get("destination_ref_protocol")?,
            destination_ref_id: row.try_get("destination_ref_id")?,
            destination_wallet: row.try_get("destination_wallet")?,
            destination_tx_hash: row.try_get("destination_tx_hash")?,
            destination_explorer_url: row.try_get("destination_explorer_url")?,
            token_ref_protocol: row.try_get("token_ref_protocol")?,
            token_ref_id: row.try_get("token_ref_id")?,
            token: row.try_get("token")?,
            token_symbol: row.try_get("token_symbol")?,
            amount: row.try_get("amount")?,
            amount_formatted: row.try_get("amount_formatted")?,
            status: row.try_get("transfer_status")?,
            emitter_chain: row.try_get("emitter_chain")?,
            emitter_address: row.try_get("emitter_address")?,
            sequence: row.try_get("sequence")?,
            cctp_source_domain: row.try_get("cctp_source_domain")?,
            cctp_nonce: row.try_get("cctp_nonce")?,
        });
    }

    Ok(Some(StoredTransactionWithTransfers {
        transaction,
        transfers,
    }))
}

/// Filters and keyset position for [`list_transfers`]. Every `Option` is an omitted
/// query parameter; the caller has already validated enum-like values.
#[derive(Debug, Clone, Default)]
pub struct TransferListQuery {
    pub limit: i64,
    /// Exclusive position: only rows strictly older than this `(created_at, id)` pair.
    pub cursor: Option<(DateTime<Utc>, i64)>,
    pub protocol: Option<String>,
    pub direction: Option<String>,
    pub status: Option<String>,
    /// A resolved `chains.id`, matched against source or destination.
    pub chain_id: Option<i16>,
    pub token: Option<String>,
    /// Inclusive lower bound on `created_at`.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `created_at`.
    pub to: Option<DateTime<Utc>>,
}

/// One row of the list, with the keyset columns the cursor is built from.
pub struct ListedTransfer {
    pub id: i64,
    pub created_at: DateTime<Utc>,
    pub transfer: StoredTransfer,
}

const LIST_SELECT: &str = r#"
SELECT b.id, b.created_at,
       b.program, b.protocol, b.direction, b.observed_tx_hash, b.event_index,
       b.via, b.via_ref,
       b.source_ref_protocol, b.source_ref_id, b.source_tx_hash,
       b.source_wallet, b.source_explorer_url,
       b.destination_ref_protocol, b.destination_ref_id, b.destination_wallet,
       b.destination_tx_hash, b.destination_explorer_url,
       b.token_ref_protocol, b.token_ref_id, b.token, b.token_symbol,
       b.amount::text AS amount, b.amount_formatted,
       b.status AS transfer_status,
       w.emitter_chain, w.emitter_address, w.sequence::text AS sequence,
       c.source_domain AS cctp_source_domain, c.nonce AS cctp_nonce
FROM bridge_transfers b
LEFT JOIN wormhole_messages w ON w.transfer_id = b.id
LEFT JOIN cctp_messages c ON c.transfer_id = b.id
WHERE TRUE
"#;

/// One keyset page, newest first. Fetches `limit + 1` rows so the caller can tell
/// whether another page exists without a count or an extra query.
pub async fn list_transfers(
    pool: &PgPool,
    query: &TransferListQuery,
) -> Result<Vec<ListedTransfer>> {
    let mut sql = QueryBuilder::<Postgres>::new(LIST_SELECT);

    if let Some((created_at, id)) = query.cursor {
        sql.push(" AND (b.created_at, b.id) < (")
            .push_bind(created_at)
            .push(", ")
            .push_bind(id)
            .push(")");
    }
    if let Some(protocol) = &query.protocol {
        sql.push(" AND b.protocol = ").push_bind(protocol.clone());
    }
    if let Some(direction) = &query.direction {
        sql.push(" AND b.direction = ").push_bind(direction.clone());
    }
    if let Some(status) = &query.status {
        sql.push(" AND b.status = ").push_bind(status.clone());
    }
    if let Some(chain_id) = query.chain_id {
        sql.push(" AND (b.source_chain_id = ")
            .push_bind(chain_id)
            .push(" OR b.destination_chain_id = ")
            .push_bind(chain_id)
            .push(")");
    }
    if let Some(token) = &query.token {
        sql.push(" AND b.token = ").push_bind(token.clone());
    }
    if let Some(from) = query.from {
        sql.push(" AND b.created_at >= ").push_bind(from);
    }
    if let Some(to) = query.to {
        sql.push(" AND b.created_at < ").push_bind(to);
    }

    sql.push(" ORDER BY b.created_at DESC, b.id DESC LIMIT ")
        .push_bind(query.limit + 1);

    let rows = sql.build().fetch_all(pool).await?;

    let mut listed = Vec::with_capacity(rows.len());
    for row in &rows {
        let transfer = StoredTransfer {
            program: row.try_get("program")?,
            protocol: row.try_get("protocol")?,
            direction: row.try_get("direction")?,
            observed_tx_hash: row.try_get("observed_tx_hash")?,
            event_index: row.try_get("event_index")?,
            via: row.try_get("via")?,
            via_ref: row.try_get("via_ref")?,
            source_ref_protocol: row.try_get("source_ref_protocol")?,
            source_ref_id: row.try_get("source_ref_id")?,
            source_tx_hash: row.try_get("source_tx_hash")?,
            source_wallet: row.try_get("source_wallet")?,
            source_explorer_url: row.try_get("source_explorer_url")?,
            destination_ref_protocol: row.try_get("destination_ref_protocol")?,
            destination_ref_id: row.try_get("destination_ref_id")?,
            destination_wallet: row.try_get("destination_wallet")?,
            destination_tx_hash: row.try_get("destination_tx_hash")?,
            destination_explorer_url: row.try_get("destination_explorer_url")?,
            token_ref_protocol: row.try_get("token_ref_protocol")?,
            token_ref_id: row.try_get("token_ref_id")?,
            token: row.try_get("token")?,
            token_symbol: row.try_get("token_symbol")?,
            amount: row.try_get("amount")?,
            amount_formatted: row.try_get("amount_formatted")?,
            status: row.try_get("transfer_status")?,
            emitter_chain: row.try_get("emitter_chain")?,
            emitter_address: row.try_get("emitter_address")?,
            sequence: row.try_get("sequence")?,
            cctp_source_domain: row.try_get("cctp_source_domain")?,
            cctp_nonce: row.try_get("cctp_nonce")?,
        };
        listed.push(ListedTransfer {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            transfer,
        });
    }

    Ok(listed)
}
