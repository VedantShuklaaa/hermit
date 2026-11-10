//! Postgres side of EVM watching: targets, the persisted cursor, stored events and
//! the event-side join to `bridge_transfers`.

use super::decode::EventKind;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchTarget {
    pub chain_id: i16,
    /// Chain registry slug (`ethereum`), used in metrics and logs.
    pub slug: String,
    pub wormhole_chain_id: u16,
    pub core_bridge_address: String,
    pub token_bridge_address: String,
    pub finality_depth: u64,
    pub rpc_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub block_number: u64,
    pub block_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEvent {
    pub event_kind: EventKind,
    pub emitter_chain: u16,
    pub emitter_address: String,
    pub sequence: u64,
    pub tx_hash: String,
    pub log_index: u32,
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct JoinStats {
    pub matched: u64,
}

type TargetRow = (i16, String, String, String, String, i64);

/// Enabled targets, joined to the active Wormhole chain mapping. A target whose
/// mapping is not a u16 is skipped with a warning rather than failing the load.
pub async fn load_targets(pool: &PgPool) -> Result<Vec<WatchTarget>> {
    let rows: Vec<TargetRow> = sqlx::query_as(
        r#"
        SELECT t.chain_id,
               c.slug,
               m.protocol_chain_id,
               t.core_bridge_address,
               t.token_bridge_address,
               t.finality_depth
        FROM evm_watch_targets t
        JOIN chains c ON c.id = t.chain_id
        JOIN protocol_chain_ids m
          ON m.chain_id = t.chain_id
         AND m.protocol = 'wormhole'
         AND m.status = 'active'
        WHERE t.enabled
          AND c.status = 'active'
        ORDER BY c.slug
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(
            |(chain_id, slug, wormhole_id, core, token, finality_depth)| {
                let Ok(wormhole_chain_id) = wormhole_id.parse::<u16>() else {
                    tracing::warn!(%slug, %wormhole_id, "skipping EVM target with a non-u16 Wormhole chain id");
                    return None;
                };
                Some(WatchTarget {
                    chain_id,
                    slug,
                    wormhole_chain_id,
                    core_bridge_address: core,
                    token_bridge_address: token,
                    finality_depth: u64::try_from(finality_depth).unwrap_or(0),
                    rpc_url: String::new(),
                })
            },
        )
        .collect())
}

pub async fn load_cursor(pool: &PgPool, chain_id: i16) -> Result<Option<Cursor>> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT block_number, block_hash FROM evm_watch_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(|(number, hash)| {
        u64::try_from(number).ok().map(|block_number| Cursor {
            block_number,
            block_hash: hash,
        })
    }))
}

pub async fn set_cursor(
    pool: &PgPool,
    chain_id: i16,
    block_number: u64,
    block_hash: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO evm_watch_cursors (chain_id, block_number, block_hash)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE
        SET block_number = EXCLUDED.block_number,
            block_hash = EXCLUDED.block_hash,
            updated_at = NOW()
        "#,
    )
    .bind(chain_id)
    .bind(i64::try_from(block_number).unwrap_or(i64::MAX))
    .bind(block_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drops every event at or after `block_number` before the cursor is rewound.
pub async fn delete_events_from(pool: &PgPool, chain_id: i16, block_number: u64) -> Result<u64> {
    let result =
        sqlx::query("DELETE FROM evm_log_events WHERE chain_id = $1 AND block_number >= $2")
            .bind(chain_id)
            .bind(i64::try_from(block_number).unwrap_or(i64::MAX))
            .execute(pool)
            .await?;
    Ok(result.rows_affected())
}

/// Stores a range's events and advances the cursor in one transaction. Re-reading a
/// range is idempotent (`UNIQUE (chain_id, tx_hash, log_index)`).
pub async fn insert_events_and_advance(
    pool: &PgPool,
    chain_id: i16,
    events: &[NewEvent],
    block_number: u64,
    block_hash: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for event in events {
        sqlx::query(
            r#"
            INSERT INTO evm_log_events (
                chain_id, event_kind, emitter_chain, emitter_address, sequence,
                tx_hash, log_index, block_number, block_hash, block_timestamp
            )
            VALUES (
                $1, $2, $3, $4, $5::numeric,
                $6, $7, $8, $9, $10
            )
            ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(event.event_kind.as_str())
        .bind(i32::from(event.emitter_chain))
        .bind(&event.emitter_address)
        .bind(event.sequence.to_string())
        .bind(&event.tx_hash)
        .bind(i32::try_from(event.log_index).unwrap_or(i32::MAX))
        .bind(i64::try_from(event.block_number).unwrap_or(i64::MAX))
        .bind(&event.block_hash)
        .bind(event.block_timestamp)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO evm_watch_cursors (chain_id, block_number, block_hash)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE
        SET block_number = EXCLUDED.block_number,
            block_hash = EXCLUDED.block_hash,
            updated_at = NOW()
        "#,
    )
    .bind(chain_id)
    .bind(i64::try_from(block_number).unwrap_or(i64::MAX))
    .bind(block_hash)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Applies every unapplied event on `chain_id` that is still inside the retry window.
/// An event that matches no row stays unapplied and is retried on the next poll; after
/// `max_age_secs` it is left alone. The transfer-side lookup reads the same table with
/// no age limit, so an old event still fills a row built later.
pub async fn join_recent(pool: &PgPool, chain_id: i16, max_age_secs: i64) -> Result<JoinStats> {
    let events: Vec<(i64, String, i32, String, String, String)> = sqlx::query_as(
        r#"
        SELECT id, event_kind, emitter_chain, emitter_address, sequence::text, tx_hash
        FROM evm_log_events
        WHERE chain_id = $1
          AND applied_at IS NULL
          AND created_at > NOW() - ($2::double precision * INTERVAL '1 second')
        ORDER BY block_number, log_index
        "#,
    )
    .bind(chain_id)
    .bind(max_age_secs)
    .fetch_all(pool)
    .await?;

    let mut stats = JoinStats::default();
    for (id, event_kind, emitter_chain, emitter_address, sequence, tx_hash) in events {
        let message_key = format!("{emitter_chain}/{emitter_address}/{sequence}");
        let matched = match event_kind.as_str() {
            "log_message_published" => {
                inbound_join(
                    pool,
                    chain_id,
                    &emitter_chain.to_string(),
                    &message_key,
                    &tx_hash,
                )
                .await?
            }
            "transfer_redeemed" => outbound_join(pool, chain_id, &message_key, &tx_hash).await?,
            other => {
                tracing::warn!(event_kind = other, "unknown stored EVM event kind");
                false
            }
        };

        if matched {
            sqlx::query("UPDATE evm_log_events SET applied_at = NOW() WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await?;
            stats.matched += 1;
        }
    }
    Ok(stats)
}

/// A `LogMessagePublished` log is the source side of an inbound transfer. The row's
/// `source_chain_id` must be this chain, or a message from another chain would take a
/// hash from the wrong chain.
async fn inbound_join(
    pool: &PgPool,
    chain_id: i16,
    emitter_chain: &str,
    message_key: &str,
    tx_hash: &str,
) -> Result<bool> {
    let id: Option<i64> = sqlx::query_scalar(
        r#"
        UPDATE bridge_transfers b
        SET source_tx_hash = COALESCE(b.source_tx_hash, $4),
            source_explorer_url = COALESCE(
                b.source_explorer_url,
                replace(c.explorer_tx_url, '{hash}', $4)
            ),
            analysis = CASE
                WHEN b.analysis IS NULL THEN NULL
                ELSE jsonb_set(
                    jsonb_set(
                        b.analysis,
                        '{bridge_transfer,source_tx_hash}',
                        to_jsonb(COALESCE(b.source_tx_hash, $4::text)),
                        false
                    ),
                    '{bridge_transfer,source_explorer_url}',
                    COALESCE(
                        to_jsonb(
                            COALESCE(
                                b.source_explorer_url,
                                replace(c.explorer_tx_url, '{hash}', $4)
                            )
                        ),
                        'null'::jsonb
                    ),
                    false
                )
            END,
            updated_at = NOW()
        FROM chains c
        WHERE c.id = b.source_chain_id
          AND b.protocol = 'wormhole'
          AND b.direction = 'Inbound'
          AND b.source_chain_id = $1
          AND b.source_ref_protocol = 'wormhole'
          AND b.source_ref_id = $2
          AND b.message_key = $3
          AND b.status IN ('Detected', 'Pending', 'Completed')
        RETURNING b.id
        "#,
    )
    .bind(chain_id)
    .bind(emitter_chain)
    .bind(message_key)
    .bind(tx_hash)
    .fetch_optional(pool)
    .await?;
    Ok(id.is_some())
}

/// A `TransferRedeemed` log is the destination side of an outbound transfer. The row's
/// `destination_chain_id` must be this chain. The message key alone identifies the
/// message: its emitter chain is the *source* chain, so it must not be compared with
/// the row's destination reference.
async fn outbound_join(
    pool: &PgPool,
    chain_id: i16,
    message_key: &str,
    tx_hash: &str,
) -> Result<bool> {
    let id: Option<i64> = sqlx::query_scalar(
        r#"
        UPDATE bridge_transfers b
        SET destination_tx_hash = COALESCE(b.destination_tx_hash, $3),
            destination_explorer_url = COALESCE(
                b.destination_explorer_url,
                replace(c.explorer_tx_url, '{hash}', $3)
            ),
            status = CASE
                WHEN b.status IN ('Detected', 'Pending') THEN 'Completed'
                ELSE b.status
            END,
            analysis = CASE
                WHEN b.analysis IS NULL THEN NULL
                ELSE jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            b.analysis,
                            '{bridge_transfer,destination_tx_hash}',
                            to_jsonb(COALESCE(b.destination_tx_hash, $3::text)),
                            false
                        ),
                        '{bridge_transfer,destination_explorer_url}',
                        COALESCE(
                            to_jsonb(
                                COALESCE(
                                    b.destination_explorer_url,
                                    replace(c.explorer_tx_url, '{hash}', $3)
                                )
                            ),
                            'null'::jsonb
                        ),
                        false
                    ),
                    '{bridge_transfer,status}',
                    to_jsonb(
                        CASE
                            WHEN b.status IN ('Detected', 'Pending') THEN 'Completed'
                            ELSE b.status
                        END
                    ),
                    false
                )
            END,
            updated_at = NOW()
        FROM chains c
        WHERE c.id = b.destination_chain_id
          AND b.protocol = 'wormhole'
          AND b.direction = 'Outbound'
          AND b.destination_chain_id = $1
          AND b.destination_ref_protocol = 'wormhole'
          AND b.message_key = $2
          AND b.status IN ('Detected', 'Pending', 'Completed')
        RETURNING b.id
        "#,
    )
    .bind(chain_id)
    .bind(message_key)
    .bind(tx_hash)
    .fetch_optional(pool)
    .await?;
    Ok(id.is_some())
}

/// The latest matching event for the transfer-side lookup. No age filter: an event
/// recorded before the row was built is still the answer.
pub async fn lookup_tx_hash(
    pool: &PgPool,
    chain_id: i16,
    event_kind: EventKind,
    emitter_chain: u16,
    emitter_address: &str,
    sequence: u64,
) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT tx_hash
        FROM evm_log_events
        WHERE chain_id = $1
          AND event_kind = $2
          AND emitter_chain = $3
          AND emitter_address = $4
          AND sequence = $5::numeric
        ORDER BY block_number DESC, log_index DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(event_kind.as_str())
    .bind(i32::from(emitter_chain))
    .bind(emitter_address)
    .bind(sequence.to_string())
    .fetch_optional(pool)
    .await?)
}

/// The latest redeem for a message on any watched chain, used by the transfer-side
/// fallback before asking WormholeScan.
pub async fn lookup_redeem_tx(
    pool: &PgPool,
    emitter_chain: u16,
    emitter_address: &str,
    sequence: u64,
) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        r#"
        SELECT tx_hash
        FROM evm_log_events
        WHERE event_kind = 'transfer_redeemed'
          AND emitter_chain = $1
          AND emitter_address = $2
          AND sequence = $3::numeric
        ORDER BY block_number DESC, log_index DESC
        LIMIT 1
        "#,
    )
    .bind(i32::from(emitter_chain))
    .bind(emitter_address)
    .bind(sequence.to_string())
    .fetch_optional(pool)
    .await?)
}
