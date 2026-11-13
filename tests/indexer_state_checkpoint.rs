//! The ingestion checkpoint is monotonic: out-of-order log notifications must never rewind
//! it, because the next backfill resumes from that slot.

mod common;

use bridge::db::repository::{
    get_last_ingested_slot, ingest_checkpoint_key, ingest_checkpoint_subject,
    migrate_legacy_ingest_checkpoint, set_last_ingested_slot,
};
use bridge::protocols::wormhole::TOKEN_BRIDGE;

#[tokio::test]
async fn the_checkpoint_never_moves_backwards() {
    let db = common::test_db().await;
    let pool = &db.pool;

    set_last_ingested_slot(pool, "solana", 100).await.unwrap();
    assert_eq!(get_last_ingested_slot(pool, "solana").await.unwrap(), 100);

    set_last_ingested_slot(pool, "solana", 90).await.unwrap();
    assert_eq!(get_last_ingested_slot(pool, "solana").await.unwrap(), 100);

    set_last_ingested_slot(pool, "solana", 110).await.unwrap();
    assert_eq!(get_last_ingested_slot(pool, "solana").await.unwrap(), 110);
}

#[tokio::test]
async fn a_fresh_database_starts_at_slot_zero() {
    let db = common::test_db().await;

    assert_eq!(get_last_ingested_slot(&db.pool, "solana").await.unwrap(), 0);
}

#[tokio::test]
async fn the_legacy_checkpoint_moves_to_the_token_bridge_once() {
    let db = common::test_db().await;

    // The single-program ingester's last position.
    set_last_ingested_slot(&db.pool, "solana", 500)
        .await
        .unwrap();
    let legacy_updated_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT updated_at FROM indexer_state WHERE key = 'solana_last_ingested_slot'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert!(
        migrate_legacy_ingest_checkpoint(&db.pool, TOKEN_BRIDGE)
            .await
            .unwrap()
    );

    // The Token Bridge resumes at exactly the same slot: no gap, no replay.
    assert_eq!(
        get_last_ingested_slot(&db.pool, &ingest_checkpoint_subject(TOKEN_BRIDGE))
            .await
            .unwrap(),
        500
    );
    let key = ingest_checkpoint_key(TOKEN_BRIDGE);
    assert_eq!(key, format!("solana:{TOKEN_BRIDGE}_last_ingested_slot"));

    // The heartbeat is copied too, so /health does not read a falsely young age.
    let migrated_updated_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT updated_at FROM indexer_state WHERE key = $1")
            .bind(&key)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(migrated_updated_at, legacy_updated_at);

    // A second startup, or a later legacy write, must not rewind the per-program row.
    set_last_ingested_slot(&db.pool, "solana", 700)
        .await
        .unwrap();
    assert!(
        !migrate_legacy_ingest_checkpoint(&db.pool, TOKEN_BRIDGE)
            .await
            .unwrap()
    );
    assert_eq!(
        get_last_ingested_slot(&db.pool, &ingest_checkpoint_subject(TOKEN_BRIDGE))
            .await
            .unwrap(),
        500
    );

    // The new checkpoint keeps its usual monotonic behaviour.
    set_last_ingested_slot(&db.pool, &ingest_checkpoint_subject(TOKEN_BRIDGE), 600)
        .await
        .unwrap();
    assert_eq!(
        get_last_ingested_slot(&db.pool, &ingest_checkpoint_subject(TOKEN_BRIDGE))
            .await
            .unwrap(),
        600
    );
}
