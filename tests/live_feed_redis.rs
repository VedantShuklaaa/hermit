//! Redis-backed live feed. These tests need Docker (or `TEST_REDIS_URL` and
//! `TEST_DATABASE_URL` for the warm-up case, which also needs Postgres).

mod common;

use bridge::config::LiveFeedConfig;
use bridge::live_feed::{Cursor, FRAMES_KEY, INDEX_KEY, LiveFeed, WARMUP_KEY};
use redis::AsyncCommands;

fn feed(redis_url: &str, max: usize) -> LiveFeed {
    LiveFeed::new(LiveFeedConfig { enabled: true, max }, redis_url)
}

fn frame(hash: &str, status: &str) -> String {
    format!(r#"{{"hash":"{hash}","status":"{status}"}}"#)
}

async fn connection(redis_url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(redis_url)
        .expect("redis client")
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection")
}

fn hashes(page: &bridge::live_feed::LiveFeedResponse) -> Vec<&str> {
    page.items
        .iter()
        .filter_map(|item| item["hash"].as_str())
        .collect()
}

#[tokio::test]
async fn an_update_replaces_the_frame_without_moving_it() {
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 1000);

    feed.upsert_at("A", &frame("A", "Pending"), 1_000)
        .await
        .unwrap();
    let stored = feed
        .upsert_at("A", &frame("A", "Completed"), 2_000)
        .await
        .unwrap();

    assert!(stored.contains(r#""first_seen_at":1000"#));
    assert!(stored.contains(r#""updated_at":2000"#));
    assert!(stored.contains(r#""status":"Completed""#));

    let mut connection = connection(&redis.url).await;
    let score: Option<f64> = connection.zscore(INDEX_KEY, "A").await.unwrap();
    assert_eq!(score, Some(1_000.0));
    let raw: String = connection.hget(FRAMES_KEY, "A").await.unwrap();
    assert!(raw.contains(r#""first_seen_at":1000"#));
    assert!(raw.contains(r#""updated_at":2000"#));
}

#[tokio::test]
async fn the_ring_evicts_the_oldest_score_from_both_keys() {
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 3);

    for (hash, at) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
        feed.upsert_at(hash, &frame(hash, "Detected"), at)
            .await
            .unwrap();
    }

    let page = feed.read_page(10, None).await.unwrap();
    assert_eq!(page.total, 3);
    assert_eq!(hashes(&page), vec!["d", "c", "b"]);

    let mut connection = connection(&redis.url).await;
    let index_len: i64 = connection.zcard(INDEX_KEY).await.unwrap();
    let frame_len: i64 = connection.hlen(FRAMES_KEY).await.unwrap();
    assert_eq!((index_len, frame_len), (3, 3));
    assert_eq!(
        connection
            .zscore::<_, _, Option<f64>>(INDEX_KEY, "a")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        connection
            .hget::<_, _, Option<String>>(FRAMES_KEY, "a")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        connection
            .zscore::<_, _, Option<f64>>(INDEX_KEY, "d")
            .await
            .unwrap(),
        Some(4.0)
    );
}

#[tokio::test]
async fn cursor_paging_visits_every_frame_once() {
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 1000);

    for (hash, at) in [("a", 100), ("b", 101), ("c", 102), ("d", 103), ("e", 104)] {
        feed.upsert_at(hash, &frame(hash, "Detected"), at)
            .await
            .unwrap();
    }

    let first = feed.read_page(2, None).await.unwrap();
    assert_eq!(hashes(&first), vec!["e", "d"]);
    let cursor = Cursor::decode(first.next_cursor.as_deref().unwrap()).unwrap();

    let second = feed.read_page(2, Some(&cursor)).await.unwrap();
    assert_eq!(hashes(&second), vec!["c", "b"]);
    let cursor = Cursor::decode(second.next_cursor.as_deref().unwrap()).unwrap();

    let third = feed.read_page(2, Some(&cursor)).await.unwrap();
    assert_eq!(hashes(&third), vec!["a"]);
    assert!(third.next_cursor.is_none());
}

#[tokio::test]
async fn cursor_paging_breaks_ties_by_tx_hash() {
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 1000);

    // Equal scores are ordered lexicographically descending by ZREVRANGE.
    for hash in ["a", "b", "c"] {
        feed.upsert_at(hash, &frame(hash, "Detected"), 500)
            .await
            .unwrap();
    }

    let first = feed.read_page(1, None).await.unwrap();
    assert_eq!(hashes(&first), vec!["c"]);
    let cursor = Cursor::decode(first.next_cursor.as_deref().unwrap()).unwrap();

    let second = feed.read_page(1, Some(&cursor)).await.unwrap();
    assert_eq!(hashes(&second), vec!["b"]);
    let cursor = Cursor::decode(second.next_cursor.as_deref().unwrap()).unwrap();

    let third = feed.read_page(1, Some(&cursor)).await.unwrap();
    assert_eq!(hashes(&third), vec!["a"]);
    assert!(third.next_cursor.is_none());
}

#[tokio::test]
async fn the_warm_up_lock_has_one_holder() {
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 1000);

    assert!(feed.try_warmup_lock().await.unwrap());
    assert!(!feed.try_warmup_lock().await.unwrap());

    let mut connection = connection(&redis.url).await;
    let _: i64 = connection.del(WARMUP_KEY).await.unwrap();
    assert!(feed.try_warmup_lock().await.unwrap());
}

#[tokio::test]
async fn a_cold_feed_warms_up_from_the_newest_rows() {
    let db = common::test_db().await;
    let redis = common::test_redis().await;
    let feed = feed(&redis.url, 1000);

    for (hash, sequence) in [("first", 1), ("second", 2)] {
        let tx = common::sample_tx(
            hash,
            sequence,
            bridge::domain::bridge_transfer::ChainId::Ethereum,
            Some("1"),
        );
        bridge::db::repository::persist_transaction(&db.pool, &tx)
            .await
            .unwrap();
    }

    let page = feed.snapshot(&db.pool, 15, None).await.unwrap();
    assert_eq!(page.total, 2);
    assert_eq!(hashes(&page).len(), 2);
    assert!(page.items.iter().all(|item| item["first_seen_at"].is_i64()));

    // The lock is still held, so a second cold-start caller only sees the ring the
    // first one populated.
    let again = feed.snapshot(&db.pool, 15, None).await.unwrap();
    assert_eq!(again.total, 2);
}
