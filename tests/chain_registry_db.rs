mod common;

use bridge::chains::{ChainKey, ChainRef, ChainRegistry, NS_EVM, RawChainRef};

const WORMHOLE_SEED: [(u16, &str); 10] = [
    (1, "solana"),
    (2, "ethereum"),
    (4, "bsc"),
    (5, "polygon"),
    (6, "avalanche"),
    (15, "near"),
    (23, "arbitrum"),
    (24, "optimism"),
    (25, "gnosis"),
    (30, "base"),
];

const EVM_SEED: [(u64, &str); 8] = [
    (1, "ethereum"),
    (56, "bsc"),
    (137, "polygon"),
    (43114, "avalanche"),
    (42161, "arbitrum"),
    (10, "optimism"),
    (100, "gnosis"),
    (8453, "base"),
];

fn key_of(reg: &ChainRegistry, raw: &RawChainRef) -> ChainKey {
    reg.resolve(raw)
        .key()
        .unwrap_or_else(|| panic!("{raw:?} should resolve"))
}

#[tokio::test]
async fn migration_seeds_the_ten_known_chains() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();
    let snap = reg.snapshot();

    for (id, slug) in WORMHOLE_SEED {
        let key = key_of(&reg, &RawChainRef::wormhole(id));
        assert_eq!(snap.slug(key), Some(slug), "wormhole {id}");
    }
    for (id, slug) in EVM_SEED {
        let key = key_of(&reg, &RawChainRef::new(NS_EVM, id));
        assert_eq!(snap.slug(key), Some(slug), "evm {id}");
    }

    assert_eq!(
        snap.slug(key_of(&reg, &RawChainRef::cctp(7))),
        Some("polygon"),
        "cctp domain 7"
    );
    assert_eq!(
        snap.slug(key_of(&reg, &RawChainRef::cctp(15))),
        Some("monad"),
        "cctp domain 15"
    );
    assert_eq!(
        snap.slug(key_of(&reg, &RawChainRef::cctp(19))),
        Some("hyperevm"),
        "cctp domain 19"
    );
    assert_eq!(
        snap.slug(key_of(&reg, &RawChainRef::cctp(26))),
        Some("arc"),
        "cctp domain 26"
    );
}

#[tokio::test]
async fn the_same_chain_resolves_to_one_key_from_every_namespace() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();

    // Wormhole 2 and EVM 1 are both Ethereum. Wormhole 1 is Solana, EVM 1 is not.
    assert_eq!(
        key_of(&reg, &RawChainRef::wormhole(2)),
        key_of(&reg, &RawChainRef::new(NS_EVM, 1))
    );
    assert_ne!(
        key_of(&reg, &RawChainRef::wormhole(1)),
        key_of(&reg, &RawChainRef::new(NS_EVM, 1))
    );
}

#[tokio::test]
async fn explorer_urls_come_from_the_table() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();
    let snap = reg.snapshot();

    let sol = key_of(&reg, &RawChainRef::wormhole(1));
    assert_eq!(
        snap.explorer_tx_url(sol, "abc").as_deref(),
        Some("https://explorer.solana.com/tx/abc")
    );
    let base = key_of(&reg, &RawChainRef::wormhole(30));
    assert_eq!(
        snap.explorer_tx_url(base, "0x1").as_deref(),
        Some("https://basescan.org/tx/0x1")
    );
}

#[tokio::test]
async fn unknown_ids_are_unresolved_not_errors() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();

    assert_eq!(
        reg.resolve(&RawChainRef::wormhole(65000)),
        ChainRef::Unresolved {
            protocol: "wormhole".into(),
            id: "65000".into()
        }
    );
}

#[tokio::test]
async fn pending_review_mappings_resolve_only_once_activated() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();
    let raw = RawChainRef::wormhole(9999);

    sqlx::query(
        "INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, status, origin)
         SELECT 'wormhole', '9999', id, 'pending_review', 'llm' FROM chains WHERE slug = 'base'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    reg.refresh(&db.pool).await.unwrap();
    assert!(
        !reg.resolve(&raw).is_resolved(),
        "an unverified proposal must not label a transfer"
    );

    sqlx::query("UPDATE protocol_chain_ids SET status = 'active' WHERE protocol_chain_id = '9999'")
        .execute(&db.pool)
        .await
        .unwrap();
    reg.refresh(&db.pool).await.unwrap();
    assert_eq!(
        reg.snapshot().slug(key_of(&reg, &raw)),
        Some("base"),
        "refresh picks up the activation"
    );
}

#[tokio::test]
async fn an_active_mapping_to_a_pending_chain_does_not_resolve() {
    let db = common::test_db().await;

    sqlx::query(
        "INSERT INTO chains (slug, name, kind, status, origin)
         VALUES ('newchain', 'New Chain', 'evm', 'pending_review', 'llm')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id)
         SELECT 'wormhole', '9998', id FROM chains WHERE slug = 'newchain'",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let reg = ChainRegistry::load(&db.pool).await.unwrap();
    assert!(!reg.resolve(&RawChainRef::wormhole(9998)).is_resolved());
}

#[tokio::test]
async fn the_database_rejects_unknown_status_values() {
    let db = common::test_db().await;

    let bad_chain = sqlx::query(
        "INSERT INTO chains (slug, name, kind, status) VALUES ('x', 'x', 'evm', 'bogus')",
    )
    .execute(&db.pool)
    .await;
    assert!(bad_chain.is_err());

    let bad_kind = sqlx::query("INSERT INTO chains (slug, name, kind) VALUES ('y', 'y', 'moon')")
        .execute(&db.pool)
        .await;
    assert!(bad_kind.is_err());
}

#[tokio::test]
async fn a_failed_refresh_keeps_the_previous_snapshot() {
    let db = common::test_db().await;
    let reg = ChainRegistry::load(&db.pool).await.unwrap();

    db.pool.close().await;
    assert!(reg.refresh(&db.pool).await.is_err());
    assert!(
        reg.resolve(&RawChainRef::wormhole(1)).is_resolved(),
        "registry must survive a database error"
    );
}
