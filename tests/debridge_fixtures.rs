//! Golden tests for the deBridge DLN adapter over real captured Solana mainnet
//! transactions: a `createOrderWithNonce` source order and a `fulfillOrder`
//! destination fill. The order id, token, amount and chains are taken from the
//! on-chain `CreatedOrderId`/`CreatedOrder` and `Fulfilled` events and the
//! `fulfill_order` args, so the decoded transfer is checked against them directly.
//!
//! Captured with `scripts/capture_fixtures.sh` (`SKIP_WORMHOLE=1`) from the public
//! Solana RPC; no Helius key or WormholeScan was involved.

use bridge::chains::{RawChainRef, Snapshot};
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::protocols::debridge::DebridgeAdapter;
use bridge::protocols::{BridgeAdapter, Classification, Direction, MessageId, SolanaTx};
use bridge::services::detect;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

const ORDER_TX: &str =
    "3ggTVbZvk38HfKTQ8fcTYkvTeDc1uw75KTGUzpm1MVHFJWHhFSbQBALshBmszepZDULqTqxnMJhAPiT6UgXMUK5d";
/// A mainnet `fulfillOrder` taken from the official parser's destination test:
/// <https://github.com/debridge-finance/solana-tx-parser-public/blob/master/tests/parseDlnDstTransaction.test.ts>
const FILL_TX: &str =
    "4V5XFQ8ViuWi4VxHWb7bCZtWGzyPgZnKTf6ABnC1VUXd2y5wgfrn8EGmzwst1iP19ySPA7jwx87KWX3S2GYUh8Lr";

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

#[test]
fn the_create_order_with_nonce_decodes_the_order() {
    let raw = read("helius_debridge_create_order_with_nonce.json");
    let tx =
        SolanaTx::from_helius(&raw, ORDER_TX).unwrap_or_else(|e| panic!("from_helius failed: {e}"));

    let Classification::Transfers(mut found) = DebridgeAdapter.classify(&tx) else {
        panic!("expected the create order to decode");
    };
    assert_eq!(found.len(), 1, "transfers in the tx");
    let transfer = found.remove(0);

    assert_eq!(transfer.program, "debridge");
    assert_eq!(transfer.direction, Direction::Outbound);
    assert_eq!(transfer.observed_tx, ORDER_TX);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::debridge("7565164"));
    assert_eq!(transfer.destination, RawChainRef::debridge("1"));
    assert_eq!(
        transfer.message,
        MessageId::Debridge {
            order_id: "3c0854471cc53705e9fcc39d6a5cd5ab01f2c9974fe9ae76855e8b3c29bc2086".into(),
        }
    );
    assert_eq!(
        transfer.message.key(),
        "3c0854471cc53705e9fcc39d6a5cd5ab01f2c9974fe9ae76855e8b3c29bc2086"
    );
    assert_eq!(
        transfer.amount_raw.as_ref().map(|a| a.as_str()),
        Some("3010559575")
    );
    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::debridge("7565164"));
    assert_eq!(
        token.address,
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    );
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some("0x6f050bb29fcc910891be4f17d3a123cb57cd9a50")
    );
    assert_eq!(
        transfer.source_wallet.as_deref(),
        Some("5cSGMBfJt451RSXPQEMMr7k8T3f9GQjc7uh3MWnaGLRV")
    );
}

#[test]
fn the_fulfill_order_decodes_as_a_completed_inbound_fill() {
    let raw = read("helius_debridge_fulfill_order.json");
    let tx =
        SolanaTx::from_helius(&raw, FILL_TX).unwrap_or_else(|e| panic!("from_helius failed: {e}"));

    let classification = DebridgeAdapter.classify(&tx);
    let Classification::Transfers(mut found) = classification else {
        panic!("expected the fulfill_order to decode: {classification:?}");
    };
    assert_eq!(found.len(), 1, "transfers in the tx");
    let transfer = found.remove(0);

    assert_eq!(transfer.program, "debridge");
    assert_eq!(transfer.direction, Direction::Inbound);
    assert_eq!(transfer.observed_tx, FILL_TX);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::debridge("8453"));
    assert_eq!(transfer.destination, RawChainRef::debridge("7565164"));
    assert_eq!(
        transfer.message,
        MessageId::Debridge {
            order_id: "0001679d6d52361adac3c391d7b1e72f85ee830c2914f8be5620fbae0549a66b".into(),
        }
    );
    assert_eq!(
        transfer.message.key(),
        "0001679d6d52361adac3c391d7b1e72f85ee830c2914f8be5620fbae0549a66b"
    );
    assert_eq!(
        transfer.amount_raw.as_ref().map(|a| a.as_str()),
        Some("438490169")
    );
    let token = transfer.token.as_ref().expect("token");
    assert_eq!(token.chain, RawChainRef::debridge("8453"));
    assert_eq!(token.address, "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    assert_eq!(
        transfer.source_wallet.as_deref(),
        Some("0x2672113e354667adce50c38296ee7dfd65ac6666")
    );
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some("5FV8hNqbBpMwsUXhazL2Givohy92MUTkv6MvcPrmGQ2A")
    );

    // The observed Solana tx is the destination leg, so the row is Completed.
    let row = detect::debridge_destination_row_values(&transfer, &Snapshot::default());
    assert!(matches!(row.status, BridgeStatus::Completed));
    assert_eq!(row.destination_tx_hash.as_deref(), Some(FILL_TX));
    assert_eq!(row.source_tx_hash, None);
}
