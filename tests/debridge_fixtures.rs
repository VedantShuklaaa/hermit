//! Golden test for the deBridge DLN adapter over a real captured Solana mainnet
//! `createOrderWithNonce` transaction. The order id, token, amount and destination
//! are taken from the on-chain `CreatedOrderId`/`CreatedOrder` events, so the
//! decoded transfer is checked against them directly.
//!
//! Captured with `scripts/capture_fixtures.sh` (`SKIP_WORMHOLE=1`) from the public
//! Solana RPC; no Helius key or WormholeScan was involved.

use bridge::chains::RawChainRef;
use bridge::protocols::debridge::DebridgeAdapter;
use bridge::protocols::{BridgeAdapter, Classification, Direction, MessageId, SolanaTx};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

const ORDER_TX: &str =
    "3ggTVbZvk38HfKTQ8fcTYkvTeDc1uw75KTGUzpm1MVHFJWHhFSbQBALshBmszepZDULqTqxnMJhAPiT6UgXMUK5d";

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
