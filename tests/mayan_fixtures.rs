//! Golden test for the Mayan Swift V2 adapter over a real captured Solana
//! mainnet transaction: an `init_order` source order. The order hash and amount
//! come from the `OrderInitialized` event, the destination chain and wallet from
//! the `init_order` args, and the source mint and trader from its accounts, so the
//! decoded transfer is checked against them directly.
//!
//! Captured with `scripts/capture_fixtures.sh` (`SKIP_WORMHOLE=1`,
//! `MAX_TX_VERSION=1`) from the public Solana RPC; no Helius key or WormholeScan
//! was involved.

use bridge::chains::{ChainRef, ChainRow, MappingRow, RawChainRef, RegistryStatus, Snapshot};
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::protocols::mayan::MayanAdapter;
use bridge::protocols::{BridgeAdapter, Classification, Direction, MessageId, SolanaTx};
use bridge::services::detect;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;

const INIT_ORDER_TX: &str =
    "QCaMXBE6VSZgajW5wJCHqxesQwjjdRgUUhMCoX8cwd2HXwiQZpVzGrWDMUE4s9dpj5msdN9KPZfmXjN2Qn9Hkes";
const ORDER_HASH: &str = "4a35d4d47964cc51b38580f12e918aad9bd854593ee0f60e3812cc981ab75318";
const TRADER: &str = "D232Pb6Z4sandj5uSPAYmAuuQG9SZjCWfRyKAGwVxVGR";
const MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

fn read() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/helius_mayan_swift_init_order.json");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("fixture: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("fixture: invalid json: {e}"))
}

fn classify(raw: &Value) -> Classification {
    let tx = SolanaTx::from_helius(raw, INIT_ORDER_TX)
        .unwrap_or_else(|e| panic!("from_helius failed: {e}"));
    MayanAdapter.classify(&tx)
}

fn only_transfer(classification: Classification) -> bridge::protocols::DetectedTransfer {
    match classification {
        Classification::Transfers(mut found) => {
            assert_eq!(found.len(), 1, "transfers in the tx");
            found.remove(0)
        }
        other => panic!("expected a transfer, got {other:?}"),
    }
}

#[test]
fn the_init_order_decodes_the_source_transfer() {
    let transfer = only_transfer(classify(&read()));

    assert_eq!(transfer.program, "swift_v2");
    assert_eq!(transfer.direction, Direction::Outbound);
    assert!(!transfer.delivered);
    assert_eq!(transfer.observed_tx, INIT_ORDER_TX);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::wormhole(1));
    assert_eq!(transfer.destination, RawChainRef::wormhole(4));
    assert_eq!(
        transfer.message,
        MessageId::Mayan {
            order_hash: ORDER_HASH.into(),
        }
    );
    assert_eq!(transfer.message.key(), ORDER_HASH);
    assert_eq!(
        transfer.amount_raw.as_ref().map(|a| a.as_str()),
        Some("2804362")
    );
    assert_eq!(transfer.source_wallet.as_deref(), Some(TRADER));
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some("0xe7e57fc4d20dda379ba52aa8f7993a3e46aae9a3")
    );
    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::wormhole(1));
    assert_eq!(token.address, MINT);
}

#[test]
fn the_row_is_a_pending_source_observation() {
    let transfer = only_transfer(classify(&read()));
    let row = detect::mayan_row_values(&transfer, &Snapshot::default());

    assert!(matches!(row.status, BridgeStatus::Pending));
    assert_eq!(row.source_tx_hash.as_deref(), Some(INIT_ORDER_TX));
    assert_eq!(row.destination_tx_hash, None);
    assert_eq!(row.source_wallet.as_deref(), Some(TRADER));
    assert_eq!(
        row.destination_wallet.as_deref(),
        Some("0xe7e57fc4d20dda379ba52aa8f7993a3e46aae9a3")
    );
    assert_eq!(row.token.as_deref(), Some(MINT));
    assert_eq!(row.amount.as_deref(), Some("2804362"));
}

#[test]
fn the_fixture_destination_chain_resolves_through_the_wormhole_namespace() {
    let transfer = only_transfer(classify(&read()));
    let chains = Snapshot::build(
        vec![ChainRow {
            id: 1,
            slug: "bsc".to_string(),
            name: "BNB Smart Chain".to_string(),
            kind: "evm".to_string(),
            explorer_tx_url: None,
            status: RegistryStatus::Active,
        }],
        vec![MappingRow {
            protocol: "wormhole".to_string(),
            protocol_chain_id: "4".to_string(),
            chain_id: 1,
            status: RegistryStatus::Active,
        }],
    );

    let ChainRef::Resolved(key) = chains.resolve(&transfer.destination) else {
        panic!(
            "fixture destination chain should resolve: {:?}",
            transfer.destination
        );
    };
    assert_eq!(chains.slug(key), Some("bsc"));
}

#[test]
fn a_failed_transaction_is_never_a_transfer() {
    let mut raw = read();
    raw["meta"]["err"] = json!({"InstructionError": [0, {"Custom": 1}]});
    let Classification::Rejected(reject) = classify(&raw) else {
        panic!("a failed tx must not decode");
    };
    assert_eq!(reject.reason(), "transaction failed");
}

#[test]
fn a_transaction_without_the_order_initialized_event_is_not_a_transfer() {
    let mut raw = read();
    let logs = raw["meta"]["logMessages"]
        .as_array_mut()
        .expect("logMessages");
    logs.retain(|line| {
        !line
            .as_str()
            .is_some_and(|line| line.starts_with("Program data: ") && line.contains("tHYs+aYZKFF"))
    });
    let Classification::Rejected(reject) = classify(&raw) else {
        panic!("an init_order without its event must not decode");
    };
    assert_eq!(reject.reason(), "no mayan OrderInitialized event");
}
