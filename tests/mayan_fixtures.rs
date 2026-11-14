//! Golden tests for the Mayan Swift V2 adapter over real captured Solana
//! mainnet transactions.
//!
//! `helius_mayan_swift_init_order.json` is an `init_order` source order. The order
//! hash and amount come from the `OrderInitialized` event, the destination chain and
//! wallet from the `init_order` args, and the source mint and trader from its
//! accounts, so the decoded transfer is checked against them directly.
//!
//! `helius_mayan_swift_fulfill.json` is a destination-side `fulfill` on Solana. Its
//! state account is a separate `getAccountInfo` fixture,
//! `account_mayan_swift_fulfill_state.json`, preloaded the way the fetch step does
//! through `required_accounts`, because the instruction itself carries only the
//! unlocker.
//!
//! Captured with `scripts/capture_fixtures.sh` (`SKIP_WORMHOLE=1`,
//! `MAX_TX_VERSION=1`) from the public Solana RPC; no Helius key or WormholeScan
//! was involved.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::{ChainRef, ChainRow, MappingRow, RawChainRef, RegistryStatus, Snapshot};
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::protocols::mayan::MayanAdapter;
use bridge::protocols::{
    AccountData, BridgeAdapter, Classification, Direction, MessageId, SolanaTx,
};
use bridge::services::detect;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;

const INIT_ORDER_TX: &str =
    "QCaMXBE6VSZgajW5wJCHqxesQwjjdRgUUhMCoX8cwd2HXwiQZpVzGrWDMUE4s9dpj5msdN9KPZfmXjN2Qn9Hkes";
const ORDER_HASH: &str = "4a35d4d47964cc51b38580f12e918aad9bd854593ee0f60e3812cc981ab75318";
const TRADER: &str = "D232Pb6Z4sandj5uSPAYmAuuQG9SZjCWfRyKAGwVxVGR";
const MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

const FULFILL_TX: &str =
    "5z3cGCEYyjPsGscYuvok4ELrWTFmeWeGUvLzjYxAUPycG1hrP87xuZT94mvFjbFZR8eyKdHNuZE6nrCxCp5m8AEu";
const FULFILL_STATE: &str = "15Nz2BwPds7VE1nqirgASFDsaHcsaH6ujRfQCajtftz";
const FULFILL_ORDER_HASH: &str = "1082fbec84af6e6c8547d80d8fa3fdeb718485c4c2e70f659ff3f9a9f219ab28";
/// BSC in the Wormhole chain namespace.
const FULFILL_SOURCE_CHAIN: u16 = 4;
const FULFILL_TOKEN: &str = "0x55d398326f99059ff775485246999027b3197955";
const FULFILL_AMOUNT: &str = "95749899";

fn read_file(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn read() -> Value {
    read_file("helius_mayan_swift_init_order.json")
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

fn fulfill_view() -> SolanaTx {
    let raw = read_file("helius_mayan_swift_fulfill.json");
    SolanaTx::from_helius(&raw, FULFILL_TX).unwrap_or_else(|e| panic!("from_helius: {e}"))
}

/// Loads the captured state account the way the fetch step does: ask the adapter
/// which accounts it needs, then put the `getAccountInfo` result in the view.
fn preload_fulfill_state(tx: &mut SolanaTx) {
    let keys = MayanAdapter.required_accounts(tx);
    assert_eq!(keys, vec![FULFILL_STATE.to_string()]);

    let state = read_file("account_mayan_swift_fulfill_state.json");
    let value = &state["value"];
    let encoded = value["data"]
        .as_array()
        .and_then(|d| d.first())
        .and_then(Value::as_str)
        .expect("base64 account data");
    tx.accounts.insert(
        FULFILL_STATE.to_string(),
        AccountData {
            owner: value["owner"].as_str().expect("account owner").to_string(),
            data: STANDARD.decode(encoded).expect("base64 account data"),
        },
    );
}

fn classify_fulfill(raw: &Value) -> Classification {
    let tx = SolanaTx::from_helius(raw, FULFILL_TX).unwrap_or_else(|e| panic!("from_helius: {e}"));
    MayanAdapter.classify(&tx)
}

#[test]
fn the_fulfill_decodes_the_destination_transfer() {
    let mut tx = fulfill_view();
    preload_fulfill_state(&mut tx);

    let transfer = only_transfer(MayanAdapter.classify(&tx));

    assert_eq!(transfer.program, "swift_v2");
    assert_eq!(transfer.direction, Direction::Inbound);
    assert!(transfer.delivered);
    assert_eq!(transfer.observed_tx, FULFILL_TX);
    assert_eq!(
        transfer.message,
        MessageId::Mayan {
            order_hash: FULFILL_ORDER_HASH.into(),
        }
    );
    assert_eq!(transfer.message.key(), FULFILL_ORDER_HASH);
    assert_eq!(transfer.source, RawChainRef::wormhole(FULFILL_SOURCE_CHAIN));
    assert_eq!(transfer.destination, RawChainRef::wormhole(1));
    assert_eq!(transfer.source_wallet, None);
    assert_eq!(transfer.destination_wallet, None);
    assert_eq!(
        transfer.amount_raw.as_ref().map(|a| a.as_str()),
        Some(FULFILL_AMOUNT)
    );
    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::wormhole(FULFILL_SOURCE_CHAIN));
    assert_eq!(token.address, FULFILL_TOKEN);
}

#[test]
fn the_fulfill_row_is_a_completed_destination_observation() {
    let mut tx = fulfill_view();
    preload_fulfill_state(&mut tx);
    let transfer = only_transfer(MayanAdapter.classify(&tx));
    let row = detect::mayan_destination_row_values(&transfer, &Snapshot::default());

    assert!(matches!(row.status, BridgeStatus::Completed));
    assert_eq!(row.source_tx_hash, None);
    assert_eq!(row.destination_tx_hash.as_deref(), Some(FULFILL_TX));
    assert_eq!(row.source_wallet, None);
    assert_eq!(row.destination_wallet, None);
    assert_eq!(row.token.as_deref(), Some(FULFILL_TOKEN));
    assert_eq!(row.amount.as_deref(), Some(FULFILL_AMOUNT));
}

#[test]
fn required_accounts_asks_for_the_destination_state_account() {
    let tx = fulfill_view();
    assert_eq!(
        MayanAdapter.required_accounts(&tx),
        vec![FULFILL_STATE.to_string()]
    );
}

#[test]
fn a_fulfill_without_its_state_account_is_not_a_transfer() {
    let Classification::Rejected(reject) = MayanAdapter.classify(&fulfill_view()) else {
        panic!("a fulfill without its state account must not decode");
    };
    assert_eq!(
        reject.reason(),
        "inbound fulfill: destination state account was not loaded"
    );
}

#[test]
fn a_failed_fulfill_is_never_a_transfer() {
    let mut raw = read_file("helius_mayan_swift_fulfill.json");
    raw["meta"]["err"] = json!({"InstructionError": [0, {"Custom": 1}]});
    let Classification::Rejected(reject) = classify_fulfill(&raw) else {
        panic!("a failed tx must not decode");
    };
    assert_eq!(reject.reason(), "transaction failed");
}
