//! The frozen `/api/v1` response, captured from the current pipeline over the real
//! fixtures. A10 moves `/api/v1` behind a compatibility mapper; these snapshots are the
//! contract that mapper must reproduce byte for byte.
//!
//! Everything here is offline: the Helius and WormholeScan fixtures stand in for the
//! providers. Token metadata is `None` (it needs the chain adapter), so `token_symbol`
//! is null in the snapshots.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::{ChainRow, MappingRow, NS_WORMHOLE, RegistryStatus, Snapshot};
use bridge::db::repository::{StoredTransaction, StoredTransactionWithTransfers, StoredTransfer};
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::domain::transaction::{NormalisedTransaction, TxStatus};
use bridge::normaliser;
use bridge::protocols::registry::default_registry;
use bridge::protocols::{AccountData, DetectedTransfer, SolanaTx};
use bridge::routes::v1::compat;
use bridge::services::api_v2;
use bridge::services::detect::{self, Enrichment};
use chrono::DateTime;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

/// Fixtures that the chain-first path turns into a transfer. `admin_call` is rejected by
/// the adapter and does not produce a v1 transfer, so it is not a response snapshot.
const FIXTURES: [&str; 5] = [
    "completed_outbound",
    "payload3_executor",
    "payload3_plain",
    "inbound_redeem",
    "inbound_redeem_tb",
];

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn snapshot_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots/v1")
}

/// The chains the fixtures touch, with explorer templates so URLs are snapshotted too.
fn chains() -> Snapshot {
    let chain = |id: i16, slug: &str| ChainRow {
        id,
        slug: slug.into(),
        name: slug.into(),
        kind: "other".into(),
        explorer_tx_url: Some(format!("https://{slug}.example/tx/{{hash}}")),
        status: RegistryStatus::Active,
    };
    let mapping = |wormhole_id: &str, chain_id: i16| MappingRow {
        protocol: NS_WORMHOLE.into(),
        protocol_chain_id: wormhole_id.into(),
        chain_id,
        status: RegistryStatus::Active,
    };
    Snapshot::build(
        vec![
            chain(1, "solana"),
            chain(2, "ethereum"),
            chain(5, "polygon"),
        ],
        vec![mapping("1", 1), mapping("2", 2), mapping("5", 5)],
    )
}

fn solana_tx(name: &str, op: &Value) -> SolanaTx {
    let raw = read(&format!("helius_{name}.json"));
    let side = if op["sourceChain"]["chainId"] == 1 {
        "/sourceChain/transaction/txHash"
    } else {
        "/targetChain/transaction/txHash"
    };
    let signature = op
        .pointer(side)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{name}: no Solana signature at {side}"));
    let mut tx = SolanaTx::from_helius(&raw, signature)
        .unwrap_or_else(|e| panic!("{name}: from_helius failed: {e}"));

    // A redeem needs the posted VAA account, exactly as the fetch step preloads it.
    let needed = default_registry().required_accounts(&tx);
    if !needed.is_empty() {
        let account = read(&format!("account_{name}_vaa.json"));
        let value = &account["value"];
        let data = STANDARD
            .decode(value["data"][0].as_str().expect("account data"))
            .expect("base64 account data");
        tx.accounts.insert(
            needed[0].clone(),
            AccountData {
                owner: value["owner"].as_str().expect("account owner").to_string(),
                data,
            },
        );
    }
    tx
}

struct Current {
    tx: NormalisedTransaction,
    detected: DetectedTransfer,
}

/// What `/api/v1/transactions/analyse` returns today for the fixture, built from the
/// same pure pieces `analyse_tx` uses.
fn current_v1(name: &str) -> Current {
    let op = read(&format!("wormhole_{name}.json"));
    let raw = read(&format!("helius_{name}.json"));
    let tx_view = solana_tx(name, &op);
    let mut tx = normaliser::solana::normalise(raw, &tx_view.signature)
        .unwrap_or_else(|e| panic!("{name}: normalise failed: {e}"));

    let found = detect::detect(&default_registry(), &tx_view);
    let mut found = found.into_iter();
    let first = found
        .next()
        .unwrap_or_else(|| panic!("{name}: no transfer decoded"));
    let mut transfer = detect::to_bridge_transfer(&first, None, &chains())
        .unwrap_or_else(|e| panic!("{name}: to_bridge_transfer failed: {e}"));
    if detect::apply_wormholescan(&mut transfer, &op, &chains()) == Enrichment::Applied {
        tx.bridge_event = normaliser::wormhole::normalise(op).ok();
    }
    tx.bridge_transfer = Some(transfer);
    Current {
        tx,
        detected: first,
    }
}

/// The rows `persist_transaction` would write for the fixture, so the v2 assembly and
/// the v1 mapper are exercised on the real stored shape.
fn stored(name: &str, current: &Current) -> StoredTransactionWithTransfers {
    let tx = &current.tx;
    let detected = &current.detected;
    let bridge = tx
        .bridge_transfer
        .as_ref()
        .unwrap_or_else(|| panic!("{name}: no transfer"));
    let token = detected.token.as_ref().filter(|_| bridge.token.is_some());

    StoredTransactionWithTransfers {
        transaction: StoredTransaction {
            hash: tx.hash.clone(),
            chain: tx.chain.wormhole_id() as i16,
            status: tx_status(&tx.status).to_string(),
            slot: tx.slot as i64,
            timestamp: tx.timestamp.and_then(|s| DateTime::from_timestamp(s, 0)),
            fee_lamports: tx.fee_lamports as i64,
            signer: tx.signer.clone(),
        },
        transfers: vec![StoredTransfer {
            program: bridge.program.clone(),
            protocol: "wormhole".to_string(),
            direction: bridge.direction().as_str().to_string(),
            observed_tx_hash: detected.observed_tx.clone(),
            event_index: detected.event_index as i16,
            via: detected.via.as_ref().map(|v| v.name.clone()),
            via_ref: detected.via.as_ref().and_then(|v| v.reference.clone()),
            source_ref_protocol: detected.source.protocol.clone(),
            source_ref_id: detected.source.id.clone(),
            source_tx_hash: bridge.source_tx_hash.clone(),
            source_wallet: bridge.source_wallet.clone(),
            source_explorer_url: bridge.source_explorer_url.clone(),
            destination_ref_protocol: detected.destination.protocol.clone(),
            destination_ref_id: detected.destination.id.clone(),
            destination_wallet: bridge.destination_wallet.clone(),
            destination_tx_hash: bridge.destination_tx_hash.clone(),
            destination_explorer_url: bridge.destination_explorer_url.clone(),
            token_ref_protocol: token.map(|t| t.chain.protocol.clone()),
            token_ref_id: token.map(|t| t.chain.id.clone()),
            token: bridge.token.clone(),
            token_symbol: bridge.token_symbol.clone(),
            amount: bridge.amount.clone(),
            amount_formatted: bridge.amount_formatted.clone(),
            status: bridge_status(&bridge.status).to_string(),
            emitter_chain: Some(i32::from(bridge.message_id.emitter_chain)),
            emitter_address: Some(bridge.message_id.emitter_address.clone()),
            sequence: Some(bridge.message_id.sequence.to_string()),
        }],
    }
}

fn tx_status(status: &TxStatus) -> &'static str {
    match status {
        TxStatus::Success => "Success",
        TxStatus::Failed => "Failed",
    }
}

fn bridge_status(status: &BridgeStatus) -> &'static str {
    match status {
        BridgeStatus::Detected => "Detected",
        BridgeStatus::Pending => "Pending",
        BridgeStatus::Completed => "Completed",
    }
}

fn pretty(tx: &NormalisedTransaction) -> String {
    let mut s = serde_json::to_string_pretty(tx).expect("serialise v1 response");
    s.push('\n');
    s
}

/// A row from another program is present in `/api/v2` and explicitly rejected by
/// `/api/v1`, never mapped as if it were a Token Bridge transfer.
#[test]
fn a_non_token_bridge_row_is_only_in_v2() {
    let current = current_v1("completed_outbound");
    let mut stored = stored("completed_outbound", &current);
    stored.transfers[0].program = "ntt".to_string();

    let response = api_v2::assemble(stored, &chains()).expect("v2 assemble");
    assert_eq!(response.transfers.len(), 1);
    assert_eq!(response.transfers[0].program, "ntt");

    assert!(matches!(
        compat::to_v1(&response, current.tx.bridge_event.clone()),
        Err(bridge::error::AppError::Normalisation(_))
    ));
}

#[test]
fn v1_responses_match_the_saved_snapshots() {
    let update = std::env::var_os("UPDATE_V1_SNAPSHOTS").is_some();
    if update {
        fs::create_dir_all(snapshot_dir()).expect("create snapshot dir");
    }

    for name in FIXTURES {
        let actual = pretty(&current_v1(name).tx);
        let path = snapshot_dir().join(format!("{name}.json"));
        if update {
            fs::write(&path, &actual).unwrap_or_else(|e| panic!("{name}: write: {e}"));
            continue;
        }
        let expected =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: read snapshot: {e}"));
        assert_eq!(actual, expected, "{name}: frozen v1 response changed");
    }
}

/// A10: `/api/v1` is now served by the generalised model through the compatibility
/// mapper. Its output must equal the frozen snapshot exactly.
#[test]
fn the_v1_mapper_reproduces_the_saved_snapshots() {
    for name in FIXTURES {
        let current = current_v1(name);
        let expected = fs::read_to_string(snapshot_dir().join(format!("{name}.json")))
            .unwrap_or_else(|e| panic!("{name}: read snapshot: {e}"));

        let response = api_v2::assemble(stored(name, &current), &chains())
            .unwrap_or_else(|e| panic!("{name}: v2 assemble failed: {e}"));
        let mapped = compat::to_v1(&response, current.tx.bridge_event.clone())
            .unwrap_or_else(|e| panic!("{name}: v1 mapper failed: {e}"));

        assert_eq!(
            pretty(&mapped),
            expected,
            "{name}: the v1 mapper changed the frozen response"
        );
    }
}
