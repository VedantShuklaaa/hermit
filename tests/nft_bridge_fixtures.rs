//! Golden tests for the NFT Bridge adapter over real captured Solana transactions.
//!
//! Each send fixture is a pair: the transaction the adapter reads and WormholeScan's
//! operation for the same message. The independent check is the signed VAA inside the
//! operation, not WormholeScan's summary fields (which are empty for NFT transfers):
//! the adapter must decode from the instructions exactly what the guardians signed.
//!
//! The completion fixture's posted VAA account was closed on chain after redemption
//! (the core bridge closes it), so `account_nft_complete_wrapped_vaa.json` is the
//! account rebuilt from the real signed VAA for that message: same identity and
//! payload, with the two bookkeeping fields the adapter never reads left zero.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::Snapshot;
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::protocols::registry::{ProtocolRegistry, default_registry};
use bridge::protocols::wormhole::nft_bridge::NftBridgeAdapter;
use bridge::protocols::{
    AccountData, Classification, DetectedTransfer, Direction, NFT_BRIDGE_PROGRAM, Protocol, Reject,
    SolanaTx,
};
use bridge::services::detect;
use bridge::vaa::decode::decode_destination_address;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn registry() -> ProtocolRegistry {
    let mut registry = default_registry();
    registry.register(Arc::new(NftBridgeAdapter));
    registry
}

/// The operation and transaction of one captured message.
fn fixture(name: &str) -> (Value, SolanaTx) {
    let op = read(&format!("wormhole_{name}.json"));
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
    let tx = SolanaTx::from_helius(&raw, signature)
        .unwrap_or_else(|e| panic!("{name}: from_helius failed: {e}"));
    (op, tx)
}

fn transfers(tx: &SolanaTx) -> Vec<DetectedTransfer> {
    let mut results = registry().classify(tx);
    assert_eq!(results.len(), 1, "adapters that matched");
    let (protocol, classification) = results.remove(0);
    assert_eq!(protocol, Protocol::Wormhole);
    match classification {
        Classification::Transfers(t) => t,
        other => panic!("expected transfers, got {other:?}"),
    }
}

fn only_transfer(tx: &SolanaTx) -> DetectedTransfer {
    let mut found = transfers(tx);
    assert_eq!(found.len(), 1, "transfers in the tx");
    found.remove(0)
}

/// The transfer payload the guardians signed, from the operation's VAA.
fn signed_payload(op: &Value) -> Vec<u8> {
    let raw = STANDARD
        .decode(
            op["vaa"]["raw"]
                .as_str()
                .unwrap_or_else(|| panic!("no signed VAA in {}", op["id"])),
        )
        .expect("base64 VAA");
    let signatures = raw[5] as usize;
    let body = &raw[6 + signatures * 66..];
    let payload = body[51..].to_vec();
    assert_eq!(payload.first(), Some(&1), "transfer payload id");
    payload
}

struct Signed {
    token_address: [u8; 32],
    token_chain: u16,
    symbol: Option<String>,
    token_id: [u8; 32],
    to: [u8; 32],
    to_chain: u16,
}

/// Mirrors the payload layout in the NFT Bridge program's `messages.rs`.
fn decode_signed(payload: &[u8]) -> Signed {
    let word = |from: usize| -> [u8; 32] { payload[from..from + 32].try_into().expect("32 bytes") };
    let symbol = {
        let bytes = &payload[35..67];
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        let value: String = String::from_utf8_lossy(&bytes[..end])
            .chars()
            .filter(|c| *c != '\u{FFFD}')
            .collect();
        (!value.is_empty()).then_some(value)
    };
    let uri_len = payload[131] as usize;
    let to_at = 132 + uri_len;
    Signed {
        token_address: word(1),
        token_chain: u16::from_be_bytes([payload[33], payload[34]]),
        symbol,
        token_id: word(99),
        to: word(to_at),
        to_chain: u16::from_be_bytes([payload[to_at + 32], payload[to_at + 33]]),
    }
}

/// The token the adapter should name: the mint for a Solana-native NFT, otherwise the
/// collection/contract on its origin chain.
fn expected_token(s: &Signed) -> (String, String) {
    let address = if s.token_chain == 1 {
        bs58::encode(s.token_id).into_string()
    } else {
        decode_destination_address(s.token_chain, &hex::encode(s.token_address))
            .expect("token address")
    };
    (s.token_chain.to_string(), address)
}

fn assert_matches_signed(t: &DetectedTransfer, s: &Signed) {
    assert_eq!(t.program, NFT_BRIDGE_PROGRAM);
    assert_eq!(t.amount_raw.as_ref().map(|a| a.as_str()), Some("1"));
    assert_eq!(t.token_symbol, s.symbol);
    let token = t.token.as_ref().expect("token");
    assert_eq!(
        (token.chain.id.clone(), token.address.clone()),
        expected_token(s)
    );
    let expected_wallet =
        decode_destination_address(s.to_chain, &hex::encode(s.to)).expect("recipient address");
    assert_eq!(
        t.destination_wallet.as_deref(),
        Some(expected_wallet.as_str())
    );
}

#[test]
fn a_native_nft_send_is_decoded_and_matches_the_signed_vaa() {
    let (op, tx) = fixture("nft_send_native");
    let t = only_transfer(&tx);
    let s = decode_signed(&signed_payload(&op));

    assert_eq!(t.direction, Direction::Outbound);
    assert_eq!(t.message.key(), op["id"].as_str().expect("operation id"));
    assert_eq!(t.event_index, 0);
    assert_eq!(t.observed_tx, tx.signature);
    assert_eq!(t.source.id, "1");
    assert_eq!(t.destination.id, s.to_chain.to_string());
    assert!(t.source_wallet.is_some());
    // Solana-origin: the mint is the token id, and the token address is the dummy.
    assert_eq!(s.token_chain, 1);
    assert_eq!(s.token_address, [1u8; 32]);
    assert_matches_signed(&t, &s);
    assert_eq!(t.token_symbol.as_deref(), Some("PRT"));
}

#[test]
fn a_wrapped_nft_send_is_decoded_and_matches_the_signed_vaa() {
    let (op, tx) = fixture("nft_send_wrapped");
    let t = only_transfer(&tx);
    let s = decode_signed(&signed_payload(&op));

    assert_eq!(t.direction, Direction::Outbound);
    assert_eq!(t.message.key(), op["id"].as_str().expect("operation id"));
    assert_eq!(t.destination.id, s.to_chain.to_string());
    assert_ne!(s.token_chain, 1, "a wrapped NFT comes from another chain");
    assert_matches_signed(&t, &s);
    // This fixture's symbol field is empty, so the indexer falls back to metadata.
    assert_eq!(t.token_symbol, None);
}

fn with_vaa_account(name: &str, tx: &mut SolanaTx) {
    let needed = registry().required_accounts(tx);
    assert_eq!(needed.len(), 1, "{name}: accounts a redeem needs");
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

#[test]
fn a_completion_is_decoded_from_the_posted_vaa() {
    let (op, mut tx) = fixture("nft_complete_wrapped");
    with_vaa_account("nft_complete_wrapped", &mut tx);
    let t = only_transfer(&tx);
    let s = decode_signed(&signed_payload(&op));

    assert_eq!(t.direction, Direction::Inbound);
    assert_eq!(t.message.key(), op["id"].as_str().expect("operation id"));
    assert_eq!(t.source.id, op["emitterChain"].to_string());
    assert_eq!(t.destination.id, "1");
    assert_eq!(t.source_wallet, None);
    assert_matches_signed(&t, &s);
    assert_eq!(t.token_symbol.as_deref(), Some("CAC"));

    // Cross-check against WormholeScan's own decoding of the same message.
    let payload = &op["content"]["payload"];
    assert_eq!(t.token_symbol.as_deref(), payload["symbol"].as_str());
    assert_eq!(
        t.destination_wallet.as_deref(),
        Some("GtNrJDGsA3ZWULKu5N6CK8fv8gLzjPHTbDcVWJnTtfPQ")
    );
    assert_eq!(payload["tokenChain"], 2);
}

#[test]
fn an_outbound_nft_stays_pending_even_when_wormholescan_names_a_destination() {
    let (op, tx) = fixture("nft_send_native");
    let detected = only_transfer(&tx);
    let mut bridge = detect::to_bridge_transfer(&detected, None, &Snapshot::default())
        .expect("the transfer is representable");

    assert!(matches!(bridge.status, BridgeStatus::Pending));
    detect::apply_wormholescan(&mut bridge, &op, &Snapshot::default());

    // No watched chain event confirms the EVM side of an NFT transfer, so the hint is
    // recorded but the status does not advance (known limitation).
    assert!(
        bridge.destination_tx_hash.is_some(),
        "WormholeScan's destination tx is still recorded"
    );
    assert!(matches!(bridge.status, BridgeStatus::Pending));
}

#[test]
fn a_completion_without_its_account_is_unsupported() {
    let (op, tx) = fixture("nft_complete_wrapped");
    assert_eq!(op["targetChain"]["chainId"], 1);
    let mut results = registry().classify(&tx);
    assert_eq!(results.len(), 1);
    assert!(
        matches!(
            results.remove(0).1,
            Classification::Rejected(Reject::Unsupported(_))
        ),
        "the account was never loaded"
    );
}

#[test]
fn a_failed_nft_transaction_is_not_a_transfer() {
    let raw = read("helius_nft_failed.json");
    let signature = raw["transaction"]["signatures"][0]
        .as_str()
        .expect("signature");
    let tx = SolanaTx::from_helius(&raw, signature).expect("failed fixture parses");
    assert!(tx.failed);
    let mut results = registry().classify(&tx);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.remove(0).1,
        Classification::Rejected(Reject::NotTransfer("transaction failed"))
    );
}
