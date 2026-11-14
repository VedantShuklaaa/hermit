//! Golden tests for the NTT adapter over real captured Solana transactions.
//!
//! `helius_ntt_wct_release_outbound.json` is the WalletConnect WCT manager's
//! `release_wormhole_outbound` transaction, captured from mainnet with
//! `scripts/capture_fixtures.sh ntt_wct_release_outbound <sig>` with
//! `SKIP_WORMHOLE=1` (this test does not use a WormholeScan operation). The
//! transaction is read from the public Solana RPC, not Helius. The deployment's manager and
//! transceiver program IDs come from WalletConnect's `mainnet_deployment.json`.
//! The transaction contains both `transfer_burn` and the release, so the adapter has
//! to ignore the burn step and pair the release with its core bridge `PostMessage`.
//!
//! `helius_ntt_wct_receive_inbound.json` is the same deployment's inbound
//! `receive_wormhole_message` (Base to Solana, sequence 794), captured the same way.
//! The same tx also carries `redeem` and `release_inbound_mint`, so the row is
//! `Completed`; removing them leaves the receive attested but `Pending`. Its posted
//! VAA is a separate `getAccountInfo` fixture, the way the fetch step preloads it,
//! because the instruction itself carries no payload.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::{RawChainRef, Snapshot};
use bridge::domain::bridge_transfer::BridgeStatus;
use bridge::protocols::registry::{ProtocolRegistry, configured_registry, default_registry};
use bridge::protocols::wormhole::TOKEN_BRIDGE;
use bridge::protocols::wormhole::ntt::NttAdapter;
use bridge::protocols::{
    AccountData, BridgeAdapter, Classification, DetectedTransfer, Direction, MessageId, Protocol,
    Reject, SolanaTx, display_name,
};
use bridge::services::detect;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

const WCT_MANAGER: &str = "nttLq3ZsKu9uAFWuBGksdZTPAjkMRyPkxp61CJPsntA";
const WCT_TRANSCEIVER: &str = "5FksLs44iw5TPZ1yngyxKxZyvBXeUCgQM7VQfiRZVK45";
const WCT_TOKEN: &str = "WCTk5xWdn5SYg56twGj32sUF3W4WFQ48ogezLBuYTBY";
const WCT_SIGNATURE: &str =
    "2GXJXqTEq2P6wXZbXS5BHLbpvnjRYdofSvrbsQUArZ3YpRzks8oRbBNMxB9Mq5oSeoV5ccDSDyFKKTfLR51Ye2Ey";
/// WCT's inbound receive from Base, and the posted VAA it was given.
const WCT_INBOUND_SIGNATURE: &str =
    "4CoYG3o1e89ubU49ng3rQ3xbtV3Gw2eHJdgASAjQzCK6ao2rR4S7YJkmPLTk95JTcYCYoyhx7x2ieionzhTXAKR5";
const WCT_INBOUND_VAA: &str = "CHBtLw7aNDAc1WfertxJBccBfqeN2PqsWTtFr6QGznYD";
const WCT_INBOUND_EMITTER: &str =
    "0000000000000000000000003cb1d3a449a868dd8bf8f8928408836543fe2a68";
const WCT_INBOUND_SENDER: &str = "0xfcf31a76f771547f751753f44014433952dfd7f8";
const WCT_INBOUND_RECIPIENT: &str = "9XQLDx1MV5Ssv48DfFfBwFErh8ds6jfXfHZBJKj1FqXY";
/// WCT on Base, which the transfer payload names as the source token.
const WCT_BASE_TOKEN: &str = "0xef4461891dfb3ac8572ccf7c794664a8dd927945";

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn tx_from(raw: &Value) -> SolanaTx {
    let signature = raw["transaction"]["signatures"][0]
        .as_str()
        .unwrap_or_else(|| panic!("no signature in fixture"));
    SolanaTx::from_helius(raw, signature).unwrap_or_else(|e| panic!("from_helius failed: {e}"))
}

/// Loads the captured `getAccountInfo` of the posted VAA the receive was given, the way
/// the fetch step would: the registry says which account is needed, the fixture stands
/// in for the RPC.
fn preload_vaa(tx: &mut SolanaTx) {
    let needed = registry().required_accounts(tx);
    assert_eq!(needed, vec![WCT_INBOUND_VAA.to_string()]);
    let account = read("account_ntt_wct_receive_inbound_vaa.json");
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

fn registry() -> ProtocolRegistry {
    configured_registry(&[], TOKEN_BRIDGE, None, None, &[WCT_MANAGER.to_string()])
}

fn only_transfer(tx: &SolanaTx) -> DetectedTransfer {
    let mut results = registry().classify(tx);
    assert_eq!(results.len(), 1, "adapters that matched: {results:?}");
    let (protocol, classification) = results.remove(0);
    assert_eq!(protocol, Protocol::Wormhole);
    match classification {
        Classification::Transfers(mut transfers) => {
            assert_eq!(transfers.len(), 1, "transfers in the tx");
            transfers.remove(0)
        }
        other => panic!("expected transfers, got {other:?}"),
    }
}

#[test]
fn a_release_outbound_is_decoded_from_the_core_bridge_message() {
    let tx = tx_from(&read("helius_ntt_wct_release_outbound.json"));
    let transfer = only_transfer(&tx);

    assert_eq!(transfer.program, "ntt");
    assert_eq!(display_name("wormhole", &transfer.program), "NTT");
    assert_eq!(transfer.direction, Direction::Outbound);
    assert_eq!(transfer.observed_tx, WCT_SIGNATURE);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::wormhole(1));
    assert_eq!(transfer.destination, RawChainRef::wormhole(30));
    assert_eq!(transfer.source_wallet.as_deref(), tx.signer.as_deref());
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some("0x2be5187ca9bd2b043b93812657af5da27710ca0e")
    );
    assert_eq!(transfer.amount_raw.unwrap().as_str(), "15585875623");
    assert_eq!(transfer.token_symbol, None);
    assert_eq!(transfer.via, None);

    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::wormhole(1));
    assert_eq!(token.address, WCT_TOKEN);

    assert_eq!(
        transfer.message,
        MessageId::Wormhole {
            emitter_chain: 1,
            emitter_address: hex::encode(
                bs58::decode(WCT_TRANSCEIVER)
                    .into_vec()
                    .expect("transceiver address")
            ),
            sequence: 439,
        }
    );
}

#[test]
fn a_transfer_burn_without_a_release_is_not_a_message() {
    // The same real transaction, with the release instruction removed. Its
    // transfer_burn must not become a transfer.
    let raw = read("helius_ntt_wct_release_outbound.json");
    let mut tx = tx_from(&raw);
    tx.instructions.retain(|ix| {
        !ix.data
            .starts_with(&[0xca, 0x57, 0x33, 0xad, 0x8e, 0xa0, 0xbc, 0xcc])
    });

    let adapter = NttAdapter::new(vec![WCT_MANAGER.to_string()]);
    assert_eq!(
        adapter.classify(&tx),
        Classification::Rejected(Reject::NotTransfer(
            "ntt transfer_burn or transfer_lock is not a message"
        ))
    );
}

#[test]
fn a_failed_release_transaction_is_never_a_transfer() {
    let mut tx = tx_from(&read("helius_ntt_wct_release_outbound.json"));
    tx.failed = true;

    let mut results = registry().classify(&tx);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.remove(0).1,
        Classification::Rejected(Reject::NotTransfer("transaction failed"))
    );
}

#[test]
fn a_token_bridge_transaction_still_decodes_the_same_with_ntt_configured() {
    let raw = read("helius_completed_outbound.json");
    let tx = tx_from(&raw);

    let without_ntt = default_registry().classify(&tx);
    let with_ntt = registry().classify(&tx);
    assert_eq!(
        without_ntt, with_ntt,
        "adding the NTT manager must not change Token Bridge results"
    );

    assert_eq!(with_ntt.len(), 1, "only the Token Bridge adapter matches");
    assert!(matches!(with_ntt[0].1, Classification::Transfers(_)));
}

#[test]
fn the_ntt_manager_does_not_route_to_the_token_bridge_adapter() {
    let tx = tx_from(&read("helius_ntt_wct_release_outbound.json"));

    // The registry filters the manager out of the Wormhole adapter's set, so the
    // NTT adapter is the only one that matches.
    let results = registry().classify(&tx);
    assert_eq!(results.len(), 1, "adapters that matched: {results:?}");
    assert!(matches!(results[0].1, Classification::Transfers(_)));

    // A registry that knows only the Token Bridge does not claim the tx at all.
    let mut token_bridge_only = ProtocolRegistry::new();
    token_bridge_only.register(Arc::new(bridge::protocols::wormhole::WormholeAdapter));
    assert!(token_bridge_only.classify(&tx).is_empty());
}

#[test]
fn a_receive_wormhole_message_is_decoded_from_the_posted_vaa() {
    let mut tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));
    preload_vaa(&mut tx);
    let transfer = only_transfer(&tx);

    assert_eq!(transfer.program, "ntt");
    assert_eq!(display_name("wormhole", &transfer.program), "NTT");
    assert_eq!(transfer.direction, Direction::Inbound);
    assert_eq!(transfer.observed_tx, WCT_INBOUND_SIGNATURE);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::wormhole(30));
    assert_eq!(transfer.destination, RawChainRef::wormhole(1));
    assert_eq!(transfer.source_wallet.as_deref(), Some(WCT_INBOUND_SENDER));
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some(WCT_INBOUND_RECIPIENT)
    );
    assert_eq!(transfer.amount_raw.unwrap().as_str(), "287706792415");
    assert_eq!(transfer.token_symbol, None);
    assert_eq!(transfer.via, None);

    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::wormhole(30));
    assert_eq!(token.address, WCT_BASE_TOKEN);

    // The identity is the posted VAA's, which WormholeScan reports as
    // "30/0000000000000000000000003cb1d3a449a868dd8bf8f8928408836543fe2a68/794".
    assert_eq!(
        transfer.message,
        MessageId::Wormhole {
            emitter_chain: 30,
            emitter_address: WCT_INBOUND_EMITTER.into(),
            sequence: 794,
        }
    );
}

#[test]
fn an_inbound_receive_with_its_release_in_the_same_tx_is_completed() {
    let mut tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));
    preload_vaa(&mut tx);
    let transfer = only_transfer(&tx);

    // The real flow releases the tokens in the receive's own transaction.
    assert!(transfer.delivered);
    let bridge =
        detect::to_bridge_transfer(&transfer, None, &Snapshot::default()).expect("bridge transfer");
    assert!(matches!(bridge.status, BridgeStatus::Completed));
    assert_eq!(bridge.program, "ntt");
    // The tokens moved on Solana in this tx, so it is the destination side. The origin
    // tx is on Base and unknown until enrichment learns it.
    assert_eq!(bridge.source_tx_hash, None);
    assert_eq!(
        bridge.destination_tx_hash.as_deref(),
        Some(WCT_INBOUND_SIGNATURE)
    );
    assert_eq!(bridge.amount.as_deref(), Some("287706792415"));
    assert_eq!(bridge.token.as_deref(), Some(WCT_BASE_TOKEN));
}

#[test]
fn a_receive_without_its_release_stays_pending() {
    // The same real receive, with every manager instruction after it removed: the
    // message is attested, but no release has moved the tokens.
    let mut tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));
    tx.instructions.retain(|ix| {
        // 86d58f44eb66e860 is receive_wormhole_message.
        ix.program_id != WCT_MANAGER
            || ix
                .data
                .starts_with(&[0x86, 0xd5, 0x8f, 0x44, 0xeb, 0x66, 0xe8, 0x60])
    });
    preload_vaa(&mut tx);

    let transfer = only_transfer(&tx);
    assert!(!transfer.delivered);
    let bridge =
        detect::to_bridge_transfer(&transfer, None, &Snapshot::default()).expect("bridge transfer");
    assert!(matches!(bridge.status, BridgeStatus::Pending));
    assert_eq!(bridge.program, "ntt");
    // Nothing has moved on Solana yet, and neither leg has a tx in its chain's terms.
    assert_eq!(bridge.source_tx_hash, None);
    assert_eq!(bridge.destination_tx_hash, None);
}

#[test]
fn the_inbound_fixture_carries_the_receive_and_its_followup_steps() {
    // The real flow calls receive_wormhole_message, redeem and release_inbound_mint in
    // one tx. Only the receive carries the message identity, so it must be one transfer.
    let mut tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));
    preload_vaa(&mut tx);

    let manager_ixs: Vec<&[u8]> = tx
        .instructions
        .iter()
        .filter(|ix| ix.program_id == WCT_MANAGER)
        .map(|ix| ix.data.as_slice())
        .collect();
    assert_eq!(manager_ixs.len(), 3);
    assert!(manager_ixs[0].starts_with(&[0x86, 0xd5, 0x8f, 0x44, 0xeb, 0x66, 0xe8, 0x60]));
    assert!(manager_ixs[1].starts_with(&[0xb8, 0x0c, 0x56, 0x95, 0x46, 0xc4, 0x61, 0xe1]));
    assert!(manager_ixs[2].starts_with(&[0x0c, 0xe9, 0x4a, 0x41, 0x3c, 0xa8, 0x31, 0x78]));

    assert_eq!(only_transfer(&tx).direction, Direction::Inbound);
}

#[test]
fn a_receive_without_the_posted_vaa_is_unsupported() {
    let tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));

    // No preload: the fetch step would normally have read the VAA account first.
    let results = registry().classify(&tx);
    assert_eq!(results.len(), 1, "adapters that matched: {results:?}");
    assert_eq!(
        results[0].1,
        Classification::Rejected(Reject::Unsupported(
            "inbound receive: posted VAA account was not loaded"
        ))
    );
}

#[test]
fn a_failed_receive_transaction_is_never_a_transfer() {
    let mut tx = tx_from(&read("helius_ntt_wct_receive_inbound.json"));
    tx.failed = true;

    let mut results = registry().classify(&tx);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.remove(0).1,
        Classification::Rejected(Reject::NotTransfer("transaction failed"))
    );
}
