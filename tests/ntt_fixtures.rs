//! Golden tests for the NTT outbound adapter over a real captured Solana transaction.
//!
//! `helius_ntt_wct_release_outbound.json` is the WalletConnect WCT manager's
//! `release_wormhole_outbound` transaction, captured from mainnet with
//! `scripts/capture_fixtures.sh ntt_wct_release_outbound <sig>` with
//! `SKIP_WORMHOLE=1` (this test does not use a WormholeScan operation). The
//! transaction is read from the public Solana RPC, not Helius. The deployment's manager and
//! transceiver program IDs come from WalletConnect's `mainnet_deployment.json`.
//! The transaction contains both `transfer_burn` and the release, so the adapter has
//! to ignore the burn step and pair the release with its core bridge `PostMessage`.

use bridge::chains::RawChainRef;
use bridge::protocols::registry::{ProtocolRegistry, configured_registry, default_registry};
use bridge::protocols::wormhole::TOKEN_BRIDGE;
use bridge::protocols::wormhole::ntt::NttAdapter;
use bridge::protocols::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, MessageId, Protocol, Reject,
    SolanaTx, display_name,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

const WCT_MANAGER: &str = "nttLq3ZsKu9uAFWuBGksdZTPAjkMRyPkxp61CJPsntA";
const WCT_TRANSCEIVER: &str = "5FksLs44iw5TPZ1yngyxKxZyvBXeUCgQM7VQfiRZVK45";
const WCT_TOKEN: &str = "WCTk5xWdn5SYg56twGj32sUF3W4WFQ48ogezLBuYTBY";
const WCT_SIGNATURE: &str =
    "2GXJXqTEq2P6wXZbXS5BHLbpvnjRYdofSvrbsQUArZ3YpRzks8oRbBNMxB9Mq5oSeoV5ccDSDyFKKTfLR51Ye2Ey";

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
