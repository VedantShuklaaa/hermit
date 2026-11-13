//! Golden tests for the CCTP V2 adapter over two real captured Solana mainnet
//! transactions: a V2 `receive_message` completion and a `reclaim_event_account`
//! admin call. The completion's message and attestation are the chain data itself, so
//! the decoded identity, recipient, token and amount are checked against it directly.
//!
//! Captured from the public Solana RPC with `getTransaction` (`jsonParsed`); no Helius
//! key or WormholeScan was involved.

use bridge::chains::RawChainRef;
use bridge::protocols::cctp::{CctpAdapter, MESSAGE_TRANSMITTER_V2};
use bridge::protocols::{BridgeAdapter, Classification, Direction, MessageId, Reject, SolanaTx};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

const COMPLETION_TX: &str =
    "48veqJyLyD8NzY3SFZiYbELc3utotZCenqH2h5sMdpNX1sj8kWuXqMePw4ggGG4rqPhzzmfgnrM1vWgSxsA6QxDL";
const ADMIN_TX: &str =
    "3yMeC3v4hfYXtUi5acqtVa93RxZWQfpkxygjmbRnXcwJ9Vp6dZt5R17yaL4HQBtQCVZNHE5juCz18MeADCkskrCs";

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn classify(file: &str, signature: &str) -> Classification {
    let raw = read(file);
    let tx = SolanaTx::from_helius(&raw, signature)
        .unwrap_or_else(|e| panic!("{file}: from_helius failed: {e}"));
    CctpAdapter::default().classify(&tx)
}

#[test]
fn the_completion_decodes_the_message_circle_attested() {
    let Classification::Transfers(mut found) =
        classify("helius_cctp_completion.json", COMPLETION_TX)
    else {
        panic!("expected the completion to decode");
    };
    assert_eq!(found.len(), 1, "transfers in the tx");
    let transfer = found.remove(0);

    assert_eq!(transfer.program, "cctp");
    assert_eq!(transfer.direction, Direction::Inbound);
    assert_eq!(transfer.observed_tx, COMPLETION_TX);
    assert_eq!(transfer.event_index, 0);
    assert_eq!(transfer.source, RawChainRef::cctp(0));
    assert_eq!(transfer.destination, RawChainRef::cctp(5));
    assert_eq!(
        transfer.message,
        MessageId::Cctp {
            source_domain: 0,
            nonce: "3de8a610c8188cee961c9c7d8e61ea0384e10ad0c5365906e29dfa2b27f9442d".into(),
        }
    );
    assert_eq!(
        transfer.destination_wallet.as_deref(),
        Some("2DkwonAoa36nqPQadZEJVdwAbz91g1epUYU7AAvGWg41")
    );
    assert_eq!(
        transfer.source_wallet.as_deref(),
        Some("0xbf825ea5ce9d2aa69ad6ca16fb4e339c00674fe6")
    );
    let token = transfer.token.expect("token");
    assert_eq!(token.chain, RawChainRef::cctp(0));
    assert_eq!(token.address, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    assert_eq!(
        transfer.amount_raw.as_ref().map(|a| a.as_str()),
        Some("1000000000000")
    );
}

#[test]
fn the_admin_call_is_rejected_as_not_a_transfer() {
    assert_eq!(
        classify("helius_cctp_admin.json", ADMIN_TX),
        Classification::Rejected(Reject::NotTransfer("cctp admin instruction"))
    );
}

/// The admin fixture really is a call to the configured transmitter program, so the
/// rejection above is the adapter's decision and not a routing accident.
#[test]
fn the_admin_fixture_targets_the_message_transmitter() {
    let raw = read("helius_cctp_admin.json");
    let program = raw
        .pointer("/transaction/message/instructions/0/programId")
        .and_then(Value::as_str);
    assert_eq!(program, Some(MESSAGE_TRANSMITTER_V2));
}
