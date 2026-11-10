//! Golden tests: the protocol registry run over real captured transactions.
//!
//! Each fixture is a pair captured by `scripts/capture_fixtures.sh`:
//!   helius_<name>.json    the Solana transaction (what the adapter reads)
//!   wormhole_<name>.json  WormholeScan's own decoding of the same message
//!
//! WormholeScan is the independent reference: the adapter decodes the raw
//! instructions, and its output must agree with what WormholeScan reports.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bridge::chains::{ChainRow, MappingRow, NS_WORMHOLE, RawChainRef, RegistryStatus, Snapshot};
use bridge::domain::bridge_transfer::{BridgeStatus, ChainId};
use bridge::protocols::registry::default_registry;
use bridge::protocols::wormhole::TOKEN_BRIDGE;
use bridge::protocols::{
    AccountData, Classification, DetectedTransfer, Direction, Protocol, Reject, SolanaTx,
};
use bridge::services::detect::{self, Enrichment};
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;

/// Sends out of Solana, decoded from the transaction alone. The two payload-3
/// fixtures reach the Token Bridge through another program (an executor router
/// and a custom program), so the Token Bridge call is an inner instruction.
const OUTBOUND: [&str; 3] = ["completed_outbound", "payload3_executor", "payload3_plain"];
const ROUTED: [&str; 2] = ["payload3_executor", "payload3_plain"];
/// Redeems into Solana, decoded from the posted VAA account (A1b).
const INBOUND: [&str; 2] = ["inbound_redeem", "inbound_redeem_tb"];

struct Fixture {
    name: String,
    /// WormholeScan's operation.
    op: Value,
    tx: SolanaTx,
}

fn read(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: invalid json: {e}"))
}

fn fixture(name: &str) -> Fixture {
    let op = read(&format!("wormhole_{name}.json"));
    let raw = read(&format!("helius_{name}.json"));
    // The Solana signature is the source tx for a send and the destination tx
    // for a redeem.
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
    Fixture {
        name: name.to_string(),
        op,
        tx,
    }
}

/// Runs the registry the way the pipeline does. Exactly one adapter must match.
fn classify(f: &Fixture) -> Classification {
    let mut results = default_registry().classify(&f.tx);
    assert_eq!(results.len(), 1, "{}: adapters that matched", f.name);
    let (protocol, classification) = results.remove(0);
    assert_eq!(protocol, Protocol::Wormhole, "{}: protocol", f.name);
    classification
}

fn transfers(f: &Fixture) -> Vec<DetectedTransfer> {
    match classify(f) {
        Classification::Transfers(t) => t,
        other => panic!("{}: expected transfers, got {other:?}", f.name),
    }
}

fn top_level_programs(tx: &SolanaTx) -> Vec<&str> {
    tx.instructions
        .iter()
        .filter(|ix| ix.inner_of.is_none())
        .map(|ix| ix.program_id.as_str())
        .collect()
}

#[test]
fn outbound_transfers_match_wormholescan() {
    for name in OUTBOUND {
        let f = fixture(name);
        let found = transfers(&f);
        assert_eq!(found.len(), 1, "{name}: transfers in the tx");
        let t = &found[0];

        let payload = &f.op["content"]["payload"];
        let std = &f.op["content"]["standarizedProperties"];
        let s = |v: &Value| v.as_str().unwrap_or_default().to_string();

        // identity: emitter chain / emitter address / sequence
        assert_eq!(t.message.key(), s(&f.op["id"]), "{name}: message key");
        assert_eq!(t.direction, Direction::Outbound, "{name}: direction");
        assert_eq!(t.observed_tx, f.tx.signature, "{name}: observed tx");
        assert_eq!(t.event_index, 0, "{name}: event index");
        assert_eq!(t.source.id, "1", "{name}: source chain");
        assert!(t.source_wallet.is_some(), "{name}: source wallet");

        // the transfer itself
        assert_eq!(
            t.destination.id,
            payload["toChain"].to_string(),
            "{name}: destination chain"
        );
        assert_eq!(
            t.amount_raw.as_ref().map(|a| a.to_string()),
            Some(s(&payload["amount"])),
            "{name}: amount"
        );
        let token = t.token.as_ref().unwrap_or_else(|| panic!("{name}: token"));
        assert_eq!(
            token.chain.id,
            payload["tokenChain"].to_string(),
            "{name}: token chain"
        );
        assert!(
            token.address.eq_ignore_ascii_case(&s(&std["tokenAddress"])),
            "{name}: token address {} vs {}",
            token.address,
            s(&std["tokenAddress"])
        );
        let wallet = t.destination_wallet.as_deref().unwrap_or_default();
        assert!(
            wallet.eq_ignore_ascii_case(&s(&std["toAddress"])),
            "{name}: destination wallet {wallet} vs {}",
            s(&std["toAddress"])
        );
    }
}

#[test]
fn routed_transfers_are_found_through_inner_calls() {
    for name in ROUTED {
        let f = fixture(name);
        // the point of these fixtures: no top-level Token Bridge instruction
        assert!(
            !top_level_programs(&f.tx).contains(&TOKEN_BRIDGE),
            "{name}: expected the Token Bridge only as an inner call"
        );
        assert!(
            f.tx.program_ids().contains(TOKEN_BRIDGE),
            "{name}: Token Bridge missing from the tx"
        );
        assert_eq!(transfers(&f).len(), 1, "{name}: transfers");
        // a transfer with payload (id 3), as WormholeScan decodes it
        assert_eq!(f.op["content"]["payload"]["payloadType"], 3, "{name}");
    }
}

#[test]
fn an_attest_call_is_rejected_as_not_a_transfer() {
    let f = fixture("admin_call");
    // payload id 2 is an attest (asset metadata) message
    assert_eq!(f.op["content"]["payload"]["payloadType"], 2);
    assert!(
        matches!(
            classify(&f),
            Classification::Rejected(Reject::NotTransfer(_))
        ),
        "{}: {:?}",
        f.name,
        classify(&f)
    );
}

/// Loads the captured `getAccountInfo` of the posted VAA a redeem was given, the way
/// the fetch step would: the registry says which account is needed, the fixture
/// stands in for the RPC.
fn with_vaa_account(f: &mut Fixture) {
    let needed = default_registry().required_accounts(&f.tx);
    assert_eq!(needed.len(), 1, "{}: accounts a redeem needs", f.name);

    let account = read(&format!("account_{}_vaa.json", f.name));
    let value = &account["value"];
    let data = STANDARD
        .decode(value["data"][0].as_str().expect("account data"))
        .expect("base64 account data");
    f.tx.accounts.insert(
        needed[0].clone(),
        AccountData {
            owner: value["owner"].as_str().expect("account owner").to_string(),
            data,
        },
    );
}

#[test]
fn inbound_redeems_match_wormholescan() {
    for name in INBOUND {
        let mut f = fixture(name);
        with_vaa_account(&mut f);
        let found = transfers(&f);
        assert_eq!(found.len(), 1, "{name}: transfers in the tx");
        let t = &found[0];

        let payload = &f.op["content"]["payload"];
        let std = &f.op["content"]["standarizedProperties"];
        let s = |v: &Value| v.as_str().unwrap_or_default().to_string();

        // the redeem decodes to the same message the send was indexed under
        assert_eq!(t.message.key(), s(&f.op["id"]), "{name}: message key");
        assert_eq!(t.direction, Direction::Inbound, "{name}: direction");
        assert_eq!(t.observed_tx, f.tx.signature, "{name}: observed tx");
        assert_eq!(t.event_index, 0, "{name}: event index");
        assert_eq!(
            t.source.id,
            f.op["emitterChain"].to_string(),
            "{name}: source chain"
        );
        assert_eq!(t.destination.id, "1", "{name}: destination chain");
        assert_eq!(t.source_wallet, None, "{name}: source wallet");

        assert_eq!(
            t.amount_raw.as_ref().map(|a| a.to_string()),
            Some(s(&payload["amount"])),
            "{name}: amount"
        );
        let token = t.token.as_ref().unwrap_or_else(|| panic!("{name}: token"));
        assert_eq!(
            token.chain.id,
            payload["tokenChain"].to_string(),
            "{name}: token chain"
        );
        assert!(
            token.address.eq_ignore_ascii_case(&s(&std["tokenAddress"])),
            "{name}: token address {} vs {}",
            token.address,
            s(&std["tokenAddress"])
        );
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(s(&std["toAddress"]).as_str()),
            "{name}: destination wallet"
        );
    }
}

#[test]
fn inbound_redeems_without_their_account_are_unsupported() {
    for name in INBOUND {
        let f = fixture(name);
        assert_eq!(
            f.op["targetChain"]["chainId"], 1,
            "{name}: redeems into Solana"
        );
        assert!(
            matches!(
                classify(&f),
                Classification::Rejected(Reject::Unsupported(_))
            ),
            "{name}: {:?}",
            classify(&f)
        );
    }
}

/// A7: the redeem becomes an inbound transfer the way `analyse_tx` builds it. The observed
/// Solana tx is the destination side, the matched WormholeScan operation names the origin
/// tx, and the origin tx is exactly what WormholeScan reports.
#[test]
fn inbound_redeems_become_completed_inbound_transfers() {
    for name in INBOUND {
        let mut f = fixture(name);
        with_vaa_account(&mut f);
        let found = transfers(&f);
        let snapshot = Snapshot::default();

        let mut t = detect::to_bridge_transfer(&found[0], None, &snapshot)
            .unwrap_or_else(|e| panic!("{name}: {e}"));

        assert_eq!(t.direction(), Direction::Inbound, "{name}");
        assert!(matches!(t.status, BridgeStatus::Completed), "{name}");
        assert_eq!(t.destination_chain, ChainId::Solana, "{name}");
        assert_eq!(
            t.destination_tx_hash.as_deref(),
            Some(f.tx.signature.as_str()),
            "{name}: destination tx"
        );
        assert_eq!(t.source_tx_hash, None, "{name}: unknown before enrichment");

        assert_eq!(
            detect::apply_wormholescan(&mut t, &f.op, &snapshot),
            Enrichment::Applied,
            "{name}"
        );
        let expected = f.op["sourceChain"]["transaction"]["txHash"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: no origin tx in the fixture"));
        assert_eq!(
            t.source_tx_hash.as_deref(),
            Some(expected),
            "{name}: origin tx"
        );
        assert!(matches!(t.status, BridgeStatus::Completed), "{name}");
        assert_eq!(
            t.destination_tx_hash.as_deref(),
            Some(f.tx.signature.as_str()),
            "{name}: destination tx kept"
        );
    }
}

// Status matrix (1d), redefined for chain-first detection.
//
// The status no longer starts from WormholeScan's answer. An adapter decodes the
// transfer from the Solana tx alone, so every outbound send starts `Pending` (the send
// is observed, its redeem is not) and enrichment or a watched EVM log can only raise
// it. Registry membership decides whether a chain reference resolves and whether an
// explorer URL exists, never the status, so each outbound row runs against both an
// unregistered and a registered destination.
//
// `Pending`   the send is observed but no redeem is known: no VAA, a VAA for another
//             message, or a matched VAA without a destination tx
// `Completed` the matched VAA and its destination tx are both known
//
// A Solana-inbound redeem is `Completed` at detection: the observed tx is the
// destination side and a decoded redeem has succeeded.

/// What WormholeScan returns for the observed Solana tx.
#[derive(Debug, Clone, Copy)]
enum Operation {
    /// WormholeScan does not know the tx (an empty response).
    Nothing,
    /// It names a destination tx but carries no VAA. Must not advance the transfer.
    DestinationTxWithoutVaa,
    /// A VAA for a different message (same emitter, another sequence). Ignored.
    OtherMessage,
    /// The VAA of this very message, with no destination tx.
    MatchingVaa,
    /// The captured `/operations` item for this fixture.
    Captured,
}

fn operation_for(f: &Fixture, kind: Operation) -> Value {
    match kind {
        Operation::Nothing => json!({}),
        Operation::DestinationTxWithoutVaa => json!({
            "targetChain": {"transaction": {"txHash": "0xmust-not-complete"}}
        }),
        Operation::OtherMessage => {
            // A real VAA captured for another transfer: same emitter, other sequence.
            let other = if f.name == OUTBOUND[0] {
                OUTBOUND[1]
            } else {
                OUTBOUND[0]
            };
            json!({
                "vaa": {"raw": fixture(other).op["vaa"]["raw"].clone()},
                "targetChain": {"transaction": {"txHash": "0xmust-not-complete"}}
            })
        }
        Operation::MatchingVaa => json!({"vaa": {"raw": f.op["vaa"]["raw"].clone()}}),
        Operation::Captured => f.op.clone(),
    }
}

fn status_name(status: &BridgeStatus) -> &'static str {
    match status {
        BridgeStatus::Detected => "Detected",
        BridgeStatus::Pending => "Pending",
        BridgeStatus::Completed => "Completed",
    }
}

/// A registry that knows Solana and `destination`, with active rows in both tables.
fn registry_with(destination: &RawChainRef) -> Snapshot {
    let chain = |id: i16, slug: &str| ChainRow {
        id,
        slug: slug.into(),
        name: slug.into(),
        kind: "other".into(),
        explorer_tx_url: Some(format!("https://{slug}.example/tx/{{hash}}")),
        status: RegistryStatus::Active,
    };
    let mapping = |protocol: &str, id: &str, chain_id: i16| MappingRow {
        protocol: protocol.into(),
        protocol_chain_id: id.into(),
        chain_id,
        status: RegistryStatus::Active,
    };
    Snapshot::build(
        vec![chain(1, "solana"), chain(2, "destination")],
        vec![
            mapping(NS_WORMHOLE, "1", 1),
            mapping(&destination.protocol, &destination.id, 2),
        ],
    )
}

#[derive(Debug, Clone, Copy)]
struct StatusCase {
    fixture: &'static str,
    operation: Operation,
    expected: &'static str,
}

const OUTBOUND_MATRIX: [StatusCase; 7] = [
    StatusCase {
        fixture: "completed_outbound",
        operation: Operation::Nothing,
        expected: "Pending",
    },
    StatusCase {
        fixture: "completed_outbound",
        operation: Operation::DestinationTxWithoutVaa,
        expected: "Pending",
    },
    StatusCase {
        fixture: "completed_outbound",
        operation: Operation::OtherMessage,
        expected: "Pending",
    },
    StatusCase {
        fixture: "completed_outbound",
        operation: Operation::MatchingVaa,
        expected: "Pending",
    },
    StatusCase {
        fixture: "completed_outbound",
        operation: Operation::Captured,
        expected: "Completed",
    },
    StatusCase {
        fixture: "payload3_executor",
        operation: Operation::Captured,
        expected: "Completed",
    },
    StatusCase {
        fixture: "payload3_plain",
        operation: Operation::Captured,
        expected: "Pending",
    },
];

#[test]
fn status_matrix_for_chain_first_detection() {
    for case in OUTBOUND_MATRIX {
        let f = fixture(case.fixture);
        let found = transfers(&f);
        assert_eq!(found.len(), 1, "{}: transfers in the tx", case.fixture);
        let decoded = &found[0];
        assert_eq!(
            decoded.direction,
            Direction::Outbound,
            "{}: direction",
            case.fixture
        );

        for registry in ["unregistered", "registered"] {
            let snapshot = match registry {
                "registered" => registry_with(&decoded.destination),
                _ => Snapshot::default(),
            };
            let mut t = detect::to_bridge_transfer(decoded, None, &snapshot)
                .unwrap_or_else(|e| panic!("{}: {e}", case.fixture));

            // The chain alone is enough to persist the transfer.
            assert_eq!(
                status_name(&t.status),
                "Pending",
                "{}: before enrichment",
                case.fixture
            );

            detect::apply_wormholescan(&mut t, &operation_for(&f, case.operation), &snapshot);

            assert_eq!(
                status_name(&t.status),
                case.expected,
                "{} / {:?} / {registry} destination: status",
                case.fixture,
                case.operation
            );
            assert_eq!(
                t.destination_tx_hash.is_some(),
                case.expected == "Completed",
                "{} / {:?} / {registry} destination: destination tx",
                case.fixture,
                case.operation
            );
        }
    }
}

#[test]
fn status_matrix_for_inbound_redeems() {
    for name in INBOUND {
        let mut f = fixture(name);
        with_vaa_account(&mut f);
        let found = transfers(&f);
        let decoded = &found[0];
        assert_eq!(decoded.direction, Direction::Inbound, "{name}: direction");

        for registry in ["unregistered", "registered"] {
            let snapshot = match registry {
                "registered" => registry_with(&decoded.destination),
                _ => Snapshot::default(),
            };
            let mut t = detect::to_bridge_transfer(decoded, None, &snapshot)
                .unwrap_or_else(|e| panic!("{name}: {e}"));

            // The redeem itself is the destination tx, and it succeeded.
            assert_eq!(
                status_name(&t.status),
                "Completed",
                "{name} / {registry} destination: at detection"
            );
            assert_eq!(t.source_tx_hash, None, "{name}: origin tx unknown");

            // A matched operation fills the origin tx and keeps the status.
            detect::apply_wormholescan(&mut t, &f.op, &snapshot);
            assert_eq!(
                status_name(&t.status),
                "Completed",
                "{name}: after a matched operation"
            );
            assert!(t.source_tx_hash.is_some(), "{name}: origin tx filled");
        }

        // A VAA for another message changes neither the status nor the origin tx.
        let mut t = detect::to_bridge_transfer(decoded, None, &Snapshot::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let other = if name == INBOUND[0] {
            INBOUND[1]
        } else {
            INBOUND[0]
        };
        detect::apply_wormholescan(&mut t, &fixture(other).op, &Snapshot::default());
        assert_eq!(
            status_name(&t.status),
            "Completed",
            "{name}: another message is ignored"
        );
        assert_eq!(t.source_tx_hash, None, "{name}: origin tx still unknown");
    }
}
