//! Runs the pure decoders over real provider responses captured with
//! `scripts/capture_fixtures.sh`. With no fixtures present every test passes
//! vacuously, so CI stays green until you add some.
//!
//! Fixture files (in tests/fixtures/):
//!   helius_<name>.json    `result` of getTransaction (jsonParsed)
//!   wormhole_<name>.json  first element of WormholeScan `operations`

use bridge::normaliser;
use bridge::vaa::decode::decode_vaa;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures(prefix: &str) -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let Ok(entries) = fs::read_dir(&dir) else {
        return vec![];
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(prefix) && n.ends_with(".json"))
        })
        .collect();
    files.sort();
    files
}

fn name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

fn load(path: &Path) -> Value {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", name(path)));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: invalid json: {e}", name(path)))
}

/// Where a destination tx hash can live, depending on the endpoint.
const DESTINATION_PATHS: [&str; 3] = [
    "/targetChain/transaction/txHash", // /operations
    "/globalTx/destinationTx/txHash",  // what analyse_tx reads today
    "/destinationTx/txHash",           // /global-tx
];

fn destination_path(raw: &Value) -> Option<&'static str> {
    DESTINATION_PATHS
        .iter()
        .copied()
        .find(|p| raw.pointer(p).and_then(Value::as_str).is_some())
}

#[test]
fn helius_fixtures_normalise() {
    let files = fixtures("helius_");
    if files.is_empty() {
        eprintln!("no helius_*.json fixtures yet, run scripts/capture_fixtures.sh");
    }
    for path in files {
        let raw = load(&path);
        let tx = normaliser::solana::normalise(raw, "fixture")
            .unwrap_or_else(|e| panic!("{}: normalise failed: {e}", name(&path)));
        assert!(tx.slot > 0, "{}: slot", name(&path));
        assert!(tx.signer.is_some(), "{}: signer", name(&path));
    }
}

#[test]
fn wormhole_fixtures_are_self_consistent() {
    let files = fixtures("wormhole_");
    if files.is_empty() {
        eprintln!("no wormhole_*.json fixtures yet, run scripts/capture_fixtures.sh");
    }
    for path in files {
        let n = name(&path);
        let raw = load(&path);
        let Some(event) = normaliser::wormhole::normalise(raw.clone())
            .unwrap_or_else(|e| panic!("{n}: normalise failed: {e}"))
        else {
            panic!("{n}: not a Wormhole operation");
        };

        // id is "<emitter chain>/<emitter address hex>/<sequence>"
        let id = raw["id"]
            .as_str()
            .unwrap_or_else(|| panic!("{n}: missing id"));
        let parts: Vec<&str> = id.split('/').collect();
        assert_eq!(parts.len(), 3, "{n}: unexpected id format {id}");
        assert_eq!(event.emitter_chain, parts[0], "{n}: emitter chain");
        assert_eq!(event.emitter_address, parts[1], "{n}: emitter address");
        assert_eq!(event.sequence.to_string(), parts[2], "{n}: sequence");

        // decode_vaa only understands Token Bridge payloads (ids 1/3 are also
        // used by other protocols, e.g. the NEAR omni bridge), so skip VAAs
        // WormholeScan doesn't label as the Token Bridge.
        let is_token_bridge = raw
            .pointer("/content/standarizedProperties/appIds")
            .and_then(Value::as_array)
            .is_some_and(|ids| ids.iter().any(|i| i == "PORTAL_TOKEN_BRIDGE"));

        // the raw VAA header must agree with the operation metadata
        if !is_token_bridge {
            eprintln!("{n}: not a Token Bridge operation, skipping VAA decode");
        } else if let Some(vaa) = raw.pointer("/vaa/raw").and_then(Value::as_str) {
            let decoded = decode_vaa(vaa).unwrap_or_else(|e| panic!("{n}: decode_vaa: {e}"));
            assert_eq!(
                decoded.header.emitter_chain.to_string(),
                event.emitter_chain,
                "{n}: VAA emitter chain"
            );
            assert_eq!(
                decoded.header.emitter_address, event.emitter_address,
                "{n}: VAA emitter address"
            );
            assert_eq!(decoded.header.sequence, event.sequence, "{n}: VAA sequence");
        }
    }
}

/// Fixtures named wormhole_completed_*.json must carry a destination tx hash
/// at one of the known paths. Run with `--nocapture` to see which path matched:
/// that is the shape Phase 1 has to read.
#[test]
fn completed_fixtures_carry_a_destination_tx() {
    for path in fixtures("wormhole_completed") {
        let raw = load(&path);
        let found = destination_path(&raw);
        eprintln!("{}: destination tx path = {:?}", name(&path), found);
        assert!(
            found.is_some(),
            "{}: no destination tx hash at any of {:?}. Is this tx really completed? Top-level keys: {:?}",
            name(&path),
            DESTINATION_PATHS,
            raw.as_object().map(|o| o.keys().collect::<Vec<_>>())
        );
    }
}
