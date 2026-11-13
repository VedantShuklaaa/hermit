//! Chain-first detection. The adapters decode a transfer from Solana data alone, this
//! module turns it into a [`BridgeTransfer`] and WormholeScan data is layered on top
//! afterwards. An outbound send starts `Pending` until its redeem is seen; an inbound
//! redeem is `Completed` because the observed Solana tx is its destination.
//! Enrichment can raise the status but never gates persistence, and it is only applied
//! when the VAA matches the message decoded on chain.

use crate::chains::{NS_WORMHOLE, RawChainRef, Snapshot};
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use crate::error::AppError;
use crate::protocols::{
    Classification, DetectedTransfer, Direction, MessageId, NFT_BRIDGE_PROGRAM, SolanaTx,
    compose::compose, registry::ProtocolRegistry,
};
use crate::services::token_metadata::TokenMetadata;
use crate::vaa::decode::{decode_vaa, format_amount};
use serde_json::Value;

/// Runs every adapter that matches the tx and merges the result, one transfer per
/// message identity. Rejections are logged with their kind and reason.
pub fn detect(registry: &ProtocolRegistry, tx: &SolanaTx) -> Vec<DetectedTransfer> {
    let mut found = Vec::new();
    for (protocol, classification) in registry.classify(tx) {
        match classification {
            Classification::Transfers(transfers) => found.extend(transfers),
            Classification::Rejected(reject) => tracing::info!(
                protocol = protocol.as_str(),
                kind = reject.kind(),
                reason = reject.reason(),
                signature = %tx.signature,
                "not indexed as a transfer"
            ),
        }
    }
    compose(found)
}

/// The token's chain (Wormhole ID) and address, when the transfer carries a token.
pub fn token_lookup(d: &DetectedTransfer) -> Option<(u16, String)> {
    let token = d.token.as_ref()?;
    Some((wormhole_id(&token.chain).ok()?, token.address.clone()))
}

fn wormhole_id(chain: &RawChainRef) -> Result<u16, AppError> {
    if chain.protocol != NS_WORMHOLE {
        return Err(AppError::Normalisation(format!(
            "chain reference from {:?}, expected {NS_WORMHOLE}",
            chain.protocol
        )));
    }
    chain
        .id
        .parse()
        .map_err(|_| AppError::Normalisation(format!("bad wormhole chain id {:?}", chain.id)))
}

/// A decoded transfer as the API's `BridgeTransfer`.
///
/// An outbound send starts as `Pending`: the chain shows the send but no redeem has been
/// observed yet, and `Pending` is exactly that. An inbound redeem is `Completed` at
/// detection: the observed Solana tx succeeded (adapters reject a failed tx) and it is
/// the destination side of the transfer, so there is nothing left to wait for. Its source
/// tx is on the origin chain and unknown until enrichment or the EVM log watcher learns it.
pub fn to_bridge_transfer(
    d: &DetectedTransfer,
    metadata: Option<TokenMetadata>,
    chains: &Snapshot,
) -> Result<BridgeTransfer, AppError> {
    let MessageId::Wormhole {
        emitter_chain,
        emitter_address,
        sequence,
    } = &d.message;

    let source_id = wormhole_id(&d.source)?;
    let destination_id = wormhole_id(&d.destination)?;

    let raw_amount: Option<u128> = d.amount_raw.as_ref().and_then(|a| a.as_str().parse().ok());
    let decimals = metadata.as_ref().and_then(|m| m.decimals);
    let inbound = d.direction == Direction::Inbound;

    Ok(BridgeTransfer {
        program: d.program.clone(),
        source_chain: ChainId::from_wormhole_id(source_id),
        source_tx_hash: (!inbound).then(|| d.observed_tx.clone()),
        source_wallet: d.source_wallet.clone(),
        source_explorer_url: (!inbound)
            .then(|| chains.explorer_tx_url_for(&d.source, &d.observed_tx))
            .flatten(),
        destination_chain: ChainId::from_wormhole_id(destination_id),
        destination_wallet: d.destination_wallet.clone(),
        destination_tx_hash: inbound.then(|| d.observed_tx.clone()),
        destination_explorer_url: inbound
            .then(|| chains.explorer_tx_url_for(&d.destination, &d.observed_tx))
            .flatten(),
        token: d.token.as_ref().map(|t| t.address.clone()),
        // The payload's own symbol is chain data; token metadata is only a fallback.
        token_symbol: d
            .token_symbol
            .clone()
            .or_else(|| metadata.and_then(|m| m.symbol)),
        amount: d.amount_raw.as_ref().map(|a| a.to_string()),
        amount_formatted: raw_amount.map(|r| format_amount(r, decimals)),
        message_id: BridgeMessageId {
            emitter_chain: *emitter_chain,
            emitter_address: emitter_address.clone(),
            sequence: *sequence,
        },
        status: if inbound {
            BridgeStatus::Completed
        } else {
            BridgeStatus::Pending
        },
    })
}

/// Re-reads a transfer the WormholeScan-driven path built for a Solana redeem. That path
/// sets `source_tx_hash` to the observed tx, but for a transfer that lands on Solana the
/// observed tx is the destination side. Only used for a successful Solana tx, which is
/// what makes the transfer `Completed`.
pub fn as_inbound_observation(transfer: &mut BridgeTransfer, observed_tx: &str, chains: &Snapshot) {
    if transfer.direction() != Direction::Inbound {
        return;
    }
    transfer.source_tx_hash = None;
    transfer.source_explorer_url = None;
    transfer.destination_tx_hash = Some(observed_tx.to_string());
    transfer.destination_explorer_url = chains.explorer_tx_url_for(
        &RawChainRef::wormhole(transfer.destination_chain.wormhole_id()),
        observed_tx,
    );
    transfer.status = BridgeStatus::Completed;
}

/// The origin chain's tx inside a WormholeScan `/operations` item.
pub fn source_tx_from_operation(raw: &Value) -> Option<String> {
    raw.get("sourceChain")?
        .get("transaction")?
        .get("txHash")?
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enrichment {
    /// The VAA matches the decoded message: status raised, destination tx filled if known.
    Applied,
    /// WormholeScan has no usable VAA yet. The transfer is unchanged.
    NoVaa,
    /// The VAA describes a different message. Ignored, the chain wins.
    Mismatch,
}

/// Layers a WormholeScan `/operations` response onto a detected transfer.
///
/// `Pending` means WormholeScan has the VAA, `Completed` means it also knows the
/// destination tx. Anything that does not match the on-chain message changes nothing.
pub fn apply_wormholescan(
    transfer: &mut BridgeTransfer,
    wh_raw: &Value,
    chains: &Snapshot,
) -> Enrichment {
    let Some(vaa) = wh_raw
        .get("vaa")
        .and_then(|v| v.get("raw"))
        .and_then(Value::as_str)
    else {
        return Enrichment::NoVaa;
    };
    let Ok(decoded) = decode_vaa(vaa) else {
        return Enrichment::NoVaa;
    };

    let id = &transfer.message_id;
    if decoded.header.emitter_chain != id.emitter_chain
        || decoded.header.emitter_address != id.emitter_address
        || decoded.header.sequence != id.sequence
    {
        return Enrichment::Mismatch;
    }

    // Only raise: a transfer that is already `Pending` or `Completed` keeps its status.
    if matches!(transfer.status, BridgeStatus::Detected) {
        transfer.status = BridgeStatus::Pending;
    }

    if let Some(hash) = destination_tx_from_operation(wh_raw) {
        apply_destination_tx(transfer, &hash, chains);
    }

    // A redeem is stored without its origin tx. The matched operation names it.
    if let Some(hash) = source_tx_from_operation(wh_raw) {
        apply_source_tx(transfer, &hash, chains);
    }
    Enrichment::Applied
}

/// Fills the origin tx of an inbound redeem, which is stored without one. Only
/// `source_tx_hash` and `source_explorer_url` are touched, and only while the origin tx is
/// still unknown; the status never moves.
pub fn apply_source_tx(transfer: &mut BridgeTransfer, hash: &str, chains: &Snapshot) -> bool {
    let hash = hash.trim();
    if hash.is_empty()
        || transfer.direction() != Direction::Inbound
        || transfer.source_tx_hash.is_some()
    {
        return false;
    }
    let hash = normalise_source_tx_hash(transfer.source_chain.wormhole_id(), hash, chains);
    transfer.source_explorer_url = chains.explorer_tx_url_for(
        &RawChainRef::wormhole(transfer.source_chain.wormhole_id()),
        &hash,
    );
    transfer.source_tx_hash = Some(hash);
    true
}

/// `/vaas` returns a bare hex hash for EVM chains while the rest of the app stores the
/// explorer's `0x` form. Other chains already use their own encoding.
fn normalise_source_tx_hash(wormhole_id: u16, hash: &str, chains: &Snapshot) -> String {
    let evm = chains
        .resolve(&RawChainRef::wormhole(wormhole_id))
        .key()
        .and_then(|key| chains.chain(key))
        .is_some_and(|chain| chain.kind == "evm");
    if evm && !hash.starts_with("0x") && !hash.starts_with("0X") {
        format!("0x{hash}")
    } else {
        hash.to_string()
    }
}

/// Destination tx inside a WormholeScan `/operations` item. Paths are tried in order;
/// the first non-empty string wins.
pub fn destination_tx_from_operation(raw: &Value) -> Option<String> {
    const PATHS: [&[&str]; 3] = [
        &["targetChain", "transaction", "txHash"],
        &["globalTx", "destinationTx", "txHash"],
        &["destinationTx", "txHash"],
    ];
    PATHS.iter().find_map(|path| {
        let mut cur = raw;
        for key in *path {
            cur = cur.get(*key)?;
        }
        cur.as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
    })
}

/// Completes a `Pending` transfer once its destination tx is known. Never touches
/// `Detected` (no matched VAA) and never rewrites a `Completed` transfer. An outbound
/// NFT Bridge transfer records the tx but stays `Pending`: there is no watched chain
/// event confirming its destination.
pub fn apply_destination_tx(transfer: &mut BridgeTransfer, hash: &str, chains: &Snapshot) -> bool {
    let hash = hash.trim();
    if hash.is_empty() || !matches!(transfer.status, BridgeStatus::Pending) {
        return false;
    }
    transfer.destination_explorer_url = chains.explorer_tx_url_for(
        &RawChainRef::wormhole(transfer.destination_chain.wormhole_id()),
        hash,
    );

    transfer.destination_tx_hash = Some(hash.to_string());
    // An outbound NFT Bridge transfer has no chain event in the watcher set: the
    // destination is a call to the NFT bridge contract, not `TransferRedeemed` or
    // `LogMessagePublished`. WormholeScan can name that tx, but without a watched
    // chain event the row stays Pending. Known limitation, documented in REFERENCE.
    let unconfirmed_nft =
        transfer.program == NFT_BRIDGE_PROGRAM && transfer.direction() == Direction::Outbound;
    if !unconfirmed_nft {
        transfer.status = BridgeStatus::Completed;
    }
    true
}

/// Completes an outbound transfer from chain data (a `TransferRedeemed` log), which is
/// stronger evidence than a matched VAA. Unlike [`apply_destination_tx`] this also
/// accepts a `Detected` row. It never overwrites a known destination tx and never
/// rewrites a `Completed` row.
pub fn apply_chain_destination_tx(
    transfer: &mut BridgeTransfer,
    hash: &str,
    chains: &Snapshot,
) -> bool {
    let hash = hash.trim();
    if hash.is_empty()
        || transfer.direction() != Direction::Outbound
        || transfer.destination_tx_hash.is_some()
        || matches!(transfer.status, BridgeStatus::Completed)
    {
        return false;
    }
    transfer.destination_explorer_url = chains.explorer_tx_url_for(
        &RawChainRef::wormhole(transfer.destination_chain.wormhole_id()),
        hash,
    );
    transfer.destination_tx_hash = Some(hash.to_string());
    transfer.status = BridgeStatus::Completed;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::{Direction, TokenRef, amount::RawAmount};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;

    const EMITTER_HEX: &str = "0909090909090909090909090909090909090909090909090909090909090909";

    fn detected() -> DetectedTransfer {
        DetectedTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: EMITTER_HEX.into(),
                sequence: 42,
            },
            direction: Direction::Outbound,
            observed_tx: "sig".into(),
            event_index: 0,
            source: RawChainRef::wormhole(1),
            destination: RawChainRef::wormhole(2),
            source_wallet: Some("Wallet".into()),
            destination_wallet: Some("0xabc".into()),
            token: Some(TokenRef {
                chain: RawChainRef::wormhole(1),
                address: "TokenMint".into(),
            }),
            token_symbol: None,
            amount_raw: RawAmount::parse("5000000"),
            via: None,
        }
    }

    /// A VAA with no signatures and an empty payload: header and the 51 byte body prefix.
    fn vaa_b64(chain: u16, emitter: [u8; 32], sequence: u64) -> String {
        let mut bytes = vec![1u8, 0, 0, 0, 0, 0]; // version, guardian set, 0 signatures
        bytes.extend_from_slice(&[0u8; 8]); // timestamp, nonce
        bytes.extend_from_slice(&chain.to_be_bytes());
        bytes.extend_from_slice(&emitter);
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.push(1); // consistency level
        STANDARD.encode(bytes)
    }

    fn transfer() -> BridgeTransfer {
        to_bridge_transfer(&detected(), None, &snap()).unwrap()
    }

    #[test]
    fn a_detected_transfer_maps_to_the_api_shape_as_pending() {
        let t = to_bridge_transfer(
            &detected(),
            Some(TokenMetadata {
                symbol: Some("USDC".into()),
                decimals: Some(6),
            }),
            &snap(),
        )
        .unwrap();

        assert!(matches!(t.status, BridgeStatus::Pending));
        assert_eq!(t.source_chain, ChainId::Solana);
        assert_eq!(t.destination_chain, ChainId::Ethereum);
        assert_eq!(t.source_tx_hash.as_deref(), Some("sig"));
        assert_eq!(t.source_wallet.as_deref(), Some("Wallet"));
        assert_eq!(t.destination_wallet.as_deref(), Some("0xabc"));
        assert_eq!(t.token.as_deref(), Some("TokenMint"));
        assert_eq!(t.token_symbol.as_deref(), Some("USDC"));
        assert_eq!(t.amount.as_deref(), Some("5000000"));
        assert_eq!(t.amount_formatted.as_deref(), Some("5"));
        assert_eq!(t.message_id.emitter_address, EMITTER_HEX);
        assert_eq!(t.message_id.sequence, 42);
        assert!(t.destination_tx_hash.is_none());
    }

    #[test]
    fn missing_metadata_does_not_block_the_transfer() {
        let t = transfer();
        assert!(t.token_symbol.is_none());
        assert!(t.amount_formatted.is_some());
    }

    #[test]
    fn a_chain_reference_from_another_namespace_is_an_error_not_a_guess() {
        let mut d = detected();
        d.destination = RawChainRef::new("cctp", 0);
        assert!(to_bridge_transfer(&d, None, &snap()).is_err());
    }

    #[test]
    fn token_lookup_reads_the_token_chain_and_address() {
        assert_eq!(
            token_lookup(&detected()),
            Some((1, "TokenMint".to_string()))
        );
        let mut d = detected();
        d.token = None;
        assert_eq!(token_lookup(&d), None);
    }

    #[test]
    fn no_vaa_leaves_the_transfer_pending() {
        for raw in [
            json!({}),
            json!({"vaa": {}}),
            json!({"vaa": {"raw": "!!not base64"}}),
        ] {
            let mut t = transfer();
            assert_eq!(apply_wormholescan(&mut t, &raw, &snap()), Enrichment::NoVaa);
            assert!(matches!(t.status, BridgeStatus::Pending));
        }
    }

    #[test]
    fn a_matching_vaa_raises_the_status_to_pending() {
        let raw = json!({"vaa": {"raw": vaa_b64(1, [9; 32], 42)}});
        let mut t = transfer();
        assert_eq!(
            apply_wormholescan(&mut t, &raw, &snap()),
            Enrichment::Applied
        );
        assert!(matches!(t.status, BridgeStatus::Pending));
        assert!(t.destination_tx_hash.is_none());
    }

    #[test]
    fn a_known_destination_tx_completes_the_transfer() {
        let raw = json!({
            "vaa": {"raw": vaa_b64(1, [9; 32], 42)},
            "globalTx": {"destinationTx": {"txHash": "0xdest"}},
        });
        let mut t = transfer();
        assert_eq!(
            apply_wormholescan(&mut t, &raw, &snap()),
            Enrichment::Applied
        );
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xdest"));
        assert!(t.destination_explorer_url.is_some());
    }

    #[test]
    fn a_vaa_for_another_message_is_ignored() {
        for (chain, emitter, sequence) in [(1, [9; 32], 43), (1, [8; 32], 42), (2, [9; 32], 42)] {
            let raw = json!({
                "vaa": {"raw": vaa_b64(chain, emitter, sequence)},
                "globalTx": {"destinationTx": {"txHash": "0xdest"}},
            });
            let mut t = transfer();
            assert_eq!(
                apply_wormholescan(&mut t, &raw, &snap()),
                Enrichment::Mismatch
            );
            assert!(matches!(t.status, BridgeStatus::Pending));
            assert!(t.destination_tx_hash.is_none());
        }
    }

    #[test]
    fn destination_tx_is_read_from_target_chain() {
        let raw = json!({"targetChain": {"transaction": {"txHash": "0xtarget"}}});
        assert_eq!(
            destination_tx_from_operation(&raw).as_deref(),
            Some("0xtarget")
        );
    }

    #[test]
    fn target_chain_wins_over_legacy_paths() {
        let raw = json!({
            "targetChain": {"transaction": {"txHash": "0xtarget"}},
            "globalTx": {"destinationTx": {"txHash": "0xlegacy"}},
        });
        assert_eq!(
            destination_tx_from_operation(&raw).as_deref(),
            Some("0xtarget")
        );
    }

    #[test]
    fn empty_or_absent_destination_tx_is_none() {
        for raw in [
            json!({}),
            json!({"targetChain": {}}),
            json!({"targetChain": {"transaction": {"txHash": ""}}}),
        ] {
            assert_eq!(destination_tx_from_operation(&raw), None);
        }
    }

    #[test]
    fn an_operation_with_target_chain_completes_the_transfer() {
        let raw = json!({
            "vaa": {"raw": vaa_b64(1, [9; 32], 42)},
            "targetChain": {"transaction": {"txHash": "0xdest"}},
        });
        let mut t = transfer();
        assert_eq!(
            apply_wormholescan(&mut t, &raw, &snap()),
            Enrichment::Applied
        );
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xdest"));
    }

    #[test]
    fn apply_destination_tx_completes_only_a_pending_transfer() {
        let mut detected_only = transfer();
        detected_only.status = BridgeStatus::Detected;
        assert!(!apply_destination_tx(&mut detected_only, "0xdest", &snap()));
        assert!(matches!(detected_only.status, BridgeStatus::Detected));

        let mut pending = transfer();
        pending.status = BridgeStatus::Pending;
        assert!(apply_destination_tx(&mut pending, "0xdest", &snap()));
        assert!(matches!(pending.status, BridgeStatus::Completed));
        assert_eq!(pending.destination_tx_hash.as_deref(), Some("0xdest"));
    }

    #[test]
    fn an_outbound_nft_records_the_hint_but_stays_pending() {
        let mut t = transfer();
        t.program = crate::protocols::NFT_BRIDGE_PROGRAM.to_string();
        t.status = BridgeStatus::Pending;

        assert!(apply_destination_tx(&mut t, "0xdest", &snap()));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xdest"));
        assert!(
            matches!(t.status, BridgeStatus::Pending),
            "no watched chain event confirms an NFT destination"
        );
    }

    #[test]
    fn chain_destination_tx_completes_a_detected_or_pending_transfer() {
        for status in [BridgeStatus::Detected, BridgeStatus::Pending] {
            let mut t = transfer();
            t.status = status;
            assert!(apply_chain_destination_tx(&mut t, "0xdest", &snap()));
            assert!(matches!(t.status, BridgeStatus::Completed));
            assert_eq!(t.destination_tx_hash.as_deref(), Some("0xdest"));
        }
    }

    #[test]
    fn chain_destination_tx_never_rewrites_a_known_outcome() {
        let mut known = transfer();
        known.destination_tx_hash = Some("0xfirst".into());
        assert!(!apply_chain_destination_tx(&mut known, "0xsecond", &snap()));
        assert_eq!(known.destination_tx_hash.as_deref(), Some("0xfirst"));

        let mut completed = transfer();
        completed.status = BridgeStatus::Completed;
        assert!(!apply_chain_destination_tx(
            &mut completed,
            "0xdest",
            &snap()
        ));
        assert!(completed.destination_tx_hash.is_none());

        let mut inbound = inbound_transfer();
        assert!(!apply_chain_destination_tx(&mut inbound, "0xdest", &snap()));
        assert_eq!(inbound.destination_tx_hash.as_deref(), Some("redeem"));
    }

    #[test]
    fn a_completed_transfer_is_never_rewritten() {
        let mut t = transfer();
        t.status = BridgeStatus::Pending;
        assert!(apply_destination_tx(&mut t, "0xfirst", &snap()));
        assert!(!apply_destination_tx(&mut t, "0xsecond", &snap()));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xfirst"));
    }

    #[test]
    fn a_blank_hash_changes_nothing() {
        let mut t = transfer();
        t.status = BridgeStatus::Pending;
        assert!(!apply_destination_tx(&mut t, "  ", &snap()));
        assert!(matches!(t.status, BridgeStatus::Pending));
    }

    #[test]
    fn a_matching_vaa_never_lowers_a_higher_status() {
        let raw = json!({"vaa": {"raw": vaa_b64(1, [9; 32], 42)}});
        let mut t = transfer();
        t.status = BridgeStatus::Completed;
        assert_eq!(
            apply_wormholescan(&mut t, &raw, &snap()),
            Enrichment::Applied
        );
        assert!(matches!(t.status, BridgeStatus::Completed));
    }

    use crate::chains::{ChainRow, MappingRow, RegistryStatus};

    fn snap() -> Snapshot {
        snap_with_kinds("other", "other")
    }

    /// Chain 1 is Solana, chain 2 is an EVM chain, as the seeded registry has them.
    fn evm_snap() -> Snapshot {
        snap_with_kinds("solana", "evm")
    }

    fn snap_with_kinds(solana_kind: &str, ethereum_kind: &str) -> Snapshot {
        let chain = |id: i16, slug: &str, kind: &str| ChainRow {
            id,
            slug: slug.into(),
            name: slug.into(),
            kind: kind.into(),
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
                chain(1, "solana", solana_kind),
                chain(2, "ethereum", ethereum_kind),
            ],
            vec![mapping("1", 1), mapping("2", 2)],
        )
    }

    #[test]
    fn explorer_urls_come_from_the_registry() {
        let t = transfer();
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://solana.example/tx/sig")
        );

        let mut pending = transfer();
        pending.status = BridgeStatus::Pending;
        assert!(apply_destination_tx(&mut pending, "0xdest", &snap()));
        assert_eq!(
            pending.destination_explorer_url.as_deref(),
            Some("https://ethereum.example/tx/0xdest")
        );
    }

    #[test]
    fn a_chain_the_registry_does_not_know_has_no_explorer_url_but_still_works() {
        let mut d = detected();
        d.destination = RawChainRef::wormhole(9999);
        let mut t = to_bridge_transfer(&d, None, &snap()).unwrap();
        t.status = BridgeStatus::Pending;
        assert!(apply_destination_tx(&mut t, "0xdest", &snap()));
        assert!(t.destination_explorer_url.is_none());
        assert!(matches!(t.status, BridgeStatus::Completed));
    }

    fn inbound() -> DetectedTransfer {
        DetectedTransfer {
            direction: Direction::Inbound,
            observed_tx: "redeem".into(),
            source: RawChainRef::wormhole(2),
            destination: RawChainRef::wormhole(1),
            source_wallet: None,
            destination_wallet: Some("SolWallet".into()),
            message: MessageId::Wormhole {
                emitter_chain: 2,
                emitter_address: EMITTER_HEX.into(),
                sequence: 42,
            },
            ..detected()
        }
    }

    #[test]
    fn an_inbound_redeem_is_completed_with_the_observed_tx_as_destination() {
        let t = to_bridge_transfer(&inbound(), None, &snap()).unwrap();

        assert_eq!(t.direction(), Direction::Inbound);
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.source_chain, ChainId::Ethereum);
        assert_eq!(t.destination_chain, ChainId::Solana);
        assert_eq!(t.source_tx_hash, None);
        assert_eq!(t.source_explorer_url, None);
        assert_eq!(t.source_wallet, None);
        assert_eq!(t.destination_tx_hash.as_deref(), Some("redeem"));
        assert_eq!(t.destination_wallet.as_deref(), Some("SolWallet"));
    }

    fn inbound_transfer() -> BridgeTransfer {
        to_bridge_transfer(&inbound(), None, &snap()).unwrap()
    }

    fn operation_with_source(sequence: u64, source_tx: &str) -> Value {
        json!({
            "vaa": {"raw": vaa_b64(2, [9; 32], sequence)},
            "sourceChain": {"transaction": {"txHash": source_tx}},
        })
    }

    #[test]
    fn a_matching_operation_fills_the_origin_tx_of_a_redeem() {
        let mut t = inbound_transfer();
        let op = operation_with_source(42, "0xorigin");

        assert_eq!(
            apply_wormholescan(&mut t, &op, &snap()),
            Enrichment::Applied
        );
        assert_eq!(t.source_tx_hash.as_deref(), Some("0xorigin"));
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("redeem"));
    }

    #[test]
    fn an_operation_for_another_message_does_not_fill_the_origin_tx() {
        let mut t = inbound_transfer();
        let op = operation_with_source(43, "0xorigin");

        assert_eq!(
            apply_wormholescan(&mut t, &op, &snap()),
            Enrichment::Mismatch
        );
        assert_eq!(t.source_tx_hash, None);
    }

    #[test]
    fn an_origin_tx_that_is_already_known_is_not_overwritten() {
        let mut t = inbound_transfer();
        t.source_tx_hash = Some("0xfirst".into());
        let op = operation_with_source(42, "0xother");

        apply_wormholescan(&mut t, &op, &snap());
        assert_eq!(t.source_tx_hash.as_deref(), Some("0xfirst"));
    }

    #[test]
    fn an_outbound_transfer_never_takes_an_origin_tx_from_the_operation() {
        let mut t = transfer();
        let op = json!({
            "vaa": {"raw": vaa_b64(1, [9; 32], 42)},
            "sourceChain": {"transaction": {"txHash": "other"}},
        });

        apply_wormholescan(&mut t, &op, &snap());
        assert_eq!(t.source_tx_hash.as_deref(), Some("sig"));
    }

    #[test]
    fn apply_source_tx_fills_only_a_missing_inbound_origin() {
        let mut t = inbound_transfer();
        assert!(apply_source_tx(&mut t, "origin", &snap()));
        assert_eq!(t.source_tx_hash.as_deref(), Some("origin"));
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://ethereum.example/tx/origin")
        );
        assert!(matches!(t.status, BridgeStatus::Completed));

        assert!(!apply_source_tx(&mut t, "other", &snap()));
        assert_eq!(t.source_tx_hash.as_deref(), Some("origin"));

        let mut outbound = transfer();
        assert!(!apply_source_tx(&mut outbound, "other", &snap()));
        assert_eq!(outbound.source_tx_hash.as_deref(), Some("sig"));

        let mut blank = inbound_transfer();
        assert!(!apply_source_tx(&mut blank, "  ", &snap()));
        assert_eq!(blank.source_tx_hash, None);
    }

    #[test]
    fn an_evm_origin_hash_from_the_vaa_gets_the_0x_prefix() {
        let mut t = inbound_transfer();
        assert!(apply_source_tx(&mut t, "715b25c9bba7", &evm_snap()));
        assert_eq!(t.source_tx_hash.as_deref(), Some("0x715b25c9bba7"));
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://ethereum.example/tx/0x715b25c9bba7")
        );
    }

    #[test]
    fn source_hash_normalisation_follows_the_registry_kind() {
        assert_eq!(normalise_source_tx_hash(2, "abc", &evm_snap()), "0xabc");
        assert_eq!(normalise_source_tx_hash(2, "0xabc", &evm_snap()), "0xabc");
        assert_eq!(normalise_source_tx_hash(1, "abc", &evm_snap()), "abc");
        assert_eq!(normalise_source_tx_hash(9999, "abc", &evm_snap()), "abc");
    }

    #[test]
    fn source_tx_from_operation_ignores_empty_and_missing_values() {
        assert_eq!(source_tx_from_operation(&json!({})), None);
        assert_eq!(
            source_tx_from_operation(&json!({"sourceChain": {"transaction": {"txHash": " "}}})),
            None
        );
        assert_eq!(
            source_tx_from_operation(&json!({"sourceChain": {"transaction": {"txHash": " 0xa "}}})),
            Some("0xa".to_string())
        );
    }

    #[test]
    fn the_legacy_redeem_shape_is_turned_around() {
        // What the WormholeScan-driven path builds for a redeem: the observed tx is
        // recorded as the source tx.
        let mut t = inbound_transfer();
        t.source_tx_hash = Some("redeem".into());
        t.destination_tx_hash = None;
        t.status = BridgeStatus::Pending;

        as_inbound_observation(&mut t, "redeem", &snap());

        assert_eq!(t.source_tx_hash, None);
        assert_eq!(t.destination_tx_hash.as_deref(), Some("redeem"));
        assert!(matches!(t.status, BridgeStatus::Completed));
    }

    #[test]
    fn the_legacy_fix_leaves_an_outbound_transfer_alone() {
        let mut t = transfer();
        as_inbound_observation(&mut t, "sig", &snap());

        assert_eq!(t.source_tx_hash.as_deref(), Some("sig"));
        assert!(t.destination_tx_hash.is_none());
        assert!(matches!(t.status, BridgeStatus::Pending));
    }
}
