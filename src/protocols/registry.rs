use std::collections::HashMap;
use std::sync::Arc;

use super::cctp::CctpAdapter;
use super::debridge::DebridgeAdapter;
use super::wormhole::WormholeAdapter;
use super::wormhole::nft_bridge::NftBridgeAdapter;
use super::{BridgeAdapter, Classification, Protocol, SolanaTx};

/// Routes a transaction to every adapter whose program appears anywhere in it,
/// top-level or inner. An aggregator and its underlying rail both see the tx.
#[derive(Clone, Default)]
pub struct ProtocolRegistry {
    adapters: Vec<Arc<dyn BridgeAdapter>>,
    /// program ID -> indexes into `adapters`
    by_program: HashMap<String, Vec<usize>>,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an adapter under its own default program IDs.
    pub fn register(&mut self, adapter: Arc<dyn BridgeAdapter>) {
        let ids: Vec<String> = adapter
            .default_program_ids()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.register_with_programs(adapter, ids);
    }

    /// Registers an adapter under an explicit list (the `SOLANA_BRIDGE_PROGRAMS` override).
    pub fn register_with_programs(
        &mut self,
        adapter: Arc<dyn BridgeAdapter>,
        program_ids: Vec<String>,
    ) {
        let idx = self.adapters.len();
        self.adapters.push(adapter);
        for id in program_ids {
            let slot = self.by_program.entry(id).or_default();
            if !slot.contains(&idx) {
                slot.push(idx);
            }
        }
    }

    /// Every program ID to subscribe to, sorted and deduplicated.
    pub fn program_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.by_program.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Every program the ingestion pipeline subscribes to and polls, sorted and
    /// deduplicated. The Token Bridge is always registered by
    /// [`configured_registry`], so `/health` has a checkpoint to read even when the
    /// override list names other programs.
    pub fn ingestion_programs(&self) -> Vec<String> {
        self.program_ids()
    }

    pub fn protocols_for(&self, program_id: &str) -> Vec<Protocol> {
        self.by_program
            .get(program_id)
            .map(|v| v.iter().map(|&i| self.adapters[i].protocol()).collect())
            .unwrap_or_default()
    }

    /// Accounts the matching adapters need preloaded, sorted and deduplicated.
    pub fn required_accounts(&self, tx: &SolanaTx) -> Vec<String> {
        let mut keys: Vec<String> = self
            .matching(tx)
            .into_iter()
            .flat_map(|i| self.adapters[i].required_accounts(tx))
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// Indexes of the adapters whose program appears anywhere in the tx.
    fn matching(&self, tx: &SolanaTx) -> Vec<usize> {
        let mut hit: Vec<usize> = tx
            .program_ids()
            .into_iter()
            .filter_map(|p| self.by_program.get(p))
            .flatten()
            .copied()
            .collect();
        hit.sort_unstable();
        hit.dedup();
        hit
    }

    /// Runs each matching adapter once, in registration order.
    pub fn classify(&self, tx: &SolanaTx) -> Vec<(Protocol, Classification)> {
        self.matching(tx)
            .into_iter()
            .map(|i| (self.adapters[i].protocol(), self.adapters[i].classify(tx)))
            .collect()
    }
}

pub fn default_registry() -> ProtocolRegistry {
    let mut registry = ProtocolRegistry::new();
    registry.register(Arc::new(WormholeAdapter));
    registry
}

/// The production registry: every adapter under the program IDs configured for it.
/// An adapter with no configured entry keeps its verified defaults. The configured
/// Token Bridge program is always registered too, so a list that names only other
/// programs still decodes and polls the Token Bridge that `/health` watches.
///
/// The NFT Bridge is opt-in: `nft_bridge` is `None` unless the deployment names its
/// program, and when it is set that program belongs to the NFT adapter rather than
/// to the Token Bridge, which the override list would otherwise claim it for.
///
/// CCTP V2 is opt-in the same way, as a `(message transmitter, token messenger minter)`
/// pair. Its programs are removed from the Wormhole adapter's set, so a tx is never
/// classified twice.
pub fn configured_registry(
    overrides: &[(Protocol, String)],
    token_bridge: &str,
    nft_bridge: Option<&str>,
    cctp: Option<(&str, &str)>,
) -> ProtocolRegistry {
    let mut registry = ProtocolRegistry::new();

    let cctp_ids: Vec<&str> = cctp
        .map(|(transmitter, minter)| vec![transmitter, minter])
        .unwrap_or_default();
    let mut ids: Vec<String> = overrides
        .iter()
        .filter(|(protocol, id)| {
            *protocol == Protocol::Wormhole
                && nft_bridge != Some(id.as_str())
                && !cctp_ids.contains(&id.as_str())
        })
        .map(|(_, id)| id.clone())
        .collect();
    ids.push(token_bridge.to_string());
    ids.sort();
    ids.dedup();
    registry.register_with_programs(Arc::new(WormholeAdapter), ids);

    // DLN has one fixed Solana source program, so it is always registered. It is
    // not part of the Wormhole override list and never claims a Wormhole program.
    registry.register(Arc::new(DebridgeAdapter));

    if let Some(id) = nft_bridge {
        registry.register_with_programs(Arc::new(NftBridgeAdapter), vec![id.to_string()]);
    }

    if let Some((transmitter, minter)) = cctp {
        registry.register_with_programs(
            Arc::new(CctpAdapter::new(transmitter, minter)),
            vec![transmitter.to_string(), minter.to_string()],
        );
    }

    registry
}

/// Parses `SOLANA_BRIDGE_PROGRAMS`: a CSV of `protocol:program_id`.
pub fn parse_program_overrides(csv: &str) -> Result<Vec<(Protocol, String)>, String> {
    let mut out = Vec::new();
    for item in csv.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (proto, id) = item
            .split_once(':')
            .ok_or_else(|| format!("expected protocol:program_id, got {item:?}"))?;
        let protocol =
            Protocol::parse(proto.trim()).ok_or_else(|| format!("unknown protocol {proto:?}"))?;
        let id = id.trim();
        if id.is_empty() {
            return Err(format!("empty program id in {item:?}"));
        }
        out.push((protocol, id.to_string()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::debridge::DLN_SOURCE;
    use crate::protocols::{Reject, solana_tx::Instruction};

    struct Fake {
        protocol: Protocol,
        programs: &'static [&'static str],
        accounts: &'static [&'static str],
    }

    impl BridgeAdapter for Fake {
        fn protocol(&self) -> Protocol {
            self.protocol
        }
        fn default_program_ids(&self) -> &'static [&'static str] {
            self.programs
        }
        fn required_accounts(&self, _tx: &SolanaTx) -> Vec<String> {
            self.accounts.iter().map(|a| a.to_string()).collect()
        }
        fn classify(&self, _tx: &SolanaTx) -> Classification {
            Classification::Rejected(Reject::NotTransfer("fake"))
        }
    }

    fn tx_calling(programs: &[(&str, Option<usize>)]) -> SolanaTx {
        SolanaTx {
            signature: "s".into(),
            slot: 1,
            block_time: None,
            failed: false,
            signer: None,
            account_keys: vec![],
            accounts: Default::default(),
            instructions: programs
                .iter()
                .map(|(p, inner_of)| Instruction {
                    program_id: p.to_string(),
                    accounts: vec![],
                    data: vec![],
                    parsed: None,
                    inner_of: *inner_of,
                })
                .collect(),
            logs: vec![],
        }
    }

    fn registry() -> ProtocolRegistry {
        let mut r = ProtocolRegistry::new();
        r.register(Arc::new(Fake {
            protocol: Protocol::Wormhole,
            programs: &["WormA", "WormB"],
            accounts: &["acc2", "acc1"],
        }));
        r
    }

    #[test]
    fn subscribes_to_every_default_program() {
        assert_eq!(registry().program_ids(), vec!["WormA", "WormB"]);
    }

    #[test]
    fn routes_on_inner_calls_too() {
        let r = registry();
        let out = r.classify(&tx_calling(&[("Router", None), ("WormB", Some(0))]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, Protocol::Wormhole);
    }

    #[test]
    fn an_adapter_runs_once_even_if_two_of_its_programs_appear() {
        let r = registry();
        let out = r.classify(&tx_calling(&[("WormA", None), ("WormB", None)]));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn required_accounts_are_sorted_and_only_asked_of_matching_adapters() {
        let r = registry();
        assert_eq!(
            r.required_accounts(&tx_calling(&[("WormA", None), ("WormB", Some(0))])),
            vec!["acc1", "acc2"]
        );
        assert!(
            r.required_accounts(&tx_calling(&[("Other", None)]))
                .is_empty()
        );
    }

    #[test]
    fn unrelated_transactions_match_nothing() {
        assert!(
            registry()
                .classify(&tx_calling(&[("Other", None)]))
                .is_empty()
        );
    }

    #[test]
    fn override_replaces_the_defaults() {
        let mut r = ProtocolRegistry::new();
        r.register_with_programs(
            Arc::new(Fake {
                protocol: Protocol::Wormhole,
                programs: &["WormA"],
                accounts: &[],
            }),
            vec!["Custom".into()],
        );
        assert_eq!(r.program_ids(), vec!["Custom"]);
        assert_eq!(r.protocols_for("Custom"), vec![Protocol::Wormhole]);
        assert!(r.protocols_for("WormA").is_empty());
    }

    #[test]
    fn parses_the_override_list() {
        let v = parse_program_overrides("wormhole:AAA, wormhole:BBB").unwrap();
        assert_eq!(
            v,
            vec![
                (Protocol::Wormhole, "AAA".to_string()),
                (Protocol::Wormhole, "BBB".to_string())
            ]
        );
        assert!(parse_program_overrides("").unwrap().is_empty());
    }

    #[test]
    fn rejects_bad_override_entries() {
        assert!(parse_program_overrides("AAA").is_err());
        assert!(parse_program_overrides("nope:AAA").is_err());
        assert!(parse_program_overrides("wormhole:").is_err());
    }

    #[test]
    fn the_default_registry_covers_the_wormhole_token_bridge() {
        assert_eq!(
            default_registry().program_ids(),
            vec![crate::protocols::wormhole::TOKEN_BRIDGE.to_string()]
        );
    }

    #[test]
    fn a_configured_registry_uses_the_configured_programs() {
        let ids = vec![
            (Protocol::Wormhole, "ProgramA".to_string()),
            (Protocol::Wormhole, "ProgramB".to_string()),
        ];
        assert_eq!(
            configured_registry(&ids, "TokenBridge", None, None).program_ids(),
            vec![
                "ProgramA".to_string(),
                "ProgramB".to_string(),
                "TokenBridge".to_string(),
                DLN_SOURCE.to_string(),
            ],
            "the configured Token Bridge and the fixed DLN source are always registered"
        );

        assert_eq!(
            configured_registry(&[], "TokenBridge", None, None).program_ids(),
            vec!["TokenBridge".to_string(), DLN_SOURCE.to_string()],
            "with no override the configured Token Bridge and DLN are the whole set"
        );
    }

    #[test]
    fn the_nft_bridge_is_registered_only_when_configured() {
        assert_eq!(
            configured_registry(&[], "TokenBridge", None, None).program_ids(),
            vec!["TokenBridge".to_string(), DLN_SOURCE.to_string()],
            "unset means the NFT adapter adds nothing"
        );

        let registry = configured_registry(
            &[(Protocol::Wormhole, "NftBridge".to_string())],
            "TokenBridge",
            Some("NftBridge"),
            None,
        );
        assert_eq!(
            registry.program_ids(),
            vec![
                "NftBridge".to_string(),
                "TokenBridge".to_string(),
                DLN_SOURCE.to_string(),
            ]
        );
        assert_eq!(
            registry.protocols_for("NftBridge"),
            vec![Protocol::Wormhole]
        );
    }

    #[test]
    fn the_cctp_programs_are_registered_only_when_configured() {
        assert_eq!(
            configured_registry(&[], "TokenBridge", None, None).program_ids(),
            vec!["TokenBridge".to_string(), DLN_SOURCE.to_string()],
            "unset CCTP is no behaviour change beyond the fixed DLN source"
        );

        let registry = configured_registry(
            &[],
            "TokenBridge",
            None,
            Some(("MessageTransmitter", "TokenMessenger")),
        );
        assert_eq!(
            registry.program_ids(),
            vec![
                "MessageTransmitter".to_string(),
                "TokenBridge".to_string(),
                "TokenMessenger".to_string(),
                DLN_SOURCE.to_string(),
            ]
        );
        assert_eq!(
            registry.protocols_for("MessageTransmitter"),
            vec![Protocol::Cctp]
        );
        assert_eq!(
            registry.protocols_for("TokenMessenger"),
            vec![Protocol::Cctp]
        );
        assert_eq!(
            registry.protocols_for("TokenBridge"),
            vec![Protocol::Wormhole],
            "the Wormhole adapter keeps only the programs it owns"
        );
    }

    #[test]
    fn the_debridge_source_is_always_registered() {
        let registry = configured_registry(&[], "TokenBridge", None, None);
        assert_eq!(registry.protocols_for(DLN_SOURCE), vec![Protocol::Debridge]);
    }

    #[test]
    fn two_programs_route_to_their_own_adapters() {
        let mut r = ProtocolRegistry::new();
        r.register(Arc::new(Fake {
            protocol: Protocol::Wormhole,
            programs: &["ProgramA"],
            accounts: &["accountA"],
        }));
        r.register(Arc::new(Fake {
            protocol: Protocol::Wormhole,
            programs: &["ProgramB"],
            accounts: &["accountB"],
        }));

        assert_eq!(
            r.required_accounts(&tx_calling(&[("ProgramA", None)])),
            vec!["accountA"]
        );
        assert_eq!(
            r.required_accounts(&tx_calling(&[("ProgramB", None)])),
            vec!["accountB"]
        );
    }

    #[test]
    fn the_ingestion_set_is_the_registered_program_set() {
        let mut r = ProtocolRegistry::new();
        r.register(Arc::new(Fake {
            protocol: Protocol::Wormhole,
            programs: &["Other"],
            accounts: &[],
        }));
        r.register(Arc::new(Fake {
            protocol: Protocol::Wormhole,
            programs: &["TokenBridge"],
            accounts: &[],
        }));

        assert_eq!(
            r.ingestion_programs(),
            vec!["Other".to_string(), "TokenBridge".to_string()]
        );
    }
}
