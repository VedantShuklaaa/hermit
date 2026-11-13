use crate::chain_adapters::registry::AdapterRegistry;
use crate::chains::{RawChainRef, Snapshot};
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use crate::error::AppError;
use crate::services::token_metadata::{TokenMetadata, resolve_token_metadata};
use crate::vaa::decode::format_amount;

pub struct CorrelateParams {
    pub source_tx_hash: String,
    pub source_wallet: Option<String>,
    pub message_id: BridgeMessageId,
    pub destination_chain_id: u16,
    pub token: Option<String>,
    pub token_chain: u16,
    pub wormholescan_symbol_hint: Option<String>,
    pub raw_amount: Option<u128>,
    pub amount: Option<String>,
    pub destination_wallet: Option<String>,
    pub known_destination_tx: Option<String>,
}

pub async fn correlate(
    registry: &AdapterRegistry,
    params: CorrelateParams,
    chains: &Snapshot,
) -> Result<BridgeTransfer, AppError> {
    let CorrelateParams {
        source_tx_hash,
        source_wallet,
        message_id,
        destination_chain_id,
        token,
        token_chain,
        wormholescan_symbol_hint,
        raw_amount,
        amount,
        destination_wallet,
        known_destination_tx,
    } = params;

    let destination_chain = ChainId::from_wormhole_id(destination_chain_id);

    let (destination_tx_hash, status) = if let Some(known) = known_destination_tx {
        (Some(known), BridgeStatus::Completed)
    } else if chains
        .resolve(&RawChainRef::wormhole(destination_chain_id))
        .is_resolved()
    {
        (None, BridgeStatus::Pending)
    } else {
        (None, BridgeStatus::Detected)
    };

    let effective_raw_amount: Option<u128> =
        raw_amount.or_else(|| amount.as_ref().and_then(|a| a.parse::<u128>().ok()));

    let t = std::time::Instant::now();
    let metadata = match &token {
        Some(token_addr) => {
            resolve_token_metadata(registry, token_chain, token_addr, wormholescan_symbol_hint)
                .await?
        }
        None => TokenMetadata {
            symbol: None,
            decimals: None,
        },
    };
    tracing::info!(
        metadata_ms = t.elapsed().as_millis() as u64,
        "token metadata"
    );

    let amount_formatted = effective_raw_amount.map(|r| format_amount(r, metadata.decimals));

    let source_explorer_url = chains.explorer_tx_url_for(
        &RawChainRef::wormhole(message_id.emitter_chain),
        &source_tx_hash,
    );

    let destination_explorer_url = destination_tx_hash
        .as_ref()
        .and_then(|h| chains.explorer_tx_url_for(&RawChainRef::wormhole(destination_chain_id), h));

    Ok(BridgeTransfer {
        program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
        source_chain: ChainId::from_wormhole_id(message_id.emitter_chain),
        source_tx_hash: Some(source_tx_hash),
        source_wallet,
        source_explorer_url,
        destination_chain,
        destination_wallet,
        destination_tx_hash,
        destination_explorer_url,
        token,
        token_symbol: metadata.symbol,
        amount: amount.or_else(|| raw_amount.map(|r| r.to_string())),
        amount_formatted,
        message_id,
        status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::{ChainRow, MappingRow, NS_WORMHOLE, RegistryStatus};

    fn snapshot(known_wormhole_ids: &[(&str, i16)]) -> Snapshot {
        let chains = known_wormhole_ids
            .iter()
            .map(|(_, id)| ChainRow {
                id: *id,
                slug: format!("chain{id}"),
                name: format!("chain{id}"),
                kind: "other".into(),
                explorer_tx_url: Some(format!("https://chain{id}.example/tx/{{hash}}")),
                status: RegistryStatus::Active,
            })
            .collect();
        let mappings = known_wormhole_ids
            .iter()
            .map(|(wormhole_id, id)| MappingRow {
                protocol: NS_WORMHOLE.into(),
                protocol_chain_id: (*wormhole_id).into(),
                chain_id: *id,
                status: RegistryStatus::Active,
            })
            .collect();
        Snapshot::build(chains, mappings)
    }

    fn params(destination_chain_id: u16, known_destination_tx: Option<&str>) -> CorrelateParams {
        CorrelateParams {
            source_tx_hash: "sig".into(),
            source_wallet: None,
            message_id: BridgeMessageId {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 7,
            },
            destination_chain_id,
            token: None,
            token_chain: 1,
            wormholescan_symbol_hint: None,
            raw_amount: Some(5_000_000),
            amount: None,
            destination_wallet: None,
            known_destination_tx: known_destination_tx.map(String::from),
        }
    }

    async fn run(chains: &Snapshot, p: CorrelateParams) -> BridgeTransfer {
        correlate(&AdapterRegistry::new(), p, chains).await.unwrap()
    }

    #[tokio::test]
    async fn a_destination_the_registry_knows_is_pending_until_its_tx_is_known() {
        let chains = snapshot(&[("1", 1), ("2", 2)]);
        let t = run(&chains, params(2, None)).await;
        assert!(matches!(t.status, BridgeStatus::Pending));
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://chain1.example/tx/sig")
        );
    }

    #[tokio::test]
    async fn an_unregistered_destination_stays_detected() {
        let chains = snapshot(&[("1", 1)]);
        let t = run(&chains, params(2, None)).await;
        assert!(matches!(t.status, BridgeStatus::Detected));
    }

    #[tokio::test]
    async fn a_known_destination_tx_completes_with_its_explorer_url() {
        let chains = snapshot(&[("1", 1), ("2", 2)]);
        let t = run(&chains, params(2, Some("0xdest"))).await;
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(
            t.destination_explorer_url.as_deref(),
            Some("https://chain2.example/tx/0xdest")
        );
    }

    #[tokio::test]
    async fn an_unknown_chain_with_a_known_tx_completes_without_an_explorer_url() {
        let chains = snapshot(&[("1", 1)]);
        let t = run(&chains, params(9999, Some("0xdest"))).await;
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert!(t.destination_explorer_url.is_none());
    }
}
