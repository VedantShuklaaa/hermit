use super::{EnrichInput, EnrichOutcome, Enriched, EnrichmentProvider, Prefetch};
use crate::chains::{ChainRegistry, Snapshot};
use crate::clients::limiter::{Limiter, UpstreamLabels, call_upstream};
use crate::clients::wormhole;
use crate::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer};
use crate::error::AppError;
use crate::normaliser;
use crate::protocols::Direction;
use crate::services::detect::{self, Enrichment};
use async_trait::async_trait;
use reqwest::Client;
use std::sync::Arc;

/// WormholeScan: the VAA from `/operations`, then the destination tx from `/global-tx`.
/// Every call goes through the shared WormholeScan limiter.
pub struct WormholeScanProvider {
    client: Client,
    base_url: String,
    limiter: Arc<Limiter>,
    chains: ChainRegistry,
}

impl WormholeScanProvider {
    pub fn new(
        client: Client,
        base_url: String,
        limiter: Arc<Limiter>,
        chains: ChainRegistry,
    ) -> Self {
        Self {
            client,
            base_url,
            limiter,
            chains,
        }
    }

    /// `/operations` is keyed by a transaction signature, and a redeem's signature is the
    /// destination side, so an empty or origin-less operation is expected there. The VAA
    /// is keyed by the message itself and still names the tx that emitted it. Only a
    /// missing inbound origin tx is filled; the status and every other field are left
    /// alone. Returns `None` when there was nothing to ask for.
    async fn fill_missing_origin(
        &self,
        transfer: &mut BridgeTransfer,
        chains: &Snapshot,
    ) -> Option<EnrichOutcome> {
        if transfer.direction() != Direction::Inbound || transfer.source_tx_hash.is_some() {
            return None;
        }

        let result = call_upstream(&self.limiter, UpstreamLabels::wormhole(), || {
            wormhole::get_vaa_source_tx(&self.client, &self.base_url, &transfer.message_id)
        })
        .await;

        match result {
            Ok(Some(hash)) => {
                detect::apply_source_tx(transfer, &hash, chains);
                Some(EnrichOutcome::Applied)
            }
            Ok(None) => Some(EnrichOutcome::NoVaa),
            Err(error) => {
                tracing::debug!(%error, "vaa lookup failed");
                Some(outcome_for_error(&error))
            }
        }
    }
}

/// "WormholeScan does not know this tx yet" is an answer. Anything else means the
/// provider could not be asked.
fn outcome_for_error(error: &AppError) -> EnrichOutcome {
    match error {
        AppError::TransactionNotFound(_) => EnrichOutcome::NoVaa,
        _ => EnrichOutcome::Unavailable,
    }
}

#[async_trait]
impl EnrichmentProvider for WormholeScanProvider {
    fn name(&self) -> &'static str {
        "wormholescan"
    }

    async fn enrich(&self, input: EnrichInput<'_>, transfer: &mut BridgeTransfer) -> Enriched {
        let fetched;
        let operation = match input.prefetch {
            Prefetch::Done(result) => result,
            Prefetch::None => {
                fetched = call_upstream(&self.limiter, UpstreamLabels::wormhole(), || {
                    wormhole::get_operation_by_tx_hash(
                        &self.client,
                        &self.base_url,
                        input.source_tx_hash,
                    )
                })
                .await;
                fetched.as_ref()
            }
        };

        let chains = self.chains.snapshot();
        match operation {
            Ok(operation) => match detect::apply_wormholescan(transfer, operation, &chains) {
                Enrichment::Applied => {
                    // VAA matched but /operations carried no destination tx: ask global-tx,
                    // keyed by the on-chain message id.
                    if matches!(transfer.status, BridgeStatus::Pending)
                        && let Some(hash) = self.destination_tx(&transfer.message_id).await
                    {
                        detect::apply_destination_tx(transfer, &hash, &chains);
                    }
                    // The matched operation can still lack the origin tx. The provider
                    // already applied what it had, so a fallback failure changes nothing.
                    self.fill_missing_origin(transfer, &chains).await;
                    Enriched {
                        outcome: EnrichOutcome::Applied,
                        bridge_event: normaliser::wormhole::normalise(operation.clone())
                            .ok()
                            .flatten(),
                    }
                }
                Enrichment::NoVaa => match self.fill_missing_origin(transfer, &chains).await {
                    Some(outcome) => Enriched::of(outcome),
                    None => Enriched::of(EnrichOutcome::NoVaa),
                },
                Enrichment::Mismatch => Enriched::of(EnrichOutcome::Mismatch),
            },
            Err(error) => {
                tracing::debug!(signature = %input.source_tx_hash, %error, "wormholescan unavailable, keeping on-chain data");
                let outcome = outcome_for_error(error);
                if outcome == EnrichOutcome::NoVaa {
                    match self.fill_missing_origin(transfer, &chains).await {
                        Some(fallback) => Enriched::of(fallback),
                        None => Enriched::of(EnrichOutcome::NoVaa),
                    }
                } else {
                    Enriched::of(outcome)
                }
            }
        }
    }

    /// Best effort: a failure is logged and treated as "not known yet", it never fails
    /// the analysis.
    async fn destination_tx(&self, id: &BridgeMessageId) -> Option<String> {
        call_upstream(&self.limiter, UpstreamLabels::wormhole(), || {
            wormhole::get_destination_tx(&self.client, &self.base_url, id)
        })
        .await
        .inspect_err(|error| tracing::debug!(%error, "global-tx lookup failed"))
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::{ChainRow, MappingRow, NS_WORMHOLE, RegistryStatus};
    use crate::domain::bridge_transfer::ChainId;
    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use governor::{Quota, RateLimiter};
    use serde_json::json;
    use std::num::NonZeroU32;

    fn provider() -> WormholeScanProvider {
        // No external network: tests pass a prefetched response, and the fallback tests
        // point at a loopback stub. Nothing here reaches Helius or WormholeScan.
        provider_with("http://127.0.0.1:1", ChainRegistry::default())
    }

    fn provider_with(base_url: &str, chains: ChainRegistry) -> WormholeScanProvider {
        WormholeScanProvider::new(
            Client::new(),
            base_url.into(),
            Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(100).unwrap(),
            ))),
            chains,
        )
    }

    fn registry(kind: &str) -> ChainRegistry {
        let chain = |id: i16, slug: &str| ChainRow {
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
        ChainRegistry::new(Snapshot::build(
            vec![chain(1, "solana"), chain(2, "ethereum")],
            vec![mapping("1", 1), mapping("2", 2)],
        ))
    }

    /// A loopback WormholeScan: `/operations` is always empty (the redeem signature is
    /// unknown there) and `/vaas` answers `vaa` or 404.
    async fn serve_vaa(vaa: Option<&'static str>) -> String {
        let app = Router::new()
            .route(
                "/api/v1/operations",
                get(|| async { axum::Json(json!({"operations": []})) }),
            )
            .route(
                "/api/v1/vaas/:chain/:emitter/:sequence",
                get(move || async move {
                    match vaa {
                        Some(hash) => axum::Json(json!({"data": {"txHash": hash}})).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let address = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve stub");
        });
        format!("http://{address}")
    }

    fn inbound_transfer() -> BridgeTransfer {
        BridgeTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain: ChainId::Ethereum,
            source_tx_hash: None,
            source_wallet: None,
            source_explorer_url: None,
            destination_chain: ChainId::Solana,
            destination_wallet: None,
            destination_tx_hash: Some("redeem".into()),
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount: None,
            amount_formatted: None,
            message_id: BridgeMessageId {
                emitter_chain: 2,
                emitter_address: "09".repeat(32),
                sequence: 42,
            },
            status: BridgeStatus::Completed,
        }
    }

    async fn run_with(
        provider: WormholeScanProvider,
        prefetch: Prefetch<'_>,
        transfer: &mut BridgeTransfer,
    ) -> EnrichOutcome {
        provider
            .enrich(
                EnrichInput {
                    source_tx_hash: "redeem",
                    prefetch,
                },
                transfer,
            )
            .await
            .outcome
    }

    fn transfer() -> BridgeTransfer {
        BridgeTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            source_chain: ChainId::Solana,
            source_tx_hash: Some("sig".into()),
            source_wallet: None,
            source_explorer_url: None,
            destination_chain: ChainId::Ethereum,
            destination_wallet: None,
            destination_tx_hash: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount: None,
            amount_formatted: None,
            message_id: BridgeMessageId {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 42,
            },
            status: BridgeStatus::Detected,
        }
    }

    fn vaa_b64(sequence: u64) -> String {
        let mut bytes = vec![1u8, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&[0u8; 8]);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(&[9u8; 32]);
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.push(1);
        STANDARD.encode(bytes)
    }

    async fn run(prefetch: Prefetch<'_>, transfer: &mut BridgeTransfer) -> EnrichOutcome {
        provider()
            .enrich(
                EnrichInput {
                    source_tx_hash: "sig",
                    prefetch,
                },
                transfer,
            )
            .await
            .outcome
    }

    #[tokio::test]
    async fn a_known_destination_tx_completes_the_transfer() {
        let op = json!({
            "vaa": {"raw": vaa_b64(42)},
            "targetChain": {"transaction": {"txHash": "0xdest"}},
        });
        let mut t = transfer();
        assert_eq!(
            run(Prefetch::Done(Ok(&op)), &mut t).await,
            EnrichOutcome::Applied
        );
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xdest"));
    }

    #[tokio::test]
    async fn a_vaa_for_another_message_changes_nothing() {
        let op = json!({"vaa": {"raw": vaa_b64(43)}});
        let mut t = transfer();
        assert_eq!(
            run(Prefetch::Done(Ok(&op)), &mut t).await,
            EnrichOutcome::Mismatch
        );
        assert!(matches!(t.status, BridgeStatus::Detected));
    }

    #[tokio::test]
    async fn no_vaa_yet_leaves_the_transfer_detected() {
        let op = json!({});
        let mut t = transfer();
        assert_eq!(
            run(Prefetch::Done(Ok(&op)), &mut t).await,
            EnrichOutcome::NoVaa
        );
        assert!(matches!(t.status, BridgeStatus::Detected));
    }

    #[tokio::test]
    async fn not_found_is_an_answer_but_other_errors_are_not() {
        let not_found = AppError::TransactionNotFound("sig".into());
        let rate_limited = AppError::UpstreamRateLimited("wormhole");

        let mut t = transfer();
        assert_eq!(
            run(Prefetch::Done(Err(&not_found)), &mut t).await,
            EnrichOutcome::NoVaa
        );
        assert_eq!(
            run(Prefetch::Done(Err(&rate_limited)), &mut t).await,
            EnrichOutcome::Unavailable
        );
        assert!(matches!(t.status, BridgeStatus::Detected));
    }

    #[tokio::test]
    async fn a_completed_transfer_is_never_moved_backwards() {
        let op = json!({"vaa": {"raw": vaa_b64(42)}});
        let mut t = transfer();
        t.status = BridgeStatus::Completed;
        t.destination_tx_hash = Some("0xfirst".into());
        run(Prefetch::Done(Ok(&op)), &mut t).await;
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("0xfirst"));
    }

    #[tokio::test]
    async fn empty_operations_fall_back_to_the_vaa_origin_tx() {
        let base = serve_vaa(Some("origin-sig")).await;
        let mut t = inbound_transfer();

        assert_eq!(
            run_with(
                provider_with(&base, registry("other")),
                Prefetch::None,
                &mut t
            )
            .await,
            EnrichOutcome::Applied
        );
        assert_eq!(t.source_tx_hash.as_deref(), Some("origin-sig"));
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://ethereum.example/tx/origin-sig")
        );
        assert!(matches!(t.status, BridgeStatus::Completed));
        assert_eq!(t.destination_tx_hash.as_deref(), Some("redeem"));
    }

    #[tokio::test]
    async fn an_evm_vaa_hash_is_stored_with_the_0x_prefix() {
        let base = serve_vaa(Some("715b25c9bba7")).await;
        let not_found = AppError::TransactionNotFound("redeem".into());
        let mut t = inbound_transfer();

        assert_eq!(
            run_with(
                provider_with(&base, registry("evm")),
                Prefetch::Done(Err(&not_found)),
                &mut t,
            )
            .await,
            EnrichOutcome::Applied
        );
        assert_eq!(t.source_tx_hash.as_deref(), Some("0x715b25c9bba7"));
        assert_eq!(
            t.source_explorer_url.as_deref(),
            Some("https://ethereum.example/tx/0x715b25c9bba7")
        );
    }

    #[tokio::test]
    async fn a_vaa_404_leaves_the_row_unchanged() {
        let base = serve_vaa(None).await;
        let not_found = AppError::TransactionNotFound("redeem".into());
        let mut t = inbound_transfer();

        assert_eq!(
            run_with(
                provider_with(&base, registry("evm")),
                Prefetch::Done(Err(&not_found)),
                &mut t,
            )
            .await,
            EnrichOutcome::NoVaa
        );
        assert_eq!(t.source_tx_hash, None);
        assert_eq!(t.source_explorer_url, None);
        assert!(matches!(t.status, BridgeStatus::Completed));
    }

    #[tokio::test]
    async fn a_known_origin_tx_is_never_asked_for_again() {
        // The dead base URL proves no request is made when the row already has an origin.
        let not_found = AppError::TransactionNotFound("redeem".into());
        let mut t = inbound_transfer();
        t.source_tx_hash = Some("0xfirst".into());

        assert_eq!(
            run_with(
                provider_with("http://127.0.0.1:1", registry("evm")),
                Prefetch::Done(Err(&not_found)),
                &mut t,
            )
            .await,
            EnrichOutcome::NoVaa
        );
        assert_eq!(t.source_tx_hash.as_deref(), Some("0xfirst"));
    }
}
