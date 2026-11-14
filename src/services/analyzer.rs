use crate::clients::limiter::{
    UpstreamLabels, acquire_token_within, call_upstream, call_upstream_after_token,
};
use crate::clients::{helius, wormhole};
use crate::domain::analysis::{Analysis, DecodedTransfer};
use crate::domain::bridge_transfer::BridgeMessageId;
use crate::domain::transaction::{NormalisedTransaction, TxStatus};
use crate::enrichment::{EnrichInput, EnrichOutcome, Prefetch};
use crate::error::AppError;
use crate::normaliser;
use crate::protocols::{Direction, Protocol, SolanaTx};
use crate::services::correlator;
use crate::services::detect;
use crate::services::token_metadata::resolve_token_metadata;
use crate::state::AppState;
use crate::vaa::decode::{decode_destination_address, decode_vaa};
use serde_json::Value;

pub async fn analyse_tx(state: &AppState, chain: &str, hash: &str) -> Result<Analysis, AppError> {
    analyse_tx_inner(state, chain, hash, false).await
}

/// A background refresh variant: the caller has already reserved one Helius and one
/// WormholeScan token, so the first upstream attempt of each does not wait on the
/// limiter. Retries still take their own tokens.
pub async fn analyse_tx_prepaid(
    state: &AppState,
    chain: &str,
    hash: &str,
) -> Result<Analysis, AppError> {
    analyse_tx_inner(state, chain, hash, true).await
}

async fn analyse_tx_inner(
    state: &AppState,
    chain: &str,
    hash: &str,
    prepaid: bool,
) -> Result<Analysis, AppError> {
    if hash.trim().is_empty() {
        return Err(AppError::InvalidTransactionHash(hash.to_string()));
    }

    let t_total = std::time::Instant::now();

    // Take the Helius token before either upstream call starts: a request that
    // times out here must not spend a WormholeScan call whose result is then
    // discarded. The token pays for the first getTransaction attempt below.
    // A prepaid refresh already holds that token.
    let first_attempt_paid = if chain == "solana" {
        if !prepaid {
            acquire_token_within(
                &state.helius_limiter,
                state.helius_wait,
                UpstreamLabels::helius(state.lane.as_str()),
            )
            .await?;
        }
        true
    } else {
        false
    };

    let source_started = std::time::Instant::now();
    let (mut tx, solana_tx) =
        fetch_source_transaction(state, chain, hash, first_attempt_paid).await?;
    tracing::info!(
        helius_ms = source_started.elapsed().as_millis() as u64,
        "source fetch"
    );
    tx.bridge_transfer = None;

    // The registry is asked before WormholeScan: a definite "not a transfer" answer
    // must not spend an upstream call whose result is then discarded.
    let detection = solana_tx
        .as_ref()
        .map(|view| detect::detect_all(&state.protocols, view))
        .unwrap_or_default();

    // Chain first: if an adapter decodes a transfer from the Solana tx alone, it is
    // stored whatever WormholeScan says (outbound as `Pending`, inbound as
    // `Completed`). WormholeScan only enriches.
    if solana_tx.is_some() {
        // Sends out of Solana and redeems into it are both stored (A7). For a redeem the
        // observed Solana tx is the destination side, see `detect::to_bridge_transfer`.
        let detected = &detection.transfers;

        // A non-Wormhole adapter matched and said it does not decode this transfer
        // yet. WormholeScan cannot add a Wormhole message the tx does not have, so
        // this is a final no-transfer, not a failure to dead-letter.
        if detection.unsupported_outside_wormhole() {
            tracing::info!(
                signature = %hash,
                "adapter rejected the transaction as unsupported; not asking WormholeScan"
            );
            return Ok(Analysis {
                tx,
                transfers: Vec::new(),
            });
        }

        if !detected.is_empty() {
            let wormhole_result = fetch_wormhole_operation(state, hash, prepaid).await;
            if detected.len() > 1 {
                tracing::info!(
                    signature = %hash,
                    count = detected.len(),
                    "several transfers in one tx"
                );
            }

            let snapshot = state.chains.snapshot();
            let mut decoded = Vec::with_capacity(detected.len());
            for (position, transfer) in detected.iter().enumerate() {
                let (bridge, row) = match transfer.message.protocol() {
                    Protocol::Wormhole => {
                        // Best effort: a slow or failing metadata lookup never blocks detection.
                        let metadata = match detect::token_lookup(transfer) {
                            Some((token_chain, address)) => tokio::time::timeout(
                                std::time::Duration::from_secs(2),
                                resolve_token_metadata(
                                    &state.registry,
                                    token_chain,
                                    &address,
                                    None,
                                ),
                            )
                            .await
                            .ok()
                            .and_then(Result::ok),
                            None => None,
                        };

                        let mut bridge = detect::to_bridge_transfer(transfer, metadata, &snapshot)?;
                        let enriched = state
                            .enrichment
                            .enrich(
                                EnrichInput {
                                    source_tx_hash: hash,
                                    prefetch: Prefetch::Done(wormhole_result.as_ref()),
                                },
                                &mut bridge,
                            )
                            .await;
                        // The v1 body carries the first transfer and its WormholeScan event.
                        if position == 0 && enriched.outcome == EnrichOutcome::Applied {
                            tx.bridge_event = enriched.bridge_event;
                        }
                        let row = detect::row_values(&bridge);
                        (Some(bridge), row)
                    }
                    // CCTP has no v1 representation and no WormholeScan enrichment: the
                    // chain alone carries everything stored.
                    Protocol::Cctp => (None, detect::cctp_row_values(transfer, &snapshot)),
                    // deBridge: the observed tx is the source for a create and the
                    // destination for a fill, so the row's status follows direction.
                    Protocol::Debridge => (
                        None,
                        if transfer.direction == Direction::Inbound {
                            detect::debridge_destination_row_values(transfer, &snapshot)
                        } else {
                            detect::debridge_row_values(transfer, &snapshot)
                        },
                    ),
                    // Mayan Swift V2: `init_order` is the source and `fulfill` the
                    // destination, so the row's status follows direction.
                    Protocol::Mayan => (
                        None,
                        if transfer.direction == Direction::Inbound {
                            detect::mayan_destination_row_values(transfer, &snapshot)
                        } else {
                            detect::mayan_row_values(transfer, &snapshot)
                        },
                    ),
                };
                decoded.push(DecodedTransfer {
                    detected: transfer.clone(),
                    bridge,
                    row,
                });
            }
            tx.bridge_transfer = decoded.first().and_then(|decoded| decoded.bridge.clone());

            tracing::info!(
                total_ms = t_total.elapsed().as_millis() as u64,
                "analyse_tx total (chain-first)"
            );
            return Ok(Analysis {
                tx,
                transfers: decoded,
            });
        }
    }

    // No adapter produced a transfer (inbound redeem, another Wormhole program, ...):
    // keep the WormholeScan-driven path until those adapters exist.
    let wormhole_result = fetch_wormhole_operation(state, hash, prepaid).await;
    match wormhole_result {
        Ok(wh_raw) => {
            // WormholeScan also indexes operations that are not Wormhole messages
            // (CCTP, executor requests). They have no VAA and no transfer to store.
            let Some(bridge_event) = normaliser::wormhole::normalise(wh_raw.clone())? else {
                tracing::info!(
                    signature = %hash,
                    "WormholeScan operation is not a Wormhole message; not a transfer"
                );
                return Ok(Analysis {
                    tx,
                    transfers: Vec::new(),
                });
            };

            let vaa_b64 = wh_raw
                .get("vaa")
                .and_then(|v| v.get("raw"))
                .and_then(|r| r.as_str())
                .ok_or_else(|| AppError::VaaNotAvailable(hash.to_string()))?;

            let decoded = decode_vaa(vaa_b64)?;

            let message_id = BridgeMessageId {
                emitter_chain: decoded.header.emitter_chain,
                emitter_address: decoded.header.emitter_address.clone(),
                sequence: decoded.header.sequence,
            };

            let destination_chain_id: Option<u16> =
                decoded.transfer.as_ref().map(|t| t.to_chain).or_else(|| {
                    bridge_event
                        .target_chain
                        .as_ref()
                        .and_then(|c| c.parse().ok())
                });

            fn extract_wormholescan_number_field(raw: &Value, field: &str) -> Option<u16> {
                raw.get("content")
                    .and_then(|c| c.get("standarizedProperties"))
                    .and_then(|p| p.get(field))
                    .and_then(Value::as_u64)
                    .map(|n| n as u16)
            }

            let token_chain: Option<u16> = decoded
                .transfer
                .as_ref()
                .map(|t| t.token_chain)
                .or_else(|| extract_wormholescan_number_field(&wh_raw, "tokenChain"));

            let raw_amount: Option<u128> = decoded.transfer.as_ref().map(|t| t.amount);

            let amount: Option<String> = decoded
                .transfer
                .as_ref()
                .map(|t| t.amount.to_string())
                .or_else(|| bridge_event.amount.clone());

            let token: Option<String> = decoded
                .transfer
                .as_ref()
                .and_then(|t| token_chain.map(|c| decode_destination_address(c, &t.token_address)))
                //                ^^^^^^^^^^^ was destination_chain_id — token_address decodes per token_chain, not destination
                .flatten()
                .or_else(|| extract_wormholescan_field(&wh_raw, "tokenAddress"));

            let wormholescan_symbol_hint: Option<String> =
                extract_wormholescan_field(&wh_raw, "tokenSymbol");

            let destination_wallet: Option<String> = decoded
                .transfer
                .as_ref()
                .and_then(|t| {
                    destination_chain_id.map(|c| decode_destination_address(c, &t.to_address))
                })
                .flatten()
                .or_else(|| extract_wormholescan_field(&wh_raw, "toAddress"));

            let known_destination_tx: Option<String> =
                match detect::destination_tx_from_operation(&wh_raw) {
                    Some(hash) => Some(hash),
                    None => state.enrichment.destination_tx(&message_id).await,
                };

            let t_corr = std::time::Instant::now();
            let correlated = correlator::correlate(
                &state.registry,
                correlator::CorrelateParams {
                    source_tx_hash: hash.to_string(),
                    source_wallet: tx.signer.clone(),
                    message_id,
                    destination_chain_id: destination_chain_id.unwrap_or(0),
                    token,
                    token_chain: token_chain.unwrap_or(0),
                    wormholescan_symbol_hint,
                    raw_amount,
                    amount,
                    destination_wallet,
                    known_destination_tx,
                },
                &state.chains.snapshot(),
            )
            .await?;
            tracing::info!(
                correlate_ms = t_corr.elapsed().as_millis() as u64,
                "correlate"
            );
            // A redeem that reaches this path (its VAA account could not be loaded) is
            // still the destination side of the transfer.
            let mut correlated = correlated;
            if matches!(tx.status, TxStatus::Success) {
                detect::as_inbound_observation(&mut correlated, hash, &state.chains.snapshot());
            }
            tx.bridge_transfer = Some(correlated);

            tx.bridge_event = Some(bridge_event);
        }

        Err(other) => return Err(other),
    }

    tracing::info!(
        total_ms = t_total.elapsed().as_millis() as u64,
        "analyse_tx total"
    );
    Ok(Analysis {
        tx,
        transfers: Vec::new(),
    })
}

/// The WormholeScan operation for a tx, through the shared limiter. Called only when
/// the chain-first adapters need it: to enrich a decoded transfer, or when no adapter
/// could decide.
async fn fetch_wormhole_operation(
    state: &AppState,
    hash: &str,
    prepaid: bool,
) -> Result<Value, AppError> {
    let started = std::time::Instant::now();
    let operation =
        || wormhole::get_operation_by_tx_hash(&state.http_client, &state.config.wormhole_url, hash);
    let labels = UpstreamLabels::wormhole();
    let result = if prepaid {
        call_upstream_after_token(&state.wormhole_limiter, labels, operation).await
    } else {
        call_upstream(&state.wormhole_limiter, labels, operation).await
    };
    tracing::info!(
        wormhole_ms = started.elapsed().as_millis() as u64,
        "wormholescan"
    );
    result
}

fn extract_wormholescan_field(raw: &Value, field: &str) -> Option<String> {
    raw.get("content")
        .and_then(|c| c.get("standarizedProperties"))
        .and_then(|p| p.get(field))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Reads the accounts the adapters asked for into the tx view, and reports why when
/// the read failed. The caller decides whether that is fatal: a live redeem is worth
/// retrying, while the reconcile tool keeps the best-effort behaviour.
async fn preload_accounts(state: &AppState, tx: &mut SolanaTx) -> Result<(), AppError> {
    let keys = state.protocols.required_accounts(tx);
    if keys.is_empty() {
        return Ok(());
    }
    let accounts = call_upstream(
        &state.helius_limiter,
        UpstreamLabels::helius(state.lane.as_str()),
        || helius::get_multiple_accounts(&state.http_client, &state.helius_url, &keys),
    )
    .await?;

    for (key, account) in keys.into_iter().zip(accounts) {
        if let Some(account) = account {
            tx.accounts.insert(key, account);
        }
    }
    Ok(())
}

/// Fetches one Solana transaction and preloads the accounts the adapters ask for,
/// without running any classifier. The reconcile tool reuses this so it classifies
/// exactly the view `analyse_tx` would.
pub async fn load_solana_tx(state: &AppState, hash: &str) -> Result<Option<SolanaTx>, AppError> {
    // Reconcile is read-only and runs behind its own limiter: a failed preload must
    // not turn into a retry loop there, so it stays best effort.
    let (_, view) = fetch_solana_source(state, "solana", hash, false, false).await?;
    Ok(view)
}

/// The normalised tx, plus the pure `SolanaTx` view adapters read (`None` if the raw
/// response could not be viewed, in which case only the WormholeScan path can run).
async fn fetch_source_transaction(
    state: &AppState,
    chain: &str,
    hash: &str,
    first_attempt_paid: bool,
) -> Result<(NormalisedTransaction, Option<SolanaTx>), AppError> {
    // A live message is retried by the consumer, so a transient preload failure must
    // surface instead of silently degrading the row.
    let (raw, solana_tx) =
        fetch_solana_source(state, chain, hash, first_attempt_paid, true).await?;
    Ok((normaliser::solana::normalise(raw, hash)?, solana_tx))
}

/// The raw `getTransaction` response and its pure view, with adapter accounts
/// preloaded. Shared by `analyse_tx` and the reconcile tool.
async fn fetch_solana_source(
    state: &AppState,
    chain: &str,
    hash: &str,
    first_attempt_paid: bool,
    preload_errors_are_fatal: bool,
) -> Result<(Value, Option<SolanaTx>), AppError> {
    match chain {
        "solana" => {
            let get_transaction =
                || helius::get_transaction(&state.http_client, &state.helius_url, hash);
            // Retries still wait on the limiter in both branches.
            let labels = UpstreamLabels::helius(state.lane.as_str());
            let raw = if first_attempt_paid {
                call_upstream_after_token(&state.helius_limiter, labels, get_transaction).await?
            } else {
                call_upstream(&state.helius_limiter, labels, get_transaction).await?
            };

            let mut solana_tx = SolanaTx::from_helius(&raw, hash)
                .inspect_err(|error| tracing::warn!(signature = %hash, %error, "unreadable tx"))
                .ok();

            if let Some(view) = solana_tx.as_mut()
                && let Err(error) = preload_accounts(state, view).await
            {
                // Without the posted VAA account a redeem is reported Unsupported and
                // falls back to the WormholeScan-driven path, which can store a row with
                // the wrong source wallet (or dead-letter when WormholeScan is down).
                // A transient failure is worth another delivery instead.
                if preload_errors_are_fatal && error.is_transient() {
                    return Err(error);
                }
                tracing::warn!(
                    signature = %hash,
                    %error,
                    "could not preload accounts"
                );
            }

            Ok((raw, solana_tx))
        }
        other => Err(AppError::Normalisation(format!(
            "unsupported chain: {other}"
        ))),
    }
}
