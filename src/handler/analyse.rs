use axum::Json;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::error::AppError;
use crate::routes::v1::compat;
use crate::services::read_through::{self, V2Analysis};
use crate::state::AppState;

fn default_chain() -> String {
    "solana".to_string()
}

/// Only Solana is a supported source chain. Reject anything else before the read
/// path, so an unsupported chain never reaches a cache or spends an upstream call.
/// The slug is lowercased so `Solana` and `solana` share every per-chain memo.
fn supported_chain(chain: &str) -> Result<String, AppError> {
    let chain = chain.to_ascii_lowercase();
    if chain == "solana" {
        Ok(chain)
    } else {
        Err(AppError::BadRequest(format!("unsupported chain: {chain}")))
    }
}

#[derive(Deserialize)]
pub struct AnalyseQuery {
    pub transaction_hash: String,
    #[serde(default = "default_chain")]
    pub chain: String,
}

pub async fn analyse_handler(
    State(state): State<AppState>,
    Query(params): Query<AnalyseQuery>,
) -> Result<Response, AppError> {
    let chain = supported_chain(&params.chain)?;
    let analysed =
        read_through::analyse_cached_v2(&state, &chain, &params.transaction_hash).await?;
    v1_response(&analysed)
}

fn json_bytes_response(body: &axum::body::Bytes) -> Response {
    let mut response = Response::new(Body::from(body.clone()));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// A v1 cache hit serves the body serialized when the entry was filled. An entry that
/// v1 cannot represent falls back to the old map-and-serialize path, so its error is
/// unchanged.
fn v1_response(analysed: &V2Analysis) -> Result<Response, AppError> {
    if let Some(body) = analysed.v1_body() {
        return Ok(json_bytes_response(body));
    }

    Ok(Json(compat::to_v1(
        analysed.response(),
        analysed.bridge_event().cloned(),
    )?)
    .into_response())
}

pub async fn analyse_v2_handler(
    State(state): State<AppState>,
    Query(params): Query<AnalyseQuery>,
) -> Result<Response, AppError> {
    let chain = supported_chain(&params.chain)?;
    let analysed =
        read_through::analyse_cached_v2(&state, &chain, &params.transaction_hash).await?;
    Ok(v2_response(&analysed))
}

/// A v2 cache hit serves the body serialized when the entry was filled. If that
/// serialization failed at fill time, fall back to the old clone-and-serialize path.
fn v2_response(analysed: &V2Analysis) -> Response {
    if let Some(body) = analysed.v2_body() {
        return json_bytes_response(body);
    }
    Json(analysed.response().clone()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::NS_WORMHOLE;
    use crate::domain::api::{
        ChainDetails, ChainView, TransactionResponse, TransferStatus, TransferView,
    };
    use crate::domain::transaction::TxStatus;
    use crate::protocols::{Direction, MessageId, TOKEN_BRIDGE_PROGRAM};

    fn chain(protocol: &str, id: &str) -> ChainView {
        ChainView {
            protocol: protocol.to_string(),
            id: id.to_string(),
            chain: None,
        }
    }

    fn transfer(protocol: &str) -> TransferView {
        TransferView {
            protocol: protocol.to_string(),
            program: TOKEN_BRIDGE_PROGRAM.to_string(),
            direction: Direction::Outbound,
            via: None,
            observed_tx_hash: "sig".to_string(),
            event_index: 0,
            message_id: MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 1,
            },
            source: chain(NS_WORMHOLE, "1"),
            destination: chain(NS_WORMHOLE, "2"),
            source_wallet: None,
            destination_wallet: None,
            source_tx_hash: Some("sig".to_string()),
            destination_tx_hash: None,
            source_explorer_url: None,
            destination_explorer_url: None,
            token: None,
            token_symbol: None,
            amount_raw: None,
            amount_formatted: None,
            status: TransferStatus::Completed,
        }
    }

    fn response(transfers: Vec<TransferView>) -> TransactionResponse {
        TransactionResponse {
            hash: "sig".to_string(),
            chain: ChainView {
                protocol: NS_WORMHOLE.to_string(),
                id: "1".to_string(),
                chain: Some(ChainDetails {
                    id: 1,
                    slug: "solana".to_string(),
                    name: "Solana".to_string(),
                }),
            },
            status: TxStatus::Success,
            slot: 1,
            timestamp: None,
            fee_lamports: 0,
            signer: None,
            transfers,
        }
    }

    async fn bytes_of(response: Response) -> axum::body::Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body")
    }

    /// The cached bytes must equal what the pre-change handler returned: axum's
    /// `Json` over the v1 compatibility mapping, status and headers included.
    #[tokio::test]
    async fn a_cache_hit_matches_the_old_json_response_byte_for_byte() {
        let response = response(vec![transfer("wormhole")]);
        let old =
            Json(compat::to_v1(&response, None).expect("v1 can represent it")).into_response();
        let expected_status = old.status();
        let expected_headers = old.headers().clone();
        let expected_body = bytes_of(old).await;

        let entry = V2Analysis::new(response, None);

        // Two hits: the first and a later one both serve the pre-serialized body.
        for hit_number in 0..2 {
            let hit = v1_response(&entry).expect("cache hit");
            assert_eq!(hit.status(), expected_status, "hit {hit_number}");
            assert_eq!(hit.headers(), &expected_headers, "hit {hit_number}");
            assert_eq!(
                hit.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json"))
            );
            assert_eq!(bytes_of(hit).await, expected_body, "hit {hit_number}");
        }
    }

    #[tokio::test]
    async fn an_unrepresentable_entry_falls_back_to_the_old_error() {
        let entry = V2Analysis::new(response(vec![transfer("cctp")]), None);
        assert!(entry.v1_body().is_none());
        assert!(matches!(
            v1_response(&entry),
            Err(AppError::Normalisation(_))
        ));
    }

    /// The cached v2 bytes must equal what the old handler returned: axum's `Json`
    /// over a clone of the response, status and headers included.
    #[tokio::test]
    async fn a_v2_cache_hit_matches_the_old_json_response_byte_for_byte() {
        let response = response(vec![transfer("wormhole")]);
        let old = Json(response.clone()).into_response();
        let expected_status = old.status();
        let expected_headers = old.headers().clone();
        let expected_body = bytes_of(old).await;

        let entry = V2Analysis::new(response, None);
        assert!(entry.v2_body().is_some());

        for hit_number in 0..2 {
            let hit = v2_response(&entry);
            assert_eq!(hit.status(), expected_status, "hit {hit_number}");
            assert_eq!(hit.headers(), &expected_headers, "hit {hit_number}");
            assert_eq!(bytes_of(hit).await, expected_body, "hit {hit_number}");
        }
    }

    #[test]
    fn only_solana_is_supported_and_the_slug_is_lowercased() {
        assert_eq!(supported_chain("solana").expect("supported"), "solana");
        assert_eq!(supported_chain("Solana").expect("supported"), "solana");
        for unsupported in ["ethereum", "SOLANA ", ""] {
            assert!(
                matches!(supported_chain(unsupported), Err(AppError::BadRequest(_))),
                "{unsupported:?} must be a bad request"
            );
        }
    }

    /// Validation runs before `read_through`, so the unconnectable test state is
    /// never used and no Helius or WormholeScan call can happen.
    #[tokio::test]
    async fn both_handlers_reject_an_unsupported_chain_before_the_read_path() {
        let state = crate::state::AppState::test_state(crate::config::AppConfig::test_config());
        let params = || AnalyseQuery {
            transaction_hash: "sig".to_string(),
            chain: "ethereum".to_string(),
        };

        let err = analyse_handler(State(state.clone()), Query(params()))
            .await
            .expect_err("unsupported chain");
        assert!(matches!(err, AppError::BadRequest(_)));

        let err = analyse_v2_handler(State(state), Query(params()))
            .await
            .expect_err("unsupported chain");
        assert!(matches!(err, AppError::BadRequest(_)));
    }
}
