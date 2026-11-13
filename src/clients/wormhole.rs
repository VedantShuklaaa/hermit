use crate::domain::bridge_transfer::BridgeMessageId;
use crate::error::AppError;
use reqwest::Client;
use serde_json::Value;

pub async fn get_operation_by_tx_hash(
    client: &Client,
    base_url: &str,
    hash: &str,
) -> Result<Value, AppError> {
    let url = format!("{base_url}/api/v1/operations?txHash={hash}");
    let resp = client.get(&url).send().await?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(AppError::UpstreamRateLimited("wormhole"));
    }

    if !resp.status().is_success() {
        return Err(AppError::UpstreamProvider {
            provider: "wormhole",
            message: format!("status {}", resp.status()),
            retryable: crate::clients::helius::status_is_retryable(resp.status()),
        });
    }

    let payload: Value = resp.json().await?;
    let ops = payload.get("operations").and_then(Value::as_array); // was: payload.as_array()

    match ops {
        Some(arr) if !arr.is_empty() => Ok(arr[0].clone()),
        _ => Err(AppError::TransactionNotFound(hash.to_string())),
    }
}

pub async fn get_destination_tx(
    client: &Client,
    base_url: &str,
    id: &BridgeMessageId,
) -> Result<Option<String>, AppError> {
    let url = format!(
        "{base_url}/api/v1/global-tx/{}/{}/{}",
        id.emitter_chain, id.emitter_address, id.sequence
    );
    let resp = client.get(&url).send().await?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(AppError::UpstreamRateLimited("wormhole"));
    }
    if !resp.status().is_success() {
        return Err(AppError::UpstreamProvider {
            provider: "wormhole",
            message: format!("status {}", resp.status()),
            retryable: crate::clients::helius::status_is_retryable(resp.status()),
        });
    }

    let payload: Value = resp.json().await?;
    Ok(destination_tx_from_global_tx(&payload))
}

/// The origin tx of a message, keyed by its Wormhole message id. `/operations` is keyed
/// by a transaction signature and does not know a redeem's Solana signature, but the VAA
/// itself is keyed by the message and carries the tx that emitted it.
pub async fn get_vaa_source_tx(
    client: &Client,
    base_url: &str,
    id: &BridgeMessageId,
) -> Result<Option<String>, AppError> {
    let url = format!(
        "{base_url}/api/v1/vaas/{}/{}/{}",
        id.emitter_chain, id.emitter_address, id.sequence
    );
    let resp = client.get(&url).send().await?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(AppError::UpstreamRateLimited("wormhole"));
    }
    if !resp.status().is_success() {
        return Err(AppError::UpstreamProvider {
            provider: "wormhole",
            message: format!("status {}", resp.status()),
            retryable: crate::clients::helius::status_is_retryable(resp.status()),
        });
    }

    let payload: Value = resp.json().await?;
    Ok(source_tx_from_vaa(&payload))
}

/// Pure: reads `data.txHash` from a `vaas` payload. EVM chains return bare hex here;
/// `detect::apply_source_tx` adds the `0x` prefix from the chain registry.
pub fn source_tx_from_vaa(payload: &Value) -> Option<String> {
    payload
        .get("data")?
        .get("txHash")?
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Pure: reads `destinationTx.txHash` from a `global-tx` payload.
pub fn destination_tx_from_global_tx(payload: &Value) -> Option<String> {
    payload
        .get("destinationTx")
        .and_then(|d| d.get("txHash"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::get;
    use serde_json::json;

    /// A loopback WormholeScan whose every route answers 429, like the real API does
    /// when the caller is over its budget.
    async fn serve_429() -> String {
        async fn too_many() -> (StatusCode, axum::Json<Value>) {
            (
                StatusCode::TOO_MANY_REQUESTS,
                axum::Json(json!({"status": 429, "message": "Too many requests"})),
            )
        }

        let app = Router::new().fallback(get(too_many));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let address = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve stub");
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn a_429_is_a_transient_rate_limit() {
        let base = serve_429().await;
        let client = Client::new();
        let id = BridgeMessageId {
            emitter_chain: 2,
            emitter_address: "09".repeat(32),
            sequence: 42,
        };

        let errors = [
            get_operation_by_tx_hash(&client, &base, "sig")
                .await
                .expect_err("a 429 is not an operation"),
            get_destination_tx(&client, &base, &id)
                .await
                .expect_err("a 429 is not a destination tx"),
            get_vaa_source_tx(&client, &base, &id)
                .await
                .expect_err("a 429 is not a source tx"),
        ];

        for error in errors {
            assert!(
                matches!(error, AppError::UpstreamRateLimited("wormhole")),
                "{error:?}"
            );
            assert!(error.is_transient(), "{error:?} should be transient");
        }
    }

    #[test]
    fn reads_the_destination_tx() {
        let p = json!({"destinationTx": {"txHash": "0xdest"}});
        assert_eq!(destination_tx_from_global_tx(&p).as_deref(), Some("0xdest"));
    }

    #[test]
    fn missing_or_empty_is_none() {
        for p in [
            json!({}),
            json!({"destinationTx": {}}),
            json!({"destinationTx": {"txHash": ""}}),
            json!({"destinationTx": {"txHash": "  "}}),
        ] {
            assert_eq!(destination_tx_from_global_tx(&p), None);
        }
    }

    #[test]
    fn reads_the_vaa_source_tx() {
        let p = json!({"data": {"txHash": "715b25c9bba7"}});
        assert_eq!(source_tx_from_vaa(&p).as_deref(), Some("715b25c9bba7"));
    }

    #[test]
    fn a_vaa_without_a_source_tx_is_none() {
        for p in [
            json!({}),
            json!({"data": {}}),
            json!({"data": {"txHash": ""}}),
            json!({"data": {"txHash": "  "}}),
        ] {
            assert_eq!(source_tx_from_vaa(&p), None);
        }
    }
}
