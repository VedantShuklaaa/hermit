//! Minimal Ethereum JSON-RPC client for the log watcher. Everything goes through the
//! shared EVM limiter, so retries cannot exceed the plan's request rate.

use crate::clients::helius::{json_rpc_error_is_retryable, status_is_retryable};
use crate::clients::limiter::{Limiter, UpstreamLabels, call_upstream};
use crate::error::AppError;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{Value, json};
use std::sync::Arc;

pub const PROVIDER: &str = "evm";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub number: u64,
    pub hash: String,
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log {
    pub address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u32,
    pub removed: bool,
}

#[derive(Clone)]
pub struct EvmClient {
    client: Client,
    url: String,
    limiter: Arc<Limiter>,
}

impl EvmClient {
    pub fn new(client: Client, url: String, limiter: Arc<Limiter>) -> Self {
        Self {
            client,
            url,
            limiter,
        }
    }

    pub async fn block_number(&self) -> Result<u64, AppError> {
        let labels = UpstreamLabels::evm();
        let result = call_upstream(&self.limiter, labels, || {
            self.rpc("eth_blockNumber", json!([]))
        })
        .await?;
        result
            .as_str()
            .and_then(parse_quantity)
            .ok_or_else(|| bad("eth_blockNumber: unexpected result shape"))
    }

    pub async fn block_by_number(&self, number: u64) -> Result<Block, AppError> {
        self.block(json!([format!("0x{number:x}"), false])).await
    }

    pub async fn finalized_block(&self) -> Result<Block, AppError> {
        self.block(json!(["finalized", false])).await
    }

    async fn block(&self, params: Value) -> Result<Block, AppError> {
        let labels = UpstreamLabels::evm();
        let result = call_upstream(&self.limiter, labels, || {
            self.rpc("eth_getBlockByNumber", params.clone())
        })
        .await?;
        parse_block(&result)
    }

    /// Logs in `[from, to]` from either bridge contract, matching either event topic.
    pub async fn logs(
        &self,
        from: u64,
        to: u64,
        addresses: &[&str],
        topics: &[&str],
    ) -> Result<Vec<Log>, AppError> {
        let params = json!([{
            "fromBlock": format!("0x{from:x}"),
            "toBlock": format!("0x{to:x}"),
            "address": addresses,
            "topics": [topics],
        }]);
        let labels = UpstreamLabels::evm();
        let result = call_upstream(&self.limiter, labels, || {
            self.rpc("eth_getLogs", params.clone())
        })
        .await?;

        let values = result
            .as_array()
            .ok_or_else(|| bad("eth_getLogs: result is not an array"))?;
        Ok(values.iter().filter_map(parse_log).collect())
    }

    async fn rpc(&self, method: &'static str, params: Value) -> Result<Value, AppError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let resp = self.client.post(&self.url).json(&body).send().await?;
        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(AppError::UpstreamRateLimited(PROVIDER));
        }

        // A JSON-RPC error can arrive with a non-2xx status: Alchemy answers an
        // over-wide `eth_getLogs` with HTTP 400. Read the body before deciding the
        // request failed, so that answer can still halve the range.
        let payload: Value = match resp.json().await {
            Ok(payload) => payload,
            Err(error) => {
                if status.is_success() {
                    return Err(error.into());
                }
                return Err(upstream_status_error(status));
            }
        };

        if let Some(error) = payload.get("error") {
            // Only eth_getLogs can be too large. A 429, timeout or unknown error is
            // left alone: the watcher keeps its current range and backs off.
            if method == "eth_getLogs" && is_range_too_large_error(error) {
                return Err(AppError::UpstreamRangeTooLarge(PROVIDER));
            }
            return Err(AppError::UpstreamProvider {
                provider: PROVIDER,
                message: error.to_string(),
                retryable: json_rpc_error_is_retryable(error),
            });
        }

        if !status.is_success() {
            return Err(upstream_status_error(status));
        }

        Ok(payload.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn upstream_status_error(status: reqwest::StatusCode) -> AppError {
    AppError::UpstreamProvider {
        provider: PROVIDER,
        message: format!("status {status}"),
        retryable: status_is_retryable(status),
    }
}

/// A genuine range-too-large answer: the message names the block range and says it is
/// over a limit. Alchemy's free tier answers HTTP 400 with `-32600` and
/// `Under the Free tier plan ... up to a 10 block range`; other plans answer with
/// `-32602` and `invalid argument 0: eth_getLogs is limited to a 2,000 block range`.
/// A generic invalid-params answer, a timeout, or an unknown error must not halve the
/// range.
pub(crate) fn is_range_too_large_error(error: &Value) -> bool {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    let mentions_range = message.contains("block range") || message.contains("eth_getlogs");
    let too_large = [
        "too large",
        "too many",
        "exceed",
        "limit",
        "maximum",
        "more than",
        "up to",
    ]
    .iter()
    .any(|needle| message.contains(needle));

    (mentions_range && too_large) || (code == Some(-32602) && mentions_range)
}

fn parse_block(value: &Value) -> Result<Block, AppError> {
    let number = value
        .get("number")
        .and_then(Value::as_str)
        .and_then(parse_quantity)
        .ok_or_else(|| bad("eth_getBlockByNumber: block without a number"))?;
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| bad("eth_getBlockByNumber: block without a hash"))?;
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_quantity)
        .and_then(|seconds| i64::try_from(seconds).ok())
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0));

    Ok(Block {
        number,
        hash: hash.to_string(),
        timestamp,
    })
}

/// One log, or `None` for a malformed entry. A bad log is skipped rather than failing
/// the whole range; the next poll would hit the same entry.
fn parse_log(value: &Value) -> Option<Log> {
    let address = value.get("address")?.as_str()?.to_string();
    let topics = value
        .get("topics")?
        .as_array()?
        .iter()
        .map(|topic| topic.as_str().map(String::from))
        .collect::<Option<Vec<_>>>()?;
    let data = value
        .get("data")
        .and_then(Value::as_str)
        .map(strip_0x)
        .and_then(|raw| hex::decode(raw).ok())
        .unwrap_or_default();
    let block_number = value
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_quantity)?;
    let block_hash = value.get("blockHash")?.as_str()?.to_string();
    let tx_hash = value.get("transactionHash")?.as_str()?.to_string();
    let log_index = value
        .get("logIndex")
        .and_then(Value::as_str)
        .and_then(parse_quantity)
        .and_then(|index| u32::try_from(index).ok())?;
    let removed = value
        .get("removed")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Some(Log {
        address,
        topics,
        data,
        block_number,
        block_hash,
        tx_hash,
        log_index,
        removed,
    })
}

fn parse_quantity(raw: &str) -> Option<u64> {
    let raw = strip_0x(raw);
    if raw.is_empty() {
        return None;
    }
    u64::from_str_radix(raw, 16).ok()
}

fn strip_0x(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

fn bad(message: &str) -> AppError {
    AppError::UpstreamProvider {
        provider: PROVIDER,
        message: message.to_string(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alchemys_free_tier_range_error_halves_the_range() {
        for error in [
            json!({
                "code": -32602,
                "message": "invalid argument 0: eth_getLogs is limited to a 2,000 block range"
            }),
            // Alchemy's current free-tier answer, delivered with HTTP 400.
            json!({
                "code": -32600,
                "message": "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range"
            }),
        ] {
            assert!(is_range_too_large_error(&error), "{error}");
        }
    }

    #[test]
    fn another_range_limit_phrasing_is_recognised() {
        for message in [
            "block range too large",
            "eth_getLogs block range exceeds the maximum",
            "query block range is more than allowed",
        ] {
            assert!(
                is_range_too_large_error(&json!({"code": -32005, "message": message})),
                "{message}"
            );
        }
    }

    #[test]
    fn timeouts_rate_limits_and_unknown_errors_do_not_halve_the_range() {
        for error in [
            json!({"code": -32000, "message": "execution timeout"}),
            json!({"code": -32000, "message": "internal error"}),
            json!({"code": -32005, "message": "rate limit exceeded"}),
            json!({"code": -32602, "message": "invalid argument 0: cannot unmarshal"}),
            json!({"code": -32600, "message": "invalid request"}),
            json!({"message": "unknown"}),
        ] {
            assert!(
                !is_range_too_large_error(&error),
                "{error} must not halve the range"
            );
        }
    }

    #[test]
    fn quantities_and_blocks_parse_from_hex() {
        assert_eq!(parse_quantity("0x10"), Some(16));
        assert_eq!(parse_quantity("10"), Some(16));
        assert_eq!(parse_quantity("0x"), None);
        assert_eq!(parse_quantity("nope"), None);

        let block = parse_block(&json!({
            "number": "0x10",
            "hash": "0xabc",
            "timestamp": "0x5f5e100"
        }))
        .expect("block");
        assert_eq!(block.number, 16);
        assert_eq!(block.hash, "0xabc");
        assert_eq!(block.timestamp.map(|t| t.timestamp()), Some(100_000_000));
    }

    #[test]
    fn a_log_without_its_required_fields_is_skipped() {
        let value = json!({
            "address": "0xabc",
            "topics": ["0x1"],
            "blockNumber": "0x1",
            "blockHash": "0xhash",
            "transactionHash": "0xtx",
            "logIndex": "0x0"
        });
        assert!(parse_log(&value).is_some());

        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("transactionHash");
        assert!(parse_log(&missing).is_none());
    }
}
