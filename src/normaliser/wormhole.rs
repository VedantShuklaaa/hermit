use serde_json::Value;

use crate::domain::bridge::BridgeEvent;
use crate::error::AppError;

/// A WormholeScan operation as the event the analyzer stores, or `None` when the
/// operation is not a classic Wormhole message at all.
///
/// WormholeScan also indexes operations that never touched the core bridge (CCTP
/// sends, executor requests). Their ids are synthetic (`"<signature>-0"`) and they
/// carry no VAA, so they cannot become a [`BridgeEvent`]. A Wormhole operation with a
/// malformed id is still an error.
pub fn normalise(raw: Value) -> Result<Option<BridgeEvent>, AppError> {
    let Some(sequence) = raw
        .get("sequence")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return if is_non_wormhole_operation(&raw) {
            Ok(None)
        } else {
            Err(AppError::Normalisation("missing sequence".into()))
        };
    };

    let emitter_chain = raw
        .get("emitterChain")
        .and_then(Value::as_u64)
        .ok_or_else(|| AppError::Normalisation("missing emitterChain".into()))?;

    let emitter_address = raw
        .get("emitterAddress")
        .and_then(|e| e.get("hex"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let props = raw
        .get("content")
        .and_then(|c| c.get("standarizedProperties")); // note: their spelling

    let target_chain = props
        .and_then(|p| p.get("toChain"))
        .and_then(Value::as_u64)
        .filter(|&c| c != 0)
        .map(|c| c.to_string());

    let amount = props
        .and_then(|p| p.get("amount"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from);

    Ok(Some(BridgeEvent {
        sequence,
        emitter_chain: emitter_chain.to_string(),
        emitter_address,
        target_chain,
        amount,
    }))
}

/// An operation named only by non-Wormhole apps, with no VAA, is one of the other
/// routes WormholeScan indexes (CCTP, the executor). A real Wormhole operation always
/// names a Portal or NTT app; an operation whose apps are missing or include one of
/// those is treated as a malformed Wormhole operation.
fn is_non_wormhole_operation(raw: &Value) -> bool {
    let apps: Vec<&str> = raw
        .pointer("/content/standarizedProperties/appIds")
        .and_then(Value::as_array)
        .map(|ids| ids.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    let only_non_wormhole_apps = !apps.is_empty()
        && apps.iter().all(|app| {
            !(*app == "WORMHOLE" || app.starts_with("PORTAL") || app.starts_with("NTT"))
        });
    let no_vaa = raw.get("vaa").is_none_or(Value::is_null);

    only_non_wormhole_apps && no_vaa
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cctp_operation() -> Value {
        serde_json::from_str(include_str!(
            "../../tests/fixtures/cctp_send/wormhole_cctp_send.json"
        ))
        .expect("cctp fixture")
    }

    #[test]
    fn a_cctp_operation_is_not_a_wormhole_transfer() {
        let operation = cctp_operation();
        assert_eq!(operation["sequence"], "synthetic-signature-0");
        assert!(matches!(normalise(operation), Ok(None)));
    }

    #[test]
    fn a_malformed_wormhole_operation_is_still_an_error() {
        let raw = json!({
            "sequence": "not-a-number",
            "emitterChain": 1,
            "emitterAddress": {"hex": "0x01"},
            "content": {"standarizedProperties": {"appIds": ["PORTAL_TOKEN_BRIDGE"]}},
            "vaa": {"raw": "AQID"},
        });
        assert!(matches!(
            normalise(raw),
            Err(AppError::Normalisation(reason)) if reason == "missing sequence"
        ));
    }

    #[test]
    fn a_classic_wormhole_operation_still_normalises() {
        let raw = json!({
            "sequence": "42",
            "emitterChain": 1,
            "emitterAddress": {"hex": "0x01"},
            "content": {"standarizedProperties": {"appIds": ["PORTAL_TOKEN_BRIDGE"], "toChain": 2, "amount": "10"}},
        });
        let event = normalise(raw).expect("classic operation").expect("event");
        assert_eq!(event.sequence, 42);
        assert_eq!(event.emitter_chain, "1");
        assert_eq!(event.target_chain.as_deref(), Some("2"));
        assert_eq!(event.amount.as_deref(), Some("10"));
    }
}
