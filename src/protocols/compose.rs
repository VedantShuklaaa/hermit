//! One transfer per message identity. When an outer protocol and an underlying
//! rail both describe the same transfer, it is indexed once and the router is
//! recorded as `via`. The database enforces the same rule with
//! `UNIQUE (protocol, message_key)`.

use super::DetectedTransfer;
use std::collections::HashMap;

/// Merges duplicates of the same `(protocol, message key)` within one transaction.
///
/// On-chain fields come from the first observation, so adapters of the underlying
/// rail should be run first. `via` is the first `Some`. Order of first appearance
/// is kept.
pub fn compose(transfers: Vec<DetectedTransfer>) -> Vec<DetectedTransfer> {
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    let mut out: Vec<DetectedTransfer> = Vec::new();

    for t in transfers {
        let key = (t.message.protocol().as_str().to_string(), t.message.key());
        match index.get(&key) {
            Some(&i) => {
                let kept = &mut out[i];
                if kept.via.is_none() {
                    kept.via = t.via;
                }
                // A second observation may be the one that saw the delivery.
                kept.delivered |= t.delivered;
                // Fill gaps only; never overwrite what the rail's adapter decoded.
                if kept.source_wallet.is_none() {
                    kept.source_wallet = t.source_wallet;
                }
                if kept.destination_wallet.is_none() {
                    kept.destination_wallet = t.destination_wallet;
                }
                if kept.token.is_none() {
                    kept.token = t.token;
                }
                if kept.token_symbol.is_none() {
                    kept.token_symbol = t.token_symbol;
                }
                if kept.amount_raw.is_none() {
                    kept.amount_raw = t.amount_raw;
                }
            }
            None => {
                index.insert(key, out.len());
                out.push(t);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::RawChainRef;
    use crate::protocols::{Direction, MessageId, Via, amount::RawAmount};

    fn transfer(seq: u64) -> DetectedTransfer {
        DetectedTransfer {
            program: crate::protocols::TOKEN_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "aa".into(),
                sequence: seq,
            },
            direction: Direction::Outbound,
            delivered: false,
            observed_tx: "sig".into(),
            event_index: 0,
            source: RawChainRef::wormhole(1),
            destination: RawChainRef::wormhole(2),
            source_wallet: None,
            destination_wallet: None,
            token: None,
            token_symbol: None,
            amount_raw: None,
            via: None,
        }
    }

    #[test]
    fn same_message_seen_twice_is_one_transfer() {
        let rail = transfer(7);
        let mut outer = transfer(7);
        outer.via = Some(Via {
            name: "router".into(),
            reference: Some("order-1".into()),
        });

        let out = compose(vec![rail, outer]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].via.as_ref().unwrap().name, "router");
    }

    #[test]
    fn rail_fields_win_and_gaps_are_filled() {
        let mut rail = transfer(7);
        rail.amount_raw = RawAmount::parse("100");
        let mut outer = transfer(7);
        outer.amount_raw = RawAmount::parse("999");
        outer.destination_wallet = Some("dest".into());

        let out = compose(vec![rail, outer]);
        assert_eq!(out[0].amount_raw.as_ref().unwrap().as_str(), "100");
        assert_eq!(out[0].destination_wallet.as_deref(), Some("dest"));
    }

    #[test]
    fn different_messages_in_one_tx_stay_separate_and_ordered() {
        let mut a = transfer(1);
        a.event_index = 0;
        let mut b = transfer(2);
        b.event_index = 1;

        let out = compose(vec![a, b]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].event_index, 0);
        assert_eq!(out[1].event_index, 1);
    }

    #[test]
    fn empty_in_empty_out() {
        assert!(compose(vec![]).is_empty());
    }
}
