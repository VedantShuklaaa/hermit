//! Pure planning for the startup backfill: no network, no state.

use serde_json::Value;

use super::types::CandidateTransaction;

/// What one `getSignaturesForAddress` page contributes to the backfill.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackfillPage {
    /// Successful transactions newer than the checkpoint, newest first.
    pub candidates: Vec<CandidateTransaction>,
    /// The page reached the checkpoint (or a cap), so there is nothing older to fetch.
    pub done: bool,
    /// The page stopped because the next signature is older than the age cap.
    pub truncated_by_age: bool,
}

/// What one connection's backfill did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackfillStats {
    /// Signatures published to the stream.
    pub published: usize,
    /// Signatures the Redis dedupe suppressed because they were already published.
    pub skipped: usize,
}

/// A fresh database has checkpoint 0, and walking the whole history of a busy bridge program
/// from there would burn the Helius quota for nothing. Backfill only runs once there is a
/// checkpoint to resume from; live ingestion starts the record from "now".
pub fn should_backfill(checkpoint_slot: u64) -> bool {
    checkpoint_slot > 0
}

/// Picks the candidates out of one page, newest first, as Helius returns it.
///
/// Stops at the first signature at or below the cutoff, which is `checkpoint_slot`
/// rewound by `rewind_slots` (floored at 0). Rewinding re-reads the checkpoint's own
/// slot and the slots just before it, where transactions can be missed on a reconnect;
/// the Redis dedupe drops whatever was already published. Everything after the cutoff
/// is older, so the page is done. Failed transactions and entries without a slot or
/// signature are skipped, and no more than `remaining` candidates are returned.
///
/// `max_age_secs` also stops the page at the first signature whose `blockTime` is
/// older than `now_unix - max_age_secs`; `0` disables that check. A signature with
/// no `blockTime` cannot be judged, so it is kept. The input is newest first, so
/// everything after an over-age signature is older too.
pub fn plan_page(
    signatures: &[Value],
    checkpoint_slot: u64,
    rewind_slots: u64,
    remaining: usize,
    max_age_secs: u64,
    now_unix: u64,
) -> BackfillPage {
    let cutoff = checkpoint_slot.saturating_sub(rewind_slots);
    let mut page = BackfillPage::default();

    for info in signatures {
        let Some(slot) = info.get("slot").and_then(Value::as_u64) else {
            continue;
        };

        if slot <= cutoff {
            page.done = true;
            break;
        }

        if max_age_secs > 0
            && let Some(block_time) = info.get("blockTime").and_then(Value::as_u64)
            && now_unix.saturating_sub(block_time) > max_age_secs
        {
            page.done = true;
            page.truncated_by_age = true;
            break;
        }

        if info.get("err").is_some_and(|err| !err.is_null()) {
            continue;
        }

        let Some(signature) = info.get("signature").and_then(Value::as_str) else {
            continue;
        };

        if page.candidates.len() >= remaining {
            page.done = true;
            break;
        }

        page.candidates.push(CandidateTransaction {
            chain: "solana".to_string(),
            tx_hash: signature.to_string(),
            slot,
        });
    }

    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A fixed clock keeps age-cap tests deterministic. A zero cap disables the
    /// check in every test that is not about it.
    const NOW: u64 = 1_000_000;
    const NO_AGE_CAP: u64 = 0;

    fn sig(signature: &str, slot: u64) -> Value {
        json!({ "signature": signature, "slot": slot, "err": null, "blockTime": NOW })
    }

    fn sig_at(signature: &str, slot: u64, block_time: u64) -> Value {
        json!({ "signature": signature, "slot": slot, "err": null, "blockTime": block_time })
    }

    fn hashes(page: &BackfillPage) -> Vec<&str> {
        page.candidates.iter().map(|c| c.tx_hash.as_str()).collect()
    }

    #[test]
    fn fresh_database_does_not_backfill() {
        assert!(!should_backfill(0));
        assert!(should_backfill(1));
    }

    #[test]
    fn takes_everything_newer_than_the_checkpoint() {
        let page = plan_page(
            &[sig("c", 30), sig("b", 20), sig("a", 10)],
            5,
            0,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["c", "b", "a"]);
        assert!(!page.done);
    }

    #[test]
    fn stops_at_the_checkpoint_slot_itself_without_a_rewind() {
        let page = plan_page(
            &[sig("c", 30), sig("b", 20), sig("a", 10)],
            20,
            0,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["c"]);
        assert!(page.done);
    }

    #[test]
    fn includes_transactions_in_the_checkpoints_own_slot() {
        let page = plan_page(
            &[sig("e", 105), sig("d", 100), sig("c", 95), sig("b", 80)],
            100,
            20,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["e", "d", "c"]);
        assert!(page.done);
    }

    #[test]
    fn includes_the_whole_rewind_window_and_stops_at_its_edge() {
        let page = plan_page(
            &[sig("f", 100), sig("e", 99), sig("d", 90), sig("c", 81)],
            100,
            20,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["f", "e", "d", "c"]);
        assert!(!page.done);

        // Slot 80 is the rewound cutoff itself, one slot too old.
        let page = plan_page(
            &[sig("d", 90), sig("c", 80), sig("b", 79)],
            100,
            20,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["d"]);
        assert!(page.done);
    }

    #[test]
    fn a_checkpoint_below_the_rewind_floors_at_slot_zero() {
        let page = plan_page(
            &[sig("c", 10), sig("b", 5), sig("a", 0)],
            5,
            20,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["c", "b"]);
        assert!(page.done);
    }

    #[test]
    fn the_rewind_never_exceeds_the_remaining_cap() {
        let page = plan_page(
            &[sig("d", 100), sig("c", 99), sig("b", 90), sig("a", 81)],
            100,
            20,
            2,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["d", "c"]);
        assert!(page.done);
    }

    #[test]
    fn skips_failed_transactions() {
        let failed =
            json!({ "signature": "bad", "slot": 25, "err": { "InstructionError": [0, "Custom"] } });
        let page = plan_page(
            &[sig("c", 30), failed, sig("a", 10)],
            5,
            0,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["c", "a"]);
    }

    #[test]
    fn skips_entries_missing_a_slot_or_signature() {
        let no_slot = json!({ "signature": "x", "err": null });
        let no_signature = json!({ "slot": 40, "err": null });
        let page = plan_page(
            &[no_slot, no_signature, sig("a", 10)],
            5,
            0,
            100,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["a"]);
    }

    #[test]
    fn never_returns_more_than_remaining() {
        let page = plan_page(
            &[sig("c", 30), sig("b", 20), sig("a", 10)],
            5,
            0,
            2,
            NO_AGE_CAP,
            NOW,
        );

        assert_eq!(hashes(&page), ["c", "b"]);
        assert!(page.done);
    }

    #[test]
    fn exactly_remaining_candidates_is_not_yet_done() {
        let page = plan_page(&[sig("b", 20), sig("a", 10)], 5, 0, 2, NO_AGE_CAP, NOW);

        assert_eq!(hashes(&page), ["b", "a"]);
        assert!(!page.done);
    }

    #[test]
    fn nothing_remaining_returns_nothing() {
        let page = plan_page(&[sig("a", 10)], 5, 0, 0, NO_AGE_CAP, NOW);

        assert!(page.candidates.is_empty());
        assert!(page.done);
    }

    #[test]
    fn an_empty_page_is_not_done_by_itself() {
        let page = plan_page(&[], 5, 20, 10, NO_AGE_CAP, NOW);

        assert_eq!(page, BackfillPage::default());
    }

    #[test]
    fn candidates_carry_chain_and_slot() {
        let page = plan_page(&[sig("a", 10)], 5, 0, 10, NO_AGE_CAP, NOW);

        assert_eq!(page.candidates[0].chain, "solana");
        assert_eq!(page.candidates[0].slot, 10);
    }

    #[test]
    fn stops_at_the_first_signature_older_than_the_age_cap() {
        let page = plan_page(
            &[
                sig_at("new", 30, NOW - 10),
                sig_at("edge", 20, NOW - 60),
                sig_at("old", 10, NOW - 61),
            ],
            5,
            0,
            100,
            60,
            NOW,
        );

        // The signature exactly at the cap is still planned; the next one is not.
        assert_eq!(hashes(&page), ["new", "edge"]);
        assert!(page.done);
        assert!(page.truncated_by_age);
    }

    #[test]
    fn an_age_cap_of_zero_disables_the_check() {
        let page = plan_page(
            &[sig_at("very-old", 30, 1), sig_at("older", 20, 0)],
            5,
            0,
            100,
            0,
            NOW,
        );

        assert_eq!(hashes(&page), ["very-old", "older"]);
        assert!(!page.done);
        assert!(!page.truncated_by_age);
    }

    #[test]
    fn a_signature_without_a_block_time_is_kept() {
        let no_time = json!({ "signature": "unknown-age", "slot": 30, "err": null });
        let page = plan_page(&[no_time, sig_at("old", 20, NOW - 100)], 5, 0, 100, 60, NOW);

        assert_eq!(hashes(&page), ["unknown-age"]);
        assert!(page.truncated_by_age);
    }

    #[test]
    fn a_page_fully_inside_the_age_window_is_not_truncated() {
        let page = plan_page(
            &[sig_at("a", 30, NOW - 1), sig_at("b", 20, NOW - 2)],
            5,
            0,
            100,
            60,
            NOW,
        );

        assert_eq!(hashes(&page), ["a", "b"]);
        assert!(!page.done);
        assert!(!page.truncated_by_age);
    }
}
