//! Wormhole Native Token Transfers (NTT) outbound adapter for Solana. Pure: no IO,
//! no clock, no database.
//!
//! Verification sources:
//!
//! * instruction names and layout — the `ntt_transceiver` IDL in
//!   <https://github.com/wormhole-foundation/native-token-transfers/tree/main/solana/programs/ntt-transceiver>
//!   (`release_wormhole_outbound`, `transfer_burn`, `transfer_lock`)
//! * instruction discriminators — Anchor's `sha256("global:<snake_case>")[..8]`,
//!   confirmed against two real mainnet transactions
//! * wire format — `wormhole-io/src/messages/transceiver.rs` and
//!   `wormhole-io/src/messages/ntt.rs` (TransceiverMessage, NttManagerMessage,
//!   NativeTokenTransfer)
//! * deployments — WalletConnect's
//!   <https://github.com/WalletConnect/contracts/blob/main/ntt/mainnet_deployment.json>
//!   and Sky's
//!   <https://github.com/sky-ecosystem/sky-ntt-migration/blob/main/docs/UserGuide.md>
//!
//! NTT is per-token: each deployment has its own manager program and a core emitter
//! PDA derived as `[b"emitter"]`. In the verified deployments the manager program
//! hosts `release_wormhole_outbound` and derives that PDA, so the configured
//! program IDs are manager programs, not a shared NTT program. A deployment that
//! moves the transceiver into a separate program must configure that program ID
//! instead. Only `release_wormhole_outbound` is decoded: it posts the transfer
//! message through the core bridge, which gives a message identity in the same
//! namespace as the Token Bridge. The adapter takes the emitter address from the
//! core bridge `PostMessage` accounts instead of re-deriving it, so the key matches
//! the signed VAA. `transfer_burn` and `transfer_lock` are part of the same flow but
//! carry no message, and every other instruction is admin or the inbound receive
//! path.
//!
//! Inbound NTT (`receive_wormhole_message`) is not indexed yet.

use super::{SOLANA, pair_post_messages};
use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, MessageId, NTT_PROGRAM, Protocol,
    Reject, SolanaTx, TokenRef,
};
use crate::vaa::decode::decode_destination_address;

/// Anchor discriminator for `release_wormhole_outbound`.
const RELEASE_WORMHOLE_OUTBOUND: [u8; 8] = [0xca, 0x57, 0x33, 0xad, 0x8e, 0xa0, 0xbc, 0xcc];
/// Anchor discriminator for `transfer_burn` (the first step of a burn-mode send).
const TRANSFER_BURN: [u8; 8] = [0x4b, 0x90, 0x1a, 0xe8, 0x27, 0x0c, 0x4b, 0xde];
/// Anchor discriminator for `transfer_lock` (the first step of a lock-mode send).
const TRANSFER_LOCK: [u8; 8] = [0xb3, 0x9e, 0x92, 0x94, 0x97, 0x2e, 0xb0, 0xc8];

/// TransceiverMessage prefix: `0x9945FF10`.
const TRANSCEIVER_PREFIX: [u8; 4] = [0x99, 0x45, 0xFF, 0x10];
/// NativeTokenTransfer prefix: `0x994E5454`.
const NATIVE_TRANSFER_PREFIX: [u8; 4] = [0x99, 0x4E, 0x54, 0x54];

/// `TRIMMED_DECIMALS` in the NTT programs: the 8-decimal cap applied to amounts.
const MAX_DECIMALS: u8 = 8;

/// Prefix and the two 32-byte manager addresses.
const MANAGER_MESSAGE_AT: usize = 4 + 32 + 32;
/// NttManagerMessage: message id and sender, then the payload length.
const NATIVE_TRANSFER_AT: usize = 32 + 32;
/// Fixed NativeTokenTransfer: prefix, decimals, amount, source token, recipient,
/// recipient chain.
const NATIVE_TRANSFER_FIXED: usize = 4 + 1 + 8 + 32 + 32 + 2;

/// NTT adapter. Each configured program is one deployment's manager program; the
/// list is opt-in because there is no shared NTT program ID.
pub struct NttAdapter {
    programs: Vec<String>,
}

impl NttAdapter {
    pub fn new(programs: Vec<String>) -> Self {
        Self { programs }
    }
}

impl BridgeAdapter for NttAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Wormhole
    }

    /// NTT has no fixed default: every deployment is its own manager program. The
    /// registry registers this adapter with explicit IDs; the empty slice keeps a
    /// plain `register` from subscribing to anything unintended.
    fn default_program_ids(&self) -> &'static [&'static str] {
        &[]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let ntt_ixs: Vec<usize> = tx
            .instructions
            .iter()
            .enumerate()
            .filter(|(_, ix)| self.programs.iter().any(|p| p == &ix.program_id))
            .map(|(pos, _)| pos)
            .collect();
        if ntt_ixs.is_empty() {
            return Classification::Rejected(Reject::NotTransfer("no ntt instruction"));
        }

        let mut outbound = Vec::new();
        let mut prepared = false;
        let mut other = false;
        for &pos in &ntt_ixs {
            let Some(discriminator) = tx.instructions[pos].data.get(..8) else {
                return Classification::Rejected(Reject::Malformed("ntt instruction without data"));
            };
            if discriminator == RELEASE_WORMHOLE_OUTBOUND {
                outbound.push(pos);
            } else if discriminator == TRANSFER_BURN || discriminator == TRANSFER_LOCK {
                // The burn or lock step of a send. It moves tokens but posts no
                // message, so on its own it is not an indexed transfer.
                prepared = true;
            } else {
                other = true;
            }
        }

        if outbound.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(if prepared {
                "ntt transfer_burn or transfer_lock is not a message"
            } else if other {
                "ntt instruction is not a release_wormhole_outbound"
            } else {
                "no release_wormhole_outbound instruction"
            }));
        }

        match decode_outbound(tx, &outbound) {
            Ok(transfers) => Classification::Transfers(transfers),
            Err(reject) => Classification::Rejected(reject),
        }
    }
}

fn decode_outbound(tx: &SolanaTx, outbound: &[usize]) -> Result<Vec<DetectedTransfer>, Reject> {
    let messages = pair_post_messages(tx, outbound, "no PostMessage under the ntt instruction")?;
    let mut transfers = Vec::with_capacity(messages.len());

    for (event_index, message) in messages.into_iter().enumerate() {
        let decoded = parse_payload(message.payload)?;
        let to_hex = hex::encode(decoded.to);

        transfers.push(DetectedTransfer {
            program: NTT_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: SOLANA,
                emitter_address: message.emitter_address,
                sequence: message.sequence,
            },
            direction: Direction::Outbound,
            observed_tx: tx.signature.clone(),
            event_index: event_index as u16,
            source: RawChainRef::wormhole(SOLANA),
            destination: RawChainRef::wormhole(decoded.to_chain),
            source_wallet: tx.signer.clone(),
            destination_wallet: decode_destination_address(decoded.to_chain, &to_hex),
            token: Some(TokenRef {
                chain: RawChainRef::wormhole(SOLANA),
                address: bs58::encode(decoded.source_token).into_string(),
            }),
            token_symbol: None,
            amount_raw: Some(RawAmount::from_u128(u128::from(decoded.amount))),
            via: None,
        });
    }

    Ok(transfers)
}

struct NativeTransfer {
    amount: u64,
    source_token: [u8; 32],
    to: [u8; 32],
    to_chain: u16,
}

/// The transfer inside a transceiver payload:
///
/// ```text
/// TransceiverMessage
///   0    4   prefix 0x9945FF10
///   4   32   source NTT manager
///  36   32   recipient NTT manager
///  68    2   manager message length (u16 BE)
///  70    n   NttManagerMessage
///             0   32   message id
///            32   32   sender
///            64    2   native transfer length (u16 BE)
///            66    m   NativeTokenTransfer
///                       0    4   prefix 0x994E5454
///                       4    1   decimals
///                       5    8   amount, trimmed (u64 BE)
///                      13   32   source token
///                      45   32   recipient
///                      77    2   recipient chain (u16 BE)
///                      79    n   optional additional payload (u16 length + bytes)
/// ```
fn parse_payload(p: &[u8]) -> Result<NativeTransfer, Reject> {
    if p.get(..4) != Some(&TRANSCEIVER_PREFIX) {
        return Err(Reject::NotTransfer(
            "payload is not an ntt transceiver message",
        ));
    }

    let manager_len = read_u16(p, MANAGER_MESSAGE_AT)? as usize;
    let manager_at = MANAGER_MESSAGE_AT + 2;
    let manager = p
        .get(manager_at..manager_at + manager_len)
        .ok_or(Reject::Malformed("ntt manager message is truncated"))?;

    let native_len = read_u16(manager, NATIVE_TRANSFER_AT)? as usize;
    let native_at = NATIVE_TRANSFER_AT + 2;
    let native = manager
        .get(native_at..native_at + native_len)
        .ok_or(Reject::Malformed("ntt native token transfer is truncated"))?;

    if native.get(..4) != Some(&NATIVE_TRANSFER_PREFIX) {
        return Err(Reject::Malformed(
            "ntt payload is not a native token transfer",
        ));
    }
    if native.len() < NATIVE_TRANSFER_FIXED {
        return Err(Reject::Malformed("ntt native token transfer is truncated"));
    }

    let decimals = native[4];
    if decimals > MAX_DECIMALS {
        return Err(Reject::Malformed("ntt amount has more than 8 decimals"));
    }
    let amount = u64::from_be_bytes(
        native[5..13]
            .try_into()
            .map_err(|_| Reject::Malformed("ntt amount is truncated"))?,
    );

    Ok(NativeTransfer {
        amount,
        source_token: word(native, 13)?,
        to: word(native, 45)?,
        to_chain: u16::from_be_bytes([native[77], native[78]]),
    })
}

fn read_u16(data: &[u8], at: usize) -> Result<u16, Reject> {
    let bytes = data
        .get(at..at + 2)
        .ok_or(Reject::Malformed("ntt payload is truncated"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn word(data: &[u8], at: usize) -> Result<[u8; 32], Reject> {
    data.get(at..at + 32)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(Reject::Malformed("ntt transfer is truncated"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::Instruction;
    use crate::protocols::registry::ProtocolRegistry;
    use crate::protocols::wormhole::{CORE_BRIDGE, CORE_POST_MESSAGE};
    use std::sync::Arc;

    const MANAGER: &str = "nttLq3ZsKu9uAFWuBGksdZTPAjkMRyPkxp61CJPsntA";
    const USER: &str = "UserWallet1111111111111111111111111111111111";
    const OTHER_PROGRAM: &str = "RouterProgram11111111111111111111111111111";

    fn emitter(n: u8) -> String {
        bs58::encode([n; 32]).into_string()
    }

    fn ix(program: &str, accounts: &[&str], data: Vec<u8>, inner_of: Option<usize>) -> Instruction {
        Instruction {
            program_id: program.into(),
            accounts: accounts.iter().map(|s| s.to_string()).collect(),
            data,
            parsed: None,
            inner_of,
        }
    }

    fn release_data() -> Vec<u8> {
        let mut data = RELEASE_WORMHOLE_OUTBOUND.to_vec();
        data.push(0); // the instruction's bool argument
        data
    }

    fn transfer_burn_data() -> Vec<u8> {
        let mut data = TRANSFER_BURN.to_vec();
        data.extend_from_slice(&1_000u64.to_be_bytes());
        data
    }

    fn native_payload(
        decimals: u8,
        amount: u64,
        source_token: [u8; 32],
        to: [u8; 32],
        to_chain: u16,
    ) -> Vec<u8> {
        let mut native = NATIVE_TRANSFER_PREFIX.to_vec();
        native.push(decimals);
        native.extend_from_slice(&amount.to_be_bytes());
        native.extend_from_slice(&source_token);
        native.extend_from_slice(&to);
        native.extend_from_slice(&to_chain.to_be_bytes());
        // Optional additional payload: a zero length, as real deployments write.
        native.extend_from_slice(&0u16.to_be_bytes());

        let mut manager = vec![0x11; 32]; // id
        manager.extend_from_slice(&[0x22; 32]); // sender
        manager.extend_from_slice(&(native.len() as u16).to_be_bytes());
        manager.extend_from_slice(&native);

        let mut payload = TRANSCEIVER_PREFIX.to_vec();
        payload.extend_from_slice(&[0x33; 32]); // source manager
        payload.extend_from_slice(&[0x44; 32]); // recipient manager
        payload.extend_from_slice(&(manager.len() as u16).to_be_bytes());
        payload.extend_from_slice(&manager);
        payload.extend_from_slice(&0u16.to_be_bytes()); // transceiver payload length
        payload
    }

    fn post_message_data(payload: &[u8]) -> Vec<u8> {
        let mut data = vec![CORE_POST_MESSAGE];
        data.extend_from_slice(&7u32.to_le_bytes()); // nonce
        data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        data.extend_from_slice(payload);
        data.push(1); // consistency level
        data
    }

    fn core_accounts(emitter: &str) -> [&str; 9] {
        [
            "bridge", "message", emitter, "sequence", USER, "fees", "clock", "rent", "system",
        ]
    }

    fn logs_for(sequence: u64) -> Vec<String> {
        vec![
            format!("Program {MANAGER} invoke [1]"),
            format!("Program {MANAGER} success"),
            format!("Program {MANAGER} invoke [1]"),
            format!("Program {CORE_BRIDGE} invoke [2]"),
            format!("Program log: Sequence: {sequence}"),
            format!("Program {CORE_BRIDGE} success"),
            format!("Program {MANAGER} success"),
        ]
    }

    fn tx(instructions: Vec<Instruction>, logs: Vec<String>) -> SolanaTx {
        SolanaTx {
            signature: "sig".into(),
            slot: 1,
            block_time: None,
            failed: false,
            signer: Some(USER.into()),
            account_keys: vec![],
            accounts: Default::default(),
            instructions,
            logs,
        }
    }

    /// A burn-mode send: transfer_burn first, then release_wormhole_outbound with the
    /// core bridge PostMessage under it.
    fn simple_release(payload: Vec<u8>, sequence: u64) -> SolanaTx {
        let e = emitter(9);
        tx(
            vec![
                ix(MANAGER, &[USER], transfer_burn_data(), None),
                ix(MANAGER, &[USER, &e], release_data(), None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e),
                    post_message_data(&payload),
                    Some(1),
                ),
            ],
            logs_for(sequence),
        )
    }

    fn adapter() -> NttAdapter {
        NttAdapter::new(vec![MANAGER.to_string()])
    }

    fn only_transfer(c: Classification) -> DetectedTransfer {
        match c {
            Classification::Transfers(mut v) if v.len() == 1 => v.remove(0),
            other => panic!("expected one transfer, got {other:?}"),
        }
    }

    #[test]
    fn decodes_a_release_outbound_and_ignores_transfer_burn() {
        let token = [0x07; 32];
        let to = [0x2b; 32];
        let t = only_transfer(adapter().classify(&simple_release(
            native_payload(8, 15_585_875_623, token, to, 30),
            439,
        )));

        assert_eq!(t.program, NTT_PROGRAM);
        assert_eq!(t.direction, Direction::Outbound);
        assert_eq!(t.observed_tx, "sig");
        assert_eq!(t.event_index, 0);
        assert_eq!(t.source, RawChainRef::wormhole(1));
        assert_eq!(t.destination, RawChainRef::wormhole(30));
        assert_eq!(t.source_wallet.as_deref(), Some(USER));
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(format!("0x{}", "2b".repeat(20)).as_str())
        );
        assert_eq!(t.amount_raw.unwrap().as_str(), "15585875623");
        assert_eq!(t.token_symbol, None);
        assert_eq!(t.via, None);
        let token_ref = t.token.unwrap();
        assert_eq!(token_ref.chain, RawChainRef::wormhole(1));
        assert_eq!(token_ref.address, bs58::encode(token).into_string());
        assert_eq!(
            t.message,
            MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 439,
            }
        );
    }

    #[test]
    fn a_payload_origin_other_than_solana_is_still_the_solana_token_account() {
        // The source token is always a Solana mint/key, whatever the destination.
        let t = only_transfer(adapter().classify(&simple_release(
            native_payload(6, 1, [0xAA; 32], [0xBB; 32], 2),
            1,
        )));
        assert_eq!(t.token.unwrap().chain, RawChainRef::wormhole(1));
        assert_eq!(t.destination, RawChainRef::wormhole(2));
    }

    #[test]
    fn transfer_burn_or_lock_alone_is_not_a_message() {
        for data in [transfer_burn_data(), {
            let mut d = TRANSFER_LOCK.to_vec();
            d.extend_from_slice(&1_000u64.to_be_bytes());
            d
        }] {
            let t = tx(vec![ix(MANAGER, &[USER], data, None)], vec![]);
            assert_eq!(
                adapter().classify(&t),
                Classification::Rejected(Reject::NotTransfer(
                    "ntt transfer_burn or transfer_lock is not a message"
                ))
            );
        }
    }

    #[test]
    fn an_admin_instruction_is_rejected() {
        // Any manager instruction that is not a release is admin or an inbound path.
        let t = tx(vec![ix(MANAGER, &[USER], vec![0x00; 8], None)], vec![]);
        assert!(matches!(
            adapter().classify(&t),
            Classification::Rejected(Reject::NotTransfer(_))
        ));
    }

    #[test]
    fn another_program_is_not_an_ntt_instruction() {
        use crate::protocols::wormhole::TOKEN_BRIDGE;

        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![5], None)], vec![]);
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::NotTransfer("no ntt instruction"))
        );
    }

    #[test]
    fn a_top_level_release_also_pairs() {
        // The real manager invokes the transceiver logic directly, so release is
        // top-level; this keeps the same shape as the captured fixture.
        let e = emitter(9);
        let t = tx(
            vec![
                ix(MANAGER, &[USER, &e], release_data(), None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e),
                    post_message_data(&native_payload(8, 5, [1; 32], [2; 32], 30)),
                    Some(0),
                ),
            ],
            logs_for(7),
        );
        let t = only_transfer(adapter().classify(&t));
        assert!(matches!(t.message, MessageId::Wormhole { sequence: 7, .. }));
    }

    #[test]
    fn unknown_or_empty_instruction_data_is_malformed() {
        for data in [vec![], vec![0xCA, 0x57, 0x33]] {
            let t = tx(vec![ix(MANAGER, &[USER], data, None)], vec![]);
            assert!(matches!(
                adapter().classify(&t),
                Classification::Rejected(Reject::Malformed(_))
            ));
        }
    }

    #[test]
    fn a_failed_transaction_is_never_a_transfer() {
        let mut t = simple_release(native_payload(8, 1, [1; 32], [2; 32], 30), 1);
        t.failed = true;
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn a_missing_sequence_log_is_malformed_not_a_guess() {
        let mut t = simple_release(native_payload(8, 1, [1; 32], [2; 32], 30), 1);
        t.logs.clear();
        assert!(matches!(
            adapter().classify(&t),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn malformed_payloads_are_rejected_without_panicking() {
        let good = native_payload(8, 1, [1; 32], [2; 32], 30);
        let native_at = MANAGER_MESSAGE_AT + 2 + NATIVE_TRANSFER_AT + 2;

        let mut wrong_transceiver_prefix = good.clone();
        wrong_transceiver_prefix[0] = 0;
        let mut wrong_native_prefix = good.clone();
        wrong_native_prefix[native_at] = 0;
        let mut too_many_decimals = good.clone();
        too_many_decimals[native_at + 4] = 9;

        for bad in [
            vec![],
            vec![0x99, 0x45, 0xFF, 0x10],
            good[..67].to_vec(),
            wrong_transceiver_prefix,
            wrong_native_prefix,
            too_many_decimals,
        ] {
            let c = adapter().classify(&simple_release(bad, 1));
            assert!(matches!(c, Classification::Rejected(_)), "{c:?}");
        }
    }

    #[test]
    fn a_non_ntt_payload_is_not_a_transfer() {
        let c = adapter().classify(&simple_release(vec![2; 200], 1));
        assert!(matches!(
            c,
            Classification::Rejected(Reject::NotTransfer(_))
        ));
    }

    #[test]
    fn the_discriminators_match_the_verified_values() {
        assert_eq!(hex::encode(RELEASE_WORMHOLE_OUTBOUND), "ca5733ad8ea0bccc");
        assert_eq!(hex::encode(TRANSFER_BURN), "4b901ae8270c4bde");
        assert_eq!(hex::encode(TRANSFER_LOCK), "b39e9294972eb0c8");
    }

    #[test]
    fn registers_under_every_configured_manager_program() {
        let mut r = ProtocolRegistry::new();
        r.register_with_programs(
            Arc::new(NttAdapter::new(vec![MANAGER.to_string()])),
            vec![MANAGER.to_string()],
        );
        assert_eq!(r.program_ids(), vec![MANAGER.to_string()]);
        assert_eq!(r.protocols_for(MANAGER), vec![Protocol::Wormhole]);
    }

    #[test]
    fn does_not_subscribe_to_a_default_program() {
        assert!(adapter().default_program_ids().is_empty());
    }

    #[test]
    fn an_unrelated_post_message_does_not_steal_the_pairing() {
        let (router_e, ntt_e) = (emitter(5), emitter(9));
        let t = tx(
            vec![
                ix(OTHER_PROGRAM, &[USER], vec![0], None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&router_e),
                    post_message_data(&[1, 2, 3]),
                    Some(0),
                ),
                ix(MANAGER, &[USER, &ntt_e], release_data(), Some(0)),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&ntt_e),
                    post_message_data(&native_payload(8, 77, [1; 32], [2; 32], 30)),
                    Some(0),
                ),
            ],
            vec![
                format!("Program {OTHER_PROGRAM} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 10".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {MANAGER} invoke [2]"),
                format!("Program {CORE_BRIDGE} invoke [3]"),
                "Program log: Sequence: 11".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {MANAGER} success"),
                format!("Program {OTHER_PROGRAM} success"),
            ],
        );
        let t = only_transfer(adapter().classify(&t));
        assert!(matches!(
            t.message,
            MessageId::Wormhole { sequence: 11, .. }
        ));
        assert_eq!(t.amount_raw.unwrap().as_str(), "77");
    }

    #[test]
    fn the_native_transfer_layout_is_pinned() {
        // 79 fixed bytes: 4 prefix, 1 decimals, 8 amount, 32 source token, 32 to, 2 chain.
        assert_eq!(NATIVE_TRANSFER_FIXED, 79);
        assert_eq!(NATIVE_TRANSFER_AT + 2 + NATIVE_TRANSFER_FIXED, 145);
    }
}
