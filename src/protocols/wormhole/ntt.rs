//! Wormhole Native Token Transfers (NTT) adapter for Solana, both directions. Pure: no
//! IO, no clock, no database.
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
//! carry no message, and every other instruction is admin.
//!
//! Inbound transfers are decoded from `receive_wormhole_message`. Its fourth account
//! is the posted VAA (owned by the core bridge), which the fetch step preloads after
//! asking [`BridgeAdapter::required_accounts`]. The VAA carries the TransceiverMessage
//! and NativeTokenTransfer, and its identity (emitter chain, emitter address, sequence)
//! is the same one the outbound side uses, so the two legs converge on one row.
//! `receive_wormhole_message` only attests the message; `redeem` attests it to the
//! transceiver and `release_inbound_mint` / `release_inbound_unlock` move the tokens.
//! A release in the receive's own tx marks the detection delivered, so the row is
//! Completed. A release in a later tx is not indexed, so that row stays Pending (see
//! the README's known gaps).

use super::{CORE_BRIDGE, SOLANA, exec_key, pair_post_messages, posted_vaa};
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
/// Anchor discriminator for `receive_wormhole_message` (the inbound attest step).
const RECEIVE_WORMHOLE_MESSAGE: [u8; 8] = [0x86, 0xd5, 0x8f, 0x44, 0xeb, 0x66, 0xe8, 0x60];
/// Anchor discriminator for `redeem` (validates an attested inbound message).
const REDEEM: [u8; 8] = [0xb8, 0x0c, 0x56, 0x95, 0x46, 0xc4, 0x61, 0xe1];
/// Anchor discriminator for `release_inbound_mint`.
const RELEASE_INBOUND_MINT: [u8; 8] = [0x0c, 0xe9, 0x4a, 0x41, 0x3c, 0xa8, 0x31, 0x78];
/// Anchor discriminator for `release_inbound_unlock`.
const RELEASE_INBOUND_UNLOCK: [u8; 8] = [0xb6, 0xa2, 0x3e, 0xce, 0xc5, 0x89, 0x53, 0x62];

/// Index of the posted VAA among `receive_wormhole_message` accounts:
/// payer, config, peer, vaa, transceiver_message, system_program.
const RECEIVE_VAA_ACCOUNT: usize = 3;

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
        let mut inbound = Vec::new();
        let mut prepared = false;
        let mut followup = false;
        let mut released = false;
        let mut other = false;
        for &pos in &ntt_ixs {
            let Some(discriminator) = tx.instructions[pos].data.get(..8) else {
                return Classification::Rejected(Reject::Malformed("ntt instruction without data"));
            };
            if discriminator == RELEASE_WORMHOLE_OUTBOUND {
                outbound.push(pos);
            } else if discriminator == RECEIVE_WORMHOLE_MESSAGE {
                inbound.push(pos);
            } else if discriminator == TRANSFER_BURN || discriminator == TRANSFER_LOCK {
                // The burn or lock step of a send. It moves tokens but posts no
                // message, so on its own it is not an indexed transfer.
                prepared = true;
            } else if discriminator == RELEASE_INBOUND_MINT
                || discriminator == RELEASE_INBOUND_UNLOCK
            {
                // The token-moving step of an already-attested inbound message. It
                // carries no message of its own; the receive is what is indexed, and
                // its presence in the same tx is what makes that row `Completed`.
                followup = true;
                released = true;
            } else if discriminator == REDEEM {
                // Attests the message to the transceiver. It moves no tokens, so on
                // its own it does not complete the receive.
                followup = true;
            } else {
                other = true;
            }
        }

        if outbound.is_empty() && inbound.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(if prepared {
                "ntt transfer_burn or transfer_lock is not a message"
            } else if followup {
                "ntt redeem or release_inbound is not a receive_wormhole_message"
            } else if other {
                "ntt instruction is neither a release_wormhole_outbound nor a receive_wormhole_message"
            } else {
                "no ntt message instruction"
            }));
        }

        let mut transfers = Vec::new();
        if !outbound.is_empty() {
            match decode_outbound(tx, &outbound) {
                Ok(found) => transfers.extend(found),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        if !inbound.is_empty() {
            match decode_inbound(tx, &inbound, transfers.len(), released) {
                Ok(found) => transfers.extend(found),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        Classification::Transfers(transfers)
    }

    fn required_accounts(&self, tx: &SolanaTx) -> Vec<String> {
        if tx.failed {
            return Vec::new();
        }
        let mut keys: Vec<String> = tx
            .instructions
            .iter()
            .filter(|ix| self.programs.iter().any(|p| p == &ix.program_id))
            .filter(|ix| ix.data.get(..8) == Some(RECEIVE_WORMHOLE_MESSAGE.as_slice()))
            .filter_map(|ix| ix.accounts.get(RECEIVE_VAA_ACCOUNT).cloned())
            .collect();
        keys.sort();
        keys.dedup();
        keys
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
            delivered: false,
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

/// Receives into Solana. The identity and transfer come from the posted VAA account
/// the instruction was given. `first_index` continues the event index after any
/// outbound transfers decoded from the same tx.
fn decode_inbound(
    tx: &SolanaTx,
    inbound: &[usize],
    first_index: usize,
    released: bool,
) -> Result<Vec<DetectedTransfer>, Reject> {
    let mut ordered = inbound.to_vec();
    ordered.sort_by_key(|&pos| exec_key(tx, pos));

    let mut transfers = Vec::with_capacity(ordered.len());
    for (n, &pos) in ordered.iter().enumerate() {
        let vaa_key = tx.instructions[pos]
            .accounts
            .get(RECEIVE_VAA_ACCOUNT)
            .ok_or(Reject::Malformed(
                "receive_wormhole_message without a VAA account",
            ))?;
        let account = tx.account(vaa_key).ok_or(Reject::Unsupported(
            "inbound receive: posted VAA account was not loaded",
        ))?;
        // The manager accepts only a core bridge account, so this is a consistency
        // check on what the fetch step returned.
        if account.owner != CORE_BRIDGE {
            return Err(Reject::Malformed(
                "VAA account is not owned by the core bridge",
            ));
        }
        let vaa = posted_vaa::parse(&account.data)
            .ok_or(Reject::Malformed("VAA account does not parse"))?;
        let decoded = parse_payload(vaa.payload)?;
        if decoded.to_chain != SOLANA {
            return Err(Reject::Malformed(
                "receive payload is not addressed to Solana",
            ));
        }

        let source_chain = vaa.emitter_chain;
        let to_hex = hex::encode(decoded.to);
        let sender_hex = hex::encode(decoded.sender);
        let token_hex = hex::encode(decoded.source_token);

        transfers.push(DetectedTransfer {
            program: NTT_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: source_chain,
                emitter_address: hex::encode(vaa.emitter_address),
                sequence: vaa.sequence,
            },
            direction: Direction::Inbound,
            // The receive only attests the message. The tokens moved here only if the
            // same tx also carries release_inbound_mint or release_inbound_unlock.
            delivered: released,
            observed_tx: tx.signature.clone(),
            event_index: (first_index + n) as u16,
            source: RawChainRef::wormhole(source_chain),
            destination: RawChainRef::wormhole(SOLANA),
            // The NttManagerMessage carries the sender on the origin chain.
            source_wallet: decode_destination_address(source_chain, &sender_hex),
            destination_wallet: decode_destination_address(SOLANA, &to_hex),
            // The payload names the token on the origin chain, as the Token Bridge
            // payload does. The local mint is the deployment's and is not in the VAA.
            token: decode_destination_address(source_chain, &token_hex).map(|address| TokenRef {
                chain: RawChainRef::wormhole(source_chain),
                address,
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
    /// The sender on the source chain, from the NttManagerMessage.
    sender: [u8; 32],
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
        sender: word(manager, 32)?,
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
    use crate::protocols::registry::ProtocolRegistry;
    use crate::protocols::wormhole::{CORE_BRIDGE, CORE_POST_MESSAGE, TOKEN_BRIDGE};
    use crate::protocols::{AccountData, Instruction};
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

    const VAA_KEY: &str = "PostedVaa1111111111111111111111111111111";

    fn receive_data() -> Vec<u8> {
        RECEIVE_WORMHOLE_MESSAGE.to_vec()
    }

    /// A core bridge posted VAA account around an NTT transceiver payload.
    fn posted_vaa_bytes(sequence: u64, chain: u16, emitter: [u8; 32], payload: &[u8]) -> Vec<u8> {
        let mut d = Vec::new();
        d.extend_from_slice(b"vaa");
        d.push(1); // version
        d.push(1); // consistency level
        d.extend_from_slice(&1u32.to_le_bytes()); // vaa time
        d.extend_from_slice(&[7; 32]); // signature set
        d.extend_from_slice(&2u32.to_le_bytes()); // submission time
        d.extend_from_slice(&3u32.to_le_bytes()); // nonce
        d.extend_from_slice(&sequence.to_le_bytes());
        d.extend_from_slice(&chain.to_le_bytes());
        d.extend_from_slice(&emitter);
        d.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        d.extend_from_slice(payload);
        d
    }

    fn receive_tx(payload: Vec<u8>, sequence: u64, chain: u16, emitter: [u8; 32]) -> SolanaTx {
        let mut t = tx(
            vec![ix(
                MANAGER,
                &[
                    USER,
                    "config",
                    "peer",
                    VAA_KEY,
                    "transceiverMessage",
                    "system",
                ],
                receive_data(),
                None,
            )],
            vec![],
        );
        t.accounts.insert(
            VAA_KEY.into(),
            AccountData {
                owner: CORE_BRIDGE.into(),
                data: posted_vaa_bytes(sequence, chain, emitter, &payload),
            },
        );
        t
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
        assert_eq!(hex::encode(RECEIVE_WORMHOLE_MESSAGE), "86d58f44eb66e860");
        assert_eq!(hex::encode(REDEEM), "b80c569546c461e1");
        assert_eq!(hex::encode(RELEASE_INBOUND_MINT), "0ce94a413ca83178");
        assert_eq!(hex::encode(RELEASE_INBOUND_UNLOCK), "b6a23ecec5895362");
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

    #[test]
    fn decodes_a_receive_wormhole_message_as_inbound() {
        let token = [0x07; 32];
        let to = [0x2b; 32];
        let t = only_transfer(adapter().classify(&receive_tx(
            native_payload(8, 15_585_875_623, token, to, 1),
            794,
            30,
            [0xAA; 32],
        )));

        assert_eq!(t.program, NTT_PROGRAM);
        assert_eq!(t.direction, Direction::Inbound);
        assert_eq!(t.observed_tx, "sig");
        assert_eq!(t.event_index, 0);
        assert_eq!(t.source, RawChainRef::wormhole(30));
        assert_eq!(t.destination, RawChainRef::wormhole(1));
        let expected_sender = format!("0x{}", "22".repeat(20));
        let expected_recipient = bs58::encode(to).into_string();
        assert_eq!(t.source_wallet.as_deref(), Some(expected_sender.as_str()));
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(expected_recipient.as_str())
        );
        assert_eq!(t.amount_raw.unwrap().as_str(), "15585875623");
        assert_eq!(t.token_symbol, None);
        assert_eq!(t.via, None);
        assert!(!t.delivered, "a bare receive has not released the tokens");
        let token_ref = t.token.unwrap();
        assert_eq!(token_ref.chain, RawChainRef::wormhole(30));
        assert_eq!(token_ref.address, format!("0x{}", "07".repeat(20)));
        assert_eq!(
            t.message,
            MessageId::Wormhole {
                emitter_chain: 30,
                emitter_address: "aa".repeat(32),
                sequence: 794,
            }
        );
    }

    #[test]
    fn a_receive_without_a_loaded_vaa_account_is_unsupported() {
        let mut t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 1), 1, 2, [3; 32]);
        t.accounts.clear();
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::Unsupported(
                "inbound receive: posted VAA account was not loaded"
            ))
        );
    }

    #[test]
    fn a_vaa_account_owned_by_another_program_is_malformed() {
        let mut t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 1), 1, 2, [3; 32]);
        t.accounts.get_mut(VAA_KEY).unwrap().owner = TOKEN_BRIDGE.into();
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::Malformed(
                "VAA account is not owned by the core bridge"
            ))
        );
    }

    #[test]
    fn a_receive_payload_for_another_chain_is_malformed() {
        let t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 30), 1, 2, [3; 32]);
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::Malformed(
                "receive payload is not addressed to Solana"
            ))
        );
    }

    #[test]
    fn a_receive_with_broken_vaa_data_is_malformed() {
        let mut t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 1), 1, 2, [3; 32]);
        t.accounts.get_mut(VAA_KEY).unwrap().data = vec![1, 2, 3];
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::Malformed("VAA account does not parse"))
        );
    }

    #[test]
    fn required_accounts_asks_for_the_posted_vaa() {
        let t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 1), 1, 2, [3; 32]);
        assert_eq!(adapter().required_accounts(&t), vec![VAA_KEY.to_string()]);

        let mut failed = t.clone();
        failed.failed = true;
        assert!(adapter().required_accounts(&failed).is_empty());
        assert!(
            adapter()
                .required_accounts(&simple_release(
                    native_payload(8, 1, [1; 32], [2; 32], 30),
                    1
                ))
                .is_empty()
        );
    }

    #[test]
    fn a_redeem_or_release_without_a_receive_is_not_a_transfer() {
        for discriminator in [REDEEM, RELEASE_INBOUND_MINT, RELEASE_INBOUND_UNLOCK] {
            let t = tx(
                vec![ix(MANAGER, &[USER], discriminator.to_vec(), None)],
                vec![],
            );
            assert_eq!(
                adapter().classify(&t),
                Classification::Rejected(Reject::NotTransfer(
                    "ntt redeem or release_inbound is not a receive_wormhole_message"
                ))
            );
        }
    }

    #[test]
    fn a_receive_alongside_its_redeem_and_release_is_one_inbound_transfer() {
        let mut t = receive_tx(native_payload(8, 5, [1; 32], [2; 32], 1), 794, 30, [3; 32]);
        t.instructions
            .push(ix(MANAGER, &[USER], REDEEM.to_vec(), None));
        let mut release = RELEASE_INBOUND_MINT.to_vec();
        release.push(1); // ReleaseStatus::Released
        t.instructions.push(ix(MANAGER, &[USER], release, None));

        let t = only_transfer(adapter().classify(&t));
        assert_eq!(t.direction, Direction::Inbound);
        assert_eq!(t.amount_raw.unwrap().as_str(), "5");
        assert!(t.delivered, "the release in the same tx moved the tokens");
    }

    #[test]
    fn a_redeem_without_a_release_does_not_complete_the_receive() {
        let mut t = receive_tx(native_payload(8, 5, [1; 32], [2; 32], 1), 794, 30, [3; 32]);
        t.instructions
            .push(ix(MANAGER, &[USER], REDEEM.to_vec(), None));

        let t = only_transfer(adapter().classify(&t));
        assert!(
            !t.delivered,
            "redeem attests the message but moves no tokens"
        );
    }

    #[test]
    fn a_failed_receive_is_never_a_transfer() {
        let mut t = receive_tx(native_payload(8, 1, [1; 32], [2; 32], 1), 1, 2, [3; 32]);
        t.failed = true;
        assert_eq!(
            adapter().classify(&t),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn an_outbound_and_an_inbound_with_the_same_identity_share_one_row_key() {
        // The row is keyed by (protocol, message_key). Both directions build the same
        // Wormhole identity, so the two legs converge instead of writing two rows.
        let emitter = [0x09; 32];
        let outbound = only_transfer(adapter().classify(&simple_release(
            native_payload(8, 5, emitter, [0x2b; 32], 30),
            439,
        )));
        let inbound = only_transfer(adapter().classify(&receive_tx(
            native_payload(8, 5, [0x07; 32], [0x2b; 32], 1),
            439,
            1,
            emitter,
        )));

        assert_eq!(outbound.message.key(), inbound.message.key());
        assert_eq!(outbound.program, inbound.program);
        assert_eq!(outbound.direction, Direction::Outbound);
        assert_eq!(inbound.direction, Direction::Inbound);
    }
}
