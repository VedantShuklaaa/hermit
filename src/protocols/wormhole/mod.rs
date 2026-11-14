//! Wormhole Token Bridge adapter for Solana. Pure: no IO, no clock, no database.
//!
//! Program IDs come from Wormhole's contract address reference. Instruction
//! indexes and layouts come from `solana/modules/token_bridge` and
//! `solana/bridge` in wormhole-foundation/wormhole.
//!
//! Outbound transfers are decoded from the transaction alone:
//!
//! * the Token Bridge instruction (transfer native/wrapped, with or without
//!   payload) calls the core bridge `PostMessage` as a CPI;
//! * that inner instruction carries the transfer payload (amount, token,
//!   recipient, destination chain);
//! * its second signer account is the emitter, and the core bridge logs
//!   `Sequence: N` while it runs, which together give the message identity.
//!
//! Inbound redeems (`complete_*`) have empty instruction data: the message they
//! redeem is the posted VAA account (third account of the instruction, owned by the
//! core bridge). The fetch step preloads it into `SolanaTx::accounts` after asking
//! [`BridgeAdapter::required_accounts`], and the identity and transfer payload are
//! decoded from it here. Without the account an inbound redeem is `Unsupported`.

pub mod nft_bridge;
pub mod ntt;
pub mod posted_vaa;

use super::amount::RawAmount;
use super::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, MessageId, Protocol, Reject,
    SolanaTx, TOKEN_BRIDGE_PROGRAM, TokenRef,
};
use crate::chains::RawChainRef;
use crate::vaa::decode::decode_destination_address;

pub const TOKEN_BRIDGE: &str = "wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb";
pub const CORE_BRIDGE: &str = "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth";

/// Wormhole's chain ID for Solana.
const SOLANA: u16 = 1;

/// Core bridge `PostMessage` instruction index.
const CORE_POST_MESSAGE: u8 = 1;

/// Index of the emitter among the core bridge `PostMessage` accounts:
/// bridge, message, emitter, sequence, payer, fee collector, clock.
const CORE_EMITTER_ACCOUNT: usize = 2;

/// Index of the posted VAA among the Token Bridge `complete_*` accounts:
/// payer, config, vaa, claim, endpoint, ... (the same for all four variants).
const REDEEM_VAA_ACCOUNT: usize = 2;
/// Fixed part of a transfer payload: id, amount, token, token chain, to, to chain.
const PAYLOAD_FIXED: usize = 1 + 32 + 32 + 2 + 32 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Outbound,
    Inbound,
    Admin,
    Unknown,
}

/// Token Bridge instruction index, in the order of the `solitaire!` dispatch table.
fn kind(index: u8) -> Kind {
    match index {
        // transfer_wrapped, transfer_native, and their with-payload variants
        4 | 5 | 11 | 12 => Kind::Outbound,
        // complete_native, complete_wrapped, and their with-payload variants
        2 | 3 | 9 | 10 => Kind::Inbound,
        // initialize, attest_token, register_chain, create_wrapped, upgrade_contract,
        // set_pauser_addresses, pause, unpause, freeze, unpause_expired
        0 | 1 | 6..=8 | 13..=17 => Kind::Admin,
        _ => Kind::Unknown,
    }
}

pub struct WormholeAdapter;

impl BridgeAdapter for WormholeAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Wormhole
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[TOKEN_BRIDGE]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let token_ixs: Vec<usize> = tx
            .instructions
            .iter()
            .enumerate()
            .filter(|(_, ix)| ix.program_id == TOKEN_BRIDGE)
            .map(|(pos, _)| pos)
            .collect();
        if token_ixs.is_empty() {
            return Classification::Rejected(Reject::NotTransfer("no token bridge instruction"));
        }

        let mut outbound: Vec<usize> = Vec::new();
        let mut inbound: Vec<usize> = Vec::new();
        let mut unknown = false;
        for &pos in &token_ixs {
            match tx.instructions[pos].data.first().copied().map(kind) {
                None => {
                    return Classification::Rejected(Reject::Malformed(
                        "token bridge instruction without data",
                    ));
                }
                Some(Kind::Outbound) => outbound.push(pos),
                Some(Kind::Inbound) => inbound.push(pos),
                Some(Kind::Unknown) => unknown = true,
                Some(Kind::Admin) => {}
            }
        }

        if outbound.is_empty() && inbound.is_empty() {
            return Classification::Rejected(if unknown {
                Reject::Unsupported("unknown token bridge instruction")
            } else {
                Reject::NotTransfer("admin or attestation instruction")
            });
        }

        let mut transfers = Vec::new();
        if !outbound.is_empty() {
            match decode_outbound(tx, &outbound) {
                Ok(found) => transfers.extend(found),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        if !inbound.is_empty() {
            match decode_inbound(tx, &inbound, transfers.len()) {
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
        let mut keys: Vec<String> = redeems(tx)
            .filter_map(|ix| ix.accounts.get(REDEEM_VAA_ACCOUNT).cloned())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}

/// Position in execution order. A top-level instruction sorts before its own
/// inner instructions, and inner instructions keep their recorded order.
fn exec_key(tx: &SolanaTx, pos: usize) -> (usize, usize) {
    match tx.instructions[pos].inner_of {
        Some(parent) => (parent, pos),
        None => (pos, 0),
    }
}

/// The top-level instruction whose execution this instruction belongs to.
fn parent(tx: &SolanaTx, pos: usize) -> usize {
    tx.instructions[pos].inner_of.unwrap_or(pos)
}

/// One core bridge message made by a bridge instruction.
struct CoreMessage<'a> {
    emitter_address: String,
    sequence: u64,
    payload: &'a [u8],
}

/// Pairs each bridge instruction in `outbound` with the core bridge `PostMessage` it
/// made: same top-level parent, executed after it, with the core bridge's emitter
/// account among the instruction's accounts. `missing_reason` names the caller when no
/// message pairs.
///
/// Every PostMessage in the tx is considered in execution order, whoever emitted it.
/// The core bridge logs one `Sequence:` line per PostMessage, so the k-th message pairs
/// with the k-th line.
fn pair_post_messages<'a>(
    tx: &'a SolanaTx,
    outbound: &[usize],
    missing_reason: &'static str,
) -> Result<Vec<CoreMessage<'a>>, Reject> {
    let mut core: Vec<usize> = tx
        .instructions
        .iter()
        .enumerate()
        .filter(|(_, ix)| {
            ix.program_id == CORE_BRIDGE && ix.data.first() == Some(&CORE_POST_MESSAGE)
        })
        .map(|(pos, _)| pos)
        .collect();
    core.sort_by_key(|&pos| exec_key(tx, pos));

    let sequences = core_sequences(&tx.logs);
    if sequences.len() != core.len() {
        return Err(Reject::Malformed(
            "Sequence log lines do not match PostMessage instructions",
        ));
    }

    let mut ordered: Vec<usize> = outbound.to_vec();
    ordered.sort_by_key(|&pos| exec_key(tx, pos));

    let mut claimed = vec![false; core.len()];
    let mut messages = Vec::with_capacity(ordered.len());

    for &bridge_pos in &ordered {
        let bridge_ix = &tx.instructions[bridge_pos];
        let bridge_key = exec_key(tx, bridge_pos);
        let bridge_parent = parent(tx, bridge_pos);

        let rank = core
            .iter()
            .enumerate()
            .find(|&(rank, &core_pos)| {
                !claimed[rank]
                    && parent(tx, core_pos) == bridge_parent
                    && exec_key(tx, core_pos) > bridge_key
                    && tx.instructions[core_pos]
                        .accounts
                        .get(CORE_EMITTER_ACCOUNT)
                        .is_some_and(|emitter| bridge_ix.accounts.contains(emitter))
            })
            .map(|(rank, _)| rank)
            .ok_or(Reject::Malformed(missing_reason))?;
        claimed[rank] = true;

        let core_ix = &tx.instructions[core[rank]];
        let emitter = core_ix
            .accounts
            .get(CORE_EMITTER_ACCOUNT)
            .ok_or(Reject::Malformed("PostMessage without an emitter account"))?;
        let emitter_address = bs58::decode(emitter)
            .into_vec()
            .ok()
            .filter(|b| b.len() == 32)
            .map(hex::encode)
            .ok_or(Reject::Malformed("emitter is not a 32 byte address"))?;

        let payload = post_message_payload(&core_ix.data)
            .ok_or(Reject::Malformed("PostMessage data does not parse"))?;

        messages.push(CoreMessage {
            emitter_address,
            sequence: sequences[rank],
            payload,
        });
    }

    Ok(messages)
}

fn decode_outbound(tx: &SolanaTx, outbound: &[usize]) -> Result<Vec<DetectedTransfer>, Reject> {
    let messages = pair_post_messages(
        tx,
        outbound,
        "no PostMessage under the token bridge instruction",
    )?;
    let mut transfers = Vec::with_capacity(messages.len());

    for (event_index, message) in messages.into_iter().enumerate() {
        let decoded = parse_transfer_payload(message.payload)?;

        let to_hex = hex::encode(decoded.to);
        let token_hex = hex::encode(decoded.token_address);

        transfers.push(DetectedTransfer {
            program: TOKEN_BRIDGE_PROGRAM.to_string(),
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
            token: decode_destination_address(decoded.token_chain, &token_hex).map(|address| {
                TokenRef {
                    chain: RawChainRef::wormhole(decoded.token_chain),
                    address,
                }
            }),
            token_symbol: None,
            // The VAA amount: normalised to at most 8 decimals, as the VAA carries it.
            amount_raw: Some(decoded.amount),
            via: None,
        });
    }

    Ok(transfers)
}

/// Core bridge `PostMessage` data: index, nonce (u32 LE), payload (u32 LE length,
/// then bytes), consistency level (u8). Anything else is not a PostMessage we know.
fn post_message_payload(data: &[u8]) -> Option<&[u8]> {
    if *data.first()? != CORE_POST_MESSAGE {
        return None;
    }
    let len = u32::from_le_bytes(data.get(5..9)?.try_into().ok()?) as usize;
    let end = 9usize.checked_add(len)?;
    let payload = data.get(9..end)?;
    (data.len() == end + 1).then_some(payload)
}

/// Token Bridge redeem instructions (`complete_*`), top-level or inner.
fn redeems(tx: &SolanaTx) -> impl Iterator<Item = &super::Instruction> {
    tx.instructions.iter().filter(|ix| {
        ix.program_id == TOKEN_BRIDGE && ix.data.first().copied().map(kind) == Some(Kind::Inbound)
    })
}

/// Redeems into Solana. The identity and transfer come from the posted VAA account the
/// instruction was given. `first_index` continues the event index after any outbound
/// transfers decoded from the same tx.
fn decode_inbound(
    tx: &SolanaTx,
    inbound: &[usize],
    first_index: usize,
) -> Result<Vec<DetectedTransfer>, Reject> {
    let mut ordered: Vec<usize> = inbound.to_vec();
    ordered.sort_by_key(|&pos| exec_key(tx, pos));

    let mut transfers = Vec::with_capacity(ordered.len());
    for (n, &pos) in ordered.iter().enumerate() {
        let vaa_key = tx.instructions[pos]
            .accounts
            .get(REDEEM_VAA_ACCOUNT)
            .ok_or(Reject::Malformed("redeem without a VAA account"))?;
        let account = tx.account(vaa_key).ok_or(Reject::Unsupported(
            "inbound redeem: posted VAA account was not loaded",
        ))?;
        // The Token Bridge only accepts a core bridge account, so this is a
        // consistency check on what the fetch step returned.
        if account.owner != CORE_BRIDGE {
            return Err(Reject::Malformed(
                "VAA account is not owned by the core bridge",
            ));
        }
        let vaa = posted_vaa::parse(&account.data)
            .ok_or(Reject::Malformed("VAA account does not parse"))?;
        let decoded = parse_transfer_payload(vaa.payload)?;
        if decoded.to_chain != SOLANA {
            return Err(Reject::Malformed(
                "redeem payload is not addressed to Solana",
            ));
        }

        let to_hex = hex::encode(decoded.to);
        let token_hex = hex::encode(decoded.token_address);

        transfers.push(DetectedTransfer {
            program: TOKEN_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: vaa.emitter_chain,
                emitter_address: hex::encode(vaa.emitter_address),
                sequence: vaa.sequence,
            },
            direction: Direction::Inbound,
            // The redeem moved the tokens on Solana in this tx.
            delivered: true,
            observed_tx: tx.signature.clone(),
            event_index: (first_index + n) as u16,
            source: RawChainRef::wormhole(vaa.emitter_chain),
            destination: RawChainRef::wormhole(SOLANA),
            // The sender is on the origin chain and is not in a plain transfer payload.
            source_wallet: None,
            // The recipient's token account, as the VAA carries it.
            destination_wallet: decode_destination_address(SOLANA, &to_hex),
            token: decode_destination_address(decoded.token_chain, &token_hex).map(|address| {
                TokenRef {
                    chain: RawChainRef::wormhole(decoded.token_chain),
                    address,
                }
            }),
            token_symbol: None,
            amount_raw: Some(decoded.amount),
            via: None,
        });
    }
    Ok(transfers)
}

struct TransferFields {
    amount: RawAmount,
    token_address: [u8; 32],
    token_chain: u16,
    to: [u8; 32],
    to_chain: u16,
}

/// Token Bridge transfer payloads: id 1 is a plain transfer (133 bytes, ends with the
/// fee), id 3 is a transfer with payload (the sender address and arbitrary bytes follow).
fn parse_transfer_payload(p: &[u8]) -> Result<TransferFields, Reject> {
    match p.first() {
        Some(1) if p.len() == PAYLOAD_FIXED + 32 => {}
        Some(3) if p.len() >= PAYLOAD_FIXED + 32 => {}
        Some(1) | Some(3) => {
            return Err(Reject::Malformed("transfer payload has the wrong length"));
        }
        _ => return Err(Reject::NotTransfer("payload is not a token transfer")),
    }

    let word = |from: usize| -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(&p[from..from + 32]);
        out
    };

    Ok(TransferFields {
        amount: RawAmount::from_be_bytes(&p[1..33])
            .ok_or(Reject::Malformed("amount does not fit a u256"))?,
        token_address: word(33),
        token_chain: u16::from_be_bytes([p[65], p[66]]),
        to: word(67),
        to_chain: u16::from_be_bytes([p[99], p[100]]),
    })
}

/// Values of `Sequence: N` logged by the core bridge, in log order. Only lines
/// printed while the core bridge is the running program count.
fn core_sequences(logs: &[String]) -> Vec<u64> {
    let mut stack: Vec<&str> = Vec::new();
    let mut out = Vec::new();

    for line in logs {
        let Some(rest) = line.strip_prefix("Program ") else {
            continue;
        };
        if let Some(n) = rest.strip_prefix("log: Sequence: ") {
            if stack.last() == Some(&CORE_BRIDGE)
                && let Ok(n) = n.trim().parse::<u64>()
            {
                out.push(n);
            }
        } else if let Some((id, tail)) = rest.split_once(' ') {
            if tail.starts_with("invoke [") {
                stack.push(id);
            } else if tail == "success" || tail.starts_with("failed") {
                stack.pop();
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::Instruction;

    const USER: &str = "UserWallet1111111111111111111111111111111111";
    const OTHER_PROGRAM: &str = "RouterProgram11111111111111111111111111111";

    /// Any valid 32-byte address works as an emitter in tests.
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

    fn transfer_payload(id: u8, amount: u64, to_chain: u16, extra: &[u8]) -> Vec<u8> {
        let mut p = vec![id];
        let mut amt = [0u8; 32];
        amt[24..].copy_from_slice(&amount.to_be_bytes());
        p.extend_from_slice(&amt);
        p.extend_from_slice(&[0xAB; 32]); // token address
        p.extend_from_slice(&1u16.to_be_bytes()); // token chain: Solana
        p.extend_from_slice(&[0xCD; 32]); // recipient
        p.extend_from_slice(&to_chain.to_be_bytes());
        p.extend_from_slice(extra); // fee (id 1) or sender + payload (id 3)
        p
    }

    fn post_message_data(payload: &[u8]) -> Vec<u8> {
        let mut d = vec![CORE_POST_MESSAGE];
        d.extend_from_slice(&7u32.to_le_bytes()); // nonce
        d.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        d.extend_from_slice(payload);
        d.push(1); // consistency level
        d
    }

    fn core_accounts(emitter: &str) -> [&str; 7] {
        [
            "bridge", "message", emitter, "sequence", USER, "fees", "clock",
        ]
    }

    fn logs_for(sequences: &[u64]) -> Vec<String> {
        let mut l = vec![format!("Program {TOKEN_BRIDGE} invoke [1]")];
        for s in sequences {
            l.push(format!("Program {CORE_BRIDGE} invoke [2]"));
            l.push(format!("Program log: Sequence: {s}"));
            l.push(format!("Program {CORE_BRIDGE} success"));
        }
        l.push(format!("Program {TOKEN_BRIDGE} success"));
        l
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

    /// One top-level transfer_native with the PostMessage CPI under it.
    fn simple_transfer(payload: Vec<u8>, sequence: u64) -> SolanaTx {
        let e = emitter(9);
        tx(
            vec![
                ix(TOKEN_BRIDGE, &[USER, &e], vec![5], None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e),
                    post_message_data(&payload),
                    Some(0),
                ),
            ],
            logs_for(&[sequence]),
        )
    }

    fn only_transfer(c: Classification) -> DetectedTransfer {
        match c {
            Classification::Transfers(mut v) if v.len() == 1 => v.remove(0),
            other => panic!("expected one transfer, got {other:?}"),
        }
    }

    #[test]
    fn decodes_an_outbound_transfer_to_ethereum() {
        let t = only_transfer(WormholeAdapter.classify(&simple_transfer(
            transfer_payload(1, 5_000_000, 2, &[0; 32]),
            4242,
        )));

        assert_eq!(t.direction, Direction::Outbound);
        assert_eq!(t.observed_tx, "sig");
        assert_eq!(t.event_index, 0);
        assert_eq!(t.source, RawChainRef::wormhole(1));
        assert_eq!(t.destination, RawChainRef::wormhole(2));
        assert_eq!(t.source_wallet.as_deref(), Some(USER));
        assert_eq!(t.amount_raw.unwrap().as_str(), "5000000");
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(format!("0x{}", "cd".repeat(20)).as_str())
        );
        let token = t.token.unwrap();
        assert_eq!(token.chain, RawChainRef::wormhole(1));
        assert_eq!(token.address, bs58::encode([0xAB; 32]).into_string());
        assert_eq!(
            t.message,
            MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 4242,
            }
        );
        assert_eq!(t.via, None);
    }

    #[test]
    fn decodes_a_transfer_with_payload() {
        let mut extra = vec![0x11; 32]; // sender
        extra.extend_from_slice(b"arbitrary app bytes");
        let t = only_transfer(
            WormholeAdapter.classify(&simple_transfer(transfer_payload(3, 1, 4, &extra), 9)),
        );
        assert_eq!(t.destination, RawChainRef::wormhole(4));
        assert_eq!(t.amount_raw.unwrap().as_str(), "1");
    }

    #[test]
    fn attest_is_not_a_transfer() {
        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![1], None)], vec![]);
        assert_eq!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::NotTransfer("admin or attestation instruction"))
        );
    }

    const VAA_ACCOUNT: &str = "PostedVaa1111111111111111111111111111111111";

    fn posted_vaa_bytes(sequence: u64, chain: u16, payload: &[u8]) -> Vec<u8> {
        let mut d = b"vaa".to_vec();
        d.extend_from_slice(&[1, 1]);
        d.extend_from_slice(&1u32.to_le_bytes());
        d.extend_from_slice(&[7; 32]);
        d.extend_from_slice(&2u32.to_le_bytes());
        d.extend_from_slice(&3u32.to_le_bytes());
        d.extend_from_slice(&sequence.to_le_bytes());
        d.extend_from_slice(&chain.to_le_bytes());
        d.extend_from_slice(&[0xEE; 32]);
        d.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        d.extend_from_slice(payload);
        d
    }

    /// A top-level `complete_*` redeem; the VAA account is its third account.
    fn redeem(index: u8, loaded: Option<(&str, Vec<u8>)>) -> SolanaTx {
        let mut t = tx(
            vec![ix(
                TOKEN_BRIDGE,
                &[USER, "config", VAA_ACCOUNT, "claim"],
                vec![index],
                None,
            )],
            vec![],
        );
        if let Some((owner, data)) = loaded {
            t.accounts.insert(
                VAA_ACCOUNT.into(),
                crate::protocols::AccountData {
                    owner: owner.into(),
                    data,
                },
            );
        }
        t
    }

    #[test]
    fn decodes_an_inbound_redeem_from_the_posted_vaa() {
        for index in [2u8, 3, 9, 10] {
            let payload = transfer_payload(1, 1_641_561_313_809, 1, &[0; 32]);
            let t = only_transfer(WormholeAdapter.classify(&redeem(
                index,
                Some((CORE_BRIDGE, posted_vaa_bytes(697_687, 2, &payload))),
            )));

            assert_eq!(t.direction, Direction::Inbound, "index {index}");
            assert_eq!(t.observed_tx, "sig");
            assert_eq!(t.source, RawChainRef::wormhole(2));
            assert_eq!(t.destination, RawChainRef::wormhole(1));
            assert_eq!(t.source_wallet, None);
            assert_eq!(
                t.destination_wallet.as_deref(),
                Some(bs58::encode([0xCD; 32]).into_string().as_str())
            );
            assert_eq!(t.amount_raw.unwrap().as_str(), "1641561313809");
            assert_eq!(
                t.message,
                MessageId::Wormhole {
                    emitter_chain: 2,
                    emitter_address: "ee".repeat(32),
                    sequence: 697_687,
                }
            );
        }
    }

    #[test]
    fn a_redeem_asks_for_its_vaa_account_only() {
        assert_eq!(
            WormholeAdapter.required_accounts(&redeem(3, None)),
            vec![VAA_ACCOUNT.to_string()]
        );
        // sends and admin calls need no account
        let send = simple_transfer(transfer_payload(1, 1, 2, &[0; 32]), 1);
        assert!(WormholeAdapter.required_accounts(&send).is_empty());
        let mut failed = redeem(3, None);
        failed.failed = true;
        assert!(WormholeAdapter.required_accounts(&failed).is_empty());
    }

    #[test]
    fn a_redeem_without_its_account_loaded_is_unsupported() {
        let c = WormholeAdapter.classify(&redeem(3, None));
        assert!(
            matches!(c, Classification::Rejected(Reject::Unsupported(_))),
            "{c:?}"
        );
    }

    #[test]
    fn a_redeem_with_too_few_accounts_is_malformed() {
        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![2], None)], vec![]);
        assert!(matches!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn a_redeem_rejects_an_account_the_core_bridge_does_not_own() {
        let payload = transfer_payload(1, 1, 1, &[0; 32]);
        let c =
            WormholeAdapter.classify(&redeem(3, Some((USER, posted_vaa_bytes(1, 2, &payload)))));
        assert!(
            matches!(c, Classification::Rejected(Reject::Malformed(_))),
            "{c:?}"
        );
    }

    #[test]
    fn a_redeem_rejects_garbage_and_payloads_for_other_chains() {
        let garbage = redeem(3, Some((CORE_BRIDGE, vec![1, 2, 3])));
        assert!(matches!(
            WormholeAdapter.classify(&garbage),
            Classification::Rejected(Reject::Malformed(_))
        ));

        let elsewhere = transfer_payload(1, 1, 2, &[0; 32]); // addressed to Ethereum
        let c = WormholeAdapter.classify(&redeem(
            3,
            Some((CORE_BRIDGE, posted_vaa_bytes(1, 2, &elsewhere))),
        ));
        assert!(
            matches!(c, Classification::Rejected(Reject::Malformed(_))),
            "{c:?}"
        );
    }

    #[test]
    fn an_unknown_instruction_is_unsupported() {
        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![99], None)], vec![]);
        assert!(matches!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::Unsupported(_))
        ));
    }

    #[test]
    fn empty_instruction_data_is_malformed() {
        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![], None)], vec![]);
        assert!(matches!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn a_failed_transaction_is_never_a_transfer() {
        let mut t = simple_transfer(transfer_payload(1, 1, 2, &[0; 32]), 1);
        t.failed = true;
        assert_eq!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn a_missing_sequence_log_is_malformed_not_a_guess() {
        let mut t = simple_transfer(transfer_payload(1, 1, 2, &[0; 32]), 1);
        t.logs.clear();
        assert!(matches!(
            WormholeAdapter.classify(&t),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn a_sequence_line_from_another_program_is_ignored() {
        let mut t = simple_transfer(transfer_payload(1, 1, 2, &[0; 32]), 5);
        // A different program printing the same text must not count.
        t.logs
            .insert(1, format!("Program {OTHER_PROGRAM} invoke [2]"));
        t.logs.insert(2, "Program log: Sequence: 777".into());
        t.logs.insert(3, format!("Program {OTHER_PROGRAM} success"));
        let m = only_transfer(WormholeAdapter.classify(&t)).message;
        assert!(matches!(m, MessageId::Wormhole { sequence: 5, .. }));
    }

    #[test]
    fn malformed_payloads_are_rejected_without_panicking() {
        for bad in [
            vec![],
            vec![1],
            vec![1; 40],
            transfer_payload(1, 1, 2, &[0; 31]), // one byte short
            transfer_payload(3, 1, 2, &[0; 31]), // sender cut off
        ] {
            let c = WormholeAdapter.classify(&simple_transfer(bad, 1));
            assert!(matches!(c, Classification::Rejected(_)), "{c:?}");
        }
    }

    #[test]
    fn a_non_transfer_payload_is_not_a_transfer() {
        // Payload id 2 is an asset attestation.
        let c = WormholeAdapter.classify(&simple_transfer(vec![2; 100], 1));
        assert!(matches!(
            c,
            Classification::Rejected(Reject::NotTransfer(_))
        ));
    }

    #[test]
    fn truncated_post_message_data_does_not_panic() {
        let e = emitter(9);
        for data in [
            vec![CORE_POST_MESSAGE],
            vec![CORE_POST_MESSAGE, 0, 0, 0, 0, 255, 255, 255, 255],
        ] {
            let t = tx(
                vec![
                    ix(TOKEN_BRIDGE, &[USER, &e], vec![5], None),
                    ix(CORE_BRIDGE, &core_accounts(&e), data, Some(0)),
                ],
                logs_for(&[1]),
            );
            assert!(matches!(
                WormholeAdapter.classify(&t),
                Classification::Rejected(Reject::Malformed(_))
            ));
        }
    }

    #[test]
    fn two_transfers_in_one_tx_get_their_own_sequences() {
        let (e1, e2) = (emitter(9), emitter(9));
        let t = tx(
            vec![
                ix(TOKEN_BRIDGE, &[USER, &e1], vec![5], None),
                ix(TOKEN_BRIDGE, &[USER, &e2], vec![4], None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e1),
                    post_message_data(&transfer_payload(1, 10, 2, &[0; 32])),
                    Some(0),
                ),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e2),
                    post_message_data(&transfer_payload(1, 20, 4, &[0; 32])),
                    Some(1),
                ),
            ],
            vec![
                format!("Program {TOKEN_BRIDGE} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 100".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {TOKEN_BRIDGE} success"),
                format!("Program {TOKEN_BRIDGE} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 101".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {TOKEN_BRIDGE} success"),
            ],
        );
        let Classification::Transfers(v) = WormholeAdapter.classify(&t) else {
            panic!("expected transfers");
        };
        assert_eq!(v.len(), 2);
        assert_eq!((v[0].event_index, v[1].event_index), (0, 1));
        let seq = |t: &DetectedTransfer| match &t.message {
            MessageId::Wormhole { sequence, .. } => *sequence,
            other => panic!("unexpected message id: {other:?}"),
        };
        assert_eq!((seq(&v[0]), seq(&v[1])), (100, 101));
        assert_eq!(v[0].amount_raw.as_ref().unwrap().as_str(), "10");
        assert_eq!(v[1].amount_raw.as_ref().unwrap().as_str(), "20");
    }

    #[test]
    fn a_token_bridge_call_made_by_a_router_pairs_with_its_own_post_message() {
        // Top-level 0 is a router. Under it, in order: an unrelated PostMessage from
        // another emitter, then the Token Bridge instruction and its own PostMessage.
        let (router_e, tb_e) = (emitter(5), emitter(9));
        let t = tx(
            vec![
                ix(OTHER_PROGRAM, &[USER], vec![0], None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&router_e),
                    post_message_data(&[1, 2, 3]),
                    Some(0),
                ),
                ix(TOKEN_BRIDGE, &[USER, &tb_e], vec![5], Some(0)),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&tb_e),
                    post_message_data(&transfer_payload(1, 77, 2, &[0; 32])),
                    Some(0),
                ),
            ],
            vec![
                format!("Program {OTHER_PROGRAM} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 10".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {TOKEN_BRIDGE} invoke [2]"),
                format!("Program {CORE_BRIDGE} invoke [3]"),
                "Program log: Sequence: 11".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {TOKEN_BRIDGE} success"),
                format!("Program {OTHER_PROGRAM} success"),
            ],
        );
        let tr = only_transfer(WormholeAdapter.classify(&t));
        assert!(matches!(
            tr.message,
            MessageId::Wormhole { sequence: 11, .. }
        ));
        assert_eq!(tr.amount_raw.unwrap().as_str(), "77");
    }

    #[test]
    fn the_instruction_index_table_matches_the_program() {
        // Pinned against the dispatch order in token_bridge/program/src/lib.rs.
        assert_eq!(kind(4), Kind::Outbound); // TransferWrapped
        assert_eq!(kind(5), Kind::Outbound); // TransferNative
        assert_eq!(kind(11), Kind::Outbound); // TransferWrappedWithPayload
        assert_eq!(kind(12), Kind::Outbound); // TransferNativeWithPayload
        assert_eq!(kind(2), Kind::Inbound); // CompleteNative
        assert_eq!(kind(3), Kind::Inbound); // CompleteWrapped
        assert_eq!(kind(1), Kind::Admin); // AttestToken
        assert_eq!(kind(18), Kind::Unknown);
    }

    #[test]
    fn registers_under_the_token_bridge_program() {
        use crate::protocols::registry::ProtocolRegistry;
        use std::sync::Arc;

        let mut r = ProtocolRegistry::new();
        r.register(Arc::new(WormholeAdapter));
        assert_eq!(r.program_ids(), vec![TOKEN_BRIDGE.to_string()]);
    }
}
