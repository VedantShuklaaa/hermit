//! Wormhole NFT Bridge adapter for Solana. Pure: no IO, no clock, no database.
//!
//! Verification sources:
//!
//! * program ID `WnFt12ZrnzZrFZkt2xsNsaNWoQribnuQ5B5FrDbwDhD` —
//!   <https://github.com/wormhole-foundation/wormhole/blob/main/sdk/js/src/utils/consts.ts>
//!   (`MAINNET.solana.nft_bridge`)
//! * instruction order — `solitaire! { Initialize, CompleteNative, CompleteWrapped,
//!   CompleteWrappedMeta, TransferWrapped, TransferNative, RegisterChain,
//!   UpgradeContract }` in
//!   <https://github.com/wormhole-foundation/wormhole/blob/main/solana/modules/nft_bridge/program/src/lib.rs>
//! * payload layout — <https://github.com/wormhole-foundation/wormhole/blob/main/whitepapers/0006_nft_bridge.md>
//!   and `solana/modules/nft_bridge/program/src/messages.rs` (`PayloadTransfer`)
//!
//! Outbound sends pair with the core bridge `PostMessage` they made, exactly as the
//! Token Bridge adapter does: same parent instruction, executed after the NFT
//! instruction, with the core bridge's emitter account among the NFT instruction's
//! accounts. The k-th `Sequence:` log line names the k-th `PostMessage`.
//!
//! Inbound completions (`complete_native`, `complete_wrapped`) carry no data: the
//! message is the posted VAA account at index 2, preloaded by the fetch step and
//! owned by the core bridge. Without that account an inbound redeem is `Unsupported`.

use super::posted_vaa;
use super::{
    CORE_BRIDGE, CORE_EMITTER_ACCOUNT, CORE_POST_MESSAGE, SOLANA, core_sequences, exec_key, parent,
    post_message_payload,
};
use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, Instruction, MessageId,
    NFT_BRIDGE_PROGRAM, Protocol, Reject, SolanaTx, TokenRef,
};
use crate::vaa::decode::decode_destination_address;

/// The verified mainnet NFT Bridge program (see the module documentation).
pub const NFT_BRIDGE: &str = "WnFt12ZrnzZrFZkt2xsNsaNWoQribnuQ5B5FrDbwDhD";

/// Index of the posted VAA among the `complete_*` accounts:
/// payer, config, vaa, recipient, ... (the same for both variants).
const REDEEM_VAA_ACCOUNT: usize = 2;

/// Fixed part of a transfer payload: id, token address, token chain, symbol, name,
/// token id, URI length, recipient, recipient chain.
const PAYLOAD_FIXED: usize = 1 + 32 + 2 + 32 + 32 + 32 + 1 + 32 + 2;
const URI_LEN_AT: usize = 131;
const SYMBOL_AT: usize = 35;
const TOKEN_ID_AT: usize = 99;
const URI_AT: usize = 132;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Outbound,
    Inbound,
    Admin,
    Metadata,
    Unknown,
}

/// NFT Bridge instruction index, in the order of the `solitaire!` dispatch table.
fn kind(index: u8) -> Kind {
    match index {
        // transfer_wrapped, transfer_native
        4 | 5 => Kind::Outbound,
        // complete_native, complete_wrapped
        1 | 2 => Kind::Inbound,
        // initialize, register_chain, upgrade_contract
        0 | 6 | 7 => Kind::Admin,
        // complete_wrapped_meta only attaches SPL metadata to an already redeemed NFT
        3 => Kind::Metadata,
        _ => Kind::Unknown,
    }
}

pub struct NftBridgeAdapter;

impl BridgeAdapter for NftBridgeAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Wormhole
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[NFT_BRIDGE]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let nft_ixs: Vec<usize> = tx
            .instructions
            .iter()
            .enumerate()
            .filter(|(_, ix)| ix.program_id == NFT_BRIDGE)
            .map(|(pos, _)| pos)
            .collect();
        if nft_ixs.is_empty() {
            return Classification::Rejected(Reject::NotTransfer("no nft bridge instruction"));
        }

        let mut outbound: Vec<usize> = Vec::new();
        let mut inbound: Vec<usize> = Vec::new();
        let mut metadata = false;
        let mut unknown = false;
        for &pos in &nft_ixs {
            match tx.instructions[pos].data.first().copied().map(kind) {
                None => {
                    return Classification::Rejected(Reject::Malformed(
                        "nft bridge instruction without data",
                    ));
                }
                Some(Kind::Outbound) => outbound.push(pos),
                Some(Kind::Inbound) => inbound.push(pos),
                Some(Kind::Metadata) => metadata = true,
                Some(Kind::Unknown) => unknown = true,
                Some(Kind::Admin) => {}
            }
        }

        if outbound.is_empty() && inbound.is_empty() {
            return Classification::Rejected(if unknown {
                Reject::Unsupported("unknown nft bridge instruction")
            } else if metadata {
                Reject::NotTransfer("complete_wrapped_meta updates SPL metadata only")
            } else {
                Reject::NotTransfer("nft bridge admin instruction")
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

/// NFT Bridge redeem instructions (`complete_*`), top-level or inner.
fn redeems(tx: &SolanaTx) -> impl Iterator<Item = &Instruction> {
    tx.instructions.iter().filter(|ix| {
        ix.program_id == NFT_BRIDGE && ix.data.first().copied().map(kind) == Some(Kind::Inbound)
    })
}

fn decode_outbound(tx: &SolanaTx, outbound: &[usize]) -> Result<Vec<DetectedTransfer>, Reject> {
    // Every PostMessage in the tx, in execution order, whoever emitted it. The core
    // bridge logs one `Sequence:` line per PostMessage, so the k-th message pairs
    // with the k-th line.
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
    let mut transfers = Vec::with_capacity(ordered.len());

    for (event_index, &token_pos) in ordered.iter().enumerate() {
        let token_ix = &tx.instructions[token_pos];
        let token_key = exec_key(tx, token_pos);
        let token_parent = parent(tx, token_pos);

        // The PostMessage this NFT instruction made: same top-level parent, executed
        // after it, emitted by an account the instruction was given.
        let rank = core
            .iter()
            .enumerate()
            .find(|&(rank, &core_pos)| {
                !claimed[rank]
                    && parent(tx, core_pos) == token_parent
                    && exec_key(tx, core_pos) > token_key
                    && tx.instructions[core_pos]
                        .accounts
                        .get(CORE_EMITTER_ACCOUNT)
                        .is_some_and(|emitter| token_ix.accounts.contains(emitter))
            })
            .map(|(rank, _)| rank)
            .ok_or(Reject::Malformed(
                "no PostMessage under the nft bridge instruction",
            ))?;
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
        let decoded = parse_payload(payload)?;

        transfers.push(DetectedTransfer {
            program: NFT_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: SOLANA,
                emitter_address,
                sequence: sequences[rank],
            },
            direction: Direction::Outbound,
            observed_tx: tx.signature.clone(),
            event_index: event_index as u16,
            source: RawChainRef::wormhole(SOLANA),
            destination: RawChainRef::wormhole(decoded.to_chain),
            source_wallet: tx.signer.clone(),
            destination_wallet: decode_destination_address(
                decoded.to_chain,
                &hex::encode(decoded.to),
            ),
            token: token_ref(&decoded),
            token_symbol: decoded.symbol,
            // One NFT per message. The NFT's id is in the payload, not the amount.
            amount_raw: Some(RawAmount::from_u128(1)),
            via: None,
        });
    }

    Ok(transfers)
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
        // The NFT Bridge only accepts a core bridge account, so this is a consistency
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
                "redeem payload is not addressed to Solana",
            ));
        }

        transfers.push(DetectedTransfer {
            program: NFT_BRIDGE_PROGRAM.to_string(),
            message: MessageId::Wormhole {
                emitter_chain: vaa.emitter_chain,
                emitter_address: hex::encode(vaa.emitter_address),
                sequence: vaa.sequence,
            },
            direction: Direction::Inbound,
            observed_tx: tx.signature.clone(),
            event_index: (first_index + n) as u16,
            source: RawChainRef::wormhole(vaa.emitter_chain),
            destination: RawChainRef::wormhole(SOLANA),
            // The sender is on the origin chain and is not in a plain transfer payload.
            source_wallet: None,
            destination_wallet: decode_destination_address(SOLANA, &hex::encode(decoded.to)),
            token: token_ref(&decoded),
            token_symbol: decoded.symbol,
            amount_raw: Some(RawAmount::from_u128(1)),
            via: None,
        });
    }
    Ok(transfers)
}

struct NftPayload {
    token_address: [u8; 32],
    token_chain: u16,
    symbol: Option<String>,
    token_id: [u8; 32],
    to: [u8; 32],
    to_chain: u16,
}

/// [`PayloadTransfer`] from the NFT Bridge program:
///
/// ```text
/// 0     1   payload id (1 = transfer)
/// 1    32   token address on the origin chain
/// 33    2   token chain
/// 35   32   symbol, NUL padded
/// 67   32   name, NUL padded
/// 99   32   token id
/// 131   1   URI length
/// 132   n   URI, NUL padded to the declared length
/// 132+ n 32 recipient
/// 164+ n  2 recipient chain
/// ```
fn parse_payload(p: &[u8]) -> Result<NftPayload, Reject> {
    if p.first() != Some(&1) {
        return Err(Reject::NotTransfer("payload is not an nft transfer"));
    }
    let uri_len = *p
        .get(URI_LEN_AT)
        .ok_or(Reject::Malformed("nft transfer payload is truncated"))? as usize;
    if p.len() != PAYLOAD_FIXED + uri_len {
        return Err(Reject::Malformed(
            "nft transfer payload has the wrong length",
        ));
    }

    let word = |from: usize| -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(&p[from..from + 32]);
        out
    };
    let to_at = URI_AT + uri_len;

    Ok(NftPayload {
        token_address: word(1),
        token_chain: u16::from_be_bytes([p[33], p[34]]),
        symbol: payload_string(&p[SYMBOL_AT..SYMBOL_AT + 32]),
        token_id: word(TOKEN_ID_AT),
        to: word(to_at),
        to_chain: u16::from_be_bytes([p[to_at + 32], p[to_at + 33]]),
    })
}

/// Fixed-width payload strings are NUL padded. The program itself drops the padding
/// and invalid UTF-8 when it reads them (`messages.rs`); do the same and treat an
/// empty result as absent.
fn payload_string(bytes: &[u8]) -> Option<String> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let value: String = String::from_utf8_lossy(&bytes[..end])
        .chars()
        .filter(|c| *c != '\u{FFFD}')
        .collect();
    (!value.is_empty()).then_some(value)
}

/// The NFT's identity for indexing. A Solana-native NFT is sent with a fixed dummy
/// token address and the mint in the token id (see `transfer_native` in the program),
/// so the mint is the address. Anything else is identified by the collection or
/// contract on its origin chain; the token id is not persisted.
fn token_ref(payload: &NftPayload) -> Option<TokenRef> {
    if payload.token_chain == SOLANA {
        return Some(TokenRef {
            chain: RawChainRef::wormhole(SOLANA),
            address: bs58::encode(payload.token_id).into_string(),
        });
    }
    decode_destination_address(payload.token_chain, &hex::encode(payload.token_address)).map(
        |address| TokenRef {
            chain: RawChainRef::wormhole(payload.token_chain),
            address,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::wormhole::TOKEN_BRIDGE;

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

    fn fixed(bytes: &[u8], width: usize) -> Vec<u8> {
        let mut out = vec![0u8; width];
        let n = bytes.len().min(width);
        out[..n].copy_from_slice(&bytes[..n]);
        out
    }

    /// `PayloadTransfer::try_from` pads the URI to its full `[u8; 200]` and writes
    /// that width as the length, so every real payload has a 200 byte URI field.
    #[allow(clippy::too_many_arguments)]
    fn payload(
        token_address: [u8; 32],
        token_chain: u16,
        symbol: &[u8],
        name: &[u8],
        token_id: [u8; 32],
        uri: &[u8],
        to: [u8; 32],
        to_chain: u16,
    ) -> Vec<u8> {
        let mut p = vec![1];
        p.extend_from_slice(&token_address);
        p.extend_from_slice(&token_chain.to_be_bytes());
        p.extend_from_slice(&fixed(symbol, 32));
        p.extend_from_slice(&fixed(name, 32));
        p.extend_from_slice(&token_id);
        p.push(200);
        p.extend_from_slice(&fixed(uri, 200));
        p.extend_from_slice(&to);
        p.extend_from_slice(&to_chain.to_be_bytes());
        p
    }

    fn native(token_id: [u8; 32], to_chain: u16) -> Vec<u8> {
        payload(
            [1u8; 32],
            1,
            b"PRT",
            b"face token",
            token_id,
            b"https://example.test/nft",
            [0xCD; 32],
            to_chain,
        )
    }

    fn wrapped(
        symbol: &[u8],
        token_address: [u8; 32],
        token_chain: u16,
        token_id: [u8; 32],
    ) -> Vec<u8> {
        payload(
            token_address,
            token_chain,
            symbol,
            b"Chill Ape Club",
            token_id,
            b"https://www.chillapeclub.com/api/nft/308",
            [0xCD; 32],
            1,
        )
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
        let mut l = vec![format!("Program {NFT_BRIDGE} invoke [1]")];
        for s in sequences {
            l.push(format!("Program {CORE_BRIDGE} invoke [2]"));
            l.push(format!("Program log: Sequence: {s}"));
            l.push(format!("Program {CORE_BRIDGE} success"));
        }
        l.push(format!("Program {NFT_BRIDGE} success"));
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

    fn simple_send(payload: Vec<u8>, sequence: u64) -> SolanaTx {
        let e = emitter(9);
        tx(
            vec![
                ix(NFT_BRIDGE, &[USER, &e], vec![5], None),
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
    fn a_native_send_uses_the_mint_as_the_token() {
        let mint = [0x7A; 32];
        let t = only_transfer(NftBridgeAdapter.classify(&simple_send(native(mint, 2), 29165)));

        assert_eq!(t.program, NFT_BRIDGE_PROGRAM);
        assert_eq!(t.direction, Direction::Outbound);
        assert_eq!(t.source, RawChainRef::wormhole(1));
        assert_eq!(t.destination, RawChainRef::wormhole(2));
        assert_eq!(t.amount_raw.unwrap().as_str(), "1");
        assert_eq!(t.token_symbol.as_deref(), Some("PRT"));
        let token = t.token.unwrap();
        assert_eq!(token.chain, RawChainRef::wormhole(1));
        assert_eq!(token.address, bs58::encode(mint).into_string());
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(format!("0x{}", "cd".repeat(20)).as_str())
        );
        assert_eq!(
            t.message,
            MessageId::Wormhole {
                emitter_chain: 1,
                emitter_address: "09".repeat(32),
                sequence: 29165,
            }
        );
    }

    #[test]
    fn a_wrapped_send_uses_the_origin_collection() {
        let t = only_transfer(
            NftBridgeAdapter.classify(&simple_send(wrapped(b"", [0xAB; 32], 22, [0x11; 32]), 1)),
        );
        let token = t.token.unwrap();
        assert_eq!(token.chain, RawChainRef::wormhole(22));
        assert_eq!(token.address, format!("0x{}", "ab".repeat(32)));
        assert_eq!(t.token_symbol, None, "an empty symbol is absent");
        assert_eq!(t.destination, RawChainRef::wormhole(1));
    }

    #[test]
    fn a_send_to_an_unknown_chain_keeps_the_raw_reference() {
        let t = only_transfer(NftBridgeAdapter.classify(&simple_send(native([7; 32], 9999), 1)));
        assert_eq!(t.destination, RawChainRef::wormhole(9999));
    }

    const VAA_ACCOUNT: &str = "PostedVaa1111111111111111111111111111111111";

    fn posted_vaa_bytes(sequence: u64, chain: u16, payload: &[u8]) -> Vec<u8> {
        let mut d = b"vaa".to_vec();
        d.extend_from_slice(&[1, 1]);
        d.extend_from_slice(&1u32.to_le_bytes()); // vaa time
        d.extend_from_slice(&[7; 32]); // signature set
        d.extend_from_slice(&2u32.to_le_bytes()); // submission time
        d.extend_from_slice(&3u32.to_le_bytes()); // nonce
        d.extend_from_slice(&sequence.to_le_bytes());
        d.extend_from_slice(&chain.to_le_bytes());
        d.extend_from_slice(&[0xEE; 32]);
        d.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        d.extend_from_slice(payload);
        d
    }

    fn redeem(index: u8, loaded: Option<(&str, Vec<u8>)>) -> SolanaTx {
        let mut t = tx(
            vec![ix(
                NFT_BRIDGE,
                &[USER, "config", VAA_ACCOUNT, "recipient"],
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
    fn both_completion_variants_decode_from_the_posted_vaa() {
        for index in [1u8, 2] {
            let p = wrapped(b"CAC", [0x22; 32], 2, [0x33; 32]);
            let t = only_transfer(NftBridgeAdapter.classify(&redeem(
                index,
                Some((CORE_BRIDGE, posted_vaa_bytes(1317, 2, &p))),
            )));

            assert_eq!(t.direction, Direction::Inbound, "index {index}");
            assert_eq!(t.source, RawChainRef::wormhole(2));
            assert_eq!(t.destination, RawChainRef::wormhole(1));
            assert_eq!(t.source_wallet, None);
            assert_eq!(t.amount_raw.unwrap().as_str(), "1");
            assert_eq!(t.token_symbol.as_deref(), Some("CAC"));
            assert_eq!(t.token.unwrap().address, format!("0x{}", "22".repeat(20)));
            assert_eq!(
                t.destination_wallet.as_deref(),
                Some(bs58::encode([0xCD; 32]).into_string().as_str())
            );
            assert_eq!(
                t.message,
                MessageId::Wormhole {
                    emitter_chain: 2,
                    emitter_address: "ee".repeat(32),
                    sequence: 1317,
                }
            );
        }
    }

    #[test]
    fn a_redeem_asks_for_its_vaa_account_only() {
        assert_eq!(
            NftBridgeAdapter.required_accounts(&redeem(1, None)),
            vec![VAA_ACCOUNT.to_string()]
        );
        assert!(
            NftBridgeAdapter
                .required_accounts(&simple_send(native([1; 32], 2), 1))
                .is_empty()
        );
        let mut failed = redeem(1, None);
        failed.failed = true;
        assert!(NftBridgeAdapter.required_accounts(&failed).is_empty());
    }

    #[test]
    fn admin_and_metadata_instructions_are_rejected_with_a_reason() {
        for index in [0u8, 6, 7] {
            assert_eq!(
                NftBridgeAdapter.classify(&tx(
                    vec![ix(NFT_BRIDGE, &[USER], vec![index], None)],
                    vec![]
                )),
                Classification::Rejected(Reject::NotTransfer("nft bridge admin instruction")),
                "index {index}"
            );
        }
        assert_eq!(
            NftBridgeAdapter.classify(&tx(vec![ix(NFT_BRIDGE, &[USER], vec![3], None)], vec![])),
            Classification::Rejected(Reject::NotTransfer(
                "complete_wrapped_meta updates SPL metadata only"
            ))
        );
    }

    #[test]
    fn a_token_bridge_instruction_is_not_an_nft_bridge_one() {
        let t = tx(vec![ix(TOKEN_BRIDGE, &[USER], vec![5], None)], vec![]);
        assert_eq!(
            NftBridgeAdapter.classify(&t),
            Classification::Rejected(Reject::NotTransfer("no nft bridge instruction"))
        );
    }

    #[test]
    fn unknown_and_empty_instructions_are_rejected() {
        let unknown = tx(vec![ix(NFT_BRIDGE, &[USER], vec![99], None)], vec![]);
        assert!(matches!(
            NftBridgeAdapter.classify(&unknown),
            Classification::Rejected(Reject::Unsupported(_))
        ));
        let empty = tx(vec![ix(NFT_BRIDGE, &[USER], vec![], None)], vec![]);
        assert!(matches!(
            NftBridgeAdapter.classify(&empty),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn malformed_payloads_are_rejected_without_panicking() {
        for bad in [
            vec![],
            vec![1],
            vec![2; 366],
            {
                let mut p = native([1; 32], 2);
                p.pop();
                p
            },
            {
                let mut p = native([1; 32], 2);
                p[131] = 199;
                p
            },
        ] {
            let c = NftBridgeAdapter.classify(&simple_send(bad, 1));
            assert!(matches!(c, Classification::Rejected(_)), "{c:?}");
        }
    }

    #[test]
    fn a_missing_sequence_log_is_malformed_not_a_guess() {
        let mut t = simple_send(native([1; 32], 2), 1);
        t.logs.clear();
        assert!(matches!(
            NftBridgeAdapter.classify(&t),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn a_sequence_line_from_another_program_is_ignored() {
        let mut t = simple_send(native([1; 32], 2), 5);
        t.logs
            .insert(1, format!("Program {OTHER_PROGRAM} invoke [2]"));
        t.logs.insert(2, "Program log: Sequence: 777".into());
        t.logs.insert(3, format!("Program {OTHER_PROGRAM} success"));
        let m = only_transfer(NftBridgeAdapter.classify(&t)).message;
        assert!(matches!(m, MessageId::Wormhole { sequence: 5, .. }));
    }

    #[test]
    fn two_sends_in_one_tx_get_their_own_sequences() {
        let (e1, e2) = (emitter(9), emitter(9));
        let t = tx(
            vec![
                ix(NFT_BRIDGE, &[USER, &e1], vec![5], None),
                ix(NFT_BRIDGE, &[USER, &e2], vec![4], None),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e1),
                    post_message_data(&native([1; 32], 2)),
                    Some(0),
                ),
                ix(
                    CORE_BRIDGE,
                    &core_accounts(&e2),
                    post_message_data(&native([2; 32], 4)),
                    Some(1),
                ),
            ],
            vec![
                format!("Program {NFT_BRIDGE} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 100".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {NFT_BRIDGE} success"),
                format!("Program {NFT_BRIDGE} invoke [1]"),
                format!("Program {CORE_BRIDGE} invoke [2]"),
                "Program log: Sequence: 101".into(),
                format!("Program {CORE_BRIDGE} success"),
                format!("Program {NFT_BRIDGE} success"),
            ],
        );
        let Classification::Transfers(v) = NftBridgeAdapter.classify(&t) else {
            panic!("expected transfers");
        };
        assert_eq!(v.len(), 2);
        let seq = |t: &DetectedTransfer| match &t.message {
            MessageId::Wormhole { sequence, .. } => *sequence,
            other => panic!("unexpected message id: {other:?}"),
        };
        assert_eq!((seq(&v[0]), seq(&v[1])), (100, 101));
        assert_eq!(v[0].destination, RawChainRef::wormhole(2));
        assert_eq!(v[1].destination, RawChainRef::wormhole(4));
    }

    #[test]
    fn the_instruction_index_table_matches_the_program() {
        // solitaire! dispatch order in nft_bridge/program/src/lib.rs.
        assert_eq!(kind(0), Kind::Admin); // Initialize
        assert_eq!(kind(1), Kind::Inbound); // CompleteNative
        assert_eq!(kind(2), Kind::Inbound); // CompleteWrapped
        assert_eq!(kind(3), Kind::Metadata); // CompleteWrappedMeta
        assert_eq!(kind(4), Kind::Outbound); // TransferWrapped
        assert_eq!(kind(5), Kind::Outbound); // TransferNative
        assert_eq!(kind(6), Kind::Admin); // RegisterChain
        assert_eq!(kind(7), Kind::Admin); // UpgradeContract
        assert_eq!(kind(8), Kind::Unknown);
    }

    #[test]
    fn registers_under_the_nft_bridge_program() {
        use crate::protocols::registry::ProtocolRegistry;
        use std::sync::Arc;

        let mut r = ProtocolRegistry::new();
        r.register(Arc::new(NftBridgeAdapter));
        assert_eq!(r.program_ids(), vec![NFT_BRIDGE.to_string()]);
    }
}
