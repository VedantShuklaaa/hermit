//! Circle CCTP V2 on Solana. Pure: no IO, no clock, no database.
//!
//! Verification sources:
//!
//! * program IDs — <https://developers.circle.com/cctp/references/solana-programs>
//!   (`MessageTransmitterV2`, `TokenMessengerMinterV2`)
//! * message and burn-message layout — `message-transmitter-v2/src/message.rs` and
//!   `token-messenger-minter-v2/src/token_messenger_v2/burn_message.rs` in
//!   <https://github.com/circlefin/solana-cctp-contracts>
//! * instruction discriminators — Anchor's `sha256("global:<snake_case>")[..8]`,
//!   checked against the published `initialize` value `afaf6d1f0d989bed`.
//!
//! Only completions are indexed. A Solana V2 send writes an all-zero nonce into its
//! `MessageSent` account; Circle assigns the canonical nonce off-chain. Without Iris,
//! a send has no chain-derived message identity, so `deposit_for_burn` is rejected as
//! [`Reject::Unsupported`] instead of inventing one. See `docs/REFERENCE.md`.

use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, Instruction, MessageId, Protocol,
    Reject, SolanaTx, TokenRef,
};

/// Verified mainnet and devnet `MessageTransmitterV2` program.
pub const MESSAGE_TRANSMITTER_V2: &str = "CCTPV2Sm4AdWt5296sk4P66VBZ7bEhcARwFaaS9YPbeC";
/// Verified mainnet and devnet `TokenMessengerMinterV2` program.
pub const TOKEN_MESSENGER_MINTER_V2: &str = "CCTPV2vPZJS2u2BBsUoscuikbYjnpFmbFsvVuJdgUMQe";

/// Solana's Circle domain.
const SOLANA_DOMAIN: u32 = 5;
/// The V2 message header version.
const MESSAGE_VERSION_V2: u32 = 1;
/// The burn-message body version.
const BURN_VERSION: u32 = 1;

const MESSAGE_HEADER_LEN: usize = 148;
const BURN_FIXED_LEN: usize = 228;

// Anchor instruction discriminators.
const DEPOSIT_FOR_BURN: [u8; 8] = [0xd7, 0x3c, 0x3d, 0x2e, 0x72, 0x37, 0x80, 0xb0];
const DEPOSIT_FOR_BURN_WITH_HOOK: [u8; 8] = [0x6f, 0xf5, 0x3e, 0x83, 0xcc, 0x6c, 0xdf, 0x9b];
const RECEIVE_MESSAGE: [u8; 8] = [0x26, 0x90, 0x7f, 0xe1, 0x1f, 0xe1, 0xee, 0x19];
const SEND_MESSAGE: [u8; 8] = [0x39, 0x28, 0x22, 0xb2, 0xbd, 0x0a, 0x41, 0x1a];
const HANDLE_RECEIVE_FINALIZED: [u8; 8] = [0xba, 0xfc, 0xef, 0x46, 0x56, 0xb4, 0x6e, 0x5f];
const HANDLE_RECEIVE_UNFINALIZED: [u8; 8] = [0xc8, 0xa9, 0xaf, 0x14, 0xc8, 0x3a, 0xb6, 0x3d];

/// CCTP V2 adapter. Both program IDs are configurable so a deployment can point at a
/// future version; the verified values above are the defaults.
pub struct CctpAdapter {
    message_transmitter: String,
    token_messenger: String,
}

impl CctpAdapter {
    pub fn new(message_transmitter: impl Into<String>, token_messenger: impl Into<String>) -> Self {
        Self {
            message_transmitter: message_transmitter.into(),
            token_messenger: token_messenger.into(),
        }
    }
}

impl Default for CctpAdapter {
    fn default() -> Self {
        Self::new(MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_MINTER_V2)
    }
}

impl BridgeAdapter for CctpAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Cctp
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_MINTER_V2]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let cctp: Vec<&Instruction> = tx
            .instructions
            .iter()
            .filter(|ix| {
                ix.program_id == self.message_transmitter || ix.program_id == self.token_messenger
            })
            .collect();
        if cctp.is_empty() {
            return Classification::Rejected(Reject::NotTransfer("no cctp instruction"));
        }

        let mut receives = Vec::new();
        let mut saw_send = false;
        for ix in cctp {
            let Some(discriminator) = ix.data.get(..8) else {
                return Classification::Rejected(Reject::Malformed(
                    "cctp instruction without data",
                ));
            };
            if ix.program_id == self.message_transmitter && discriminator == RECEIVE_MESSAGE {
                receives.push(ix);
            } else if discriminator == DEPOSIT_FOR_BURN
                || discriminator == DEPOSIT_FOR_BURN_WITH_HOOK
            {
                saw_send = true;
            } else if discriminator == SEND_MESSAGE
                || discriminator == HANDLE_RECEIVE_FINALIZED
                || discriminator == HANDLE_RECEIVE_UNFINALIZED
            {
                // Inner CPIs made by the program under a transfer instruction.
            }
            // Everything else on these programs is admin, attest or utility.
        }

        if receives.is_empty() {
            return Classification::Rejected(if saw_send {
                Reject::Unsupported("cctp send: nonce is assigned offchain")
            } else {
                Reject::NotTransfer("cctp admin instruction")
            });
        }

        let mut transfers = Vec::with_capacity(receives.len());
        for (event_index, ix) in receives.into_iter().enumerate() {
            match decode_receive(tx, ix, event_index as u16) {
                Ok(transfer) => transfers.push(transfer),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        Classification::Transfers(transfers)
    }
}

/// One `receive_message` instruction. Its data is a Borsh pair of byte vectors:
/// `{ message: Vec<u8>, attestation: Vec<u8> }`, each with a 4-byte little-endian
/// length prefix. The attested message carries the real nonce (unlike a send).
fn decode_receive(
    tx: &SolanaTx,
    ix: &Instruction,
    event_index: u16,
) -> Result<DetectedTransfer, Reject> {
    let data = &ix.data[8..];
    let mut at = 0;
    let message = read_vec(data, &mut at).ok_or(Reject::Malformed(
        "cctp receive_message message is truncated",
    ))?;
    let _attestation = read_vec(data, &mut at).ok_or(Reject::Malformed(
        "cctp receive_message attestation is truncated",
    ))?;
    if at != data.len() {
        return Err(Reject::Malformed("cctp receive_message has trailing data"));
    }

    let parsed = parse_message(message)?;
    let source = RawChainRef::cctp(parsed.source_domain);
    let destination = RawChainRef::cctp(SOLANA_DOMAIN);

    Ok(DetectedTransfer {
        program: "cctp".to_string(),
        message: MessageId::Cctp {
            source_domain: parsed.source_domain,
            nonce: hex::encode(parsed.nonce),
        },
        direction: Direction::Inbound,
        observed_tx: tx.signature.clone(),
        event_index,
        source: source.clone(),
        destination: destination.clone(),
        // The origin sender is in the burn body. The origin tx is unknown.
        source_wallet: domain_address(parsed.source_domain, &parsed.message_sender),
        destination_wallet: domain_address(SOLANA_DOMAIN, &parsed.mint_recipient),
        token: domain_address(parsed.source_domain, &parsed.burn_token).map(|address| TokenRef {
            chain: source,
            address,
        }),
        // The burn message carries no symbol; CCTP metadata is a follow-up.
        token_symbol: None,
        amount_raw: Some(parsed.amount),
        via: None,
    })
}

fn read_vec<'a>(data: &'a [u8], at: &mut usize) -> Option<&'a [u8]> {
    let len = u32::from_le_bytes(data.get(*at..*at + 4)?.try_into().ok()?);
    *at += 4;
    let len = usize::try_from(len).ok()?;
    let end = at.checked_add(len)?;
    let out = data.get(*at..end)?;
    *at = end;
    Some(out)
}

struct Inbound {
    source_domain: u32,
    nonce: [u8; 32],
    burn_token: [u8; 32],
    mint_recipient: [u8; 32],
    amount: RawAmount,
    message_sender: [u8; 32],
}

fn parse_message(message: &[u8]) -> Result<Inbound, Reject> {
    let version =
        u32_at(message, 0).ok_or(Reject::Malformed("cctp message header is truncated"))?;
    if version == 0 {
        return Err(Reject::Unsupported("cctp v1 message"));
    }
    if version != MESSAGE_VERSION_V2 {
        return Err(Reject::Unsupported("cctp message version"));
    }
    if message.len() < MESSAGE_HEADER_LEN {
        return Err(Reject::Malformed("cctp message header is truncated"));
    }

    let source_domain =
        u32_at(message, 4).ok_or(Reject::Malformed("cctp message header is truncated"))?;
    let destination_domain =
        u32_at(message, 8).ok_or(Reject::Malformed("cctp message header is truncated"))?;
    if destination_domain != SOLANA_DOMAIN {
        return Err(Reject::Malformed("cctp message is not addressed to Solana"));
    }
    let nonce = word_at(message, 12).ok_or(Reject::Malformed("cctp message nonce is truncated"))?;

    if message.len() < MESSAGE_HEADER_LEN + BURN_FIXED_LEN {
        return Err(Reject::Malformed("cctp burn message is truncated"));
    }
    let body = &message[MESSAGE_HEADER_LEN..];
    let burn_version =
        u32_at(body, 0).ok_or(Reject::Malformed("cctp burn message is truncated"))?;
    if burn_version != BURN_VERSION {
        return Err(Reject::Unsupported("cctp burn message version"));
    }

    let burn_token = word_at(body, 4).ok_or(Reject::Malformed("cctp burn token is truncated"))?;
    let mint_recipient =
        word_at(body, 36).ok_or(Reject::Malformed("cctp mint recipient is truncated"))?;
    let amount_bytes = body
        .get(68..100)
        .ok_or(Reject::Malformed("cctp amount is truncated"))?;
    let amount = RawAmount::from_be_bytes(amount_bytes)
        .ok_or(Reject::Malformed("cctp amount is not a u256"))?;
    let message_sender =
        word_at(body, 100).ok_or(Reject::Malformed("cctp message sender is truncated"))?;

    Ok(Inbound {
        source_domain,
        nonce,
        burn_token,
        mint_recipient,
        amount,
        message_sender,
    })
}

fn u32_at(data: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(data.get(at..at + 4)?.try_into().ok()?))
}

fn word_at(data: &[u8], at: usize) -> Option<[u8; 32]> {
    data.get(at..at + 32)?.try_into().ok()
}

/// A 32-byte CCTP address word in the destination chain's own format. Only the
/// domains this PR seeds are decoded; an unseeded domain keeps the raw reference and
/// leaves the address absent rather than guessing its encoding.
fn domain_address(domain: u32, word: &[u8; 32]) -> Option<String> {
    match domain {
        SOLANA_DOMAIN => Some(bs58::encode(word).into_string()),
        0 | 1 | 3 | 6 | 7 | 15 | 19 | 26 => Some(format!("0x{}", hex::encode(&word[12..]))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u32_be(v: u32) -> [u8; 4] {
        v.to_be_bytes()
    }

    fn message(
        source: u32,
        destination: u32,
        nonce: [u8; 32],
        token: [u8; 32],
        recipient: [u8; 32],
        amount: u64,
        sender: [u8; 32],
    ) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&u32_be(1));
        m.extend_from_slice(&u32_be(source));
        m.extend_from_slice(&u32_be(destination));
        m.extend_from_slice(&nonce);
        m.extend_from_slice(&[0xAA; 32]); // sender program
        m.extend_from_slice(&[0xBB; 32]); // recipient program
        m.extend_from_slice(&[0u8; 32]); // destination caller
        m.extend_from_slice(&u32_be(1000)); // min finality
        m.extend_from_slice(&u32_be(2000)); // executed finality

        let mut body = Vec::new();
        body.extend_from_slice(&u32_be(1));
        body.extend_from_slice(&token);
        body.extend_from_slice(&recipient);
        let mut amount_word = [0u8; 32];
        amount_word[24..].copy_from_slice(&amount.to_be_bytes());
        body.extend_from_slice(&amount_word);
        body.extend_from_slice(&sender);
        body.extend_from_slice(&[0u8; 32]); // max fee
        body.extend_from_slice(&[0u8; 32]); // fee executed
        body.extend_from_slice(&[0u8; 32]); // expiration block
        m.extend_from_slice(&body);
        m
    }

    fn receive_data(message: &[u8]) -> Vec<u8> {
        let mut d = RECEIVE_MESSAGE.to_vec();
        d.extend_from_slice(&(message.len() as u32).to_le_bytes());
        d.extend_from_slice(message);
        d.extend_from_slice(&0u32.to_le_bytes()); // empty attestation
        d
    }

    fn ix(program: &str, data: Vec<u8>) -> Instruction {
        Instruction {
            program_id: program.to_string(),
            accounts: Vec::new(),
            data,
            parsed: None,
            inner_of: None,
        }
    }

    fn tx(instructions: Vec<Instruction>) -> SolanaTx {
        SolanaTx {
            signature: "sig".into(),
            slot: 1,
            block_time: None,
            failed: false,
            signer: None,
            account_keys: Vec::new(),
            accounts: Default::default(),
            instructions,
            logs: Vec::new(),
        }
    }

    fn classify(tx: &SolanaTx) -> Classification {
        CctpAdapter::default().classify(tx)
    }

    fn only_transfer(c: Classification) -> DetectedTransfer {
        match c {
            Classification::Transfers(mut v) if v.len() == 1 => v.remove(0),
            other => panic!("expected one transfer, got {other:?}"),
        }
    }

    #[test]
    fn a_v2_completion_decodes_from_the_attested_message() {
        let nonce = [0xAB; 32];
        let m = message(
            0,
            SOLANA_DOMAIN,
            nonce,
            [0x22; 32],
            [0xCD; 32],
            1_000_000,
            [0x11; 32],
        );
        let t = only_transfer(classify(&tx(vec![ix(
            MESSAGE_TRANSMITTER_V2,
            receive_data(&m),
        )])));

        assert_eq!(t.program, "cctp");
        assert_eq!(t.direction, Direction::Inbound);
        assert_eq!(t.source, RawChainRef::cctp(0));
        assert_eq!(t.destination, RawChainRef::cctp(5));
        assert_eq!(
            t.message,
            MessageId::Cctp {
                source_domain: 0,
                nonce: "ab".repeat(32),
            }
        );
        assert_eq!(
            t.source_wallet.as_deref(),
            Some(format!("0x{}", "11".repeat(20)).as_str())
        );
        assert_eq!(
            t.destination_wallet.as_deref(),
            Some(bs58::encode([0xCD; 32]).into_string().as_str())
        );
        let token = t.token.expect("token");
        assert_eq!(token.chain, RawChainRef::cctp(0));
        assert_eq!(token.address, format!("0x{}", "22".repeat(20)));
        assert_eq!(
            domain_address(7, &[0x22; 32]),
            Some(format!("0x{}", "22".repeat(20)))
        );
        assert_eq!(
            domain_address(15, &[0x22; 32]),
            Some(format!("0x{}", "22".repeat(20)))
        );
        assert_eq!(
            domain_address(19, &[0x22; 32]),
            Some(format!("0x{}", "22".repeat(20)))
        );
        assert_eq!(
            domain_address(26, &[0x22; 32]),
            Some(format!("0x{}", "22".repeat(20)))
        );
        assert_eq!(t.amount_raw.unwrap().as_str(), "1000000");
        assert_eq!(t.event_index, 0);
    }

    #[test]
    fn a_send_is_unsupported_because_its_nonce_is_offchain() {
        let mut data = DEPOSIT_FOR_BURN.to_vec();
        data.extend_from_slice(&[0u8; 8]);
        let c = classify(&tx(vec![ix(TOKEN_MESSENGER_MINTER_V2, data)]));
        assert_eq!(
            c,
            Classification::Rejected(Reject::Unsupported("cctp send: nonce is assigned offchain"))
        );
    }

    #[test]
    fn an_admin_instruction_is_not_a_transfer() {
        let c = classify(&tx(vec![ix(TOKEN_MESSENGER_MINTER_V2, vec![9u8; 16])]));
        assert_eq!(
            c,
            Classification::Rejected(Reject::NotTransfer("cctp admin instruction"))
        );
    }

    #[test]
    fn a_v1_message_is_unsupported_not_malformed() {
        let mut m = vec![0u8; 116];
        m[..4].copy_from_slice(&u32_be(0));
        let c = classify(&tx(vec![ix(MESSAGE_TRANSMITTER_V2, receive_data(&m))]));
        assert_eq!(
            c,
            Classification::Rejected(Reject::Unsupported("cctp v1 message"))
        );
    }

    #[test]
    fn truncated_receive_data_is_malformed() {
        let mut data = RECEIVE_MESSAGE.to_vec();
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(&[0u8; 10]);
        assert!(matches!(
            classify(&tx(vec![ix(MESSAGE_TRANSMITTER_V2, data)])),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn a_truncated_header_is_malformed() {
        let mut m = vec![0u8; 20];
        m[..4].copy_from_slice(&u32_be(1));
        let c = classify(&tx(vec![ix(MESSAGE_TRANSMITTER_V2, receive_data(&m))]));
        assert_eq!(
            c,
            Classification::Rejected(Reject::Malformed("cctp message header is truncated"))
        );
    }

    #[test]
    fn a_message_not_addressed_to_solana_is_malformed() {
        let m = message(
            0, 6, // Base, not Solana
            [1; 32], [2; 32], [3; 32], 1, [4; 32],
        );
        assert!(matches!(
            classify(&tx(vec![ix(MESSAGE_TRANSMITTER_V2, receive_data(&m))])),
            Classification::Rejected(Reject::Malformed(_))
        ));
    }

    #[test]
    fn an_unseeded_source_domain_keeps_the_raw_reference() {
        let m = message(
            9999,
            SOLANA_DOMAIN,
            [7; 32],
            [0x22; 32],
            [0xCD; 32],
            5,
            [0x11; 32],
        );
        let t = only_transfer(classify(&tx(vec![ix(
            MESSAGE_TRANSMITTER_V2,
            receive_data(&m),
        )])));
        assert_eq!(t.source, RawChainRef::cctp(9999));
        assert_eq!(t.source_wallet, None);
        assert_eq!(t.token, None);
    }
}
