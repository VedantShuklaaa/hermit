//! Pure decoding of the two Wormhole EVM logs we watch.
//!
//! Core Bridge emits `LogMessagePublished(address indexed sender, uint64 sequence,
//! uint32 nonce, bytes payload, uint8 consistencyLevel)` from
//! `ethereum/contracts/Implementation.sol`. Only calls made by the Token Bridge are
//! ours; the indexed `sender` topic is checked against the configured bridge address.
//!
//! Token Bridge emits `TransferRedeemed(uint16 indexed emitterChainId, bytes32 indexed
//! emitterAddress, uint64 indexed sequence)` from
//! `ethereum/contracts/bridge/Bridge.sol` (`_completeTransfer`). It names the original
//! message, which is exactly the join key.

use super::client::Log;

pub const LOG_MESSAGE_PUBLISHED_SIGNATURE: &str =
    "LogMessagePublished(address,uint64,uint32,bytes,uint8)";
pub const TRANSFER_REDEEMED_SIGNATURE: &str = "TransferRedeemed(uint16,bytes32,uint64)";

/// `keccak256("LogMessagePublished(address,uint64,uint32,bytes,uint8)")`.
pub const LOG_MESSAGE_PUBLISHED_TOPIC: &str =
    "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2";
/// `keccak256("TransferRedeemed(uint16,bytes32,uint64)")`.
pub const TRANSFER_REDEEMED_TOPIC: &str =
    "0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    LogMessagePublished,
    TransferRedeemed,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LogMessagePublished => "log_message_published",
            Self::TransferRedeemed => "transfer_redeemed",
        }
    }

    pub fn topic0(self) -> &'static str {
        match self {
            Self::LogMessagePublished => LOG_MESSAGE_PUBLISHED_TOPIC,
            Self::TransferRedeemed => TRANSFER_REDEEMED_TOPIC,
        }
    }
}

/// A log that belongs to a Wormhole Token Bridge transfer, keyed by message identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedLog {
    pub event_kind: EventKind,
    /// The source chain of the message. For a send this is the watched chain; for a
    /// redeem it is whichever chain originally emitted the message.
    pub emitter_chain: u16,
    /// 32-byte lowercase hex without `0x`.
    pub emitter_address: String,
    pub sequence: u64,
}

/// Decodes one log, or returns `None` when it is not one of ours.
///
/// `chain_wormhole_id` is the Wormhole ID of the watched chain, used as the emitter
/// chain for `LogMessagePublished` (the Core Bridge only emits for its own chain).
/// Address and topic comparisons are case-insensitive.
pub fn decode_log(
    log: &Log,
    chain_wormhole_id: u16,
    core_bridge_address: &str,
    token_bridge_address: &str,
) -> Option<DecodedLog> {
    if log.removed {
        return None;
    }
    let topic0 = log.topics.first()?.to_ascii_lowercase();
    let address = log.address.to_ascii_lowercase();

    if topic0 == LOG_MESSAGE_PUBLISHED_TOPIC {
        if address != core_bridge_address.to_ascii_lowercase() {
            return None;
        }
        let sender = parse_topic_bytes32(log.topics.get(1)?)?;
        let token_bridge = pad_address(token_bridge_address)?;
        if sender != token_bridge {
            return None;
        }
        let sequence = parse_u64_word(&log.data, 0)?;
        return Some(DecodedLog {
            event_kind: EventKind::LogMessagePublished,
            emitter_chain: chain_wormhole_id,
            emitter_address: token_bridge,
            sequence,
        });
    }

    if topic0 == TRANSFER_REDEEMED_TOPIC {
        if address != token_bridge_address.to_ascii_lowercase() {
            return None;
        }
        return Some(DecodedLog {
            event_kind: EventKind::TransferRedeemed,
            emitter_chain: parse_u16_topic(log.topics.get(1)?)?,
            emitter_address: parse_topic_bytes32(log.topics.get(2)?)?,
            sequence: parse_u64_topic(log.topics.get(3)?)?,
        });
    }

    None
}

fn strip_0x(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

/// A 20-byte address as the 32-byte left-padded word Wormhole uses.
pub fn pad_address(address: &str) -> Option<String> {
    let bytes = hex::decode(strip_0x(address)).ok()?;
    if bytes.len() != 20 {
        return None;
    }
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(&bytes);
    Some(hex::encode(word))
}

fn parse_topic_bytes32(topic: &str) -> Option<String> {
    let bytes = hex::decode(strip_0x(topic)).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    Some(hex::encode(bytes))
}

fn parse_u16_topic(topic: &str) -> Option<u16> {
    let bytes = hex::decode(strip_0x(topic)).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    Some(u16::from_be_bytes([bytes[30], bytes[31]]))
}

fn parse_u64_topic(topic: &str) -> Option<u64> {
    let bytes = hex::decode(strip_0x(topic)).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    Some(u64::from_be_bytes(bytes[24..32].try_into().ok()?))
}

/// Reads the `index`-th 32-byte word of ABI `data` as a uint64.
fn parse_u64_word(data: &[u8], index: usize) -> Option<u64> {
    let start = index.checked_mul(32)?;
    let word = data.get(start..start.checked_add(32)?)?;
    Some(u64::from_be_bytes(word[24..32].try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(address: &str, topics: Vec<String>, data: Vec<u8>) -> Log {
        Log {
            address: address.into(),
            topics,
            data,
            block_number: 1,
            block_hash: "0x".to_string() + &"ab".repeat(32),
            tx_hash: "0x".to_string() + &"cd".repeat(32),
            log_index: 0,
            removed: false,
        }
    }

    fn topic_u16(value: u16) -> String {
        format!("0x{:064x}", value)
    }

    fn topic_u64(value: u64) -> String {
        format!("0x{:064x}", value)
    }

    fn topic_address(address: &str) -> String {
        format!("0x{}", pad_address(address).unwrap())
    }

    const CORE: &str = "0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B";
    const TOKEN: &str = "0x3ee18B2214AFF97000D974cf647E7C347E8fa585";
    const EMITTER: &str = "0000000000000000000000003ee18b2214aff97000d974cf647e7c347e8fa585";

    #[test]
    fn hardcoded_topics_are_keccak_of_their_signatures() {
        assert_eq!(
            keccak::keccak256_hex(LOG_MESSAGE_PUBLISHED_SIGNATURE.as_bytes()),
            LOG_MESSAGE_PUBLISHED_TOPIC
        );
        assert_eq!(
            keccak::keccak256_hex(TRANSFER_REDEEMED_SIGNATURE.as_bytes()),
            TRANSFER_REDEEMED_TOPIC
        );
    }

    #[test]
    fn a_token_bridge_send_decodes_to_the_message_identity() {
        let mut data = vec![0u8; 4 * 32];
        data[24..32].copy_from_slice(&42u64.to_be_bytes());
        let decoded = decode_log(
            &log(
                CORE,
                vec![
                    LOG_MESSAGE_PUBLISHED_TOPIC.to_string(),
                    topic_address(TOKEN),
                ],
                data,
            ),
            2,
            CORE,
            TOKEN,
        )
        .expect("decoded");

        assert_eq!(decoded.event_kind, EventKind::LogMessagePublished);
        assert_eq!(decoded.emitter_chain, 2);
        assert_eq!(decoded.emitter_address, EMITTER);
        assert_eq!(decoded.sequence, 42);
    }

    #[test]
    fn a_send_from_another_sender_is_not_a_token_bridge_transfer() {
        let data = vec![0u8; 32];
        assert!(
            decode_log(
                &log(
                    CORE,
                    vec![
                        LOG_MESSAGE_PUBLISHED_TOPIC.to_string(),
                        topic_address("0x1111111111111111111111111111111111111111"),
                    ],
                    data,
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
    }

    #[test]
    fn a_send_from_another_contract_is_ignored() {
        let data = vec![0u8; 32];
        assert!(
            decode_log(
                &log(
                    "0x2222222222222222222222222222222222222222",
                    vec![
                        LOG_MESSAGE_PUBLISHED_TOPIC.to_string(),
                        topic_address(TOKEN),
                    ],
                    data,
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
    }

    #[test]
    fn a_redeem_decodes_the_original_message_identity() {
        let decoded = decode_log(
            &log(
                TOKEN,
                vec![
                    TRANSFER_REDEEMED_TOPIC.to_string(),
                    topic_u16(2),
                    format!("0x{EMITTER}"),
                    topic_u64(42),
                ],
                Vec::new(),
            ),
            2,
            CORE,
            TOKEN,
        )
        .expect("decoded");

        assert_eq!(decoded.event_kind, EventKind::TransferRedeemed);
        assert_eq!(decoded.emitter_chain, 2);
        assert_eq!(decoded.emitter_address, EMITTER);
        assert_eq!(decoded.sequence, 42);
    }

    #[test]
    fn a_redeem_from_another_contract_is_ignored() {
        assert!(
            decode_log(
                &log(
                    CORE,
                    vec![
                        TRANSFER_REDEEMED_TOPIC.to_string(),
                        topic_u16(2),
                        format!("0x{EMITTER}"),
                        topic_u64(42),
                    ],
                    Vec::new(),
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
    }

    #[test]
    fn removed_logs_and_unknown_topics_are_ignored() {
        let mut removed = log(
            TOKEN,
            vec![
                TRANSFER_REDEEMED_TOPIC.to_string(),
                topic_u16(2),
                format!("0x{EMITTER}"),
                topic_u64(42),
            ],
            Vec::new(),
        );
        removed.removed = true;
        assert!(decode_log(&removed, 2, CORE, TOKEN).is_none());

        assert!(decode_log(&log(TOKEN, vec![], Vec::new()), 2, CORE, TOKEN).is_none());
        assert!(
            decode_log(
                &log(TOKEN, vec!["0xdeadbeef".to_string()], Vec::new()),
                2,
                CORE,
                TOKEN
            )
            .is_none()
        );
    }

    #[test]
    fn malformed_topics_and_data_are_ignored_not_panics() {
        // A short sender topic, a short redeem topic and a send without its data word.
        assert!(
            decode_log(
                &log(
                    CORE,
                    vec![LOG_MESSAGE_PUBLISHED_TOPIC.to_string(), "0x12".to_string()],
                    vec![0u8; 32],
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
        assert!(
            decode_log(
                &log(
                    TOKEN,
                    vec![
                        TRANSFER_REDEEMED_TOPIC.to_string(),
                        "0x12".to_string(),
                        format!("0x{EMITTER}"),
                        topic_u64(42),
                    ],
                    Vec::new(),
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
        assert!(
            decode_log(
                &log(
                    CORE,
                    vec![
                        LOG_MESSAGE_PUBLISHED_TOPIC.to_string(),
                        topic_address(TOKEN),
                    ],
                    Vec::new(),
                ),
                2,
                CORE,
                TOKEN,
            )
            .is_none()
        );
    }

    #[test]
    fn address_padding_rejects_non_addresses() {
        assert!(pad_address("0x12").is_none());
        assert!(pad_address(TOKEN).is_some());
    }

    /// A test-only Keccak-256. The production code does not need a hash function; this
    /// proves the four hardcoded topic constants are the hashes of their signatures.
    mod keccak {
        const RC: [u64; 24] = [
            0x0000000000000001,
            0x0000000000008082,
            0x800000000000808a,
            0x8000000080008000,
            0x000000000000808b,
            0x0000000080000001,
            0x8000000080008081,
            0x8000000000008009,
            0x000000000000008a,
            0x0000000000000088,
            0x0000000080008009,
            0x000000008000000a,
            0x000000008000808b,
            0x800000000000008b,
            0x8000000000008089,
            0x8000000000008003,
            0x8000000000008002,
            0x8000000000000080,
            0x000000000000800a,
            0x800000008000000a,
            0x8000000080008081,
            0x8000000000008080,
            0x0000000080000001,
            0x8000000080008008,
        ];

        const RHO: [u32; 25] = [
            0, 1, 62, 28, 27, //
            36, 44, 6, 55, 20, //
            3, 10, 43, 25, 39, //
            41, 45, 15, 21, 8, //
            18, 2, 61, 56, 14,
        ];

        const RATE: usize = 136;

        pub fn keccak256_hex(input: &[u8]) -> String {
            format!("0x{}", hex::encode(keccak256(input)))
        }

        fn keccak256(input: &[u8]) -> [u8; 32] {
            let mut padded = input.to_vec();
            padded.push(0x01);
            while !padded.len().is_multiple_of(RATE) {
                padded.push(0);
            }
            let last = padded.len() - 1;
            padded[last] |= 0x80;

            let mut state = [0u64; 25];
            for block in padded.as_chunks::<RATE>().0 {
                for (lane, bytes) in state.iter_mut().zip(block.as_chunks::<8>().0) {
                    *lane ^= u64::from_le_bytes(*bytes);
                }
                keccak_f(&mut state);
            }

            let mut out = [0u8; 32];
            for (index, lane) in state.iter().take(4).enumerate() {
                out[index * 8..(index + 1) * 8].copy_from_slice(&lane.to_le_bytes());
            }
            out
        }

        fn keccak_f(a: &mut [u64; 25]) {
            for rc in RC {
                let mut c = [0u64; 5];
                for x in 0..5 {
                    c[x] = a[x] ^ a[x + 5] ^ a[x + 10] ^ a[x + 15] ^ a[x + 20];
                }
                let mut d = [0u64; 5];
                for x in 0..5 {
                    d[x] = c[(x + 4) % 5] ^ c[(x + 1) % 5].rotate_left(1);
                }
                for x in 0..5 {
                    for y in 0..5 {
                        a[x + 5 * y] ^= d[x];
                    }
                }

                let mut b = [0u64; 25];
                for x in 0..5 {
                    for y in 0..5 {
                        b[y + 5 * ((2 * x + 3 * y) % 5)] = a[x + 5 * y].rotate_left(RHO[x + 5 * y]);
                    }
                }

                for x in 0..5 {
                    for y in 0..5 {
                        a[x + 5 * y] =
                            b[x + 5 * y] ^ ((!b[(x + 1) % 5 + 5 * y]) & b[(x + 2) % 5 + 5 * y]);
                    }
                }

                a[0] ^= rc;
            }
        }

        #[test]
        fn keccak_matches_known_vectors() {
            assert_eq!(
                keccak256_hex(b""),
                "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
            );
            assert_eq!(
                keccak256_hex(b"abc"),
                "0x4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
            );
        }
    }
}
