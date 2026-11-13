//! deBridge DLN source side on Solana. Pure: no IO, no clock, no database.
//!
//! Verification sources:
//!
//! * program IDs — <https://docs.debridge.com/dln-details/overview/deployed-contracts>
//!   (`DlnSource`)
//! * instruction and event layouts — the official IDL linked from those docs,
//!   <https://github.com/debridge-finance/abis-and-idls/blob/master/idls/src.ts>
//! * chain IDs — <https://docs.debridge.com/home/architecture/supported-chains>
//! * discriminators — Anchor's `sha256("global:<snake_case>")[..8]` for instructions
//!   and `sha256("event:<EventName>")[..8]` for events. The `create_order_with_nonce`
//!   value and both event values are confirmed against mainnet tx
//!   `3ggTVbZvk38HfKTQ8fcTYkvTeDc1uw75KTGUzpm1MVHFJWHhFSbQBALshBmszepZDULTqTqxnMJhAPiT6UgXMUK5d`.
//!
//! Only order creation is indexed. The order id is read from the `CreatedOrderId`
//! event emitted in the same successful transaction, so no hashing dependency is
//! needed. Every other `DlnSource` instruction is not a transfer.

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DEBRIDGE_PROGRAM, DetectedTransfer, Direction, MessageId,
    Protocol, Reject, SolanaTx, TokenRef,
};

/// Verified mainnet `DlnSource` program.
pub const DLN_SOURCE: &str = "src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4";

/// Solana's deBridge DLN internal chain ID.
const SOLANA_CHAIN: &str = "7565164";

// Anchor instruction discriminators (`sha256("global:<snake_case>")[..8]`).
const CREATE_ORDER: [u8; 8] = [0x8d, 0x36, 0x25, 0xcf, 0xed, 0xd2, 0xfa, 0xd7];
const CREATE_ORDER_WITH_NONCE: [u8; 8] = [0x82, 0x83, 0x62, 0xbe, 0x28, 0xce, 0x44, 0x32];

// Anchor event discriminators (`sha256("event:<EventName>")[..8]`).
const CREATED_ORDER: [u8; 8] = [0x6a, 0x57, 0x4a, 0xe6, 0x20, 0x78, 0x47, 0xd2];
const CREATED_ORDER_ID: [u8; 8] = [0xbb, 0x29, 0x1b, 0x49, 0x0c, 0x3f, 0x0d, 0x54];

/// deBridge DLN source adapter. The program ID is fixed, so the adapter is always
/// registered.
pub struct DebridgeAdapter;

impl BridgeAdapter for DebridgeAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Debridge
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[DLN_SOURCE]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let mut creates = 0usize;
        let mut saw_dln = false;
        for ix in tx
            .instructions
            .iter()
            .filter(|ix| ix.program_id == DLN_SOURCE)
        {
            saw_dln = true;
            let Some(discriminator) = ix.data.get(..8) else {
                return Classification::Rejected(Reject::Malformed(
                    "debridge instruction without discriminator",
                ));
            };
            if discriminator == CREATE_ORDER || discriminator == CREATE_ORDER_WITH_NONCE {
                creates += 1;
            }
            // Everything else on DlnSource is admin, configuration or another order
            // action. None of those is a transfer.
        }

        if creates == 0 {
            return Classification::Rejected(Reject::NotTransfer(if saw_dln {
                "debridge admin instruction"
            } else {
                "no debridge instruction"
            }));
        }

        let mut orders = Vec::new();
        let mut order_ids = Vec::new();
        for event in dln_events(&tx.logs) {
            match event {
                Event::Order(payload) => orders.push(payload),
                Event::OrderId(payload) => {
                    if payload.len() != 40 {
                        return Classification::Rejected(Reject::Malformed(
                            "debridge CreatedOrderId event is truncated",
                        ));
                    }
                    let mut id = [0u8; 32];
                    id.copy_from_slice(&payload[8..40]);
                    order_ids.push(id);
                }
            }
        }

        if orders.len() != creates || order_ids.len() != creates {
            return Classification::Rejected(Reject::Malformed(
                "debridge events do not match order instructions",
            ));
        }

        let mut transfers = Vec::with_capacity(creates);
        for (index, (order, id)) in orders.iter().zip(order_ids.iter()).enumerate() {
            match build_transfer(tx, order, id, index as u16) {
                Ok(transfer) => transfers.push(transfer),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        Classification::Transfers(transfers)
    }
}

fn build_transfer(
    tx: &SolanaTx,
    payload: &[u8],
    order_id: &[u8; 32],
    event_index: u16,
) -> Result<DetectedTransfer, Reject> {
    let order = parse_order(payload)?;
    let give_chain = decimal(&order.give.chain);
    if give_chain != SOLANA_CHAIN {
        return Err(Reject::Malformed("debridge order give chain is not Solana"));
    }
    let take_chain = decimal(&order.take.chain);

    let source = RawChainRef::debridge(SOLANA_CHAIN);
    let destination = RawChainRef::debridge(take_chain.as_str());
    let token_address = solana_address(&order.give.token).ok_or(Reject::Malformed(
        "debridge order give token is not a Solana mint",
    ))?;
    let amount = RawAmount::from_be_bytes(&order.give.amount).ok_or(Reject::Malformed(
        "debridge order give amount is not a u256",
    ))?;

    Ok(DetectedTransfer {
        program: DEBRIDGE_PROGRAM.to_string(),
        message: MessageId::Debridge {
            order_id: hex::encode(order_id),
        },
        direction: Direction::Outbound,
        observed_tx: tx.signature.clone(),
        event_index,
        source: source.clone(),
        destination,
        // The maker is in the order; the fee payer is only a fallback.
        source_wallet: solana_address(&order.maker).or_else(|| tx.signer.clone()),
        // An unknown destination chain is stored unresolved, never an error.
        destination_wallet: decode_address(&take_chain, &order.receiver),
        token: Some(TokenRef {
            chain: source,
            address: token_address,
        }),
        token_symbol: None,
        amount_raw: Some(amount),
        via: None,
    })
}

struct Order {
    maker: Vec<u8>,
    give: Offer,
    take: Offer,
    receiver: Vec<u8>,
}

struct Offer {
    chain: [u8; 32],
    token: Vec<u8>,
    amount: [u8; 32],
}

/// The `CreatedOrder` event starts with the `Order` struct. Only the fields the
/// transfer needs are read; the authority and optional-call fields that follow the
/// receiver are ignored.
fn parse_order(payload: &[u8]) -> Result<Order, Reject> {
    if !payload.starts_with(&CREATED_ORDER) {
        return Err(Reject::Malformed(
            "debridge CreatedOrder event discriminator",
        ));
    }
    let mut reader = Reader::new(&payload[8..]);
    let truncated = || Reject::Malformed("debridge CreatedOrder event is truncated");

    let _maker_order_nonce = reader.u64().ok_or_else(truncated)?;
    let maker = reader.vec().ok_or_else(truncated)?.to_vec();
    let give = parse_offer(&mut reader)?;
    let take = parse_offer(&mut reader)?;
    let receiver = reader.vec().ok_or_else(truncated)?.to_vec();

    Ok(Order {
        maker,
        give,
        take,
        receiver,
    })
}

fn parse_offer(reader: &mut Reader<'_>) -> Result<Offer, Reject> {
    let truncated = || Reject::Malformed("debridge CreatedOrder event is truncated");
    Ok(Offer {
        chain: reader.array32().ok_or_else(truncated)?,
        token: reader.vec().ok_or_else(truncated)?.to_vec(),
        amount: reader.array32().ok_or_else(truncated)?,
    })
}

struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let out = self.data.get(self.at..end)?;
        self.at = end;
        Some(out)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn array32(&mut self) -> Option<[u8; 32]> {
        self.take(32)?.try_into().ok()
    }

    fn vec(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u32()?).ok()?;
        self.take(len)
    }
}

enum Event {
    Order(Vec<u8>),
    OrderId(Vec<u8>),
}

/// `CreatedOrder` and `CreatedOrderId` events printed while `DlnSource` is the
/// running program, in log order. Events from inner programs are ignored.
fn dln_events(logs: &[String]) -> Vec<Event> {
    let mut stack: Vec<&str> = Vec::new();
    let mut out = Vec::new();

    for line in logs {
        let Some(rest) = line.strip_prefix("Program ") else {
            continue;
        };
        if let Some(encoded) = rest.strip_prefix("data: ") {
            if stack.last() != Some(&DLN_SOURCE) {
                continue;
            }
            let Ok(bytes) = STANDARD.decode(encoded) else {
                continue;
            };
            if bytes.starts_with(&CREATED_ORDER) {
                out.push(Event::Order(bytes));
            } else if bytes.starts_with(&CREATED_ORDER_ID) {
                out.push(Event::OrderId(bytes));
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

#[derive(Clone, Copy)]
enum ChainKind {
    Solana,
    Evm,
}

/// The destination chains the migration seeds. An unknown chain is not an error:
/// the reference is stored unresolved and no address is decoded.
fn chain_kind(chain: &str) -> Option<ChainKind> {
    if chain == SOLANA_CHAIN {
        return Some(ChainKind::Solana);
    }
    match chain {
        "1" | "10" | "56" | "137" | "5042" | "8453" | "42161" | "43114" | "100000022"
        | "100000030" => Some(ChainKind::Evm),
        _ => None,
    }
}

fn decode_address(chain: &str, bytes: &[u8]) -> Option<String> {
    match chain_kind(chain)? {
        ChainKind::Solana => solana_address(bytes),
        ChainKind::Evm => {
            let address = match bytes.len() {
                20 => bytes,
                32 if bytes[..12].iter().all(|b| *b == 0) => &bytes[12..],
                _ => return None,
            };
            Some(format!("0x{}", hex::encode(address)))
        }
    }
}

fn solana_address(bytes: &[u8]) -> Option<String> {
    (bytes.len() == 32).then(|| bs58::encode(bytes).into_string())
}

/// A 32-byte big-endian chain ID as a decimal string.
fn decimal(bytes: &[u8]) -> String {
    RawAmount::from_be_bytes(bytes)
        .map(|amount| amount.to_string())
        .unwrap_or_else(|| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::RawChainRef;
    use crate::protocols::Instruction;

    const ORDER_ID: [u8; 32] = [0x44; 32];
    const MAKER: [u8; 32] = [0x09; 32];
    const GIVE_TOKEN: [u8; 32] = [0x07; 32];
    const TAKE_TOKEN: [u8; 20] = [0x22; 20];
    const RECEIVER: [u8; 20] = [0x33; 20];

    fn be32(value: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&value.to_be_bytes());
        out
    }

    fn put_vec(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend((bytes.len() as u32).to_le_bytes());
        out.extend(bytes);
    }

    fn created_order(
        give_chain: [u8; 32],
        give_token: &[u8],
        give_amount: [u8; 32],
        take_chain: [u8; 32],
        take_token: &[u8],
        take_amount: [u8; 32],
        receiver: &[u8],
    ) -> Vec<u8> {
        let mut out = CREATED_ORDER.to_vec();
        out.extend(0u64.to_le_bytes());
        put_vec(&mut out, &MAKER);
        out.extend(give_chain);
        put_vec(&mut out, give_token);
        out.extend(give_amount);
        out.extend(take_chain);
        put_vec(&mut out, take_token);
        out.extend(take_amount);
        put_vec(&mut out, receiver);
        out
    }

    fn order_id_event(id: &[u8; 32]) -> Vec<u8> {
        let mut out = CREATED_ORDER_ID.to_vec();
        out.extend(id);
        out
    }

    fn create_instruction(discriminator: [u8; 8]) -> Instruction {
        Instruction {
            program_id: DLN_SOURCE.to_string(),
            accounts: Vec::new(),
            data: discriminator.to_vec(),
            parsed: None,
            inner_of: None,
        }
    }

    fn tx_with(instructions: Vec<Instruction>, logs: Vec<String>) -> SolanaTx {
        SolanaTx {
            signature: "sig".to_string(),
            slot: 1,
            block_time: None,
            failed: false,
            signer: Some("signer".to_string()),
            account_keys: Vec::new(),
            accounts: Default::default(),
            instructions,
            logs,
        }
    }

    fn create_logs(order: &[u8], id: &[u8]) -> Vec<String> {
        vec![
            format!("Program {DLN_SOURCE} invoke [1]"),
            format!("Program data: {}", STANDARD.encode(order)),
            format!("Program data: {}", STANDARD.encode(id)),
            format!("Program {DLN_SOURCE} success"),
        ]
    }

    fn only_transfer(classification: Classification) -> DetectedTransfer {
        match classification {
            Classification::Transfers(mut transfers) => {
                assert_eq!(transfers.len(), 1);
                transfers.remove(0)
            }
            other => panic!("expected a transfer, got {other:?}"),
        }
    }

    #[test]
    fn decodes_an_order_from_the_created_order_and_order_id_events() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(3_000_000),
            be32(1),
            &TAKE_TOKEN,
            be32(2_000_000),
            &RECEIVER,
        );
        let tx = tx_with(
            vec![create_instruction(CREATE_ORDER_WITH_NONCE)],
            create_logs(&order, &order_id_event(&ORDER_ID)),
        );

        let transfer = only_transfer(DebridgeAdapter.classify(&tx));
        assert_eq!(transfer.program, DEBRIDGE_PROGRAM);
        assert_eq!(
            transfer.message,
            MessageId::Debridge {
                order_id: hex::encode(ORDER_ID),
            }
        );
        assert_eq!(transfer.message.key(), hex::encode(ORDER_ID));
        assert_eq!(transfer.direction, Direction::Outbound);
        assert_eq!(transfer.event_index, 0);
        assert_eq!(transfer.observed_tx, "sig");
        assert_eq!(transfer.source, RawChainRef::debridge("7565164"));
        assert_eq!(transfer.destination, RawChainRef::debridge("1"));
        assert_eq!(
            transfer.amount_raw.as_ref().map(|a| a.as_str()),
            Some("3000000")
        );
        let token = transfer.token.expect("token");
        assert_eq!(token.chain, RawChainRef::debridge("7565164"));
        assert_eq!(token.address, bs58::encode(GIVE_TOKEN).into_string());
        assert_eq!(
            transfer.destination_wallet.as_deref(),
            Some(format!("0x{}", hex::encode(RECEIVER)).as_str())
        );
        assert_eq!(
            transfer.source_wallet.as_deref(),
            Some(bs58::encode(MAKER).into_string().as_str())
        );
    }

    #[test]
    fn a_plain_create_order_instruction_is_detected_too() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(1),
            be32(56),
            &TAKE_TOKEN,
            be32(1),
            &RECEIVER,
        );
        let tx = tx_with(
            vec![create_instruction(CREATE_ORDER)],
            create_logs(&order, &order_id_event(&ORDER_ID)),
        );
        assert!(matches!(
            DebridgeAdapter.classify(&tx),
            Classification::Transfers(_)
        ));
    }

    #[test]
    fn an_unknown_destination_chain_is_unresolved_not_an_error() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(1),
            be32(999_999),
            &TAKE_TOKEN,
            be32(1),
            &RECEIVER,
        );
        let tx = tx_with(
            vec![create_instruction(CREATE_ORDER_WITH_NONCE)],
            create_logs(&order, &order_id_event(&ORDER_ID)),
        );

        let transfer = only_transfer(DebridgeAdapter.classify(&tx));
        assert_eq!(transfer.destination.id, "999999");
        assert_eq!(transfer.destination_wallet, None);
        assert!(transfer.token.is_some(), "the source token is still known");
    }

    #[test]
    fn every_other_dln_instruction_is_not_a_transfer() {
        let tx = tx_with(
            vec![create_instruction([0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0])],
            vec![
                format!("Program {DLN_SOURCE} invoke [1]"),
                format!("Program {DLN_SOURCE} success"),
            ],
        );
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("debridge admin instruction"))
        );
    }

    #[test]
    fn a_failed_transaction_is_never_a_transfer() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(1),
            be32(1),
            &TAKE_TOKEN,
            be32(1),
            &RECEIVER,
        );
        let mut tx = tx_with(
            vec![create_instruction(CREATE_ORDER_WITH_NONCE)],
            create_logs(&order, &order_id_event(&ORDER_ID)),
        );
        tx.failed = true;
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn a_create_without_an_order_id_event_is_malformed() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(1),
            be32(1),
            &TAKE_TOKEN,
            be32(1),
            &RECEIVER,
        );
        let tx = tx_with(
            vec![create_instruction(CREATE_ORDER_WITH_NONCE)],
            vec![
                format!("Program {DLN_SOURCE} invoke [1]"),
                format!("Program data: {}", STANDARD.encode(order)),
                format!("Program {DLN_SOURCE} success"),
            ],
        );
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed(
                "debridge events do not match order instructions"
            ))
        );
    }

    #[test]
    fn an_event_printed_by_an_inner_program_is_ignored() {
        let order = created_order(
            be32(7_565_164),
            &GIVE_TOKEN,
            be32(1),
            be32(1),
            &TAKE_TOKEN,
            be32(1),
            &RECEIVER,
        );
        let tx = tx_with(
            vec![create_instruction(CREATE_ORDER_WITH_NONCE)],
            vec![
                format!("Program {DLN_SOURCE} invoke [1]"),
                format!("Program data: {}", STANDARD.encode(order)),
                "Program Other invoke [2]".to_string(),
                format!(
                    "Program data: {}",
                    STANDARD.encode(order_id_event(&ORDER_ID))
                ),
                "Program Other success".to_string(),
                format!(
                    "Program data: {}",
                    STANDARD.encode(order_id_event(&ORDER_ID))
                ),
                format!("Program {DLN_SOURCE} success"),
            ],
        );
        assert!(matches!(
            DebridgeAdapter.classify(&tx),
            Classification::Transfers(_)
        ));
    }
}
