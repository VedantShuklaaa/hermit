//! deBridge DLN on Solana: source-side order creation and destination-side
//! fulfillment. Pure: no IO, no clock, no database.
//!
//! Verification sources:
//!
//! * program IDs — <https://docs.debridge.com/dln-details/overview/deployed-contracts>
//!   (`DlnSource`, `DlnDestination`)
//! * instruction and event layouts — the official IDLs linked from those docs,
//!   <https://github.com/debridge-finance/abis-and-idls/blob/master/idls/src.ts> and
//!   <https://github.com/debridge-finance/abis-and-idls/blob/master/idls/dst.ts>
//! * chain IDs — <https://docs.debridge.com/home/architecture/supported-chains>
//! * discriminators — Anchor's `sha256("global:<snake_case>")[..8]` for instructions
//!   and `sha256("event:<EventName>")[..8]` for events. The `create_order_with_nonce`
//!   value and both source event values are confirmed against mainnet tx
//!   `3ggTVbZvk38HfKTQ8fcTYkvTeDc1uw75KTGUzpm1MVHFJWHhFSbQBALshBmszepZDULTqTqxnMJhAPiT6UgXMUK5d`.
//!   The `fulfill_order` and `Fulfilled` values are confirmed against mainnet tx
//!   `4V5XFQ8ViuWi4VxHWb7bCZtWGzyPgZnKTf6ABnC1VUXd2y5wgfrn8EGmzwst1iP19ySPA7jwx87KWX3S2GYUh8Lr`.
//!
//! Source-side `createOrder`/`createOrderWithNonce` are outbound. Destination-side
//! `fulfillOrder` is inbound. Both read the 32-byte order id from an event emitted in
//! the same successful transaction (`CreatedOrderId`, `Fulfilled`), so a fill and its
//! source order share one `(protocol, message_key)` row. Cancel, claim and unlock
//! instructions are not transfers and are not indexed.

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DEBRIDGE_PROGRAM, DetectedTransfer, Direction, Instruction,
    MessageId, Protocol, Reject, SolanaTx, TokenRef,
};

/// Verified mainnet `DlnSource` program.
pub const DLN_SOURCE: &str = "src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4";
/// Verified mainnet `DlnDestination` program.
pub const DLN_DESTINATION: &str = "dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo";

/// Solana's deBridge DLN internal chain ID.
const SOLANA_CHAIN: &str = "7565164";

// Anchor instruction discriminators (`sha256("global:<snake_case>")[..8]`).
const CREATE_ORDER: [u8; 8] = [0x8d, 0x36, 0x25, 0xcf, 0xed, 0xd2, 0xfa, 0xd7];
const CREATE_ORDER_WITH_NONCE: [u8; 8] = [0x82, 0x83, 0x62, 0xbe, 0x28, 0xce, 0x44, 0x32];
const FULFILL_ORDER: [u8; 8] = [0x3d, 0xd6, 0x27, 0xf8, 0x41, 0xd4, 0x99, 0x24];

// Anchor event discriminators (`sha256("event:<EventName>")[..8]`).
const CREATED_ORDER: [u8; 8] = [0x6a, 0x57, 0x4a, 0xe6, 0x20, 0x78, 0x47, 0xd2];
const CREATED_ORDER_ID: [u8; 8] = [0xbb, 0x29, 0x1b, 0x49, 0x0c, 0x3f, 0x0d, 0x54];
const FULFILLED: [u8; 8] = [0xd2, 0xae, 0x83, 0xd5, 0x28, 0xb6, 0x53, 0x6e];

/// deBridge DLN adapter. Both program IDs are fixed, so the adapter is always
/// registered.
pub struct DebridgeAdapter;

impl BridgeAdapter for DebridgeAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Debridge
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[DLN_SOURCE, DLN_DESTINATION]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let mut creates = Vec::new();
        let mut fills = Vec::new();
        let mut saw_dln = false;
        for ix in tx
            .instructions
            .iter()
            .filter(|ix| ix.program_id == DLN_SOURCE || ix.program_id == DLN_DESTINATION)
        {
            saw_dln = true;
            let Some(discriminator) = ix.data.get(..8) else {
                return Classification::Rejected(Reject::Malformed(
                    "debridge instruction without discriminator",
                ));
            };
            if ix.program_id == DLN_SOURCE
                && (discriminator == CREATE_ORDER || discriminator == CREATE_ORDER_WITH_NONCE)
            {
                creates.push(ix);
            } else if ix.program_id == DLN_DESTINATION && discriminator == FULFILL_ORDER {
                fills.push(ix);
            }
            // Everything else on either DLN program is admin, configuration, cancel,
            // claim or unlock. None of those is a transfer.
        }

        if creates.is_empty() && fills.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(if saw_dln {
                "debridge admin instruction"
            } else {
                "no debridge instruction"
            }));
        }

        let source_events = events_under(&tx.logs, DLN_SOURCE);
        let orders: Vec<&Vec<u8>> = source_events
            .iter()
            .filter(|payload| payload.starts_with(&CREATED_ORDER))
            .collect();
        let order_ids: Vec<&[u8]> = source_events
            .iter()
            .filter(|payload| payload.starts_with(&CREATED_ORDER_ID))
            .map(Vec::as_slice)
            .collect();
        let destination_events = events_under(&tx.logs, DLN_DESTINATION);
        let fulfilled: Vec<&Vec<u8>> = destination_events
            .iter()
            .filter(|payload| payload.starts_with(&FULFILLED))
            .collect();

        if orders.len() != creates.len() || order_ids.len() != creates.len() {
            return Classification::Rejected(Reject::Malformed(
                "debridge events do not match order instructions",
            ));
        }
        if fulfilled.len() != fills.len() {
            return Classification::Rejected(Reject::Malformed(
                "debridge Fulfilled events do not match fulfill instructions",
            ));
        }

        let mut transfers = Vec::with_capacity(creates.len() + fills.len());
        for (index, (order, id)) in orders.iter().zip(order_ids.iter()).enumerate() {
            if id.len() != 40 {
                return Classification::Rejected(Reject::Malformed(
                    "debridge CreatedOrderId event is truncated",
                ));
            }
            let mut order_id = [0u8; 32];
            order_id.copy_from_slice(&id[8..40]);
            match build_transfer(tx, order, &order_id, index as u16) {
                Ok(transfer) => transfers.push(transfer),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        for (index, (ix, event)) in fills.iter().zip(fulfilled.iter()).enumerate() {
            if event.len() < 40 {
                return Classification::Rejected(Reject::Malformed(
                    "debridge Fulfilled event is truncated",
                ));
            }
            let mut order_id = [0u8; 32];
            order_id.copy_from_slice(&event[8..40]);
            let event_index = creates.len() + index;
            match build_fill(tx, ix, &order_id, event_index as u16) {
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

fn build_fill(
    tx: &SolanaTx,
    ix: &Instruction,
    order_id: &[u8; 32],
    event_index: u16,
) -> Result<DetectedTransfer, Reject> {
    let body = ix.data.get(8..).ok_or(Reject::Malformed(
        "debridge fulfill_order data is truncated",
    ))?;
    let order = parse_order_body(body)?;
    let give_chain = decimal(&order.give.chain);
    let take_chain = decimal(&order.take.chain);
    if take_chain != SOLANA_CHAIN {
        return Err(Reject::Malformed("debridge fill take chain is not Solana"));
    }

    let source = RawChainRef::debridge(give_chain.as_str());
    let destination = RawChainRef::debridge(SOLANA_CHAIN);
    // The source-side amount and token are stored on both legs of one order. The take
    // offer is what the taker delivers on Solana and can differ (fees, patch).
    let amount = RawAmount::from_be_bytes(&order.give.amount).ok_or(Reject::Malformed(
        "debridge order give amount is not a u256",
    ))?;
    let token = decode_address(&give_chain, &order.give.token).map(|address| TokenRef {
        chain: source.clone(),
        address,
    });

    Ok(DetectedTransfer {
        program: DEBRIDGE_PROGRAM.to_string(),
        message: MessageId::Debridge {
            order_id: hex::encode(order_id),
        },
        direction: Direction::Inbound,
        observed_tx: tx.signature.clone(),
        event_index,
        source,
        destination,
        // The maker is in the order; the fee payer is only a fallback.
        source_wallet: decode_address(&give_chain, &order.maker).or_else(|| tx.signer.clone()),
        // An unknown destination chain is stored unresolved, never an error.
        destination_wallet: decode_address(SOLANA_CHAIN, &order.receiver),
        token,
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
    parse_order_body(&payload[8..])
}

/// The `Order` struct as it appears at the start of a `CreatedOrder` event and after
/// the discriminator of a `fulfill_order` instruction. Reading stops at the receiver:
/// the order id and the remaining authority fields follow it and are not needed.
fn parse_order_body(data: &[u8]) -> Result<Order, Reject> {
    let mut reader = Reader::new(data);
    let truncated = || Reject::Malformed("debridge order is truncated");

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
    let truncated = || Reject::Malformed("debridge order is truncated");
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

/// Event payloads printed while `program` is the running program, in log order.
/// Events from inner programs are ignored.
fn events_under(logs: &[String], program: &str) -> Vec<Vec<u8>> {
    let mut stack: Vec<&str> = Vec::new();
    let mut out = Vec::new();

    for line in logs {
        let Some(rest) = line.strip_prefix("Program ") else {
            continue;
        };
        if let Some(encoded) = rest.strip_prefix("data: ") {
            if stack.last() != Some(&program) {
                continue;
            }
            if let Ok(bytes) = STANDARD.decode(encoded) {
                out.push(bytes);
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

    const FILL_MAKER: [u8; 20] = [0x09; 20];
    const FILL_TOKEN: [u8; 20] = [0x07; 20];
    const FILL_RECEIVER: [u8; 32] = [0x08; 32];

    /// A `fulfill_order` instruction: the discriminator, then the leading `Order`
    /// fields the adapter reads. The order id and authority fields after the receiver
    /// are deliberately omitted.
    fn fill_instruction(give_chain: [u8; 32], take_chain: [u8; 32]) -> Instruction {
        let mut data = FULFILL_ORDER.to_vec();
        data.extend(0u64.to_le_bytes());
        put_vec(&mut data, &FILL_MAKER);
        data.extend(give_chain);
        put_vec(&mut data, &FILL_TOKEN);
        data.extend(be32(3_000_000));
        data.extend(take_chain);
        put_vec(&mut data, &[]);
        data.extend(be32(2_000_000));
        put_vec(&mut data, &FILL_RECEIVER);
        Instruction {
            program_id: DLN_DESTINATION.to_string(),
            accounts: Vec::new(),
            data,
            parsed: None,
            inner_of: None,
        }
    }

    fn fulfilled_event(id: &[u8; 32]) -> Vec<u8> {
        let mut out = FULFILLED.to_vec();
        out.extend(id);
        out.extend([0u8; 32]); // taker
        out
    }

    fn fill_logs(event: &[u8]) -> Vec<String> {
        vec![
            format!("Program {DLN_DESTINATION} invoke [1]"),
            format!("Program data: {}", STANDARD.encode(event)),
            format!("Program {DLN_DESTINATION} success"),
        ]
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
    fn decodes_a_fill_from_the_fulfilled_event() {
        let tx = tx_with(
            vec![fill_instruction(be32(8453), be32(7_565_164))],
            fill_logs(&fulfilled_event(&ORDER_ID)),
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
        assert_eq!(transfer.direction, Direction::Inbound);
        assert_eq!(transfer.event_index, 0);
        assert_eq!(transfer.observed_tx, "sig");
        assert_eq!(transfer.source, RawChainRef::debridge("8453"));
        assert_eq!(transfer.destination, RawChainRef::debridge("7565164"));
        assert_eq!(
            transfer.amount_raw.as_ref().map(|a| a.as_str()),
            Some("3000000")
        );
        let token = transfer.token.expect("token");
        assert_eq!(token.chain, RawChainRef::debridge("8453"));
        assert_eq!(token.address, format!("0x{}", hex::encode(FILL_TOKEN)));
        assert_eq!(
            transfer.source_wallet.as_deref(),
            Some(format!("0x{}", hex::encode(FILL_MAKER)).as_str())
        );
        assert_eq!(
            transfer.destination_wallet.as_deref(),
            Some(bs58::encode(FILL_RECEIVER).into_string().as_str())
        );
    }

    #[test]
    fn a_fill_without_a_fulfilled_event_is_malformed() {
        let tx = tx_with(
            vec![fill_instruction(be32(8453), be32(7_565_164))],
            vec![
                format!("Program {DLN_DESTINATION} invoke [1]"),
                format!("Program {DLN_DESTINATION} success"),
            ],
        );
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed(
                "debridge Fulfilled events do not match fulfill instructions"
            ))
        );
    }

    #[test]
    fn a_fill_whose_take_chain_is_not_solana_is_malformed() {
        let tx = tx_with(
            vec![fill_instruction(be32(8453), be32(1))],
            fill_logs(&fulfilled_event(&ORDER_ID)),
        );
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed("debridge fill take chain is not Solana"))
        );
    }

    #[test]
    fn a_failed_fill_is_never_a_transfer() {
        let mut tx = tx_with(
            vec![fill_instruction(be32(8453), be32(7_565_164))],
            fill_logs(&fulfilled_event(&ORDER_ID)),
        );
        tx.failed = true;
        assert_eq!(
            DebridgeAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn cancel_claim_and_unlock_are_not_transfers() {
        let tx = tx_with(
            vec![Instruction {
                program_id: DLN_DESTINATION.to_string(),
                accounts: Vec::new(),
                data: [0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0].to_vec(),
                parsed: None,
                inner_of: None,
            }],
            vec![
                format!("Program {DLN_DESTINATION} invoke [1]"),
                format!("Program {DLN_DESTINATION} success"),
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
