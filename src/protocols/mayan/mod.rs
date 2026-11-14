//! Mayan Swift V2 on Solana: source-side order creation. Pure: no IO, no clock,
//! no database.
//!
//! Verification sources:
//!
//! * program ID — the Swift program table at <https://docs.mayan.finance> and
//!   `swift_v2` in the official SDK's
//!   <https://github.com/mayan-finance/swap-sdk/blob/main/src/addresses.ts>.
//! * instruction and event layouts — the Swift V2 program has no published IDL;
//!   the layouts are the ones the SDK serializes and were confirmed against mainnet
//!   transaction
//!   `QCaMXBE6VSZgajW5wJCHqxesQwjjdRgUUhMCoX8cwd2HXwiQZpVzGrWDMUE4s9dpj5msdN9KPZfmXjN2Qn9Hkes`.
//! * discriminators — Anchor's `sha256("global:<snake_case>")[..8]` for
//!   instructions and `sha256("event:<EventName>")[..8]` for events. `init_order`
//!   is `204c290c27a284db`; `OrderInitialized` is `b4762cf9a6192851`.
//!
//! Only `init_order` is a source-side transfer. Its `OrderInitialized` event carries
//! the 32-byte order hash, so no client-side hashing is needed. `register_order`,
//! `fulfill`, `settle`, `cancel` and every other instruction are not indexed.

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::chains::RawChainRef;
use crate::protocols::amount::RawAmount;
use crate::protocols::{
    BridgeAdapter, Classification, DetectedTransfer, Direction, Instruction, MAYAN_PROGRAM,
    MessageId, Protocol, Reject, SolanaTx, TokenRef,
};

/// Verified mainnet Swift V2 program.
pub const MAYAN_SWIFT_V2: &str = "mayan34VedncxdK2XobtvWFDXQASUTBXhUVzt2kKgny";

/// Solana's Wormhole chain ID.
const SOLANA_CHAIN: u16 = 1;

/// Anchor instruction discriminator for `init_order`.
const INIT_ORDER: [u8; 8] = [0x20, 0x4c, 0x29, 0x0c, 0x27, 0xa2, 0x84, 0xdb];
/// Anchor event discriminator for `OrderInitialized`.
const ORDER_INITIALIZED: [u8; 8] = [0xb4, 0x76, 0x2c, 0xf9, 0xa6, 0x19, 0x28, 0x51];

/// `init_order` args are 198 bytes including the discriminator. Only the fields
/// the transfer needs are read, but a shorter buffer is not the verified layout.
const INIT_ORDER_LEN: usize = 198;
/// Offset of `destAddress: [u8; 32]` in the args.
const DEST_ADDRESS_OFFSET: usize = 25;
/// Offset of `chainDest: u16` in the args.
const CHAIN_DEST_OFFSET: usize = 57;
/// `init_order` passes ten accounts; index 0 is the trader and index 6 the mint.
const MIN_ACCOUNTS: usize = 10;
const TRADER_ACCOUNT: usize = 0;
const MINT_ACCOUNT: usize = 6;

/// `OrderInitialized` is the 8-byte discriminator, a 32-byte order hash and a
/// little-endian `u64` amount in.
const EVENT_LEN: usize = 8 + 32 + 8;

/// Mayan Swift V2 adapter. The program ID is fixed, so the adapter is always
/// registered.
pub struct MayanAdapter;

impl BridgeAdapter for MayanAdapter {
    fn protocol(&self) -> Protocol {
        Protocol::Mayan
    }

    fn default_program_ids(&self) -> &'static [&'static str] {
        &[MAYAN_SWIFT_V2]
    }

    fn classify(&self, tx: &SolanaTx) -> Classification {
        if tx.failed {
            return Classification::Rejected(Reject::NotTransfer("transaction failed"));
        }

        let mut inits = Vec::new();
        let mut saw_mayan = false;
        for ix in tx
            .instructions
            .iter()
            .filter(|ix| ix.program_id == MAYAN_SWIFT_V2)
        {
            saw_mayan = true;
            if ix.data.get(..8) == Some(INIT_ORDER.as_slice()) {
                inits.push(ix);
            }
            // Everything else on the Swift program is registration, fulfillment,
            // settlement, cancellation or admin. None of those is a transfer.
        }

        if inits.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(if saw_mayan {
                "mayan instruction is not init_order"
            } else {
                "no mayan instruction"
            }));
        }

        let events: Vec<Vec<u8>> = events_under(&tx.logs, MAYAN_SWIFT_V2)
            .into_iter()
            .filter(|payload| payload.starts_with(&ORDER_INITIALIZED))
            .collect();
        // Without its event there is no order hash, so there is no transfer to store.
        if events.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(
                "no mayan OrderInitialized event",
            ));
        }
        if events.len() != inits.len() {
            return Classification::Rejected(Reject::Malformed(
                "mayan events do not match init_order instructions",
            ));
        }

        let mut transfers = Vec::with_capacity(inits.len());
        for (index, (ix, event)) in inits.iter().zip(events.iter()).enumerate() {
            match build_transfer(tx, ix, event, index as u16) {
                Ok(transfer) => transfers.push(transfer),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        Classification::Transfers(transfers)
    }
}

fn build_transfer(
    tx: &SolanaTx,
    ix: &Instruction,
    event: &[u8],
    event_index: u16,
) -> Result<DetectedTransfer, Reject> {
    if ix.data.len() < INIT_ORDER_LEN {
        return Err(Reject::Malformed("mayan init_order data is truncated"));
    }
    if ix.accounts.len() < MIN_ACCOUNTS {
        return Err(Reject::Malformed("mayan init_order accounts are truncated"));
    }
    if event.len() < EVENT_LEN {
        return Err(Reject::Malformed(
            "mayan OrderInitialized event is truncated",
        ));
    }

    let mut order_hash = [0u8; 32];
    order_hash.copy_from_slice(&event[8..40]);
    let amount_in = u64::from_le_bytes(
        event[40..48]
            .try_into()
            .map_err(|_| Reject::Malformed("mayan OrderInitialized amount is truncated"))?,
    );

    let chain_dest = u16::from_le_bytes(
        ix.data[CHAIN_DEST_OFFSET..CHAIN_DEST_OFFSET + 2]
            .try_into()
            .map_err(|_| Reject::Malformed("mayan init_order chain is truncated"))?,
    );
    let dest_address = &ix.data[DEST_ADDRESS_OFFSET..DEST_ADDRESS_OFFSET + 32];

    let source = RawChainRef::wormhole(SOLANA_CHAIN);
    let destination = RawChainRef::wormhole(chain_dest);
    let token = ix.accounts.get(MINT_ACCOUNT).map(|address| TokenRef {
        chain: source.clone(),
        address: address.clone(),
    });

    Ok(DetectedTransfer {
        program: MAYAN_PROGRAM.to_string(),
        message: MessageId::Mayan {
            order_hash: hex::encode(order_hash),
        },
        direction: Direction::Outbound,
        delivered: false,
        observed_tx: tx.signature.clone(),
        event_index,
        source,
        destination,
        source_wallet: ix.accounts.get(TRADER_ACCOUNT).cloned(),
        // An unknown destination chain is stored unresolved, never an error.
        destination_wallet: decode_destination(chain_dest, dest_address),
        token,
        token_symbol: None,
        amount_raw: Some(RawAmount::from_u128(u128::from(amount_in))),
        via: None,
    })
}

#[derive(Clone, Copy)]
enum ChainKind {
    Solana,
    Evm,
}

/// The destination chains this adapter can decode an address for. An unknown
/// chain is not an error: the reference is stored unresolved and no address is
/// decoded. The EVM IDs match `vaa::decode`'s list.
fn chain_kind(chain: u16) -> Option<ChainKind> {
    if chain == SOLANA_CHAIN {
        return Some(ChainKind::Solana);
    }
    match chain {
        2 | 4 | 5 | 6 | 23 | 24 | 25 | 30 | 36 | 39 | 46 | 47 => Some(ChainKind::Evm),
        _ => None,
    }
}

fn decode_destination(chain: u16, bytes: &[u8]) -> Option<String> {
    match chain_kind(chain)? {
        ChainKind::Solana => (bytes.len() == 32).then(|| bs58::encode(bytes).into_string()),
        ChainKind::Evm => {
            // Swift stores an EVM address left-padded to 32 bytes.
            let address = bytes.get(bytes.len().checked_sub(20)?..)?;
            Some(format!("0x{}", hex::encode(address)))
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER_HASH: [u8; 32] = [0x44; 32];
    const AMOUNT_IN: u64 = 2_804_362;
    const TRADER: &str = "D232Pb6Z4sandj5uSPAYmAuuQG9SZjCWfRyKAGwVxVGR";
    const MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

    fn init_order_data(chain_dest: u16, dest_address: [u8; 32]) -> Vec<u8> {
        let mut data = vec![0u8; INIT_ORDER_LEN];
        data[..8].copy_from_slice(&INIT_ORDER);
        data[DEST_ADDRESS_OFFSET..DEST_ADDRESS_OFFSET + 32].copy_from_slice(&dest_address);
        data[CHAIN_DEST_OFFSET..CHAIN_DEST_OFFSET + 2].copy_from_slice(&chain_dest.to_le_bytes());
        data
    }

    fn init_instruction(data: Vec<u8>) -> Instruction {
        Instruction {
            program_id: MAYAN_SWIFT_V2.to_string(),
            accounts: vec![
                TRADER.to_string(),
                TRADER.to_string(),
                "state".to_string(),
                "stateAta".to_string(),
                "relayerAta".to_string(),
                MAYAN_SWIFT_V2.to_string(),
                MINT.to_string(),
                "feeManager".to_string(),
                "tokenProgram".to_string(),
                "systemProgram".to_string(),
            ],
            data,
            parsed: None,
            inner_of: None,
        }
    }

    fn order_initialized(hash: [u8; 32], amount: u64) -> Vec<u8> {
        let mut event = ORDER_INITIALIZED.to_vec();
        event.extend(hash);
        event.extend(amount.to_le_bytes());
        event
    }

    fn logs_with(event: &[u8]) -> Vec<String> {
        vec![
            format!("Program {MAYAN_SWIFT_V2} invoke [1]"),
            "Program log: Instruction: InitOrder".to_string(),
            format!("Program data: {}", STANDARD.encode(event)),
            format!("Program {MAYAN_SWIFT_V2} success"),
        ]
    }

    fn tx_with(instructions: Vec<Instruction>, logs: Vec<String>) -> SolanaTx {
        SolanaTx {
            signature: "sig".to_string(),
            slot: 1,
            block_time: None,
            failed: false,
            signer: Some(TRADER.to_string()),
            account_keys: Vec::new(),
            accounts: Default::default(),
            instructions,
            logs,
        }
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
    fn decodes_an_evm_destination() {
        let mut address = [0u8; 32];
        address[12..].copy_from_slice(&[0x11; 20]);
        let tx = tx_with(
            vec![init_instruction(init_order_data(4, address))],
            logs_with(&order_initialized(ORDER_HASH, AMOUNT_IN)),
        );

        let transfer = only_transfer(MayanAdapter.classify(&tx));
        assert_eq!(transfer.program, MAYAN_PROGRAM);
        assert_eq!(
            transfer.message,
            MessageId::Mayan {
                order_hash: hex::encode(ORDER_HASH),
            }
        );
        assert_eq!(transfer.message.key(), hex::encode(ORDER_HASH));
        assert_eq!(transfer.direction, Direction::Outbound);
        assert!(!transfer.delivered);
        assert_eq!(transfer.event_index, 0);
        assert_eq!(transfer.observed_tx, "sig");
        assert_eq!(transfer.source, RawChainRef::wormhole(1));
        assert_eq!(transfer.destination, RawChainRef::wormhole(4));
        assert_eq!(
            transfer.amount_raw.as_ref().map(|a| a.as_str()),
            Some("2804362")
        );
        let expected_wallet = format!("0x{}", "11".repeat(20));
        assert_eq!(
            transfer.destination_wallet.as_deref(),
            Some(expected_wallet.as_str())
        );
        assert_eq!(transfer.source_wallet.as_deref(), Some(TRADER));
        let token = transfer.token.expect("token");
        assert_eq!(token.chain, RawChainRef::wormhole(1));
        assert_eq!(token.address, MINT);
    }

    #[test]
    fn decodes_a_base58_solana_destination() {
        let address = [0x22; 32];
        let tx = tx_with(
            vec![init_instruction(init_order_data(1, address))],
            logs_with(&order_initialized(ORDER_HASH, AMOUNT_IN)),
        );
        let transfer = only_transfer(MayanAdapter.classify(&tx));
        assert_eq!(transfer.destination, RawChainRef::wormhole(1));
        assert_eq!(
            transfer.destination_wallet.as_deref(),
            Some(bs58::encode(address).into_string().as_str())
        );
    }

    #[test]
    fn an_unknown_destination_chain_leaves_the_wallet_absent() {
        let tx = tx_with(
            vec![init_instruction(init_order_data(999, [0x33; 32]))],
            logs_with(&order_initialized(ORDER_HASH, AMOUNT_IN)),
        );
        let transfer = only_transfer(MayanAdapter.classify(&tx));
        assert_eq!(transfer.destination, RawChainRef::wormhole(999));
        assert_eq!(transfer.destination_wallet, None);
        assert!(transfer.token.is_some(), "the source mint is still known");
    }

    #[test]
    fn every_other_swift_instruction_is_not_a_transfer() {
        let ix = init_instruction(vec![0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0]);
        let tx = tx_with(
            vec![ix],
            vec![
                format!("Program {MAYAN_SWIFT_V2} invoke [1]"),
                format!("Program {MAYAN_SWIFT_V2} success"),
            ],
        );
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("mayan instruction is not init_order"))
        );
    }

    #[test]
    fn an_init_order_without_its_event_is_not_a_transfer() {
        let tx = tx_with(
            vec![init_instruction(init_order_data(4, [0x11; 32]))],
            vec![
                format!("Program {MAYAN_SWIFT_V2} invoke [1]"),
                "Program log: Instruction: InitOrder".to_string(),
                format!("Program {MAYAN_SWIFT_V2} success"),
            ],
        );
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("no mayan OrderInitialized event"))
        );
    }

    #[test]
    fn a_failed_transaction_is_never_a_transfer() {
        let mut tx = tx_with(
            vec![init_instruction(init_order_data(4, [0x11; 32]))],
            logs_with(&order_initialized(ORDER_HASH, AMOUNT_IN)),
        );
        tx.failed = true;
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }

    #[test]
    fn a_truncated_instruction_or_event_is_malformed() {
        let mut short_data = init_order_data(4, [0x11; 32]);
        short_data.truncate(INIT_ORDER_LEN - 1);
        let tx = tx_with(
            vec![init_instruction(short_data)],
            logs_with(&order_initialized(ORDER_HASH, AMOUNT_IN)),
        );
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed("mayan init_order data is truncated"))
        );

        let mut short_event = order_initialized(ORDER_HASH, AMOUNT_IN);
        short_event.truncate(EVENT_LEN - 1);
        let tx = tx_with(
            vec![init_instruction(init_order_data(4, [0x11; 32]))],
            logs_with(&short_event),
        );
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed(
                "mayan OrderInitialized event is truncated"
            ))
        );
    }

    #[test]
    fn an_event_printed_by_an_inner_program_is_ignored() {
        let tx = tx_with(
            vec![init_instruction(init_order_data(4, [0x11; 32]))],
            vec![
                format!("Program {MAYAN_SWIFT_V2} invoke [1]"),
                "Program Other invoke [2]".to_string(),
                format!(
                    "Program data: {}",
                    STANDARD.encode(order_initialized(ORDER_HASH, AMOUNT_IN))
                ),
                "Program Other success".to_string(),
                format!("Program {MAYAN_SWIFT_V2} success"),
            ],
        );
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("no mayan OrderInitialized event"))
        );
    }
}
