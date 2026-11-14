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
//!   is `204c290c27a284db`; `OrderInitialized` is `b4762cf9a6192851`; `fulfill` is
//!   `8f0234ceaea4f748`.
//! * destination state account — the fixed 471-byte account at `fulfill` account
//!   index 0 is the `[b"STATE_DEST", order_hash]` PDA, its order hash at offset 284.
//!   Confirmed against mainnet fulfills
//!   `2Y2EZPiVZ8bwdAGLSSQXNaTuBQtx3VoQRpM5R8SPonqGpMyYPKHHp5Nquv8aRkRkz3fRLh38d8eWe1spwVqCZwWp`
//!   and six more, and against every open 471-byte account owned by the program.
//!
//! Two instructions are transfers: source-side `init_order`, whose `OrderInitialized`
//! event carries the 32-byte order hash, and destination-side `fulfill`, whose state
//! account carries the same hash. `register_order`, `settle`, `cancel` and every other
//! instruction are not indexed.

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
/// Anchor instruction discriminator for the destination-side `fulfill`.
const FULFILL: [u8; 8] = [0x8f, 0x02, 0x34, 0xce, 0xae, 0xa4, 0xf7, 0x48];

/// `fulfill` args are the discriminator and the 32-byte `unlocker`.
const FULFILL_LEN: usize = 40;
/// `fulfill` passes eight accounts; index 0 is the destination state PDA.
const FULFILL_ACCOUNTS: usize = 8;
const STATE_ACCOUNT: usize = 0;

/// Anchor account discriminator of the Swift V2 destination order state.
const ORDER_STATE: [u8; 8] = [0x7c, 0xac, 0xf0, 0xa8, 0xf9, 0x5c, 0xf1, 0xa3];
/// Verified size of the destination state account. Every open account of this type
/// is exactly this long.
const STATE_LEN: usize = 471;
/// Offset of the source Wormhole chain ID (`u16` LE) in the state account.
const STATE_SOURCE_CHAIN: usize = 45;
/// Offset of the 20-byte source-chain token address in the state account.
const STATE_SOURCE_TOKEN: usize = 59;
/// Offset of the 32-byte order hash in the state account.
const STATE_ORDER_HASH: usize = 284;
/// Offset of the little-endian `u64` amount delivered on Solana.
const STATE_AMOUNT_OUT: usize = 422;

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
        let mut fulfills = Vec::new();
        let mut saw_mayan = false;
        for ix in tx
            .instructions
            .iter()
            .filter(|ix| ix.program_id == MAYAN_SWIFT_V2)
        {
            saw_mayan = true;
            match ix.data.get(..8) {
                Some(discriminator) if discriminator == INIT_ORDER.as_slice() => inits.push(ix),
                Some(discriminator) if discriminator == FULFILL.as_slice() => fulfills.push(ix),
                // Everything else on the Swift program is registration, settlement,
                // cancellation or admin. None of those is a transfer.
                _ => {}
            }
        }

        if inits.is_empty() && fulfills.is_empty() {
            return Classification::Rejected(Reject::NotTransfer(if saw_mayan {
                "mayan instruction is not init_order or fulfill"
            } else {
                "no mayan instruction"
            }));
        }

        let events: Vec<Vec<u8>> = events_under(&tx.logs, MAYAN_SWIFT_V2)
            .into_iter()
            .filter(|payload| payload.starts_with(&ORDER_INITIALIZED))
            .collect();
        if !inits.is_empty() {
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
        }

        let mut transfers = Vec::with_capacity(inits.len() + fulfills.len());
        for (index, (ix, event)) in inits.iter().zip(events.iter()).enumerate() {
            match build_transfer(tx, ix, event, index as u16) {
                Ok(transfer) => transfers.push(transfer),
                Err(reject) => return Classification::Rejected(reject),
            }
        }
        for (index, ix) in fulfills.iter().enumerate() {
            match build_fill(tx, ix, (inits.len() + index) as u16) {
                Ok(transfer) => transfers.push(transfer),
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
            .filter(|ix| ix.program_id == MAYAN_SWIFT_V2)
            .filter(|ix| ix.data.get(..8) == Some(FULFILL.as_slice()))
            .filter_map(|ix| ix.accounts.get(STATE_ACCOUNT).cloned())
            .collect();
        keys.sort();
        keys.dedup();
        keys
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

/// A destination-side `fulfill`. The order hash and the order's terms come from the
/// destination state account, which [`MayanAdapter::required_accounts`] asks the fetch
/// step to preload.
fn build_fill(
    tx: &SolanaTx,
    ix: &Instruction,
    event_index: u16,
) -> Result<DetectedTransfer, Reject> {
    if ix.data.len() < FULFILL_LEN {
        return Err(Reject::Malformed("mayan fulfill data is truncated"));
    }
    if ix.accounts.len() < FULFILL_ACCOUNTS {
        return Err(Reject::Malformed("mayan fulfill accounts are truncated"));
    }

    let state_key = ix
        .accounts
        .get(STATE_ACCOUNT)
        .ok_or(Reject::Malformed("mayan fulfill without a state account"))?;
    let state = tx.account(state_key).ok_or(Reject::Unsupported(
        "inbound fulfill: destination state account was not loaded",
    ))?;
    if state.owner != MAYAN_SWIFT_V2 {
        return Err(Reject::Malformed(
            "mayan state account is not owned by the Swift program",
        ));
    }
    if state.data.len() < STATE_LEN {
        return Err(Reject::Malformed(
            "mayan destination state account is truncated",
        ));
    }
    if state.data.get(..8) != Some(ORDER_STATE.as_slice()) {
        return Err(Reject::Malformed(
            "mayan state account is not a destination order",
        ));
    }

    let mut order_hash = [0u8; 32];
    order_hash.copy_from_slice(&state.data[STATE_ORDER_HASH..STATE_ORDER_HASH + 32]);
    let source_chain = u16::from_le_bytes(
        state.data[STATE_SOURCE_CHAIN..STATE_SOURCE_CHAIN + 2]
            .try_into()
            .map_err(|_| Reject::Malformed("mayan state source chain is truncated"))?,
    );
    let amount_out = u64::from_le_bytes(
        state.data[STATE_AMOUNT_OUT..STATE_AMOUNT_OUT + 8]
            .try_into()
            .map_err(|_| Reject::Malformed("mayan state amount is truncated"))?,
    );

    let source = RawChainRef::wormhole(source_chain);
    let destination = RawChainRef::wormhole(SOLANA_CHAIN);
    // The state stores the source-chain (EVM) token. Any other source chain is not
    // assumed to share that 20-byte layout, so its token is left absent.
    let token = matches!(chain_kind(source_chain), Some(ChainKind::Evm)).then(|| TokenRef {
        chain: source.clone(),
        address: format!(
            "0x{}",
            hex::encode(&state.data[STATE_SOURCE_TOKEN..STATE_SOURCE_TOKEN + 20])
        ),
    });

    Ok(DetectedTransfer {
        program: MAYAN_PROGRAM.to_string(),
        message: MessageId::Mayan {
            order_hash: hex::encode(order_hash),
        },
        direction: Direction::Inbound,
        // The fill delivered on Solana in this tx.
        delivered: true,
        observed_tx: tx.signature.clone(),
        event_index,
        source,
        destination,
        // The state carries the source trader, but its offset is not verified.
        source_wallet: None,
        destination_wallet: None,
        token,
        token_symbol: None,
        amount_raw: Some(RawAmount::from_u128(u128::from(amount_out))),
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
    use crate::protocols::AccountData;

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

    const SOURCE_CHAIN: u16 = 4;
    const SOURCE_TOKEN: [u8; 20] = [0x11; 20];
    const AMOUNT_OUT: u64 = 9_574_989;

    fn state_account(hash: [u8; 32], chain: u16, token: [u8; 20], amount: u64) -> AccountData {
        let mut data = vec![0u8; STATE_LEN];
        data[..8].copy_from_slice(&ORDER_STATE);
        data[8] = 255;
        data[STATE_SOURCE_CHAIN..STATE_SOURCE_CHAIN + 2].copy_from_slice(&chain.to_le_bytes());
        data[STATE_SOURCE_TOKEN..STATE_SOURCE_TOKEN + 20].copy_from_slice(&token);
        data[STATE_ORDER_HASH..STATE_ORDER_HASH + 32].copy_from_slice(&hash);
        data[STATE_AMOUNT_OUT..STATE_AMOUNT_OUT + 8].copy_from_slice(&amount.to_le_bytes());
        AccountData {
            owner: MAYAN_SWIFT_V2.to_string(),
            data,
        }
    }

    fn fulfill_instruction(state: &str) -> Instruction {
        let mut data = FULFILL.to_vec();
        data.extend([0u8; 32]);
        Instruction {
            program_id: MAYAN_SWIFT_V2.to_string(),
            accounts: vec![
                state.to_string(),
                "signer".to_string(),
                "stateAta".to_string(),
                MINT.to_string(),
                "recipientAta".to_string(),
                "systemProgram".to_string(),
                "tokenProgram".to_string(),
                "signer".to_string(),
            ],
            data,
            parsed: None,
            inner_of: None,
        }
    }

    fn fulfill_tx(state: AccountData) -> SolanaTx {
        let mut tx = tx_with(vec![fulfill_instruction("state")], Vec::new());
        tx.accounts.insert("state".to_string(), state);
        tx
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
            Classification::Rejected(Reject::NotTransfer(
                "mayan instruction is not init_order or fulfill"
            ))
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

    #[test]
    fn decodes_a_fulfill_from_the_destination_state_account() {
        let tx = fulfill_tx(state_account(
            ORDER_HASH,
            SOURCE_CHAIN,
            SOURCE_TOKEN,
            AMOUNT_OUT,
        ));
        let transfer = only_transfer(MayanAdapter.classify(&tx));

        assert_eq!(transfer.program, MAYAN_PROGRAM);
        assert_eq!(
            transfer.message,
            MessageId::Mayan {
                order_hash: hex::encode(ORDER_HASH),
            }
        );
        assert_eq!(transfer.message.key(), hex::encode(ORDER_HASH));
        assert_eq!(transfer.direction, Direction::Inbound);
        assert!(transfer.delivered);
        assert_eq!(transfer.observed_tx, "sig");
        assert_eq!(transfer.event_index, 0);
        assert_eq!(transfer.source, RawChainRef::wormhole(SOURCE_CHAIN));
        assert_eq!(transfer.destination, RawChainRef::wormhole(SOLANA_CHAIN));
        assert_eq!(
            transfer.amount_raw.as_ref().map(|a| a.as_str()),
            Some("9574989")
        );
        let token = transfer.token.expect("token");
        assert_eq!(token.chain, RawChainRef::wormhole(SOURCE_CHAIN));
        assert_eq!(token.address, format!("0x{}", hex::encode(SOURCE_TOKEN)));
    }

    #[test]
    fn required_accounts_asks_for_the_destination_state_account() {
        let tx = fulfill_tx(state_account(
            ORDER_HASH,
            SOURCE_CHAIN,
            SOURCE_TOKEN,
            AMOUNT_OUT,
        ));
        assert_eq!(
            MayanAdapter.required_accounts(&tx),
            vec!["state".to_string()]
        );

        let mut failed = tx;
        failed.failed = true;
        assert!(MayanAdapter.required_accounts(&failed).is_empty());
    }

    #[test]
    fn a_fulfill_without_its_state_account_is_unsupported() {
        let tx = tx_with(vec![fulfill_instruction("state")], Vec::new());
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::Unsupported(
                "inbound fulfill: destination state account was not loaded"
            ))
        );
    }

    #[test]
    fn a_fulfill_with_a_malformed_state_account_is_malformed() {
        let mut truncated = state_account(ORDER_HASH, SOURCE_CHAIN, SOURCE_TOKEN, AMOUNT_OUT);
        truncated.data.truncate(STATE_LEN - 1);
        assert_eq!(
            MayanAdapter.classify(&fulfill_tx(truncated)),
            Classification::Rejected(Reject::Malformed(
                "mayan destination state account is truncated"
            ))
        );

        let mut wrong_owner = state_account(ORDER_HASH, SOURCE_CHAIN, SOURCE_TOKEN, AMOUNT_OUT);
        wrong_owner.owner = "other".to_string();
        assert_eq!(
            MayanAdapter.classify(&fulfill_tx(wrong_owner)),
            Classification::Rejected(Reject::Malformed(
                "mayan state account is not owned by the Swift program"
            ))
        );

        let mut wrong_type = state_account(ORDER_HASH, SOURCE_CHAIN, SOURCE_TOKEN, AMOUNT_OUT);
        wrong_type.data[0] = 0;
        assert_eq!(
            MayanAdapter.classify(&fulfill_tx(wrong_type)),
            Classification::Rejected(Reject::Malformed(
                "mayan state account is not a destination order"
            ))
        );
    }

    #[test]
    fn a_fulfill_with_truncated_instruction_data_is_malformed() {
        let mut tx = fulfill_tx(state_account(
            ORDER_HASH,
            SOURCE_CHAIN,
            SOURCE_TOKEN,
            AMOUNT_OUT,
        ));
        tx.instructions[0].data.truncate(FULFILL_LEN - 1);
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::Malformed("mayan fulfill data is truncated"))
        );
    }

    #[test]
    fn a_fulfill_for_an_unknown_source_chain_leaves_the_token_absent() {
        let tx = fulfill_tx(state_account(ORDER_HASH, 999, SOURCE_TOKEN, AMOUNT_OUT));
        let transfer = only_transfer(MayanAdapter.classify(&tx));
        assert_eq!(transfer.source, RawChainRef::wormhole(999));
        assert_eq!(transfer.token, None);
        assert_eq!(
            transfer.amount_raw.as_ref().map(|a| a.as_str()),
            Some("9574989"),
            "the delivered amount is still known"
        );
    }

    #[test]
    fn a_failed_fulfill_is_never_a_transfer() {
        let mut tx = fulfill_tx(state_account(
            ORDER_HASH,
            SOURCE_CHAIN,
            SOURCE_TOKEN,
            AMOUNT_OUT,
        ));
        tx.failed = true;
        assert_eq!(
            MayanAdapter.classify(&tx),
            Classification::Rejected(Reject::NotTransfer("transaction failed"))
        );
    }
}
