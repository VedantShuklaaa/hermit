//! NFT Bridge rows obey the same persistence rules as every other transfer:
//! one row per `(protocol, message_key)`, the `program` discriminator survives, and a
//! status never moves backwards.

mod common;

use bridge::db::repository::persist_analysis;
use bridge::domain::bridge_transfer::{BridgeMessageId, BridgeStatus, BridgeTransfer, ChainId};
use bridge::domain::transaction::{Chain, NormalisedTransaction, TxStatus};
use bridge::protocols::NFT_BRIDGE_PROGRAM;
use sqlx::PgPool;

const HASH: &str = "nft-bridge-persistence-test";
const EMITTER: &str = "ee";

fn nft_transfer(status: BridgeStatus) -> BridgeTransfer {
    BridgeTransfer {
        program: NFT_BRIDGE_PROGRAM.to_string(),
        source_chain: ChainId::Solana,
        source_tx_hash: Some(HASH.to_string()),
        source_wallet: Some("SomeSignerPubkey".to_string()),
        source_explorer_url: None,
        destination_chain: ChainId::Ethereum,
        destination_wallet: Some("0xrecipient".to_string()),
        destination_tx_hash: None,
        destination_explorer_url: None,
        token: Some("0xcollection".to_string()),
        token_symbol: Some("CAC".to_string()),
        amount: Some("1".to_string()),
        amount_formatted: Some("1".to_string()),
        message_id: BridgeMessageId {
            emitter_chain: 1,
            emitter_address: EMITTER.repeat(32),
            sequence: 1317,
        },
        status,
    }
}

fn transaction(status: BridgeStatus) -> NormalisedTransaction {
    NormalisedTransaction {
        hash: HASH.to_string(),
        chain: Chain::Solana,
        status: TxStatus::Success,
        slot: 321_029_875,
        timestamp: Some(1_739_707_287),
        fee_lamports: 5000,
        signer: Some("SomeSignerPubkey".to_string()),
        bridge_event: None,
        bridge_transfer: Some(nft_transfer(status)),
    }
}

async fn row(pool: &PgPool) -> (String, String, Option<String>, i64) {
    sqlx::query_as(
        "SELECT program, status, token_symbol, COUNT(*) OVER () \
         FROM bridge_transfers WHERE protocol = 'wormhole' AND message_key = $1",
    )
    .bind(format!("1/{}/1317", EMITTER.repeat(32)))
    .fetch_one(pool)
    .await
    .expect("the transfer row")
}

#[tokio::test]
async fn an_unknown_destination_chain_is_stored_unresolved() {
    let db = common::test_db().await;
    let pool = &db.pool;

    let mut tx = transaction(BridgeStatus::Pending);
    let transfer = tx.bridge_transfer.as_mut().expect("transfer");
    transfer.destination_chain = ChainId::Unknown(9999);
    transfer.message_id.sequence = 4242;
    transfer.source_tx_hash = Some("unknown-chain-nft".to_string());
    tx.hash = "unknown-chain-nft".to_string();

    persist_analysis(pool, &tx, &[])
        .await
        .expect("an unknown chain must never be an error");

    let (chain_id, raw_id): (Option<i16>, String) = sqlx::query_as(
        "SELECT destination_chain_id, destination_ref_id FROM bridge_transfers \
         WHERE protocol = 'wormhole' AND message_key = $1",
    )
    .bind(format!("1/{}/4242", EMITTER.repeat(32)))
    .fetch_one(pool)
    .await
    .expect("the row is stored");

    assert_eq!(chain_id, None, "the registry does not know chain 9999 yet");
    assert_eq!(raw_id, "9999", "the raw reference is kept for a later pass");
}

#[tokio::test]
async fn an_nft_transfer_keeps_its_program_and_never_moves_backwards() {
    let db = common::test_db().await;
    let pool = &db.pool;

    persist_analysis(pool, &transaction(BridgeStatus::Pending), &[])
        .await
        .expect("first persist");
    assert_eq!(
        row(pool).await,
        (
            NFT_BRIDGE_PROGRAM.to_string(),
            "Pending".to_string(),
            Some("CAC".to_string()),
            1
        )
    );

    // A later pass that knows less must not lower the status, duplicate the row, or
    // lose the program.
    persist_analysis(pool, &transaction(BridgeStatus::Detected), &[])
        .await
        .expect("detected redelivery");
    assert_eq!(
        row(pool).await,
        (
            NFT_BRIDGE_PROGRAM.to_string(),
            "Pending".to_string(),
            Some("CAC".to_string()),
            1
        )
    );

    persist_analysis(pool, &transaction(BridgeStatus::Completed), &[])
        .await
        .expect("completion");
    assert_eq!(row(pool).await.1, "Completed");

    persist_analysis(pool, &transaction(BridgeStatus::Pending), &[])
        .await
        .expect("late pending view");
    assert_eq!(
        row(pool).await,
        (
            NFT_BRIDGE_PROGRAM.to_string(),
            "Completed".to_string(),
            Some("CAC".to_string()),
            1
        )
    );
}
