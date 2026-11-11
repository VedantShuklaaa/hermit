//! Connectivity probe for Helius LaserStream (gRPC).
//!
//! Run with:
//!   HELIUS_API_KEY=... cargo run --features laserstream --example laserstream_probe
//!
//! The key is only ever read from the `HELIUS_API_KEY` environment variable.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use helius_laserstream::{
    LaserstreamClient, LaserstreamConfig,
    grpc::{SubscribeRequest, SubscribeRequestFilterSlots, subscribe_update::UpdateOneof},
    subscribe,
};

const DEFAULT_ENDPOINT: &str = "https://laserstream-devnet-ewr.helius-rpc.com";
const UPDATES: usize = 20;

#[tokio::main]
async fn main() -> Result<()> {
    // This is a separate binary, so it installs the provider the main binary does.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Matches the rest of the app: .env is loaded into the process environment,
    // but the key itself is read only from HELIUS_API_KEY.
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let api_key =
        std::env::var("HELIUS_API_KEY").context("HELIUS_API_KEY is not set in the environment")?;
    let endpoint =
        std::env::var("LASERSTREAM_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());

    let config = LaserstreamConfig::new(endpoint.clone(), api_key);

    // A unary call first: it surfaces an auth or plan rejection as the exact gRPC
    // status instead of the stream client's generic "max reconnects" error.
    println!("Connecting to {endpoint}");
    let client = LaserstreamClient::connect(config.clone())
        .await
        .context("failed to open the LaserStream gRPC channel")?;
    let pong = client
        .ping(1)
        .await
        .context("LaserStream ping failed (check the API key and plan)")?;
    println!("Ping ok: count={}", pong.count);

    let slot = client
        .get_slot(None)
        .await
        .context("LaserStream get_slot failed")?;
    println!("Current slot: {}", slot.slot);

    let mut request = SubscribeRequest::default();
    request.slots.insert(
        "probe".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            ..Default::default()
        },
    );

    let (stream, _handle) = subscribe(config, request);
    tokio::pin!(stream);

    let mut seen = 0usize;
    while let Some(result) = stream.next().await {
        let update = result.context("LaserStream slot subscription error")?;

        if let Some(UpdateOneof::Slot(slot)) = update.update_oneof {
            seen += 1;
            println!("update {seen}: slot={} parent={:?}", slot.slot, slot.parent);

            if seen == UPDATES {
                break;
            }
        }
    }

    println!("Received {seen} slot updates, exiting");
    Ok(())
}
