use crate::chain_adapters::registry::AdapterRegistry;
use crate::chains::ChainRegistry;
use crate::config::{AppConfig, Lane};
use crate::enrichment::EnrichmentProvider;
use crate::enrichment::evm_first::EvmFirstProvider;
use crate::enrichment::wormholescan::WormholeScanProvider;
use crate::evm::lookup::EvmEventLookup;
use crate::live_feed::LiveFeed;
use crate::protocols::registry::{ProtocolRegistry, configured_registry};
use crate::redis::broadcast::BroadcastSink;
use crate::services::read_through::ReadThrough;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use reqwest::Client;
use sqlx::PgPool;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;

const BROADCAST_CAPACITY: usize = 1024;

pub type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// The shared state itself. Kept behind a single `Arc` in [`AppState`] so cloning
/// the state is one refcount bump instead of one per field.
#[derive(Clone)]
pub struct Inner {
    pub db: PgPool,
    pub http_client: Client,
    pub config: Arc<AppConfig>,
    pub registry: Arc<AdapterRegistry>,
    pub chains: ChainRegistry,
    pub protocols: Arc<ProtocolRegistry>,
    pub tx_broadcast: broadcast::Sender<String>,
    /// Where workers send broadcast events. Local in `ROLE=all`, Redis pub/sub in
    /// `ROLE=stream`.
    pub broadcast: BroadcastSink,
    /// The read-through cache. Built only in the roles that serve the read API.
    pub read_through: Option<Arc<ReadThrough>>,
    /// The Helius lane this state spends: its endpoint, limiter and token patience.
    pub lane: Lane,
    /// The lane's Helius endpoint. Callers read this, never the config, so a state
    /// can only ever call its own lane's key.
    pub helius_url: String,
    pub helius_limiter: Arc<Limiter>,
    pub wormhole_limiter: Arc<Limiter>,
    pub enrichment: Arc<dyn EnrichmentProvider>,
    pub helius_wait: Duration,
    /// The server-owned live feed. Off by default; when off it issues no Redis
    /// command and the endpoint is not routed.
    pub live_feed: Arc<LiveFeed>,
}

#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

impl Deref for AppState {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AppState {
    /// A state for unit tests: an unconnectable lazy pool and an empty chain registry,
    /// so a test that reaches the read path fails fast instead of touching a database.
    #[cfg(test)]
    pub(crate) fn test_state(config: AppConfig) -> Self {
        use crate::chain_adapters::setup::build_registry;
        use crate::chains::Snapshot;

        let client = Client::new();
        let registry = build_registry(&config, &client, Lane::Api);
        let chains = ChainRegistry::new(Snapshot::build(Vec::new(), Vec::new()));
        let db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy(&config.database_url)
            .expect("lazy pool");
        Self::new(db, config, client, registry, chains).expect("test state")
    }

    pub fn new(
        db: PgPool,
        config: AppConfig,
        http_client: Client,
        registry: AdapterRegistry,
        chains: ChainRegistry,
    ) -> anyhow::Result<Self> {
        let (tx_broadcast, _) = broadcast::channel(BROADCAST_CAPACITY);
        let read_through = Arc::new(ReadThrough::new(
            config.upstream_permits,
            Duration::from_secs(config.failed_cache_ttl_secs),
        ));
        let wormhole_rps = config.wormhole_rps;
        // The registry built the Helius adapter and its token metadata lookup around
        // one limiter; reuse it so metadata and getTransaction share the same quota.
        let helius_limiter = registry.helius_limiter().unwrap_or_else(|| {
            Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(config.helius_rps(Lane::Api)).unwrap(),
            )))
        });
        let helius_url = config.helius_url(Lane::Api).to_string();
        let helius_wait = config.helius_token_wait(Lane::Api);

        let wormhole_limiter: Arc<Limiter> = Arc::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(wormhole_rps).unwrap(),
        )));
        let enrichment = build_enrichment(
            &config,
            http_client.clone(),
            wormhole_limiter.clone(),
            chains.clone(),
            db.clone(),
        );
        let live_feed = Arc::new(LiveFeed::new(config.live_feed.clone(), &config.redis_url));

        let protocols = Arc::new(configured_registry(
            &config.solana_bridge_programs,
            &config.solana_token_bridge_program,
            config.solana_nft_bridge_program.as_deref(),
            config
                .solana_cctp_message_transmitter_program
                .as_deref()
                .zip(config.solana_cctp_token_messenger_minter_program.as_deref()),
            &config.solana_ntt_programs,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                http_client,
                config: Arc::new(config),
                registry: Arc::new(registry),
                chains,
                protocols,
                broadcast: BroadcastSink::Local(tx_broadcast.clone()),
                tx_broadcast,
                read_through: Some(read_through),
                lane: Lane::Api,
                helius_url,
                helius_limiter,
                wormhole_limiter,
                enrichment,
                helius_wait,
                live_feed,
            }),
        })
    }

    /// The slim state `ROLE=stream` runs with: no read-through cache and the
    /// registry passed in is the ingest-lane one (the API lane is never built).
    ///
    /// The protocol registry and the live registry stay: consumer analysis
    /// (`analyse_tx`) decodes with the former and resolves token metadata through
    /// the latter. Broadcast events go to Redis pub/sub instead of the local channel.
    pub async fn new_stream(
        db: PgPool,
        config: AppConfig,
        http_client: Client,
        registry: AdapterRegistry,
        chains: ChainRegistry,
    ) -> anyhow::Result<Self> {
        let (tx_broadcast, _) = broadcast::channel(BROADCAST_CAPACITY);
        let broadcast =
            BroadcastSink::redis(&config.redis_url, config.pubsub_channel.clone()).await?;
        let helius_limiter = registry.helius_limiter().unwrap_or_else(|| {
            Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(config.helius_rps(Lane::Ingest)).unwrap(),
            )))
        });
        let helius_url = config.helius_url(Lane::Ingest).to_string();
        let helius_wait = config.helius_token_wait(Lane::Ingest);
        let wormhole_limiter: Arc<Limiter> = Arc::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(config.wormhole_rps).unwrap(),
        )));
        let enrichment = build_enrichment(
            &config,
            http_client.clone(),
            wormhole_limiter.clone(),
            chains.clone(),
            db.clone(),
        );
        let live_feed = Arc::new(LiveFeed::new(config.live_feed.clone(), &config.redis_url));

        let protocols = Arc::new(configured_registry(
            &config.solana_bridge_programs,
            &config.solana_token_bridge_program,
            config.solana_nft_bridge_program.as_deref(),
            config
                .solana_cctp_message_transmitter_program
                .as_deref()
                .zip(config.solana_cctp_token_messenger_minter_program.as_deref()),
            &config.solana_ntt_programs,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                http_client,
                config: Arc::new(config),
                registry: Arc::new(registry),
                chains,
                protocols,
                broadcast,
                tx_broadcast,
                read_through: None,
                lane: Lane::Ingest,
                helius_url,
                helius_limiter,
                wormhole_limiter,
                enrichment,
                helius_wait,
                live_feed,
            }),
        })
    }

    /// The read-through cache. Only the api/all roles build it; the stream role has
    /// no read path, so reaching this there is a wiring bug.
    pub fn read_through(&self) -> &Arc<ReadThrough> {
        self.read_through
            .as_ref()
            .expect("read-through cache is built only in ROLE=api|all")
    }

    /// A copy of this state for the ingest lane (backfill and consumer workers): its
    /// own Helius endpoint, registry, rate limiter and token patience, so ingest
    /// traffic can never eat into the API lane's quota. Everything else (database,
    /// broadcast channel, Wormholescan limiter) stays shared.
    pub fn for_ingest(&self, registry: AdapterRegistry) -> Self {
        // Same wiring as `new`: the ingest registry's metadata lookup and the ingest
        // pipeline share one limiter, separate from the API lane's.
        let config = &self.config;
        let helius_limiter = registry.helius_limiter().unwrap_or_else(|| {
            Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(config.helius_rps(Lane::Ingest)).unwrap(),
            )))
        });
        Self {
            inner: Arc::new(Inner {
                registry: Arc::new(registry),
                lane: Lane::Ingest,
                helius_url: config.helius_url(Lane::Ingest).to_string(),
                helius_limiter,
                helius_wait: config.helius_token_wait(Lane::Ingest),
                ..(*self.inner).clone()
            }),
        }
    }
}

/// WormholeScan, wrapped by the stored-EVM-log lookup when EVM watching is enabled. The
/// wrapper answers first only for a missing source (inbound) or destination (outbound)
/// hash and otherwise delegates, so WormholeScan remains the fallback.
fn build_enrichment(
    config: &AppConfig,
    http_client: Client,
    limiter: Arc<Limiter>,
    chains: ChainRegistry,
    db: PgPool,
) -> Arc<dyn EnrichmentProvider> {
    let wormholescan: Arc<dyn EnrichmentProvider> = Arc::new(WormholeScanProvider::new(
        http_client,
        config.wormhole_url.clone(),
        limiter,
        chains.clone(),
    ));
    if config.evm.enabled {
        Arc::new(EvmFirstProvider::new(
            wormholescan,
            EvmEventLookup::new(db, chains),
        ))
    } else {
        wormholescan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain_adapters::setup::build_registry;

    /// Each lane must spend its own Helius key: the API state reads the API
    /// endpoint and `for_ingest` repoints the copy at the stream endpoint, with the
    /// ingest lane's own quota and patience.
    #[tokio::test]
    async fn each_lane_spends_its_own_helius_key() {
        let config = AppConfig::test_config();
        assert_ne!(config.helius_api_url, config.helius_stream_url);

        let api = AppState::test_state(config.clone());
        assert_eq!(api.lane, Lane::Api);
        assert_eq!(api.helius_url, config.helius_api_url);
        assert_eq!(api.helius_wait, config.helius_token_wait(Lane::Api));

        let registry = build_registry(&config, &api.http_client, Lane::Ingest);
        let ingest = api.for_ingest(registry);

        assert_eq!(ingest.lane, Lane::Ingest);
        assert_eq!(ingest.helius_url, config.helius_stream_url);
        assert_eq!(ingest.helius_wait, config.helius_live_token_wait);
        assert!(!Arc::ptr_eq(&api.helius_limiter, &ingest.helius_limiter));
        // The API state is untouched by deriving the ingest copy.
        assert_eq!(api.helius_url, config.helius_api_url);
    }
}
