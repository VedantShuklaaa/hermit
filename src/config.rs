use crate::clients::limiter::API_TOKEN_WAIT;
use crate::protocols::Protocol;
use crate::protocols::registry::parse_program_overrides;
use std::collections::BTreeMap;
use std::time::Duration;

/// Which part of the pipeline this process runs. `All` keeps the single-process
/// behaviour; the split roles connect through the broadcast channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    All,
    Api,
    Stream,
}

impl Role {
    /// Parses `ROLE`. The error is the startup message, so it names the accepted
    /// values and the value that was rejected.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "api" => Ok(Self::Api),
            "stream" => Ok(Self::Stream),
            other => anyhow::bail!("invalid ROLE {other:?}: expected one of all, api, stream"),
        }
    }

    fn from_env() -> anyhow::Result<Self> {
        match std::env::var("ROLE") {
            Ok(raw) => Self::parse(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Self::All),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this role serves the read API (and the WebSocket stream).
    pub fn runs_api(self) -> bool {
        matches!(self, Self::All | Self::Api)
    }

    /// Whether this role runs ingestion, consumers, recovery and re-enrichment.
    pub fn runs_pipeline(self) -> bool {
        matches!(self, Self::All | Self::Stream)
    }
}

/// Background re-enrichment of `Detected` and `Pending` transfers.
#[derive(Clone, Debug)]
pub struct ReenrichConfig {
    pub enabled: bool,
    /// Seconds between passes.
    pub interval_secs: u64,
    /// Rows handled per pass.
    pub batch: i64,
    /// A row is left to the normal pipeline until it is this old.
    pub min_age_secs: i64,
    /// A row is not asked again before this long after its last attempt.
    pub retry_secs: i64,
    /// Rows older than this are given up on (stale-while-revalidate can still refresh them).
    pub max_age_secs: i64,
    /// Percent of each pass's batch reserved for the missing-origin backfill, so a
    /// backlog of open rows cannot starve it. `0` disables the reservation.
    pub origin_share_percent: u8,
}

impl ReenrichConfig {
    /// Splits a pass's batch into `(open, origin)`. A configured share keeps at least
    /// one slot for the backfill whenever the batch has room for it.
    pub fn batch_split(&self, batch: i64) -> (i64, i64) {
        let batch = batch.max(0);
        let mut origin = (batch * i64::from(self.origin_share_percent) / 100).min(batch);
        if origin == 0 && self.origin_share_percent > 0 && batch > 0 {
            origin = 1;
        }
        (batch - origin, origin)
    }
}

/// EVM log watching for one or more chains. The targets and contract addresses
/// live in Postgres (`evm_watch_targets`); this only carries how to reach them
/// and how hard to poll.
#[derive(Clone, Debug)]
pub struct EvmConfig {
    /// `EVM_WATCH_ENABLED`, default off. Off starts no watchers and skips the
    /// transfer-side event lookup.
    pub enabled: bool,
    /// `EVM_RPC_<CHAIN>` and its optional `EVM_RPC_<CHAIN>_FALLBACK`, keyed by the
    /// chain registry slug (`ethereum` for `EVM_RPC_ETHEREUM`).
    pub rpc: BTreeMap<String, EvmChainRpcConfig>,
    /// Seconds between polls.
    pub poll_secs: u64,
    /// How long an unmatched stored event is retried by the event-side join.
    /// The transfer-side lookup is not age-limited.
    pub join_max_age_secs: i64,
}

/// One chain's RPC endpoints: a primary and an optional fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvmChainRpcConfig {
    pub primary: EvmRpcEndpointConfig,
    pub fallback: Option<EvmRpcEndpointConfig>,
}

impl EvmChainRpcConfig {
    /// Primary first, then the fallback when configured.
    pub fn endpoints(&self) -> impl Iterator<Item = &EvmRpcEndpointConfig> {
        std::iter::once(&self.primary).chain(self.fallback.as_ref())
    }
}

/// One EVM RPC endpoint. The URL is the only place a provider key appears; the
/// range and rate overrides are optional and fall back to the process defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvmRpcEndpointConfig {
    pub url: String,
    /// `EVM_RPC_<CHAIN>[_FALLBACK]_MAX_RANGE`, default `EVM_MAX_BLOCK_RANGE`.
    pub max_block_range: u64,
    /// `EVM_RPC_<CHAIN>[_FALLBACK]_RPS`, default `EVM_RPS`. Limits this endpoint
    /// only, because a provider quota is per key.
    pub rps: u32,
}

/// Per-client rate limiting for the public HTTP API.
#[derive(Clone, Debug)]
pub struct ApiRateLimitConfig {
    /// `API_RATE_LIMIT_RPS`, default 10. Sustained requests/sec per client.
    pub rps: u32,
    /// `API_RATE_LIMIT_BURST`, default 20. Requests a client may make at once.
    pub burst: u32,
    /// `API_TRUST_FORWARDED_FOR`, default off. Off keys the limiter by the peer
    /// address; on uses the leftmost `X-Forwarded-For` entry, which is only safe
    /// behind a proxy that overwrites the header.
    pub trust_forwarded_for: bool,
}

/// Server-owned live feed backed by Redis.
#[derive(Clone, Debug)]
pub struct LiveFeedConfig {
    /// `LIVE_FEED_REDIS_ENABLED`, default off. Off means no Redis writes, no
    /// endpoint and byte-for-byte unchanged WebSocket frames.
    pub enabled: bool,
    /// Most frames kept in the ring. `LIVE_FEED_MAX`, default 1000.
    pub max: usize,
}

/// Stream consumer and recovery worker.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Deliveries of one message before it is dead-lettered. A transient failure
    /// leaves the message pending, so this bounds how many times it can come back.
    pub max_deliveries: u32,
    /// How long a message must sit unacknowledged before recovery reclaims it.
    /// Must stay well below any outage that would justify a dead letter.
    pub reclaim_idle_ms: u64,
    /// Seconds between recovery sweeps.
    pub reclaim_interval_secs: u64,
    /// Worker tasks reading the stream.
    pub worker_count: usize,
    /// `XREADGROUP COUNT`: messages a worker reads per round.
    pub read_count: usize,
}

/// Always-on Solana ingestion (A8).
#[derive(Clone, Debug)]
pub struct IngestConfig {
    /// Kill switch. `INGEST_ENABLED=0` stops the ingester from starting at all.
    pub enabled: bool,
    /// Replay the signatures missed since the checkpoint each time a connection starts.
    pub backfill: bool,
    /// Most signatures one backfill run will publish. Bounds the Helius spend after a long
    /// outage; the checkpoint still moves forward, so the next restart does not repeat it.
    pub backfill_max: usize,
    /// Pause between backfilled signatures, to stay under the Helius plan's rate cap.
    /// A backfilled message costs up to three live Helius calls, so the pause has to
    /// be long enough that publishing cannot outrun the live limiter.
    pub backfill_pace_ms: u64,
    /// How far before the checkpoint a backfill starts, so transactions in the
    /// checkpoint's own slot are not lost on a reconnect. Never goes below slot 0.
    pub rewind_slots: u64,
    /// How long a published signature is remembered so a repeat (backfill overlapping the
    /// live feed, a reconnect) is not published twice. `0` disables the check.
    pub dedupe_ttl_secs: u64,
    /// WebSocket ping interval. A ping every this long keeps the connection observable.
    pub ping_secs: u64,
    /// Gap between programs' first connects at startup, so a provider's per-key
    /// connection limit is not hit by every `logsSubscribe` at once. `0` connects
    /// them together.
    pub connect_stagger_ms: u64,
    /// Most programs allowed a live WebSocket connection. `None` is unlimited and
    /// `0` keeps none; programs past the cap are covered by the reconciliation poll
    /// only.
    pub max_ws_connections: Option<usize>,
    /// No-frame watchdog. When not even a Pong arrives for this long the connection is
    /// treated as dead and reconnected. Always above `ping_secs`.
    pub watchdog_secs: u64,
    /// Seconds between reconciliation-poll cycles. The poll is the completeness
    /// safety net next to the low-latency WebSocket.
    pub poll_secs: u64,
    /// Each poll window starts this far before the previous cycle's end, so the part
    /// the previous window ended before seeing (the offset) is covered again.
    pub poll_overlap_secs: i64,
    /// Each poll window ends this long ago, so the ingester had a chance to see it.
    pub poll_offset_secs: i64,
    /// Most signatures one poll cycle publishes. Bounds the Helius spend after a gap.
    pub poll_max_publish: usize,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    /// `ROLE=all|api|stream`; defaults to `all`.
    pub role: Role,
    /// Redis pub/sub channel the stream role publishes broadcast events on and
    /// the api role forwards to its local WebSocket subscribers.
    pub pubsub_channel: String,
    /// Where the stream role serves its minimal `/healthz`.
    pub health_port: u16,
    pub database_url: String,
    pub redis_url: String,
    pub live_feed: LiveFeedConfig,
    /// Helius endpoint for the API lane (`/analyse`, list endpoint).
    /// `HELIUS_API_URL`, falling back to `HELIUS_URL`.
    pub helius_api_url: String,
    /// Helius endpoint for the ingest lane (ingester backfill + consumer workers).
    /// `HELIUS_STREAM_URL`, then the deprecated `HELIUS_LIVE_URL`, then `HELIUS_URL`.
    pub helius_stream_url: String,
    /// Helius endpoint for the read-only `reconcile` tool. Falls back to
    /// `HELIUS_URL` when `HELIUS_RECONCILE_URL` is unset.
    pub helius_reconcile_url: String,
    pub wormhole_url: String,
    /// Helius requests/sec for the API lane. Helius free plan: 10 requests/sec per key.
    /// While both lanes point at one key, `helius_api_rps + helius_live_rps` must add
    /// up to the plan limit; with a key per lane each can use its own plan's limit.
    pub helius_api_rps: u32,
    /// Helius requests/sec for the ingest lane (backfill + consumer workers).
    pub helius_live_rps: u32,
    /// How long a live worker waits for its first Helius token of a message. The
    /// message stays in the stream meanwhile; it must stay well below the reclaim
    /// idle time, or a message would be reclaimed before it finishes waiting.
    pub helius_live_token_wait: Duration,
    /// Helius requests/sec for the `reconcile` tool, on its own lane so a
    /// reconciliation run cannot starve the running pipeline.
    pub reconcile_rps: u32,
    /// WormholeScan requests/sec, shared by the GET API and enrichment.
    pub wormhole_rps: u32,
    /// Most upstream fetches one cold `/analyse` may run at once.
    pub upstream_permits: usize,
    /// How long a failed (`Normalisation` or transient upstream) analysis stays in the
    /// negative cache before the next request may retry it.
    pub failed_cache_ttl_secs: u64,
    pub port: u16,
    pub solana_token_bridge_program: String,
    /// Every program the Solana ingester subscribes to. `SOLANA_BRIDGE_PROGRAMS`
    /// (`protocol:program_id` CSV) when set, otherwise the single
    /// `SOLANA_TOKEN_BRIDGE_PROGRAM`. One subscription and checkpoint per entry.
    pub solana_bridge_programs: Vec<(Protocol, String)>,
    /// The Solana NFT Bridge program. Opt-in: `None` means the NFT Bridge is not
    /// subscribed to, so an existing deployment routes and ingests exactly as before.
    pub solana_nft_bridge_program: Option<String>,
    /// Programs that host NTT `release_wormhole_outbound`, one per deployment (the
    /// manager program in the verified deployments). `SOLANA_NTT_PROGRAMS` is a CSV,
    /// opt-in and empty by default: NTT has no shared program ID, so without this
    /// list the adapter is off and ingestion is unchanged.
    pub solana_ntt_programs: Vec<String>,
    /// The CCTP V2 programs. Opt-in: both are set or neither is, and unset means the
    /// CCTP adapter is off and ingestion is unchanged.
    pub solana_cctp_message_transmitter_program: Option<String>,
    pub solana_cctp_token_messenger_minter_program: Option<String>,
    pub solana_ws_url: String,
    pub allowed_origins: Vec<String>,
    pub api_rate_limit: ApiRateLimitConfig,
    pub ingest: IngestConfig,
    pub reenrich: ReenrichConfig,
    pub worker: WorkerConfig,
    pub evm: EvmConfig,
    /// How often the in-memory chain registry reloads from Postgres.
    pub chain_registry_refresh_secs: u64,
    /// `/health` answers 503 once the ingest heartbeat is older than this. Generous on
    /// purpose: quiet 25-minute gaps have happened in production.
    pub health_max_ingest_age_secs: i64,
}

impl AppConfig {
    /// The config the live pipeline runs with: everything is shared with the API
    /// except `helius_url`, which points at the live endpoint so the live registry,
    /// `analyse_tx` and account preloads spend the live key and quota.
    /// The Helius endpoint a lane spends.
    pub fn helius_url(&self, lane: Lane) -> &str {
        match lane {
            Lane::Api => &self.helius_api_url,
            Lane::Ingest => &self.helius_stream_url,
        }
    }

    /// Requests per second a lane's limiter allows.
    pub fn helius_rps(&self, lane: Lane) -> u32 {
        match lane {
            Lane::Api => self.helius_api_rps,
            Lane::Ingest => self.helius_live_rps,
        }
    }

    /// How long a caller on this lane waits for a Helius token before giving up.
    pub fn helius_token_wait(&self, lane: Lane) -> Duration {
        match lane {
            Lane::Api => API_TOKEN_WAIT,
            Lane::Ingest => self.helius_live_token_wait,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        fn required(key: &str) -> anyhow::Result<String> {
            std::env::var(key).map_err(|_| anyhow::anyhow!("{key} not set"))
        }
        fn optional(key: &str, default: &str) -> String {
            std::env::var(key).unwrap_or_else(|_| default.to_string())
        }
        fn csv_list(key: &str) -> Vec<String> {
            std::env::var(key)
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }

        fn non_empty(key: &str) -> Option<String> {
            std::env::var(key).ok().filter(|v| !v.trim().is_empty())
        }

        fn number<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        // Parsed before the other settings so a bad ROLE fails with its own message
        // rather than a missing database URL.
        let role = Role::from_env()?;
        let helius = resolve_helius_urls(
            role,
            non_empty("HELIUS_URL"),
            non_empty("HELIUS_API_URL"),
            non_empty("HELIUS_STREAM_URL"),
            non_empty("HELIUS_LIVE_URL"),
            non_empty("HELIUS_RECONCILE_URL"),
        )?;
        if helius.used_legacy_live_url {
            tracing::warn!("HELIUS_LIVE_URL is deprecated, rename it to HELIUS_STREAM_URL");
        }

        // The watchdog must outlast a ping, or a keepalive would be mistaken for silence.
        let ping_secs = number("INGEST_PING_SECS", 30u64).max(1);
        let watchdog_secs = number("INGEST_WATCHDOG_SECS", 90u64).max(ping_secs + 1);

        // EVM_RPC_<CHAIN>: the suffix is the chain registry slug. Empty values are
        // ignored so a commented-out placeholder cannot start a watcher with no URL.
        // Every endpoint defaults to the process-wide range and rate.
        let evm_rps = number("EVM_RPS", 5u32).max(1);
        let evm_max_block_range = number("EVM_MAX_BLOCK_RANGE", 10u64).max(1);
        let evm_rpc = collect_evm_rpc(std::env::vars(), evm_max_block_range, evm_rps)?;
        let evm_enabled = flag(std::env::var("EVM_WATCH_ENABLED").ok().as_deref());
        if evm_enabled && evm_rpc.is_empty() {
            anyhow::bail!("EVM_WATCH_ENABLED is set but no EVM_RPC_<CHAIN> is configured");
        }

        // The Token Bridge program is always known: it is the health signal and the
        // program the legacy single-program configuration names. The list either
        // replaces it as the subscription set or falls back to it.
        let solana_token_bridge_program = required("SOLANA_TOKEN_BRIDGE_PROGRAM")?;
        validate_program_id("SOLANA_TOKEN_BRIDGE_PROGRAM", &solana_token_bridge_program)?;
        let solana_bridge_programs = configured_programs(
            non_empty("SOLANA_BRIDGE_PROGRAMS"),
            &solana_token_bridge_program,
        )?;
        // Opt-in. Unset means the NFT Bridge is not subscribed to at all.
        let solana_nft_bridge_program = non_empty("SOLANA_NFT_BRIDGE_PROGRAM");
        if let Some(id) = &solana_nft_bridge_program {
            validate_program_id("SOLANA_NFT_BRIDGE_PROGRAM", id)?;
        }
        // NTT is opt-in per deployment and has no default program ID.
        let mut solana_ntt_programs = csv_list("SOLANA_NTT_PROGRAMS");
        for id in &solana_ntt_programs {
            validate_program_id("SOLANA_NTT_PROGRAMS", id)?;
        }
        solana_ntt_programs.sort();
        solana_ntt_programs.dedup();
        // CCTP is opt-in too, and only ever on as a pair.
        let solana_cctp_message_transmitter_program =
            non_empty("SOLANA_CCTP_MESSAGE_TRANSMITTER_PROGRAM");
        let solana_cctp_token_messenger_minter_program =
            non_empty("SOLANA_CCTP_TOKEN_MESSENGER_MINTER_PROGRAM");
        match (
            &solana_cctp_message_transmitter_program,
            &solana_cctp_token_messenger_minter_program,
        ) {
            (Some(transmitter), Some(minter)) => {
                validate_program_id("SOLANA_CCTP_MESSAGE_TRANSMITTER_PROGRAM", transmitter)?;
                validate_program_id("SOLANA_CCTP_TOKEN_MESSENGER_MINTER_PROGRAM", minter)?;
            }
            (None, None) => {}
            _ => anyhow::bail!(
                "set both SOLANA_CCTP_MESSAGE_TRANSMITTER_PROGRAM and \
                 SOLANA_CCTP_TOKEN_MESSENGER_MINTER_PROGRAM, or neither"
            ),
        }

        Ok(Self {
            role,
            pubsub_channel: optional("TX_EVENTS_CHANNEL", "bridge:tx_events"),
            health_port: std::env::var("HEALTH_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8081),
            database_url: required("DATABASE_URL")?,
            redis_url: required("REDIS_URL")?,
            live_feed: LiveFeedConfig {
                enabled: std::env::var("LIVE_FEED_REDIS_ENABLED")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                max: number("LIVE_FEED_MAX", 1000usize).clamp(1, 10_000),
            },
            helius_api_url: helius.api,
            helius_stream_url: helius.ingest,
            helius_reconcile_url: helius.reconcile,
            wormhole_url: optional("WORMHOLE_URL", "https://api.wormholescan.io"),
            helius_api_rps: number("HELIUS_API_RPS", 6u32).max(1),
            helius_live_rps: number("HELIUS_LIVE_RPS", 4u32).max(1),
            helius_live_token_wait: Duration::from_millis(number(
                "HELIUS_LIVE_TOKEN_WAIT_MS",
                20_000u64,
            )),
            reconcile_rps: number("RECONCILE_RPS", 3u32).max(1),
            wormhole_rps: number("WORMHOLE_RPS", 20u32).max(1),
            upstream_permits: number("UPSTREAM_PERMITS", 16usize).max(1),
            failed_cache_ttl_secs: number("FAILED_CACHE_TTL_SECS", 10u64).max(1),
            port: std::env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            solana_token_bridge_program,
            solana_bridge_programs,
            solana_nft_bridge_program,
            solana_ntt_programs,
            solana_cctp_message_transmitter_program,
            solana_cctp_token_messenger_minter_program,
            solana_ws_url: required("SOLANA_WS_URL")?,
            allowed_origins: csv_list("ALLOWED_ORIGINS"),
            api_rate_limit: ApiRateLimitConfig {
                rps: number("API_RATE_LIMIT_RPS", 10u32).max(1),
                burst: number("API_RATE_LIMIT_BURST", 20u32).max(1),
                trust_forwarded_for: flag(std::env::var("API_TRUST_FORWARDED_FOR").ok().as_deref()),
            },
            ingest: IngestConfig {
                enabled: std::env::var("INGEST_ENABLED")
                    .map(|v| v != "0")
                    .unwrap_or(true),
                backfill: std::env::var("LIVE_BACKFILL")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(true),
                backfill_max: number("INGEST_BACKFILL_MAX", 5000usize),
                backfill_pace_ms: number("INGEST_BACKFILL_PACE_MS", 750u64),
                rewind_slots: number("INGEST_REWIND_SLOTS", 20u64),
                dedupe_ttl_secs: number("INGEST_DEDUPE_TTL_SECS", 7200u64),
                ping_secs,
                connect_stagger_ms: number("INGEST_CONNECT_STAGGER_MS", 500u64),
                max_ws_connections: std::env::var("INGEST_MAX_WS_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.trim().parse::<usize>().ok()),
                watchdog_secs,
                poll_secs: number("INGEST_POLL_SECS", 300u64).max(30),
                poll_overlap_secs: number("INGEST_POLL_OVERLAP_SECS", 600i64).max(0),
                poll_offset_secs: number("INGEST_POLL_OFFSET_SECS", 180i64).max(0),
                poll_max_publish: number("INGEST_POLL_MAX_PUBLISH", 500usize).max(1),
            },
            worker: WorkerConfig {
                max_deliveries: number("WORKER_MAX_DELIVERIES", 10u32).max(1),
                reclaim_idle_ms: number("WORKER_RECLAIM_IDLE_MS", 300_000u64),
                reclaim_interval_secs: number("WORKER_RECLAIM_INTERVAL_SECS", 30u64).max(1),
                worker_count: number("CONSUMER_COUNT", 5usize).max(1),
                read_count: number("CONSUMER_READ_COUNT", 10usize).max(1),
            },
            evm: EvmConfig {
                enabled: evm_enabled,
                rpc: evm_rpc,
                poll_secs: number("EVM_POLL_SECS", 12u64).max(1),
                join_max_age_secs: number("EVM_JOIN_MAX_AGE_SECS", 86_400i64).max(1),
            },
            chain_registry_refresh_secs: number("CHAIN_REGISTRY_REFRESH_SECS", 60u64).max(5),
            health_max_ingest_age_secs: number("HEALTH_MAX_INGEST_AGE_SECS", 3600i64).max(1),
            reenrich: ReenrichConfig {
                enabled: std::env::var("REENRICH_ENABLED")
                    .map(|v| v != "0")
                    .unwrap_or(true),
                interval_secs: number("REENRICH_INTERVAL_SECS", 30u64).max(1),
                batch: number("REENRICH_BATCH", 50i64).max(1),
                min_age_secs: number("REENRICH_MIN_AGE_SECS", 60i64).max(0),
                retry_secs: number("REENRICH_RETRY_SECS", 120i64).max(0),
                max_age_secs: number("REENRICH_MAX_AGE_SECS", 7 * 24 * 3600i64).max(1),
                origin_share_percent: number("REENRICH_ORIGIN_SHARE", 40u8).min(100),
            },
        })
    }
}

/// A Helius provider lane: its own endpoint (so its own key and plan quota), its own
/// rate limiter and its own token patience. `Api` serves `/analyse` and the list
/// endpoint; `Ingest` is the pipeline (ingester backfill, consumer workers, recovery
/// and re-enrichment).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Api,
    Ingest,
}

impl Lane {
    /// The label value used in metrics for this lane.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Ingest => "ingest",
        }
    }
}

/// The Helius endpoints resolved from the environment.
#[derive(Debug, PartialEq, Eq)]
struct HeliusUrls {
    api: String,
    ingest: String,
    reconcile: String,
    /// `HELIUS_LIVE_URL` supplied the ingest endpoint (the pre-1c name).
    used_legacy_live_url: bool,
}

/// Picks each lane's endpoint. `HELIUS_API_URL` and `HELIUS_STREAM_URL` win; the
/// ingest lane then accepts the pre-1c `HELIUS_LIVE_URL`, and both fall back to
/// `HELIUS_URL`. Only the lanes the role runs have to resolve; a lane the role never
/// builds borrows the other one so no field is ever empty.
fn resolve_helius_urls(
    role: Role,
    base: Option<String>,
    api: Option<String>,
    stream: Option<String>,
    legacy_live: Option<String>,
    reconcile: Option<String>,
) -> anyhow::Result<HeliusUrls> {
    let used_legacy_live_url = stream.is_none() && legacy_live.is_some();
    let api = api.or_else(|| base.clone());
    let ingest = stream.or(legacy_live).or_else(|| base.clone());

    if role.runs_api() && api.is_none() {
        anyhow::bail!("HELIUS_API_URL not set (and no HELIUS_URL to fall back to)");
    }
    if role.runs_pipeline() && ingest.is_none() {
        anyhow::bail!("HELIUS_STREAM_URL not set (and no HELIUS_URL to fall back to)");
    }

    let (api, ingest) = match (api, ingest) {
        (Some(api), Some(ingest)) => (api, ingest),
        (Some(url), None) | (None, Some(url)) => (url.clone(), url),
        (None, None) => anyhow::bail!("set HELIUS_URL, or HELIUS_API_URL and HELIUS_STREAM_URL"),
    };
    let reconcile = reconcile.or(base).unwrap_or_else(|| api.clone());

    Ok(HeliusUrls {
        api,
        ingest,
        reconcile,
        used_legacy_live_url,
    })
}

#[cfg(test)]
impl AppConfig {
    /// An env-independent config for unit tests that build an `AppState` without a
    /// database or any upstream: the API and live Helius endpoints differ so a test
    /// can tell which lane a caller uses.
    pub(crate) fn test_config() -> Self {
        Self {
            role: Role::All,
            pubsub_channel: "bridge:tx_events".to_string(),
            health_port: 0,
            database_url: "postgres://127.0.0.1:1/unused".to_string(),
            redis_url: "redis://127.0.0.1:1".to_string(),
            live_feed: LiveFeedConfig {
                enabled: false,
                max: 1000,
            },
            helius_api_url: "https://api.helius.test".to_string(),
            helius_stream_url: "https://stream.helius.test".to_string(),
            helius_reconcile_url: "https://reconcile.helius.test".to_string(),
            wormhole_url: "https://wormholescan.test".to_string(),
            helius_api_rps: 20,
            helius_live_rps: 20,
            helius_live_token_wait: Duration::from_secs(5),
            reconcile_rps: 20,
            wormhole_rps: 20,
            upstream_permits: 4,
            failed_cache_ttl_secs: 10,
            port: 0,
            solana_token_bridge_program: "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth".to_string(),
            solana_bridge_programs: vec![(
                Protocol::Wormhole,
                "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth".to_string(),
            )],
            solana_nft_bridge_program: None,
            solana_ntt_programs: Vec::new(),
            solana_cctp_message_transmitter_program: None,
            solana_cctp_token_messenger_minter_program: None,
            solana_ws_url: "ws://127.0.0.1:1".to_string(),
            allowed_origins: Vec::new(),
            api_rate_limit: ApiRateLimitConfig {
                rps: 10,
                burst: 20,
                trust_forwarded_for: false,
            },
            ingest: IngestConfig {
                enabled: false,
                backfill: false,
                backfill_max: 0,
                backfill_pace_ms: 0,
                rewind_slots: 20,
                dedupe_ttl_secs: 0,
                ping_secs: 30,
                connect_stagger_ms: 0,
                max_ws_connections: None,
                watchdog_secs: 90,
                poll_secs: 300,
                poll_overlap_secs: 600,
                poll_offset_secs: 180,
                poll_max_publish: 500,
            },
            reenrich: ReenrichConfig {
                enabled: false,
                interval_secs: 1,
                batch: 1,
                min_age_secs: 0,
                retry_secs: 0,
                max_age_secs: 1,
                origin_share_percent: 40,
            },
            worker: WorkerConfig {
                max_deliveries: 1,
                reclaim_idle_ms: 1,
                reclaim_interval_secs: 1,
                worker_count: 5,
                read_count: 10,
            },
            evm: EvmConfig {
                enabled: false,
                rpc: BTreeMap::new(),
                poll_secs: 12,
                join_max_age_secs: 86_400,
            },
            chain_registry_refresh_secs: 60,
            health_max_ingest_age_secs: 3600,
        }
    }
}

/// `EVM_RPC_<CHAIN>`, `EVM_RPC_<CHAIN>_FALLBACK` and their `_MAX_RANGE` / `_RPS`
/// overrides, keyed by lower-case chain slug. Empty names and values are ignored, so
/// a commented-out placeholder cannot start a watcher with no URL. A fallback with no
/// primary is an error: it would never be used.
fn collect_evm_rpc<I, K, V>(
    vars: I,
    default_range: u64,
    default_rps: u32,
) -> anyhow::Result<BTreeMap<String, EvmChainRpcConfig>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    #[derive(Default)]
    struct RawEndpoint {
        url: Option<String>,
        max_block_range: Option<u64>,
        rps: Option<u32>,
    }

    #[derive(Default)]
    struct RawChain {
        primary: RawEndpoint,
        fallback: RawEndpoint,
    }

    enum Field {
        PrimaryUrl,
        FallbackUrl,
        PrimaryRange,
        FallbackRange,
        PrimaryRps,
        FallbackRps,
    }

    let mut raw: BTreeMap<String, RawChain> = BTreeMap::new();
    for (key, value) in vars {
        let Some(rest) = key.as_ref().strip_prefix("EVM_RPC_") else {
            continue;
        };
        let (chain, field) = if let Some(chain) = rest.strip_suffix("_FALLBACK_MAX_RANGE") {
            (chain, Field::FallbackRange)
        } else if let Some(chain) = rest.strip_suffix("_FALLBACK_RPS") {
            (chain, Field::FallbackRps)
        } else if let Some(chain) = rest.strip_suffix("_MAX_RANGE") {
            (chain, Field::PrimaryRange)
        } else if let Some(chain) = rest.strip_suffix("_RPS") {
            (chain, Field::PrimaryRps)
        } else if let Some(chain) = rest.strip_suffix("_FALLBACK") {
            (chain, Field::FallbackUrl)
        } else {
            (rest, Field::PrimaryUrl)
        };
        if chain.is_empty() {
            continue;
        }

        let value = value.as_ref().trim();
        let entry = raw.entry(chain.to_ascii_lowercase()).or_default();
        match field {
            Field::PrimaryUrl => entry.primary.url = non_empty_value(value),
            Field::FallbackUrl => entry.fallback.url = non_empty_value(value),
            Field::PrimaryRange => entry.primary.max_block_range = parse_override(value),
            Field::FallbackRange => entry.fallback.max_block_range = parse_override(value),
            Field::PrimaryRps => entry.primary.rps = parse_override(value),
            Field::FallbackRps => entry.fallback.rps = parse_override(value),
        }
    }

    let mut chains = BTreeMap::new();
    for (slug, raw) in raw {
        let RawChain { primary, fallback } = raw;
        let Some(primary_url) = primary.url else {
            if fallback.url.is_some() {
                let name = slug.to_ascii_uppercase();
                anyhow::bail!("EVM_RPC_{name}_FALLBACK is set but EVM_RPC_{name} is not");
            }
            continue;
        };

        let endpoint =
            |url: String, max_block_range: Option<u64>, rps: Option<u32>| EvmRpcEndpointConfig {
                url,
                max_block_range: max_block_range.unwrap_or(default_range).max(1),
                rps: rps.unwrap_or(default_rps).max(1),
            };
        let primary = endpoint(primary_url, primary.max_block_range, primary.rps);
        let fallback = match fallback {
            RawEndpoint {
                url: Some(url),
                max_block_range,
                rps,
            } => Some(endpoint(url, max_block_range, rps)),
            RawEndpoint { url: None, .. } => None,
        };

        chains.insert(slug, EvmChainRpcConfig { primary, fallback });
    }
    Ok(chains)
}

fn non_empty_value(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

/// A numeric override. Blank or unparseable means "use the default", matching the
/// `EVM_RPS` and `EVM_MAX_BLOCK_RANGE` handling.
fn parse_override<T: std::str::FromStr>(value: &str) -> Option<T> {
    value.parse().ok()
}

/// The boolean convention the other switches use: `1` or `true`, case-insensitive.
fn flag(value: Option<&str>) -> bool {
    value.is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// The subscription set: the configured list when present, otherwise the legacy
/// single Token Bridge program.
fn configured_programs(
    configured: Option<String>,
    token_bridge: &str,
) -> anyhow::Result<Vec<(Protocol, String)>> {
    match configured {
        Some(csv) => parse_bridge_programs(&csv),
        None => Ok(vec![(Protocol::Wormhole, token_bridge.to_string())]),
    }
}

/// Parses `SOLANA_BRIDGE_PROGRAMS`: a `protocol:program_id` CSV. Every program ID
/// must be base58 for a 32-byte Solana address, so a typo fails startup rather than
/// silently subscribing to nothing.
fn parse_bridge_programs(csv: &str) -> anyhow::Result<Vec<(Protocol, String)>> {
    let programs = parse_program_overrides(csv)
        .map_err(|error| anyhow::anyhow!("invalid SOLANA_BRIDGE_PROGRAMS: {error}"))?;
    for (_, id) in &programs {
        validate_program_id("SOLANA_BRIDGE_PROGRAMS", id)?;
    }
    Ok(programs)
}

/// A Solana program ID is a base58-encoded 32-byte public key.
fn validate_program_id(variable: &str, id: &str) -> anyhow::Result<()> {
    let bytes = bs58::decode(id)
        .into_vec()
        .map_err(|_| anyhow::anyhow!("invalid {variable}: {id:?} is not valid base58"))?;
    if bytes.len() != 32 {
        anyhow::bail!("invalid {variable}: {id:?} does not decode to a 32-byte program id");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reenrich(origin_share_percent: u8) -> ReenrichConfig {
        ReenrichConfig {
            enabled: true,
            interval_secs: 30,
            batch: 50,
            min_age_secs: 60,
            retry_secs: 120,
            max_age_secs: 3600,
            origin_share_percent,
        }
    }

    #[test]
    fn the_default_split_reserves_a_share_for_the_backfill() {
        assert_eq!(reenrich(40).batch_split(50), (30, 20));
        assert_eq!(reenrich(0).batch_split(50), (50, 0));
        assert_eq!(reenrich(100).batch_split(50), (0, 50));
    }

    #[test]
    fn a_small_batch_still_reserves_one_origin_slot() {
        assert_eq!(reenrich(40).batch_split(1), (0, 1));
        assert_eq!(reenrich(40).batch_split(2), (1, 1));
    }

    #[test]
    fn role_parses_the_three_deployments() {
        assert_eq!(Role::parse("all").unwrap(), Role::All);
        assert_eq!(Role::parse("api").unwrap(), Role::Api);
        assert_eq!(Role::parse("stream").unwrap(), Role::Stream);
        assert_eq!(Role::parse(" API ").unwrap(), Role::Api);
    }

    #[test]
    fn an_unknown_role_names_the_valid_values() {
        let error = Role::parse("wokrer").unwrap_err().to_string();
        assert!(error.contains("invalid ROLE"), "{error}");
        assert!(error.contains("wokrer"), "{error}");
        assert!(error.contains("all, api, stream"), "{error}");
    }

    #[test]
    fn the_roles_split_api_and_pipeline_work() {
        assert!(Role::All.runs_api() && Role::All.runs_pipeline());
        assert!(Role::Api.runs_api() && !Role::Api.runs_pipeline());
        assert!(!Role::Stream.runs_api() && Role::Stream.runs_pipeline());
    }

    fn s(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[test]
    fn evm_rpc_configs_are_keyed_by_lower_case_chain_slug() {
        let chains = collect_evm_rpc(
            [
                ("EVM_RPC_ETHEREUM", " https://eth "),
                ("EVM_RPC_POLYGON", "https://polygon"),
                ("EVM_RPC_ETHEREUM_FALLBACK", "https://eth-backup"),
                ("EVM_RPC_ETHEREUM_MAX_RANGE", "101"),
                ("EVM_RPC_ETHEREUM_RPS", "3"),
                ("EVM_RPC_ETHEREUM_FALLBACK_MAX_RANGE", "10"),
                ("EVM_RPC_ETHEREUM_FALLBACK_RPS", "7"),
                ("EVM_RPC_", "https://ignored"),
                ("EVM_RPC_BASE", "   "),
                ("HELIUS_URL", "https://ignored"),
            ],
            10,
            5,
        )
        .expect("config");

        assert_eq!(chains.len(), 2);
        let ethereum = &chains["ethereum"];
        assert_eq!(ethereum.primary.url, "https://eth");
        assert_eq!(ethereum.primary.max_block_range, 101);
        assert_eq!(ethereum.primary.rps, 3);

        let fallback = ethereum.fallback.as_ref().expect("fallback");
        assert_eq!(fallback.url, "https://eth-backup");
        assert_eq!(fallback.max_block_range, 10);
        assert_eq!(fallback.rps, 7);

        // Unset overrides take the process defaults.
        let polygon = &chains["polygon"];
        assert_eq!(polygon.primary.url, "https://polygon");
        assert_eq!(polygon.primary.max_block_range, 10);
        assert_eq!(polygon.primary.rps, 5);
        assert!(polygon.fallback.is_none());
    }

    #[test]
    fn a_fallback_without_a_primary_is_an_error() {
        let error = collect_evm_rpc([("EVM_RPC_BASE_FALLBACK", "https://backup")], 10, 5)
            .expect_err("fallback without a primary")
            .to_string();
        assert!(error.contains("EVM_RPC_BASE"), "{error}");
    }

    #[test]
    fn evm_watch_accepts_one_and_true_only() {
        assert!(flag(Some("1")));
        assert!(flag(Some("true")));
        assert!(flag(Some("TRUE")));
        assert!(!flag(Some("0")));
        assert!(!flag(Some("yes")));
        assert!(!flag(None));
    }

    #[test]
    fn both_lanes_fall_back_to_helius_url() {
        let urls = resolve_helius_urls(Role::All, s("base"), None, None, None, None).unwrap();
        assert_eq!((urls.api.as_str(), urls.ingest.as_str()), ("base", "base"));
        assert_eq!(urls.reconcile, "base");
        assert!(!urls.used_legacy_live_url);
    }

    #[test]
    fn each_lane_prefers_its_own_url() {
        let urls = resolve_helius_urls(
            Role::All,
            s("base"),
            s("api"),
            s("stream"),
            s("live"),
            s("reconcile"),
        )
        .unwrap();
        assert_eq!(urls.api, "api");
        assert_eq!(urls.ingest, "stream");
        assert_eq!(urls.reconcile, "reconcile");
        assert!(!urls.used_legacy_live_url);
    }

    #[test]
    fn the_old_live_url_still_feeds_the_ingest_lane_and_is_flagged() {
        let urls =
            resolve_helius_urls(Role::All, s("base"), s("api"), None, s("live"), None).unwrap();
        assert_eq!(urls.api, "api");
        assert_eq!(urls.ingest, "live");
        assert!(urls.used_legacy_live_url);
    }

    #[test]
    fn a_split_role_needs_only_its_own_lane_url() {
        let api = resolve_helius_urls(Role::Api, None, s("api"), None, None, None).unwrap();
        assert_eq!(api.api, "api");

        let stream =
            resolve_helius_urls(Role::Stream, None, None, s("stream"), None, None).unwrap();
        assert_eq!(stream.ingest, "stream");
    }

    #[test]
    fn a_missing_lane_url_names_the_variable() {
        let error = resolve_helius_urls(Role::Api, None, None, s("stream"), None, None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("HELIUS_API_URL"), "{error}");

        let error = resolve_helius_urls(Role::Stream, None, s("api"), None, None, None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("HELIUS_STREAM_URL"), "{error}");

        assert!(resolve_helius_urls(Role::All, None, None, None, None, None).is_err());
    }

    #[test]
    fn a_lane_reads_its_own_url_quota_and_patience() {
        let config = AppConfig::test_config();
        assert_ne!(
            config.helius_url(Lane::Api),
            config.helius_url(Lane::Ingest)
        );
        assert_eq!(config.helius_rps(Lane::Api), config.helius_api_rps);
        assert_eq!(config.helius_rps(Lane::Ingest), config.helius_live_rps);
        assert_eq!(config.helius_token_wait(Lane::Api), API_TOKEN_WAIT);
        assert_eq!(
            config.helius_token_wait(Lane::Ingest),
            config.helius_live_token_wait
        );
    }

    #[test]
    fn the_program_list_parses_and_keeps_every_entry() {
        use crate::protocols::wormhole::{CORE_BRIDGE, TOKEN_BRIDGE};

        assert_eq!(
            parse_bridge_programs(&format!("wormhole:{TOKEN_BRIDGE}, wormhole:{CORE_BRIDGE}"))
                .unwrap(),
            vec![
                (Protocol::Wormhole, TOKEN_BRIDGE.to_string()),
                (Protocol::Wormhole, CORE_BRIDGE.to_string())
            ]
        );
    }

    #[test]
    fn an_invalid_program_address_names_the_variable() {
        let error = parse_bridge_programs("wormhole:not-base58!").unwrap_err();
        assert!(
            error.to_string().contains("SOLANA_BRIDGE_PROGRAMS"),
            "{error}"
        );
        assert!(error.to_string().contains("not-base58!"), "{error}");
    }

    #[test]
    fn a_program_id_must_decode_to_32_bytes() {
        // Valid base58, but not a 32-byte public key.
        let error = parse_bridge_programs("wormhole:abc").unwrap_err();
        assert!(error.to_string().contains("32-byte"), "{error}");
    }

    #[test]
    fn the_legacy_single_program_is_the_fallback() {
        use crate::protocols::wormhole::{CORE_BRIDGE, TOKEN_BRIDGE};

        assert_eq!(
            configured_programs(None, TOKEN_BRIDGE).unwrap(),
            vec![(Protocol::Wormhole, TOKEN_BRIDGE.to_string())]
        );

        assert_eq!(
            configured_programs(
                Some(format!("wormhole:{TOKEN_BRIDGE},wormhole:{}", CORE_BRIDGE)),
                "ignored-because-the-list-wins",
            )
            .unwrap(),
            vec![
                (Protocol::Wormhole, TOKEN_BRIDGE.to_string()),
                (Protocol::Wormhole, CORE_BRIDGE.to_string())
            ]
        );
    }
}
