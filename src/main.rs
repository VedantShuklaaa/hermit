#[cfg(feature = "laserstream")]
use anyhow::Context;
use bridge::chain_adapters::setup::build_registry;
use bridge::chains::{ChainRegistry, db::run_refresh_loop};
use bridge::clients::limiter::Limiter;
use bridge::config::{AppConfig, IngestSourceKind, Lane, Role};
use bridge::db;
use bridge::enrichment::reenrich;
use bridge::evm::db as evm_db;
use bridge::evm::watcher::{EvmWatcher, WatcherEndpoint};
#[cfg(feature = "laserstream")]
use bridge::ingestion::laserstream::LaserStreamIngester;
use bridge::ingestion::solana::SolanaIngester;
use bridge::ingestion::source::IngestSource;
use bridge::protocols::registry::ProtocolRegistry;
use bridge::redis::broadcast;
use bridge::redis::consumer::RedisConsumer;
use bridge::redis::producer::RedisProducer;
use bridge::routes::build_router;
use bridge::services::reconcile_poll::ReconcilePoller;
use bridge::shutdown::wait_for_shutdown_signal;
use bridge::state::AppState;
use governor::{Quota, RateLimiter};
use mimalloc::MiMalloc;
use reqwest::Client;
use rustls::crypto::ring;
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn spawn_supervised<F, Fut>(task_name: &'static str, shutdown: CancellationToken, make_task: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(task = task_name, "shutdown requested, stopping worker");
                    break;
                }
                result = tokio::spawn(make_task()) => {
                    match result {
                        Ok(Ok(())) => tracing::warn!(task = task_name, "worker exited cleanly, restarting"),
                        Ok(Err(error)) => tracing::error!(task = task_name, ?error, "worker failed, restarting"),
                        Err(join_error) => tracing::error!(task = task_name, ?join_error, "worker panicked, restarting"),
                    }
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env()?;

    // Every role serves Prometheus metrics; the listener starts before any work.
    bridge::metrics::init()?;

    match config.role {
        Role::All => run_all(config).await,
        Role::Api => run_api(config).await,
        Role::Stream => run_stream(config).await,
    }
}

async fn connect_database(config: &AppConfig) -> anyhow::Result<sqlx::PgPool> {
    let db = db::connection::connect(&config.database_url).await?;
    sqlx::migrate!("./migrations").run(&db).await?;
    Ok(db)
}

fn build_http_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?)
}

/// The cancellation token every task watches; a signal cancels it.
fn install_shutdown() -> CancellationToken {
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("shutdown signal received, draining workers");
        signal.cancel();
    });
    shutdown
}

/// The single-process deployment: API and pipeline in one process, with the
/// in-process broadcast channel. This is the pre-split behaviour, unchanged.
async fn run_all(config: AppConfig) -> anyhow::Result<()> {
    let db = connect_database(&config).await?;
    let http_client = build_http_client()?;

    let registry = build_registry(&config, &http_client, Lane::Api);
    let chains = ChainRegistry::load(&db).await?;
    let state = AppState::new(
        db.clone(),
        config.clone(),
        http_client.clone(),
        registry,
        chains.clone(),
    )?;

    // The ingest lane (backfill + consumer workers) gets its own Helius endpoint, registry
    // and rate limiter, so it can't use up the API lane's quota.
    let live_registry = build_registry(&config, &http_client, Lane::Ingest);
    let live_state = state.for_ingest(live_registry);

    let redis = RedisProducer::new(&config.redis_url).await?;
    redis.test_connection().await?;

    let shutdown = install_shutdown();

    tokio::spawn(run_refresh_loop(
        chains,
        db.clone(),
        Duration::from_secs(config.chain_registry_refresh_secs),
        shutdown.clone(),
    ));

    spawn_consumers(live_state.clone(), &config, shutdown.clone());

    spawn_reenrichment(state.clone(), &config, shutdown.clone());

    spawn_ingester(
        &config,
        redis.clone(),
        db.clone(),
        http_client.clone(),
        shutdown.clone(),
    )?;

    spawn_evm_watchers(&config, db.clone(), http_client.clone(), shutdown.clone()).await;

    spawn_reconcile_poll(
        &config,
        state.protocols.clone(),
        redis,
        db,
        http_client,
        shutdown.clone(),
    );

    let app = build_router(state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(role = "all", "listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .await?;

    Ok(())
}

/// The read API: axum, the chain refresh loop, and a forwarder that feeds Redis
/// pub/sub events into this process's local broadcast channel. No pipeline.
async fn run_api(config: AppConfig) -> anyhow::Result<()> {
    let db = connect_database(&config).await?;
    let http_client = build_http_client()?;

    let registry = build_registry(&config, &http_client, Lane::Api);
    let chains = ChainRegistry::load(&db).await?;
    let state = AppState::new(
        db.clone(),
        config.clone(),
        http_client,
        registry,
        chains.clone(),
    )?;

    let shutdown = install_shutdown();

    tokio::spawn(run_refresh_loop(
        chains,
        db.clone(),
        Duration::from_secs(config.chain_registry_refresh_secs),
        shutdown.clone(),
    ));

    {
        let redis_url = state.config.redis_url.clone();
        let channel = state.config.pubsub_channel.clone();
        let tx = state.tx_broadcast.clone();
        let forward_shutdown = shutdown.clone();

        spawn_supervised("broadcast-forwarder", shutdown.clone(), move || {
            let redis_url = redis_url.clone();
            let channel = channel.clone();
            let tx = tx.clone();
            let forward_shutdown = forward_shutdown.clone();

            async move { broadcast::forward(redis_url, channel, tx, forward_shutdown).await }
        });
    }

    let app = build_router(state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(role = "api", "listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .await?;

    Ok(())
}

/// The pipeline: ingester, consumers, recovery and re-enrichment. It publishes
/// broadcast events to Redis pub/sub and serves only a minimal `/healthz`.
async fn run_stream(config: AppConfig) -> anyhow::Result<()> {
    let db = connect_database(&config).await?;
    let http_client = build_http_client()?;

    // Only the ingest lane is built; the API lane is not.
    let live_registry = build_registry(&config, &http_client, Lane::Ingest);
    let chains = ChainRegistry::load(&db).await?;
    let live_state = AppState::new_stream(
        db.clone(),
        config.clone(),
        http_client.clone(),
        live_registry,
        chains.clone(),
    )
    .await?;

    let redis = RedisProducer::new(&config.redis_url).await?;
    redis.test_connection().await?;

    let shutdown = install_shutdown();

    tokio::spawn(run_refresh_loop(
        chains,
        db.clone(),
        Duration::from_secs(config.chain_registry_refresh_secs),
        shutdown.clone(),
    ));

    spawn_consumers(live_state.clone(), &config, shutdown.clone());

    let poll_programs = live_state.protocols.clone();
    spawn_reenrichment(live_state, &config, shutdown.clone());

    spawn_ingester(
        &config,
        redis.clone(),
        db.clone(),
        http_client.clone(),
        shutdown.clone(),
    )?;

    spawn_evm_watchers(&config, db.clone(), http_client.clone(), shutdown.clone()).await;

    spawn_reconcile_poll(
        &config,
        poll_programs,
        redis,
        db,
        http_client,
        shutdown.clone(),
    );

    let health = axum::Router::new().route(
        "/healthz",
        axum::routing::get(|| async { axum::http::StatusCode::OK }),
    );
    let addr = format!("0.0.0.0:{}", config.health_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(role = "stream", "healthz listening on {addr}");
    axum::serve(listener, health)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .await?;

    Ok(())
}

fn spawn_consumers(state: AppState, config: &AppConfig, shutdown: CancellationToken) {
    for worker_id in 1..=config.worker.worker_count {
        let worker_name = format!("worker-{worker_id}");
        let redis_url = config.redis_url.clone();
        let worker_state = state.clone();
        let worker_shutdown = shutdown.clone();

        spawn_supervised("redis-consumer", shutdown.clone(), move || {
            let redis_url = redis_url.clone();
            let worker_state = worker_state.clone();
            let worker_name = worker_name.clone();
            let worker_shutdown = worker_shutdown.clone();

            async move {
                RedisConsumer::new(&redis_url, worker_state, worker_name, worker_shutdown)?
                    .run()
                    .await
            }
        });
    }

    let redis_url = config.redis_url.clone();
    let recovery_state = state.clone();
    let recovery_shutdown = shutdown.clone();

    spawn_supervised("redis-recovery", shutdown.clone(), move || {
        let redis_url = redis_url.clone();
        let recovery_state = recovery_state.clone();
        let recovery_shutdown = recovery_shutdown.clone();

        async move {
            RedisConsumer::new(
                &redis_url,
                recovery_state,
                "recovery-worker".to_string(),
                recovery_shutdown,
            )?
            .run_recovery()
            .await
        }
    });
}

fn spawn_reenrichment(state: AppState, config: &AppConfig, shutdown: CancellationToken) {
    if !config.reenrich.enabled {
        return;
    }

    spawn_supervised("reenrichment", shutdown.clone(), move || {
        let state = state.clone();
        let shutdown = shutdown.clone();

        async move { reenrich::run(state, shutdown).await }
    });
}

fn spawn_ingester(
    config: &AppConfig,
    redis: RedisProducer,
    db: sqlx::PgPool,
    http_client: Client,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    if !config.ingest.enabled {
        tracing::warn!("INGEST_ENABLED=0, Solana ingestion is disabled");
        return Ok(());
    }

    tracing::info!(
        source = config.ingest.source.as_str(),
        "starting Solana ingestion"
    );

    let source = match config.ingest.source {
        IngestSourceKind::LogsSubscribe => IngestSource::LogsSubscribe(SolanaIngester::new(
            config.solana_ws_url.clone(),
            config.solana_token_bridge_program.clone(),
            redis,
            db,
            http_client,
            config.helius_url(Lane::Ingest).to_string(),
            config.ingest.clone(),
        )),
        IngestSourceKind::LaserStream => {
            #[cfg(feature = "laserstream")]
            {
                let api_key = std::env::var("HELIUS_API_KEY")
                    .context("INGEST_SOURCE=laserstream requires HELIUS_API_KEY")?;

                IngestSource::LaserStream(LaserStreamIngester::new(
                    config.ingest.laserstream_endpoint.clone(),
                    api_key,
                    config.solana_token_bridge_program.clone(),
                    redis,
                    db,
                    config.ingest.dedupe_ttl_secs,
                ))
            }

            #[cfg(not(feature = "laserstream"))]
            {
                anyhow::bail!(
                    "INGEST_SOURCE=laserstream requires a build with --features laserstream"
                )
            }
        }
    };

    let source = Arc::new(source);

    // Always on: starts with the process and runs whether or not anyone is connected to
    // /stream, so the database is a complete record. The source reconnects on its own;
    // the supervisor only has to cover a panic.
    spawn_supervised("solana-ingester", shutdown.clone(), move || {
        let source = source.clone();
        let shutdown = shutdown.clone();

        async move { source.run(shutdown).await }
    });

    Ok(())
}

/// One watcher per enabled `evm_watch_targets` row, each matched to its chain slug's
/// `EVM_RPC_<CHAIN>` (primary) and optional `EVM_RPC_<CHAIN>_FALLBACK`. A provider
/// quota is per key, so each endpoint gets its own limiter; a URL reused by two
/// chains shares one. A missing URL skips that chain with a warning rather than
/// failing the role.
async fn spawn_evm_watchers(
    config: &AppConfig,
    db: sqlx::PgPool,
    http_client: Client,
    shutdown: CancellationToken,
) {
    if !config.evm.enabled {
        return;
    }

    let targets = match evm_db::load_targets(&db).await {
        Ok(targets) => targets,
        Err(error) => {
            tracing::error!(
                ?error,
                "could not load EVM watch targets, EVM watching is off"
            );
            return;
        }
    };
    if targets.is_empty() {
        tracing::warn!(
            "EVM_WATCH_ENABLED is set but evm_watch_targets has no enabled, active chain"
        );
        return;
    }

    let mut limiters: HashMap<String, Arc<Limiter>> = HashMap::new();

    for target in targets {
        let Some(rpc) = config.evm.rpc.get(&target.slug) else {
            tracing::warn!(
                chain = %target.slug,
                env = %format!("EVM_RPC_{}", target.slug.to_ascii_uppercase()),
                "no RPC URL configured for watched EVM chain, not watching it"
            );
            continue;
        };

        let endpoints: Vec<WatcherEndpoint> = rpc
            .endpoints()
            .map(|endpoint| {
                let limiter = limiters
                    .entry(endpoint.url.clone())
                    .or_insert_with(|| {
                        Arc::new(RateLimiter::direct(Quota::per_second(
                            NonZeroU32::new(endpoint.rps).unwrap(),
                        )))
                    })
                    .clone();
                WatcherEndpoint {
                    url: endpoint.url.clone(),
                    limiter,
                    max_block_range: endpoint.max_block_range,
                }
            })
            .collect();

        let watcher = EvmWatcher::new(
            target,
            http_client.clone(),
            endpoints,
            db.clone(),
            config.evm.join_max_age_secs,
            config.evm.poll_secs,
        );
        let watcher_shutdown = shutdown.clone();
        spawn_supervised("evm-watcher", shutdown.clone(), move || {
            let watcher = watcher.clone();
            let shutdown = watcher_shutdown.clone();
            async move { watcher.run(shutdown).await }
        });
    }
}

/// The completeness safety net next to the WebSocket ingester: a periodic
/// `getSignaturesForAddress` poll per registered program, republishing whatever the
/// store does not have yet. It uses the reconcile Helius lane, so it cannot starve
/// ingestion or the API, and it is started only when ingestion is enabled.
fn spawn_reconcile_poll(
    config: &AppConfig,
    programs: Arc<ProtocolRegistry>,
    redis: RedisProducer,
    db: sqlx::PgPool,
    http_client: Client,
    shutdown: CancellationToken,
) {
    if !config.ingest.enabled {
        tracing::warn!("INGEST_ENABLED=0, reconciliation poll is disabled");
        return;
    }

    let poller = ReconcilePoller::new(
        programs,
        db,
        redis,
        http_client,
        config.helius_reconcile_url.clone(),
        config.reconcile_rps,
        config.ingest.clone(),
    );

    spawn_supervised("reconcile-poll", shutdown.clone(), move || {
        let poller = poller.clone();
        let shutdown = shutdown.clone();

        async move { poller.run(shutdown).await }
    });
}
