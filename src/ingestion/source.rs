//! The live Solana feed the rest of the pipeline reads from.
//!
//! Both variants publish the same [`crate::ingestion::types::CandidateTransaction`]
//! to the Redis stream, so decode, persistence and the read API never know which
//! source is running. `logsSubscribe` is the production default.

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::ingestion::solana::SolanaIngester;

#[cfg(feature = "laserstream")]
use crate::ingestion::laserstream::LaserStreamIngester;

pub enum IngestSource {
    LogsSubscribe(SolanaIngester),
    #[cfg(feature = "laserstream")]
    LaserStream(LaserStreamIngester),
}

impl IngestSource {
    /// Runs one source until `stop` is cancelled, or until it fails terminally.
    pub async fn run(&self, stop: CancellationToken) -> Result<()> {
        match self {
            Self::LogsSubscribe(ingester) => ingester.run(stop).await,
            #[cfg(feature = "laserstream")]
            Self::LaserStream(ingester) => ingester.run(stop).await,
        }
    }
}
