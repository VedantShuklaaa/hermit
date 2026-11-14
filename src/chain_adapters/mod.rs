use crate::error::AppError;
use async_trait::async_trait;

pub mod registry;
pub mod setup;
pub mod solana;
pub mod token_metadata;
pub mod known_tokens;

/// Per-chain lookups the pipeline cannot read from a Solana tx alone (token symbol and
/// decimals). Destination tx lookups live in `enrichment/`.
#[async_trait]
pub trait ChainAdapter: Send + Sync {
    fn name(&self) -> &'static str;

    async fn token_decimals(&self, _token_address: &str) -> Result<Option<u8>, AppError> {
        Ok(None)
    }

    async fn token_symbol(&self, _token_address: &str) -> Result<Option<String>, AppError> {
        Ok(None)
    }

    /// Both values from one lookup. The default asks the two single-value methods,
    /// so an adapter that only implements those keeps working; an adapter that can
    /// fetch both upstream in one call overrides this.
    async fn token_metadata(
        &self,
        token_address: &str,
    ) -> Result<(Option<u8>, Option<String>), AppError> {
        let decimals = self.token_decimals(token_address).await?;
        let symbol = self.token_symbol(token_address).await?;
        Ok((decimals, symbol))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegacyAdapter;

    #[async_trait]
    impl ChainAdapter for LegacyAdapter {
        fn name(&self) -> &'static str {
            "legacy"
        }

        async fn token_decimals(&self, _token_address: &str) -> Result<Option<u8>, AppError> {
            Ok(Some(6))
        }

        async fn token_symbol(&self, _token_address: &str) -> Result<Option<String>, AppError> {
            Ok(Some("LEG".into()))
        }
    }

    #[tokio::test]
    async fn the_default_lookup_asks_both_single_value_methods() {
        let (decimals, symbol) = LegacyAdapter.token_metadata("mint").await.unwrap();
        assert_eq!(decimals, Some(6));
        assert_eq!(symbol.as_deref(), Some("LEG"));
    }
}
