//! Tokens whose symbol and decimals are fixed and well known, keyed by canonical chain
//! slug and token address. Used for protocols whose amount is the real on-chain amount
//! (CCTP, deBridge, Mayan source orders). A token missing from the table is never
//! formatted: guessing decimals would show a wrong number.

/// (chain slug, token address, symbol, decimals). EVM addresses are lowercase.
const KNOWN_TOKENS: &[(&str, &str, &str, u8)] = &[
    (
        "solana",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "USDC",
        6,
    ),
    (
        "solana",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "USDT",
        6,
    ),
    (
        "ethereum",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "USDC",
        6,
    ),
    (
        "ethereum",
        "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "USDT",
        6,
    ),
    (
        "base",
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "USDC",
        6,
    ),
    (
        "arbitrum",
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "USDC",
        6,
    ),
    (
        "arbitrum",
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        "USDT",
        6,
    ),
    (
        "polygon",
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        "USDC",
        6,
    ),
    (
        "polygon",
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
        "USDT",
        6,
    ),
    (
        "optimism",
        "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
        "USDC",
        6,
    ),
    (
        "optimism",
        "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
        "USDT",
        6,
    ),
    (
        "avalanche",
        "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        "USDC",
        6,
    ),
    // BNB Smart Chain stablecoins are 18 decimals, unlike every other chain above.
    (
        "bsc",
        "0x55d398326f99059ff775485246999027b3197955",
        "USDT",
        18,
    ),
    (
        "bsc",
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        "USDC",
        18,
    ),
];

/// Symbol and decimals of a known token. EVM addresses match case-insensitively;
/// Solana addresses are base58 and match exactly.
pub fn known_token(slug: &str, address: &str) -> Option<(&'static str, u8)> {
    KNOWN_TOKENS
        .iter()
        .find(|(s, a, _, _)| {
            *s == slug
                && if slug == "solana" {
                    *a == address
                } else {
                    a.eq_ignore_ascii_case(address)
                }
        })
        .map(|(_, _, sym, dec)| (*sym, *dec))
}

/// Formats a raw on-chain amount with the token's real decimals, trimming trailing
/// zeros. Unlike `vaa::decode::format_amount` this does not cap decimals at 8, because
/// that cap only holds for Wormhole's normalised VAA amounts.
pub fn format_units(raw: u128, decimals: u8) -> Option<String> {
    if decimals > 38 {
        return None;
    }
    let divisor = 10u128.pow(u32::from(decimals));
    let whole = raw / divisor;
    let frac = raw % divisor;
    if decimals == 0 || frac == 0 {
        return Some(whole.to_string());
    }
    let frac = format!("{frac:0width$}", width = usize::from(decimals));
    Some(format!("{whole}.{}", frac.trim_end_matches('0')))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evm_addresses_match_any_case() {
        assert_eq!(
            known_token("base", "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            Some(("USDC", 6))
        );
    }

    #[test]
    fn solana_addresses_match_exactly() {
        let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        assert_eq!(known_token("solana", usdc), Some(("USDC", 6)));
        assert_eq!(known_token("solana", &usdc.to_lowercase()), None);
    }

    #[test]
    fn unknown_token_or_wrong_chain_is_none() {
        assert_eq!(known_token("base", "0xdeadbeef"), None);
        assert_eq!(
            known_token("ethereum", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            None
        );
    }

    #[test]
    fn formats_with_real_decimals() {
        assert_eq!(format_units(1_000_000, 6).as_deref(), Some("1"));
        assert_eq!(format_units(1_234_500, 6).as_deref(), Some("1.2345"));
        assert_eq!(format_units(5, 6).as_deref(), Some("0.000005"));
        assert_eq!(format_units(0, 6).as_deref(), Some("0"));
        assert_eq!(format_units(7, 0).as_deref(), Some("7"));
    }

    #[test]
    fn does_not_cap_at_eight_decimals() {
        assert_eq!(
            format_units(1_500_000_000_000_000_000, 18).as_deref(),
            Some("1.5")
        );
        assert_eq!(format_units(1, 18).as_deref(), Some("0.000000000000000001"));
        assert_eq!(format_units(1, 39), None);
    }
}
