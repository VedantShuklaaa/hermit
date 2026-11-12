-- Enable EVM log watching for Base, Arbitrum and Avalanche.
--
-- Addresses are from the Wormhole contract-addresses reference and were confirmed
-- on-chain on 2026-11-13: each Token Bridge's wormhole() returns the Core Bridge
-- below, and each Core Bridge's chainId() returns the Wormhole chain id that the
-- chain registry already maps to the slug. evm_chain_id is the value eth_chainId
-- answered on the free public endpoint used for the check.
--
-- All three endpoints answer the `finalized` tag. finality_depth is the fallback
-- used only when a provider rejects it, sized above the largest finalized lag
-- measured in docs/scaling-log.md: Base 5,895 blocks, Arbitrum 4,338, Avalanche 5.
INSERT INTO evm_watch_targets (
    chain_id,
    core_bridge_address,
    token_bridge_address,
    finality_depth,
    finality_mode,
    evm_chain_id
)
SELECT c.id, v.core_bridge, v.token_bridge, v.finality_depth, 'finalized', v.evm_chain_id
FROM (VALUES
    ('base',      '0xbebdb6C8ddC678FfA9f8748f85C815C556Dd8ac6', '0x8d2de8d2f73F1F4cAB472AC9A881C9b123C79627', 6500, 8453),
    ('arbitrum',  '0xa5f208e072434bC67592E4C49C1B991BA79BCA46', '0x0b2402144Bb366A632D14B83F244D2e0e21bD39c', 5000, 42161),
    ('avalanche', '0x54a8e5f9c4CbA08F9943965859F6c34eAF03E26c', '0x0e082F06FF657D94310cB8cE8B0D9a04541d8052',   32, 43114)
) AS v (slug, core_bridge, token_bridge, finality_depth, evm_chain_id)
JOIN chains c ON c.slug = v.slug AND c.status = 'active';
