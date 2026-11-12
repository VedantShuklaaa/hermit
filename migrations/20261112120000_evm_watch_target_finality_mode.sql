-- Per-chain finality. `finalized` keeps the current behaviour: read the node's
-- `finalized` tag and fall back to `head - finality_depth` only when the provider
-- rejects it. `depth` always reads `head - finality_depth`; load_targets rejects a
-- `depth` row whose finality_depth is 0 at startup.
--
-- Existing rows keep the old behaviour, so Ethereum stays on `finalized`.

ALTER TABLE evm_watch_targets
    ADD COLUMN finality_mode TEXT NOT NULL DEFAULT 'finalized'
        CHECK (finality_mode IN ('finalized', 'depth'));

COMMENT ON COLUMN evm_watch_targets.finality_mode IS
    'finalized = use the finalized tag with a depth fallback; depth = always head - finality_depth';
