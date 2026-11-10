-- The EVM chain id an endpoint must answer with, checked once at watcher startup.
-- It catches an RPC URL pasted for the wrong chain, which a fallback makes more
-- likely. NULL skips the check for a target whose id is not recorded yet.

ALTER TABLE evm_watch_targets
    ADD COLUMN evm_chain_id BIGINT
        CHECK (evm_chain_id IS NULL OR evm_chain_id >= 0);

COMMENT ON COLUMN evm_watch_targets.evm_chain_id IS
    'Expected eth_chainId result; NULL means the watcher does not verify it';

UPDATE evm_watch_targets t
SET evm_chain_id = 1
FROM chains c
WHERE c.id = t.chain_id
  AND c.slug = 'ethereum';
