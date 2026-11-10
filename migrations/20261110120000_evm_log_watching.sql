-- EVM log watching for Wormhole Token Bridge transfers. One watcher per enabled
-- target, a persisted block cursor per chain, and one row per decoded log event.
-- Events are keyed by the Wormhole message identity so they can join to
-- bridge_transfers without asking WormholeScan for a tx hash.

CREATE TABLE evm_watch_targets (
    -- canonical chain; the Wormhole ID comes from protocol_chain_ids
    chain_id             SMALLINT PRIMARY KEY REFERENCES chains (id) ON DELETE CASCADE,
    core_bridge_address  TEXT NOT NULL CHECK (core_bridge_address ~ '^0x[0-9a-fA-F]{40}$'),
    token_bridge_address TEXT NOT NULL CHECK (token_bridge_address ~ '^0x[0-9a-fA-F]{40}$'),
    -- blocks below the head that are treated as final; only used when the node
    -- does not answer the `finalized` block tag
    finality_depth       BIGINT NOT NULL CHECK (finality_depth >= 0),
    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ethereum mainnet Wormhole deployments:
--   Core Bridge:  https://github.com/wormhole-foundation/wormhole/blob/main/ethereum/contracts/Implementation.sol
--   Token Bridge: https://github.com/wormhole-foundation/wormhole/blob/main/ethereum/contracts/bridge/token/TokenBridge.sol
-- Both addresses are also in the official SDK's consts.ts.
INSERT INTO evm_watch_targets (
    chain_id, core_bridge_address, token_bridge_address, finality_depth
)
SELECT id,
       '0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B',
       '0x3ee18B2214AFF97000D974cf647E7C347E8fa585',
       64
FROM chains
WHERE slug = 'ethereum'
  AND status = 'active';

CREATE TABLE evm_watch_cursors (
    chain_id     SMALLINT PRIMARY KEY REFERENCES chains (id) ON DELETE CASCADE,
    -- the highest block whose logs have been read and stored
    block_number BIGINT NOT NULL CHECK (block_number >= 0),
    -- that block's hash, the reorg check at the start of every poll
    block_hash   TEXT NOT NULL CHECK (block_hash ~ '^0x[0-9a-fA-F]{64}$'),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE evm_log_events (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- the chain the log was read from
    chain_id        SMALLINT NOT NULL REFERENCES chains (id) ON DELETE CASCADE,
    event_kind      TEXT NOT NULL
                    CHECK (event_kind IN ('log_message_published', 'transfer_redeemed')),
    -- the message identity carried by the event (the source side's chain)
    emitter_chain   INTEGER NOT NULL CHECK (emitter_chain BETWEEN 0 AND 65535),
    -- 32-byte hex, lowercase, no 0x, as Wormhole writes it
    emitter_address TEXT NOT NULL CHECK (emitter_address ~ '^[0-9a-f]{64}$'),
    sequence        NUMERIC(20, 0) NOT NULL CHECK (sequence >= 0),
    tx_hash         TEXT NOT NULL,
    log_index       INTEGER NOT NULL CHECK (log_index >= 0),
    block_number    BIGINT NOT NULL CHECK (block_number >= 0),
    block_hash      TEXT NOT NULL CHECK (block_hash ~ '^0x[0-9a-fA-F]{64}$'),
    block_timestamp TIMESTAMPTZ,
    -- set when the event-side join matched a transfer; NULL events are retried
    -- inside the EVM_JOIN_MAX_AGE_SECS window
    applied_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- one row per log: re-reading a range after a reorg is idempotent
    UNIQUE (chain_id, tx_hash, log_index)
);

CREATE INDEX idx_evm_log_events_message
    ON evm_log_events (emitter_chain, emitter_address, sequence, event_kind);
CREATE INDEX idx_evm_log_events_block
    ON evm_log_events (chain_id, block_number);
CREATE INDEX idx_evm_log_events_unapplied
    ON evm_log_events (created_at)
    WHERE applied_at IS NULL;
