-- Circle CCTP V2 message identity on Solana: the origin domain and the 32-byte nonce
-- Circle assigned, read from the attested message on a `receive_message` completion.
-- The pair is also `bridge_transfers.message_key` (`<domain>/<nonce>`), so the existing
-- unique `(protocol, message_key)` constraint keeps one row per message.

CREATE TABLE cctp_messages (
    transfer_id   BIGINT PRIMARY KEY REFERENCES bridge_transfers (id) ON DELETE CASCADE,
    source_domain INTEGER NOT NULL,
    -- Lowercase hex, 64 characters.
    nonce         TEXT NOT NULL,
    -- The attested message header version (1 for V2).
    version       INTEGER NOT NULL,
    UNIQUE (source_domain, nonce)
);

-- CCTP domains the indexer decodes. Checked against
-- https://developers.circle.com/cctp/concepts/supported-chains-and-domains
INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, evidence, verified_at)
SELECT 'cctp', m.id, c.id,
       'https://developers.circle.com/cctp/concepts/supported-chains-and-domains',
       NOW()
FROM (VALUES
    ('0', 'ethereum'),
    ('1', 'avalanche'),
    ('3', 'arbitrum'),
    ('5', 'solana'),
    ('6', 'base')
) AS m (id, slug)
JOIN chains c ON c.slug = m.slug;
