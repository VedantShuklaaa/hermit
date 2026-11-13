-- CCTP domains 7, 15 and 26 are Polygon PoS, Monad and Arc. Checked against
-- https://developers.circle.com/cctp/concepts/supported-chains-and-domains
-- Domain 27 (Stellar) is not mapped: its addresses are StrKey, not EVM words.

INSERT INTO chains (slug, name, kind)
VALUES
    ('monad', 'Monad', 'evm'),
    ('arc', 'Arc', 'evm')
ON CONFLICT (slug) DO NOTHING;

INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, evidence, verified_at)
SELECT 'cctp', d.protocol_chain_id, c.id,
       'https://developers.circle.com/cctp/concepts/supported-chains-and-domains',
       NOW()
FROM (VALUES ('7', 'polygon'), ('15', 'monad'), ('26', 'arc'))
    AS d (protocol_chain_id, slug)
JOIN chains c ON c.slug = d.slug
ON CONFLICT (protocol, protocol_chain_id) DO NOTHING;

-- Rows persisted before these mappings existed stored the origin chain as unresolved.
UPDATE bridge_transfers
SET source_chain_id = resolve_chain('cctp', source_ref_id)
WHERE protocol = 'cctp'
  AND source_ref_protocol = 'cctp'
  AND source_ref_id IN ('7', '15', '26')
  AND source_chain_id IS NULL;

UPDATE bridge_transfers
SET token_chain_id = resolve_chain('cctp', source_ref_id)
WHERE protocol = 'cctp'
  AND source_ref_protocol = 'cctp'
  AND source_ref_id IN ('7', '15', '26')
  AND token_chain_id IS NULL;
