-- CCTP domain 19 is HyperEVM. Checked against
-- https://developers.circle.com/cctp/concepts/supported-chains-and-domains

INSERT INTO chains (slug, name, kind)
VALUES ('hyperevm', 'HyperEVM', 'evm')
ON CONFLICT (slug) DO NOTHING;

INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, evidence, verified_at)
SELECT 'cctp', '19', c.id,
       'https://developers.circle.com/cctp/concepts/supported-chains-and-domains',
       NOW()
FROM chains c
WHERE c.slug = 'hyperevm'
ON CONFLICT (protocol, protocol_chain_id) DO NOTHING;

-- Rows persisted before this mapping existed stored the origin chain as unresolved.
UPDATE bridge_transfers
SET source_chain_id = resolve_chain('cctp', '19')
WHERE protocol = 'cctp'
  AND source_ref_protocol = 'cctp'
  AND source_ref_id = '19'
  AND source_chain_id IS NULL;

UPDATE bridge_transfers
SET token_chain_id = resolve_chain('cctp', '19')
WHERE protocol = 'cctp'
  AND source_ref_protocol = 'cctp'
  AND source_ref_id = '19'
  AND token_chain_id IS NULL;
