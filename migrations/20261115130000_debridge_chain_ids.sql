-- deBridge DLN internal chain IDs, checked against
-- https://docs.debridge.com/home/architecture/supported-chains
--
-- Only chains already in the registry are mapped. HyperEVM and Monad use DLN's
-- internal IDs (100000022 and 100000030), not their EVM chain IDs. Gnosis is not
-- on deBridge's supported list; Linea and the other listed chains are not in this
-- registry yet.

INSERT INTO protocol_chain_ids (protocol, protocol_chain_id, chain_id, evidence, verified_at)
SELECT 'debridge', d.protocol_chain_id, c.id,
       'https://docs.debridge.com/home/architecture/supported-chains',
       NOW()
FROM (VALUES
    ('7565164',   'solana'),
    ('1',         'ethereum'),
    ('56',        'bsc'),
    ('137',       'polygon'),
    ('43114',     'avalanche'),
    ('42161',     'arbitrum'),
    ('10',        'optimism'),
    ('8453',      'base'),
    ('5042',      'arc'),
    ('100000022', 'hyperevm'),
    ('100000030', 'monad')
) AS d (protocol_chain_id, slug)
JOIN chains c ON c.slug = d.slug
ON CONFLICT (protocol, protocol_chain_id) DO NOTHING;
