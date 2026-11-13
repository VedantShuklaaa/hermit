-- The protocol sub-program that decoded a transfer. Every row written before this
-- migration was decoded by the Wormhole Token Bridge, so it is backfilled to
-- `token_bridge`. `/api/v1` serves only these rows; `/api/v2` exposes the column
-- additively.
ALTER TABLE bridge_transfers ADD COLUMN program TEXT;

UPDATE bridge_transfers
SET program = 'token_bridge'
WHERE program IS NULL;

ALTER TABLE bridge_transfers ALTER COLUMN program SET NOT NULL;
ALTER TABLE bridge_transfers ALTER COLUMN program SET DEFAULT 'token_bridge';
