#!/usr/bin/env bash
# Capture a real Helius tx + WormholeScan operation as test fixtures.
#
#   HELIUS_URL=<rpc url with key> scripts/capture_fixtures.sh <name> <solana_signature> [account_pubkey]
#
# Writes tests/fixtures/helius_<name>.json and wormhole_<name>.json.
# With an account pubkey, also writes account_<name>_state.json (getAccountInfo), for
# adapters that preload an account through required_accounts.
# The Helius URL (which holds your key) is never written to disk.
# For a tx that isn't a bridge transfer, set SKIP_WORMHOLE=1.
#
# Name fixtures by what they are, e.g. completed_outbound, pending,
# inbound, payload3, non_bridge. Fixtures named completed_* are expected to
# carry a destination tx hash (checked in tests/fixtures_decode.rs).
set -euo pipefail

name="${1:?fixture name, e.g. completed_outbound}"
sig="${2:?solana signature}"
account="${3:-}"
: "${HELIUS_URL:?set HELIUS_URL (do not commit it)}"
WORMHOLE_URL="${WORMHOLE_URL:-https://api.wormholescan.io}"
MAX_TX_VERSION="${MAX_TX_VERSION:-0}"

dir="$(cd "$(dirname "$0")/.." && pwd)/tests/fixtures"
mkdir -p "$dir"

curl -sS --fail "$HELIUS_URL" -H 'content-type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":[\"$sig\",{\"encoding\":\"jsonParsed\",\"maxSupportedTransactionVersion\":$MAX_TX_VERSION,\"commitment\":\"confirmed\"}]}" \
  | jq -e '.result' > "$dir/helius_${name}.json"
echo "saved tests/fixtures/helius_${name}.json"

if [ -n "$account" ]; then
  curl -sS --fail "$HELIUS_URL" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$account\",{\"encoding\":\"base64\",\"commitment\":\"confirmed\"}]}" \
    | jq -e '.result' > "$dir/account_${name}_state.json"
  echo "saved tests/fixtures/account_${name}_state.json"
fi

if [ "${SKIP_WORMHOLE:-0}" = "1" ]; then
  exit 0
fi

curl -sS --fail "$WORMHOLE_URL/api/v1/operations?txHash=$sig" \
  | jq -e '.operations[0]' > "$dir/wormhole_${name}.json"
echo "saved tests/fixtures/wormhole_${name}.json"

echo "operation top-level keys:"
jq -c 'keys' "$dir/wormhole_${name}.json"
echo "destination tx candidates (null = path absent):"
jq -c '{targetChain: .targetChain?.transaction?.txHash, globalTx: .globalTx?.destinationTx?.txHash}' \
  "$dir/wormhole_${name}.json"