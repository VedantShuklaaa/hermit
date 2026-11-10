#!/usr/bin/env bash
# Capture real Ethereum Wormhole logs as test fixtures.
#
#   EVM_RPC_ETHEREUM=<rpc url with key> scripts/capture_evm_fixtures.sh <name> <from_block> <to_block>
#
# Writes tests/fixtures/evm_ethereum_<name>.json as the raw eth_getLogs response.
# The RPC URL holds your provider key and is never written to disk. Fixtures from a
# paid plan can span a wide range; a free-tier key caps eth_getLogs at 2,000 blocks.
#
# The two events are the Core Bridge LogMessagePublished and the Token Bridge
# TransferRedeemed, filtered to the official mainnet contract addresses.
set -euo pipefail

name="${1:?fixture name, e.g. log_message_published}"
from_block="${2:?from block}"
to_block="${3:?to block}"
: "${EVM_RPC_ETHEREUM:?set EVM_RPC_ETHEREUM (do not commit it)}"

dir="$(cd "$(dirname "$0")/.." && pwd)/tests/fixtures"
mkdir -p "$dir"

payload="$(jq -n \
  --arg from "0x$(printf '%x' "$from_block")" \
  --arg to "0x$(printf '%x' "$to_block")" \
  '{
    jsonrpc: "2.0",
    id: 1,
    method: "eth_getLogs",
    params: [{
      fromBlock: $from,
      toBlock: $to,
      address: [
        "0x98f3c9e6E3fAce36bAAd05FE09d375Ef1464288B",
        "0x3ee18B2214AFF97000D974cf647E7C347E8fa585"
      ],
      topics: [[
        "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2",
        "0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"
      ]]
    }]
  }')"

curl -sS --fail "$EVM_RPC_ETHEREUM" -H 'content-type: application/json' \
  -d "$payload" \
  | jq -e 'if .error then error("provider error: \(.error)") else . end' \
  > "$dir/evm_ethereum_${name}.json"

echo "saved tests/fixtures/evm_ethereum_${name}.json"
jq -c '(.result // []) | length' "$dir/evm_ethereum_${name}.json" \
  | xargs -I{} echo "{} logs captured"
