#!/usr/bin/env bash
# Measure the largest eth_getLogs block range an EVM JSON-RPC endpoint accepts.
#
# The range is doubled from one block until the endpoint rejects it, then
# binary-searched to the exact boundary. Only the endpoint host is printed, never
# the URL, so an API key in the URL does not reach the terminal or a log.
#
# The default filter is the two Wormhole topic0 hashes with no address filter, so
# the result set stays small while the block range is what is measured. Pass
# --address with the verified Core/Token Bridge addresses to also include caps
# that depend on the matched logs.
#
# Usage:
#   EVM_RPC_ETHEREUM=<url> scripts/probe_evm_range.sh ethereum
#   scripts/probe_evm_range.sh base --url <url>
#   EVM_RPC_BASE_FALLBACK=<url> scripts/probe_evm_range.sh base --fallback
#   scripts/probe_evm_range.sh arbitrum --url <url> --address 0x...,0x...
#
# Environment:
#   EVM_RPC_<CHAIN>            primary endpoint when --url is omitted
#   EVM_RPC_<CHAIN>_FALLBACK   fallback endpoint when --fallback is given
#   PROBE_SLEEP_SECS           pause between requests, default 0.3
set -euo pipefail

LOG_MESSAGE_PUBLISHED_TOPIC="0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2"
TRANSFER_REDEEMED_TOPIC="0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"

usage() {
  cat <<'EOF'
Measure the largest eth_getLogs block range an EVM endpoint accepts.

Usage:
  EVM_RPC_<CHAIN>=<url> scripts/probe_evm_range.sh <chain>
  scripts/probe_evm_range.sh <chain> --url <url>
  EVM_RPC_<CHAIN>_FALLBACK=<url> scripts/probe_evm_range.sh <chain> --fallback

Options:
  --url <url>       endpoint; defaults to the environment variable
  --fallback        read EVM_RPC_<CHAIN>_FALLBACK instead of EVM_RPC_<CHAIN>
  --address <list>  comma-separated contract addresses (default: no filter)
  --topics <list>   comma-separated topic0 hashes (default: both Wormhole topics)
  --max <blocks>    highest range to try (default: 100000)
  --sleep <secs>    pause between requests (default: 0.3)
  --json            print one JSON object instead of key=value lines
  -h, --help        this text
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac

chain="${1:-}"
if [ -z "$chain" ]; then
  usage >&2
  exit 2
fi
case "$chain" in
  *[!a-z0-9_]*) echo "chain must be a lower-case registry slug" >&2; exit 2 ;;
esac
shift

url=""
use_fallback=0
addresses=""
topics="$LOG_MESSAGE_PUBLISHED_TOPIC,$TRANSFER_REDEEMED_TOPIC"
max_range=100000
sleep_secs="${PROBE_SLEEP_SECS:-0.3}"
json=0

while [ $# -gt 0 ]; do
  case "$1" in
    --url)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      url="$2"; shift 2 ;;
    --fallback)
      use_fallback=1; shift ;;
    --address)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      addresses="$2"; shift 2 ;;
    --topics)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      topics="$2"; shift 2 ;;
    --max)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      max_range="$2"; shift 2 ;;
    --sleep)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      sleep_secs="$2"; shift 2 ;;
    --json)
      json=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$max_range" in
  ''|*[!0-9]*) echo "--max must be a positive integer" >&2; exit 2 ;;
esac
[ "$max_range" -ge 1 ] || { echo "--max must be at least 1" >&2; exit 2; }
[ -n "$topics" ] || { echo "--topics must not be empty" >&2; exit 2; }

upper="$(printf '%s' "$chain" | tr '[:lower:]' '[:upper:]')"
role="primary"
if [ -z "$url" ]; then
  if [ "$use_fallback" -eq 1 ]; then
    var="EVM_RPC_${upper}_FALLBACK"
    role="fallback"
  else
    var="EVM_RPC_${upper}"
  fi
  url="${!var:-}"
  if [ -z "$url" ]; then
    echo "no endpoint: pass --url or set $var" >&2
    exit 2
  fi
elif [ "$use_fallback" -eq 1 ]; then
  role="fallback"
fi

# Host only: strip the scheme, then the path, query and fragment.
host="$(printf '%s' "$url" | sed -E 's#^[A-Za-z][A-Za-z0-9+.-]*://##; s#[/?#].*$##')"
[ -n "$host" ] || host="(unparseable)"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/probe_evm_range.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
body="$tmp/body.json"

rpc_call() { # method params_json out_file -> prints HTTP status
  local method="$1" params="$2" out="$3" payload status
  payload="$(jq -n --arg method "$method" --argjson params "$params" \
    '{jsonrpc:"2.0",id:1,method:$method,params:$params}')"
  status="$(curl -sS --max-time 30 -o "$out" -w '%{http_code}' \
    -H 'content-type: application/json' -d "$payload" "$url")" || status="000"
  printf '%s' "$status"
}

expected_chain_id() { # chain slug -> decimal chain id, empty if unknown
  case "$1" in
    ethereum) printf '1' ;;
    polygon) printf '137' ;;
    avalanche) printf '43114' ;;
    arbitrum) printf '42161' ;;
    optimism) printf '10' ;;
    base) printf '8453' ;;
    *) printf '' ;;
  esac
}

expected="$(expected_chain_id "$chain")"
if [ -n "$expected" ]; then
  status="$(rpc_call eth_chainId '[]' "$body")"
  if jq -e 'type == "object" and (.result | type) == "string"' "$body" >/dev/null 2>&1; then
    actual_hex="$(jq -r '.result' "$body")"
    case "$actual_hex" in
      0x*|0X*) ;;
      *) echo "eth_chainId returned an unexpected value: $actual_hex" >&2; exit 1 ;;
    esac
    actual="$((actual_hex))"
    if [ "$actual" -ne "$expected" ]; then
      echo "endpoint is chain id $actual, but $chain is $expected; check the URL" >&2
      exit 1
    fi
  else
    echo "warning: could not verify the endpoint chain id (HTTP $status); continuing" >&2
  fi
fi

is_range_message() {
  printf '%s' "$1" | grep -Eiq '(block range|eth_getlogs|fromBlock|toBlock|range)' &&
    printf '%s' "$1" | grep -Eiq '(too large|too many|exceed|limit|maximum|more than|up to|max|over [0-9,]+ blocks|not supported on (the )?free plan)'
}

is_transient_message() {
  printf '%s' "$1" | grep -Eiq '(rate limit|too many requests|429|timeout|temporarily unavailable|try again|busy)'
}

VERDICT=""
MESSAGE=""
LAST_STATUS=""

classify() { # body_file http_status
  local file="$1" status="$2" msg
  VERDICT="error"
  MESSAGE=""
  LAST_STATUS="$status"
  if jq -e 'type == "object" and (.error | type) == "object"' "$file" >/dev/null 2>&1; then
    msg="$(jq -r '.error.message // (.error | tostring)' "$file" 2>/dev/null \
      | tr '\n' ' ' | sed -e 's/  */ /g' -e 's/^ //' -e 's/ $//' | cut -c1-240)"
    if is_range_message "$msg"; then
      VERDICT="range"
    fi
    MESSAGE="$msg"
  elif jq -e 'type == "object" and (.result | type) == "array"' "$file" >/dev/null 2>&1; then
    VERDICT="ok"
  else
    MESSAGE="unexpected response (HTTP $status)"
  fi
}

head=0
effective_n=0

probe() { # requested range -> 0 on ok/range, 1 on a hard error
  local requested="$1" attempt status params from
  effective_n="$requested"
  if [ "$requested" -gt "$((head + 1))" ]; then
    effective_n=$((head + 1))
  fi
  from=$((head - effective_n + 1))
  params="$(jq -n \
    --arg from "0x$(printf '%x' "$from")" \
    --arg to "0x$(printf '%x' "$head")" \
    --arg addresses "$addresses" \
    --arg topics "$topics" \
    '[(
      {fromBlock: $from, toBlock: $to}
      + (if $addresses == "" then {} else {address: ($addresses | split(","))} end)
      + {topics: [($topics | split(","))]}
    )]')"

  attempt=1
  while [ "$attempt" -le 3 ]; do
    status="$(rpc_call eth_getLogs "$params" "$body")"
    classify "$body" "$status"
    # A 429 is never a range cap, whatever its message says.
    if [ "$status" = "429" ]; then
      VERDICT="error"
    fi
    if [ "$VERDICT" = "ok" ] || [ "$VERDICT" = "range" ]; then
      return 0
    fi
    transient=0
    if [ "$status" = "429" ]; then
      transient=1
    fi
    case "$status" in
      5??) transient=1 ;;
    esac
    if is_transient_message "$MESSAGE"; then
      transient=1
    fi
    if [ "$transient" -eq 1 ] && [ "$attempt" -lt 3 ]; then
      echo "transient error at $effective_n blocks (HTTP $status), retrying: $MESSAGE" >&2
      sleep 2
      attempt=$((attempt + 1))
      continue
    fi
    echo "probe failed at $effective_n blocks (HTTP $status): $MESSAGE" >&2
    return 1
  done
  return 1
}

head_hex=""
attempt=1
while [ "$attempt" -le 3 ]; do
  status="$(rpc_call eth_blockNumber '[]' "$body")"
  if jq -e 'type == "object" and (.result | type) == "string"' "$body" >/dev/null 2>&1; then
    head_hex="$(jq -r '.result' "$body")"
    break
  fi
  msg="$(jq -r '.error.message // empty' "$body" 2>/dev/null || true)"
  [ -n "$msg" ] || msg="unexpected response (HTTP $status)"
  transient=0
  if [ "$status" = "429" ]; then
    transient=1
  fi
  case "$status" in
    5??) transient=1 ;;
  esac
  if is_transient_message "$msg"; then
    transient=1
  fi
  if [ "$transient" -eq 1 ] && [ "$attempt" -lt 3 ]; then
    echo "transient error for eth_blockNumber (HTTP $status), retrying: $msg" >&2
    sleep 2
    attempt=$((attempt + 1))
    continue
  fi
  echo "eth_blockNumber failed (HTTP $status): $msg" >&2
  exit 1
done
if [ -z "$head_hex" ]; then
  echo "eth_blockNumber did not return a block number" >&2
  exit 1
fi
case "$head_hex" in
  0x*|0X*) ;;
  *) echo "eth_blockNumber returned an unexpected value: $head_hex" >&2; exit 1 ;;
esac
head=$((head_hex))

n=1
last_ok=0
first_bad=0
covered_whole=0
bad_message=""

while :; do
  [ "$n" -le "$max_range" ] || break
  probe "$n" || exit 1
  if [ "$VERDICT" = "ok" ]; then
    last_ok="$effective_n"
    if [ "$effective_n" -eq "$((head + 1))" ]; then
      covered_whole=1
      break
    fi
    sleep "$sleep_secs"
    if [ "$n" -lt "$max_range" ] && [ "$((n * 2))" -gt "$max_range" ]; then
      n="$max_range"
    else
      n=$((n * 2))
    fi
  else
    first_bad="$effective_n"
    bad_message="$MESSAGE"
    break
  fi
done

if [ "$covered_whole" -eq 0 ] && [ "$first_bad" -eq 1 ]; then
  [ -n "$bad_message" ] || bad_message="(empty response body)"
  echo "endpoint rejects a 1-block eth_getLogs (HTTP $LAST_STATUS): $bad_message" >&2
  exit 1
fi

if [ "$first_bad" -gt 0 ]; then
  lo="$last_ok"
  hi="$first_bad"
  while [ "$((hi - lo))" -gt 1 ]; do
    sleep "$sleep_secs"
    mid=$(((lo + hi) / 2))
    probe "$mid" || exit 1
    if [ "$VERDICT" = "ok" ]; then
      lo="$mid"
    else
      hi="$mid"
      bad_message="$MESSAGE"
    fi
  done
  max_accepted="$lo"
  first_rejected="$((lo + 1))"
else
  max_accepted="$last_ok"
  first_rejected=""
fi

no_rejection_up_to=""
if [ "$first_bad" -eq 0 ] && [ "$covered_whole" -eq 0 ]; then
  no_rejection_up_to="$last_ok"
fi

if [ "$json" -eq 1 ]; then
  jq -n \
    --arg chain "$chain" \
    --arg role "$role" \
    --arg endpoint "$host" \
    --arg max "$max_accepted" \
    --arg first_rejected "$first_rejected" \
    --arg message "$bad_message" \
    --arg whole "$covered_whole" \
    --arg no_rejection "$no_rejection_up_to" \
    '{chain: $chain,
      role: $role,
      endpoint: $endpoint,
      max_block_range: ($max | tonumber),
      first_rejected: (if $first_rejected == "" then null else ($first_rejected | tonumber) end),
      whole_chain: ($whole == "1"),
      no_rejection_up_to: (if $no_rejection == "" then null else ($no_rejection | tonumber) end),
      message: (if $message == "" then null else $message end)}'
else
  printf 'chain=%s role=%s endpoint=%s max_block_range=%s' \
    "$chain" "$role" "$host" "$max_accepted"
  if [ -n "$first_rejected" ]; then
    printf ' first_rejected=%s' "$first_rejected"
  fi
  if [ "$covered_whole" -eq 1 ]; then
    printf ' whole_chain=yes'
  fi
  if [ -n "$no_rejection_up_to" ]; then
    printf ' no_rejection_up_to=%s' "$no_rejection_up_to"
  fi
  printf '\n'
  if [ -n "$bad_message" ]; then
    printf 'message=%s\n' "$bad_message"
  fi
fi
