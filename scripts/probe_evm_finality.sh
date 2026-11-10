#!/usr/bin/env bash
# Report the block number and timestamp behind the latest, safe and finalized
# tags, and how far each tag lags the head. A tag the endpoint does not support
# is reported as unsupported; that is a result, not a failure.
#
# Only the endpoint host is printed, never the URL, so an API key in the URL does
# not reach the terminal or a log.
#
# Usage:
#   EVM_RPC_ETHEREUM=<url> scripts/probe_evm_finality.sh ethereum
#   scripts/probe_evm_finality.sh base --url <url>
#   EVM_RPC_BASE_FALLBACK=<url> scripts/probe_evm_finality.sh base --fallback
#
# Environment:
#   EVM_RPC_<CHAIN>            primary endpoint when --url is omitted
#   EVM_RPC_<CHAIN>_FALLBACK   fallback endpoint when --fallback is given
#   PROBE_SLEEP_SECS           pause between requests, default 0.3
set -euo pipefail

usage() {
  cat <<'EOF'
Report the lag of the latest, safe and finalized block tags.

Usage:
  EVM_RPC_<CHAIN>=<url> scripts/probe_evm_finality.sh <chain>
  scripts/probe_evm_finality.sh <chain> --url <url>
  EVM_RPC_<CHAIN>_FALLBACK=<url> scripts/probe_evm_finality.sh <chain> --fallback

Options:
  --url <url>     endpoint; defaults to the environment variable
  --fallback      read EVM_RPC_<CHAIN>_FALLBACK instead of EVM_RPC_<CHAIN>
  --sleep <secs>  pause between requests (default: 0.3)
  --json          print one JSON object instead of key=value lines
  -h, --help      this text
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
sleep_secs="${PROBE_SLEEP_SECS:-0.3}"
json=0

while [ $# -gt 0 ]; do
  case "$1" in
    --url)
      [ $# -ge 2 ] || { usage >&2; exit 2; }
      url="$2"; shift 2 ;;
    --fallback)
      use_fallback=1; shift ;;
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

host="$(printf '%s' "$url" | sed -E 's#^[A-Za-z][A-Za-z0-9+.-]*://##; s#[/?#].*$##')"
[ -n "$host" ] || host="(unparseable)"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/probe_evm_finality.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

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
  out="$tmp/chainid.json"
  status="$(rpc_call eth_chainId '[]' "$out")"
  if jq -e 'type == "object" and (.result | type) == "string"' "$out" >/dev/null 2>&1; then
    actual_hex="$(jq -r '.result' "$out")"
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

is_transient_message() {
  printf '%s' "$1" | grep -Eiq '(rate limit|too many requests|429|timeout|temporarily unavailable|try again|busy)'
}

is_unsupported_tag() {
  printf '%s' "$1" | grep -Eiq '(finalized|safe|invalid block|unknown block|not supported|unsupported|header not found|block tag)'
}

FETCH_N=""
FETCH_TS=""
TAG_ERROR=""
TAG_LABEL="error"

fetch_tag() { # tag out_file; sets FETCH_N/FETCH_TS, or TAG_ERROR/TAG_LABEL
  local tag="$1" out="$2" status nhex thex attempt transient
  attempt=1
  while :; do
    status="$(rpc_call eth_getBlockByNumber "$(jq -n --arg t "$tag" '[$t, false]')" "$out")"
    FETCH_N=""
    FETCH_TS=""
    TAG_ERROR=""
    TAG_LABEL="error"
    if jq -e 'type == "object" and (.result | type) == "object"
              and (.result.number | type) == "string"
              and (.result.timestamp | type) == "string"' "$out" >/dev/null 2>&1; then
      nhex="$(jq -r '.result.number' "$out")"
      thex="$(jq -r '.result.timestamp' "$out")"
      case "$nhex" in
        0x*|0X*) ;;
        *) TAG_ERROR="unexpected block number: $nhex"; return 1 ;;
      esac
      case "$thex" in
        0x*|0X*) ;;
        *) TAG_ERROR="unexpected block timestamp: $thex"; return 1 ;;
      esac
      FETCH_N="$((nhex))"
      FETCH_TS="$((thex))"
      return 0
    fi
    TAG_ERROR="$(jq -r '.error.message // empty' "$out" 2>/dev/null \
      | tr '\n' ' ' | sed -e 's/  */ /g' -e 's/^ //' -e 's/ $//' | cut -c1-240)"
    [ -n "$TAG_ERROR" ] || TAG_ERROR="unexpected response (HTTP $status)"
    transient=0
    if [ "$status" = "429" ]; then
      transient=1
    fi
    case "$status" in
      5??) transient=1 ;;
    esac
    if is_transient_message "$TAG_ERROR"; then
      transient=1
    fi
    if [ "$transient" -eq 1 ] && [ "$attempt" -lt 3 ]; then
      echo "transient error for $tag (HTTP $status), retrying: $TAG_ERROR" >&2
      sleep 2
      attempt=$((attempt + 1))
      continue
    fi
    if is_unsupported_tag "$TAG_ERROR"; then
      TAG_LABEL="unsupported"
    fi
    return 1
  done
}

report() { # text; stderr when --json keeps stdout parseable
  if [ "$json" -eq 1 ]; then
    printf '%s\n' "$1" >&2
  else
    printf '%s\n' "$1"
  fi
}

latest_n=""
latest_ts=""
safe_n=""
safe_ts=""
fin_n=""
fin_ts=""
fin_supported="false"
safe_supported="false"
fin_label="unsupported"
safe_label="unsupported"
fin_error=""
safe_error=""

for tag in latest safe finalized; do
  out="$tmp/$tag.json"
  if fetch_tag "$tag" "$out"; then
    case "$tag" in
      latest)
        latest_n="$FETCH_N"; latest_ts="$FETCH_TS" ;;
      safe)
        safe_n="$FETCH_N"; safe_ts="$FETCH_TS"; safe_supported="true" ;;
      finalized)
        fin_n="$FETCH_N"; fin_ts="$FETCH_TS"; fin_supported="true" ;;
    esac
    report "$(printf 'tag=%-9s number=%-12s timestamp=%s' "$tag" "$FETCH_N" "$FETCH_TS")"
  else
    case "$tag" in
      safe)
        safe_label="$TAG_LABEL"; safe_error="$TAG_ERROR" ;;
      finalized)
        fin_label="$TAG_LABEL"; fin_error="$TAG_ERROR" ;;
    esac
    report "$(printf 'tag=%-9s %s: %s' "$tag" "$TAG_LABEL" "$TAG_ERROR")"
  fi
  sleep "$sleep_secs"
done

if [ -z "$latest_n" ]; then
  echo "the endpoint did not answer the latest tag; cannot compute lag" >&2
  exit 1
fi

fin_blocks=""
fin_secs=""
safe_blocks=""
safe_secs=""
if [ "$fin_supported" = "true" ]; then
  fin_blocks=$((latest_n - fin_n))
  fin_secs=$((latest_ts - fin_ts))
fi
if [ "$safe_supported" = "true" ]; then
  safe_blocks=$((latest_n - safe_n))
  safe_secs=$((latest_ts - safe_ts))
fi

if [ "$json" -eq 1 ]; then
  jq -n \
    --arg chain "$chain" \
    --arg role "$role" \
    --arg endpoint "$host" \
    --arg latest_n "$latest_n" --arg latest_ts "$latest_ts" \
    --arg safe_n "$safe_n" --arg safe_ts "$safe_ts" \
    --arg fin_n "$fin_n" --arg fin_ts "$fin_ts" \
    --arg fin_supported "$fin_supported" \
    --arg safe_supported "$safe_supported" \
    --arg fin_error "$fin_error" \
    --arg safe_error "$safe_error" \
    '{chain: $chain,
      role: $role,
      endpoint: $endpoint,
      latest: {number: ($latest_n | tonumber), timestamp: ($latest_ts | tonumber)},
      safe: (if $safe_supported == "true"
             then {number: ($safe_n | tonumber), timestamp: ($safe_ts | tonumber),
                   lag_blocks: (($latest_n | tonumber) - ($safe_n | tonumber)),
                   lag_seconds: (($latest_ts | tonumber) - ($safe_ts | tonumber))}
             else null end),
      safe_error: (if $safe_error == "" then null else $safe_error end),
      finalized: (if $fin_supported == "true"
                  then {number: ($fin_n | tonumber), timestamp: ($fin_ts | tonumber),
                        lag_blocks: (($latest_n | tonumber) - ($fin_n | tonumber)),
                        lag_seconds: (($latest_ts | tonumber) - ($fin_ts | tonumber))}
                  else null end),
      finalized_error: (if $fin_error == "" then null else $fin_error end)}'
else
  printf 'chain=%s role=%s endpoint=%s\n' "$chain" "$role" "$host"
  if [ -n "$fin_blocks" ]; then
    printf 'finalized_lag_blocks=%s finalized_lag_seconds=%s\n' "$fin_blocks" "$fin_secs"
  else
    printf 'finalized_lag=%s\n' "$fin_label"
  fi
  if [ -n "$safe_blocks" ]; then
    printf 'safe_lag_blocks=%s safe_lag_seconds=%s\n' "$safe_blocks" "$safe_secs"
  else
    printf 'safe_lag=%s\n' "$safe_label"
  fi
  if [ -n "$fin_blocks" ]; then
    printf 'block_seconds_latest_finalized=%s\n' \
      "$(awk -v dn="$((latest_n - fin_n))" -v dt="$((latest_ts - fin_ts))" \
        'BEGIN { if (dn > 0) printf "%.3f", dt / dn; else printf "n/a" }')"
  fi
  if [ -n "$safe_blocks" ]; then
    printf 'block_seconds_latest_safe=%s\n' \
      "$(awk -v dn="$((latest_n - safe_n))" -v dt="$((latest_ts - safe_ts))" \
        'BEGIN { if (dn > 0) printf "%.3f", dt / dn; else printf "n/a" }')"
  fi
fi
