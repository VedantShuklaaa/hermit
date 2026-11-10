# Wormholescan dependency audit

Read-only inventory of every runtime dependency on Wormholescan
(`WORMHOLE_URL`, default `https://api.wormholescan.io`). Based on the tree at
`main` (2026-11-10). No source was changed.

The client deserialises responses as `serde_json::Value`; there are no typed
response structs. The "fields read" below are the exact JSON pointers the code
dereferences.

## Endpoints and configuration

| Item | Where |
| --- | --- |
| `WORMHOLE_URL` config field | `src/config.rs:161` |
| Default URL | `src/config.rs:290` (`optional("WORMHOLE_URL", "https://api.wormholescan.io")`) |
| `WORMHOLE_RPS` (default 20) | `src/config.rs:175-176`, `src/config.rs:298` |
| Example env | `.env.example:12` |
| Client module | `src/clients/wormhole.rs` |
| Provider | `src/enrichment/wormholescan.rs:16-36` |
| Limiter construction | `src/state.rs:98-117` (API/all), `src/state.rs:165-173` (stream) |

Three endpoints are called:

| Function | Endpoint | Response read |
| --- | --- | --- |
| `get_operation_by_tx_hash` (`src/clients/wormhole.rs:6-29`) | `GET /api/v1/operations?txHash={signature}` (`:11`) | `operations[]`, returns `operations[0]` (`:23-26`); empty/absent → `TransactionNotFound` (`:27`) |
| `get_destination_tx` (`src/clients/wormhole.rs:31-55`) | `GET /api/v1/global-tx/{emitter_chain}/{emitter_address}/{sequence}` (`:36-39`) | `destinationTx.txHash` (`:99-107`); 404 → `Ok(None)` (`:42-44`) |
| `get_vaa_source_tx` (`src/clients/wormhole.rs:60-84`) | `GET /api/v1/vaas/{emitter_chain}/{emitter_address}/{sequence}` (`:65-68`) | `data.txHash` (`:88-96`); 404 → `Ok(None)` (`:71-73`) |

## Call sites

### 1. Cold analysis: `/api/v2` and `/api/v1` analyse, and every stream consumer

`src/services/analyzer.rs:64-89`. `analyse_tx` always launches
`get_operation_by_tx_hash` in parallel with the Helius `getTransaction`, for
every Solana analysis, even when an adapter decodes the transfer from Solana
data. This is the unconditional prefetch (`:73-85`). It is reached from:

* `GET /api/v1/transactions/analyse` and `GET /api/v2/.../analyse`:
  `src/handler/analyse.rs:36-43`, `:69-77` → `read_through::analyse_cached[_v2]`
  (`src/services/read_through.rs:439-525`, `:530-...`) → `analyse_tx`.
* Every Redis consumer message: `src/redis/consumer.rs:163`
  (`analyse_with_retry`).
* Background stale refresh: `src/services/read_through.rs:364-392`
  (`refresh` → `analyse_tx_prepaid`).
* Reconcile precision sample and replay: see sites 4 and 5.

Response fields used:

* Chain-first path (an adapter matched): the response is only handed to the
  enrichment provider as `Prefetch::Done` (`src/services/analyzer.rs:126-135`).
  The provider uses `vaa.raw` to check that Wormholescan's VAA matches the
  on-chain message, then raises the status (`src/services/detect.rs:153-191`).
* Fallback path (no adapter matched): `src/services/analyzer.rs:160-269` reads
  `vaa.raw` (`:164-170`), `sequence` / `emitterChain` /
  `emitterAddress.hex` (via `normaliser::wormhole::normalise`, `:162`),
  `content.standarizedProperties.{toChain, tokenChain, tokenAddress,
  tokenSymbol, toAddress, amount}` (`:186-226`), and
  `targetChain.transaction.txHash` / `globalTx.destinationTx.txHash` /
  `destinationTx.txHash` (`:228-232`, helper `detect.rs:230-246`).

Failure behaviour:

* Chain-first: the prefetch error is not returned. Enrichment sees
  `Prefetch::Done(Err(..))` and reports `Unavailable`; the local row is still
  persisted (`src/enrichment/wormholescan.rs:128-139`). No consumer retry is
  triggered; the row is retried later by the re-enrichment worker.
* Fallback: the error is returned (`src/services/analyzer.rs:268`). The
  consumer retries `VaaNotAvailable` every 5 s for up to 90 s
  (`src/redis/consumer.rs:151-173`), retries transient errors up to 5
  attempts (`:177-191`), then leaves the message pending; after
  `WORKER_MAX_DELIVERIES` (default 10, `src/config.rs:327`) it is
  dead-lettered (`src/redis/consumer.rs:493-511`, `:597-601`).

`/operations` returns only `operations[0]`. If one transaction contains
several messages, only the first operation can ever be applied; other detected
transfers see a non-matching VAA and report `Mismatch`
(`src/services/detect.rs:169-175`).

### 2. Enrichment provider (re-enrichment worker, and callers without a prefetch)

`src/enrichment/wormholescan.rs:86-141`.

* `enrich` with `Prefetch::None` → `/operations?txHash=<observed signature>`
  (`:91-98`).
* After a matched VAA, while status is `Pending` →
  `/global-tx/{chain}/{emitter}/{sequence}` (`:107-113`, `:145-153`).
* Inbound rows missing an origin tx → `/vaas/{chain}/{emitter}/{sequence}`
  (`:43-68`, `:116`, `:122-125`).
* Response fields: `vaa.raw` (decoded and matched in `apply_wormholescan`,
  `src/services/detect.rs:158-175`), destination tx paths (above), and
  `sourceChain.transaction.txHash` (`src/services/detect.rs:129-137`).

Failure behaviour: `TransactionNotFound` from `/operations` is treated as "no
VAA yet" (`NoVaa`), any other error as `Unavailable`
(`src/enrichment/wormholescan.rs:71-78`, `:128-139`). `/global-tx` and
`/vaas` are best effort: an error is logged and becomes `None`
(`:143-153`, `:63-67`). Status only ever rises (`detect.rs:177-180`,
`:250-263`).

### 3. Re-enrichment worker

`src/enrichment/reenrich.rs:90-126` calls
`state.enrichment.enrich(..., Prefetch::None, ...)`, so it hits
`/operations`, then possibly `/global-tx` and `/vaas`. Defaults:
`REENRICH_INTERVAL_SECS=30`, `REENRICH_BATCH=50`,
`REENRICH_MIN_AGE_SECS=60`, `REENRICH_RETRY_SECS=120`,
`REENRICH_MAX_AGE_SECS=7 days`, `REENRICH_ORIGIN_SHARE=40`
(`src/config.rs:335-344`). The origin backfill deliberately has no max age
(`src/enrichment/reenrich.rs:7-13`). A pass stops after 3 consecutive
`Unavailable` outcomes (`:35-37`, `:162-171`). Every attempt writes
`last_enrich_attempt_at`, including failures (`:14-20`, `:147-149`).

### 4. `reconcile --precision-sample`

`src/services/precision.rs:21-73`. Re-runs `analyse_tx`, then calls
`/global-tx` for the transfer's message id as the reference (`:32-40`). A
reference error is recorded as `reference:<code>`, not as a verdict (`:57-60`).

Important: the reconcile tool's **recall ground truth is not Wormholescan**.
It is `getSignaturesForAddress` over the program (`src/services/reconcile.rs:5-10`,
`src/bin/reconcile.rs:7-10`). Wormholescan is only the optional precision
reference.

### 5. `replay` binary

`src/bin/replay.rs:143-205`. Runs `analyse_tx`, then `/global-tx` as the
reference for `replay::compare` (`:166-173`; comparison
`src/services/replay.rs:55-75`).

### 6. Read-through refresh admission

`src/services/read_through.rs:329-341`. Every background refresh must reserve
one Wormholescan token (`state.wormhole_limiter.check()`); if the limiter has
no token the refresh is skipped (`:410-418`). The refresh itself calls
`analyse_tx_prepaid` (`:364-392`), which spends that token on the prefetch.

### 7. Process wiring

`src/state.rs:98-117` and `:165-173` build one `WormholeScanProvider` and one
governor limiter per process, for both API/all and stream roles.
`for_ingest` copies the same limiter (`src/state.rs:210-229`), so API and
ingest share one Wormholescan budget inside a process.

### 8. List endpoint

`GET /api/v2/transfers` does **not** call Wormholescan:
`src/handler/transfers.rs:11-16` → `src/services/transfers.rs:179-200` →
`repository::list_transfers`, a Postgres read only. `/api/v1` has no list
endpoint (`src/routes/v1/mod.rs`).

### 9. Tests, benches, scripts

* Fake upstream implements all three endpoints:
  `src/bin/fake_upstream.rs:468-487` (`/operations`), `:489-509`
  (`/global-tx`), `:513-534` (`/vaas`); routes at `:595-599`; call counters
  include `wormhole:operations`, `wormhole:global-tx`, `wormhole:vaas`
  (`:472`, `:493`, `:517`).
* Integration tests point `wormhole_url` at that loopback fake:
  `tests/consumer_reliability.rs:52`, `tests/reenrich_backfill.rs:33`,
  `tests/reconcile_precision_read_only.rs:73`.
* Offline fixture-oracle tests: `tests/golden_fixtures.rs:47-66`, `:94-143`,
  `:202-249`; `tests/fixtures_decode.rs:43-54`, `:72-137`;
  `tests/v1_api_snapshots.rs:113-135`. Fixtures live in `tests/fixtures/`.
* No `benches/` directory. `src/bin/bench_write_path.rs` prints
  `wormhole:operations` per message (`:173-185`); `scripts/bench_cold.sh`
  requires `WORMHOLE_URL` to point at the fake (`:26`, `:56`) and reports
  `wormhole:operations` / `wormhole:global-tx` counts (`:647-648`).
* `scripts/capture_fixtures.sh:18`, `:32-34` calls the live
  `/api/v1/operations?txHash=`; developer tooling, not runtime or tests.

## Field source table

Labels: **WS only** = Wormholescan only; **Solana RPC** = decoded from the
Solana transaction/account (Helius); **Both** = both can supply it; **Local** =
computed from other fields plus the chain registry. "Normal path" means an
adapter matched. The fallback path is the no-adapter path in
`src/services/analyzer.rs:160-269`.

| BridgeTransfer field | Source | Where it is set | Notes |
| --- | --- | --- | --- |
| `source_chain` | Solana RPC | `detect::to_bridge_transfer` `src/services/detect.rs:81`; adapter `src/protocols/wormhole/mod.rs:249`, `:335` | From decoded VAA/payload chain id. Fallback: `correlator.rs:85`. |
| `source_tx_hash` (outbound) | Solana RPC | `detect.rs:82` | The observed Solana signature. |
| `source_tx_hash` (inbound) | **WS only** | `detect::apply_source_tx` `src/services/detect.rs:196-211`, called from `apply_wormholescan` `:186-189` and `fill_missing_origin` `src/enrichment/wormholescan.rs:43-68` | `/operations` `sourceChain.transaction.txHash`, else `/vaas` `data.txHash`. `to_bridge_transfer` leaves it `None` (`detect.rs:82`). |
| `source_wallet` | Solana RPC | `detect.rs:83`; adapter `src/protocols/wormhole/mod.rs:251`, `:338` | Outbound: tx signer. Inbound: always `None` from the adapter (`:338`); the fallback path puts the Solana signer here (`analyzer.rs:239`), which is the redeemer, not the origin wallet. Wormholescan supplies no source wallet. |
| `source_explorer_url` | Local | `detect.rs:84-86`, `:205-208` | Registry template + hash. |
| `destination_chain` | Solana RPC | `detect.rs:87`; adapter `src/protocols/wormhole/mod.rs:250`, `:336` | Payload `to_chain`. Fallback: decoded VAA or WS `toChain` (`analyzer.rs:178-184`). |
| `destination_wallet` | Solana RPC | `detect.rs:88`; adapter `:252`, `:340` | Payload `to` decoded per chain. Fallback: decoded VAA `to_address` or WS `toAddress` (`analyzer.rs:219-226`). |
| `destination_tx_hash` (inbound) | Solana RPC | `detect.rs:89` | The observed Solana redeem. |
| `destination_tx_hash` (outbound) | **WS only** | `detect::apply_destination_tx` `src/services/detect.rs:250-263`; consumed from `apply_wormholescan` `:182-184` or provider `/global-tx` `src/enrichment/wormholescan.rs:107-113`, `:145-153` | `/operations` `targetChain.transaction.txHash` (preferred), then `globalTx.destinationTx.txHash`, then `destinationTx.txHash` (`detect.rs:230-246`). |
| `destination_explorer_url` | Local | `detect.rs:255-259` | Registry template + hash. |
| `token` | Solana RPC | `detect.rs:93`; adapter `src/protocols/wormhole/mod.rs:253-258`, `:341-346` | Payload token address decoded per token chain. Fallback: decoded VAA `token_address`, else WS `tokenAddress` (`analyzer.rs:208-214`). |
| `token_symbol` | **Both** (local wins) | `detect.rs:94`; `src/services/token_metadata.rs:42-45` | Helius `getAsset` (`src/chain_adapters/solana.rs:95-103`) always wins. The WS `tokenSymbol` is passed only as a last-resort hint on the fallback path (`analyzer.rs:216-217`, `:244`); the normal path passes `None` (`analyzer.rs:117`). |
| `amount` | Solana RPC | `detect.rs:95`; adapter `src/protocols/wormhole/mod.rs:260`, `:347` | The VAA-normalised amount from the transfer payload. Fallback: decoded VAA amount, else WS `amount` (`analyzer.rs:200-206`). |
| `amount_formatted` | Local | `detect.rs:96`; `format_amount` `src/vaa/decode.rs:121` | Raw amount + token decimals. |
| `message_id` (emitter chain/address/sequence) | Solana RPC | `detect.rs:97-101`; outbound from `Sequence:` logs and `PostMessage` accounts (`src/protocols/wormhole/mod.rs:183-245`); inbound from the posted VAA account (`:314-331`, `posted_vaa.rs:36-56`) | The VAA header is only used by Wormholescan enrichment to confirm the match (`detect.rs:169-175`). Fallback: VAA header itself (`analyzer.rs:170-176`). |
| `status` | **Both** (WS only raises) | Local base: `detect.rs:102-106` (outbound `Detected`, inbound `Completed`); WS: `detect.rs:177-180` (`Detected`→`Pending`), `:250-263` (`Pending`→`Completed`) | Never lowered; DB upsert preserves `Completed` and refuses `Pending`→`Detected` (`src/db/repository.rs:213-215`). Fallback: `correlator.rs:43-52`, then `as_inbound_observation` (`detect.rs:114-126`). |

### Fields outside `BridgeTransfer`

| Field | Source | Notes |
| --- | --- | --- |
| `transactions.timestamp` | Solana RPC | Helius `blockTime` (`src/protocols/solana_tx.rs:127`); WS `sourceChain.timestamp` / `targetChain.timestamp` is never read. |
| `transactions.fee_lamports` | Solana RPC | Helius `meta.fee` (`src/normaliser/solana.rs:29`). |
| Wormhole bridge fee | Not stored | WS `standarizedProperties.fee` / `sourceChain.fee` is never read. For payload id 1 the trailing 32-byte fee in the instruction payload is also ignored (`src/protocols/wormhole/mod.rs:362-388`); there is no fee column on `BridgeTransfer`. |
| `bridge_event` (v1 response only) | **WS only today** | Built by `normaliser::wormhole::normalise` from `/operations` (`src/normaliser/wormhole.rs:6-48`): `sequence`, `emitterChain`, `emitterAddress.hex`, `content.standarizedProperties.toChain`/`amount`. In the chain-first path it is only populated when enrichment reports `Applied` (`src/services/analyzer.rs:136-139`). It is cached in the analysis JSON and replayed by `/api/v1` (`src/routes/v1/compat.rs:17-37`). All four values are already present in the decoded transfer/message, so this is wiring, not new chain decoding. |
| `wormhole_messages` detail row | Solana RPC | Written from the decoded `MessageId` (`src/db/repository.rs:253-...`). |

## Behaviours that depend on Wormholescan semantics

1. **`Pending` means "Wormholescan has the VAA"; `Completed` means it also
   knows the destination tx.** This is the exact rule in
   `src/services/detect.rs:149-184` and `src/services/replay.rs:54-75`.
   A matching VAA raises `Detected`→`Pending` (`detect.rs:177-180`); a
   destination tx raises `Pending`→`Completed` (`detect.rs:250-263`).
   Inbound redeems are `Completed` locally at detection (`detect.rs:102-106`).
   The fallback path instead infers `Pending` from "destination chain is
   resolved" and `Completed` from a known destination tx
   (`src/services/correlator.rs:43-52`).
2. **The VAA is also the match guard.** Enrichment only applies if the decoded
   VAA's emitter chain/address/sequence equal the on-chain message
   (`detect.rs:169-175`); a mismatch is ignored (`Mismatch`).
3. **Reconcile is not grounded in Wormholescan.** Recall ground truth is
   `getSignaturesForAddress` (`src/services/reconcile.rs:5-10`). Only the
   optional `--precision-sample` uses `/global-tx` as a reference
   (`src/services/precision.rs:32-40`), as does `replay`
   (`src/bin/replay.rs:166-173`). Neither writes.
4. **Caches are of derived analyses, not Wormholescan responses.** Positive
   caches: completed 600 s / 50 000 entries, open 15 s / 10 000 entries
   (`src/services/read_through.rs:224-239`). Negative caches:
   `TransactionNotFound` 30 s (`:241-244`), other failures
   `FAILED_CACHE_TTL_SECS` default 10 s (`:245-248`,
   `src/config.rs:300`). Postgres freshness is 60 s (`read_through.rs:24`);
   stale entries are served while a background refresh runs (`:469-478`).
   Refresh cooldown is 15 s (`:251-254`).
5. **Re-enrichment is the convergence loop for WS-dependent fields.** Open
   rows are retried after `REENRICH_RETRY_SECS` (120 s) for up to 7 days;
   completed inbound rows missing an origin tx are retried indefinitely, with
   40 % of each batch reserved for them (`src/enrichment/reenrich.rs:7-20`,
   `:191-234`; `src/config.rs:335-344`). A provider outage stops a pass after
   3 consecutive `Unavailable` results (`reenrich.rs:35-37`, `:162-171`).
6. **One global Wormholescan rate limit.** `WORMHOLE_RPS` default 20 is a
   governor `RateLimiter::direct` shared by every Wormholescan call in the
   process (`src/state.rs:98-117`). `call_upstream` waits at most 2 s for a
   token, then returns `Overloaded`; retries are up to 3 extra attempts with
   exponential backoff + jitter, and every retry takes a token
   (`src/clients/limiter.rs:96-166`). Only `UpstreamRateLimited`, timeouts and
   `UpstreamProvider { retryable: true }` are retried (`:13-23`). The client
   maps HTTP 5xx and 408 to retryable, but **not 429**
   (`src/clients/helius.rs:60-62`), so a Wormholescan 429 is not retried by
   `call_upstream` and surfaces as `Unavailable`.
7. **A background refresh is admitted only if a WS token is free.**
   `try_reserve_refresh` calls `state.wormhole_limiter.check()`
   (`src/services/read_through.rs:329-341`); no token means the refresh is
   skipped (`:410-418`), even when the refresh might not need Wormholescan.
8. **Consumer retry schedules are tuned to VAA latency.** A missing VAA in the
   fallback path is retried every 5 s for up to 90 s because guardian
   finalisation takes longer than ordinary transient errors
   (`src/redis/consumer.rs:151-157`, `:166-173`).
9. **Chain-first analysis pays one `/operations` call per transaction**
   (`src/services/analyzer.rs:64-89`), even though it only needs the VAA to
   raise the status. This is the largest steady-state call volume: one per
   cold analysis plus one per re-enrichment attempt.

## Removable now (derivable from Solana data we already parse)

The normal path never needs Wormholescan for these; they are already decoded
by `src/protocols/wormhole/mod.rs`:

* message identity (emitter chain, emitter address, sequence) — outbound from
  `PostMessage` accounts plus `Sequence:` logs (`:168-266`, `:392-415`),
  inbound from the posted VAA account (`:290-352`, `posted_vaa.rs:36-56`);
* direction, source/destination chain, source wallet (outbound), destination
  wallet, token address/chain, raw amount (`:240-262`, `:326-349`);
* `amount_formatted` and explorer URLs (local, `detect.rs:89-96`);
* inbound `destination_tx_hash` and inbound `Completed` status
  (`detect.rs:102-106`);
* `token_symbol` (Helius `getAsset`, with WS only a fallback hint);
* the v1 `bridge_event` values (`sequence`, emitter, target chain, amount) —
  already present in the decoded message/transfer, but they currently reach
  `/api/v1` only through `normaliser::wormhole::normalise`, so the wiring must
  be replaced (`analyzer.rs:136-139`, `normaliser/wormhole.rs:6-48`).

What cannot be removed from the normal path without losing behaviour:

* outbound `destination_tx_hash` and `Completed`;
* outbound `Pending` (VAA existence);
* inbound `source_tx_hash` and its explorer URL.

## Needs new decoding

* **Wormhole bridge fee** (if it is ever required): the id-1 payload already
  contains the 32-byte fee at the end, but `parse_transfer_payload` ignores it
  (`src/protocols/wormhole/mod.rs:362-388`). Id-3 payloads carry the sender
  after the fixed fields, also currently ignored. There is no schema field to
  store either.
* **Inbound source wallet for transfer-with-payload (id 3)**: the sender
  address is in the payload after `PAYLOAD_FIXED` (`mod.rs:364-368`) but is
  not parsed. Plain id-1 transfers do not carry a sender at all.
* **The core bridge message account for outbound sends is already named in the
  instruction accounts** (`accounts[1]`; see the layout comment at
  `src/protocols/wormhole/mod.rs:40-43` and the emitter at `:221-225`), but
  `required_accounts` only preloads redeem VAAs (`:141-151`). Preloading and
  parsing it would add message-level data, but **not** VAA existence: the
  account exists as soon as `PostMessage` runs, before guardians sign.

## Needs destination-chain data

* **Outbound `destination_tx_hash` and `Completed`.** The transfer payload
  names the destination chain and recipient, not the tx that redeemed it. The
  Solana message/VAA does not contain it. This needs an RPC on the destination
  chain (or an equivalent index).
* **Outbound `Pending` / VAA existence.** `apply_wormholescan` raises to
  `Pending` only when a guardian-signed VAA is known
  (`src/services/detect.rs:158-180`). The Solana `PostMessage` account is
  created at send time, so treating its existence as `Pending` would change
  the meaning. Confirming the VAA requires the destination chain, a guardian
  RPC, or another VAA source.
* **Inbound `source_tx_hash` and `source_explorer_url`.** The posted VAA
  account on Solana carries the message header and payload, but not the tx
  that emitted it (`posted_vaa.rs:27-56`). This needs an RPC on the origin
  chain (or an equivalent index). The same applies to an inbound
  `source_wallet` for plain id-1 transfers.
