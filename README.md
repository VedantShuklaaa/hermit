# bridgeIndexer

A real-time indexer for bridge transactions on Solana. It decodes bridge
activity straight from Solana data, in both directions (sends out of Solana,
redeems and solver fills into it), across the major bridge protocols on Solana.
Each transfer is persisted from on-chain data alone, enriched afterwards with
its counterpart leg and final status from protocol APIs, and streamed live to
clients over WebSocket.

The product is moving from a Wormhole transfer indexer to a protocol-agnostic
core with one adapter per bridge protocol (see [Principles](#principles)).
Today only the Wormhole Token Bridge is decoded. Sends out of Solana are stored
chain-first and WormholeScan only enriches them. Redeems into Solana are decoded
but not stored yet. The [Roadmap](#roadmap) tracks the move.

## Status

| Area | State |
| --- | --- |
| Wormhole Token Bridge (WTT) | Outbound is indexed chain-first (`Detected` from Solana data alone, WormholeScan enriches). Inbound redeems are decoded from the posted VAA account (A1b) and stored (A7): `Completed` at detection, the redeem tx is the destination tx, the origin tx is filled in by enrichment |
| Detection | **Chain-first for Wormhole outbound (A5).** A transfer decoded from Solana data is persisted as `Detected` with WormholeScan fully unavailable. WormholeScan only enriches. Transactions that decode to no transfer (other Wormhole programs, a redeem whose VAA account could not be loaded) still go through the WormholeScan-gated path |
| Domain model and schema | **Generalised (A2).** `bridge_transfers` is identified by `(protocol, message_key)` with `direction`, `via`, raw chain references plus an optional resolved chain, and `NUMERIC(78,0)` amounts. Wormhole fields live in `wormhole_messages`. The pipeline still writes only Wormhole sends out of Solana (A5 wires the adapter) |
| Chains | Registry tables, loader and the SQL `resolve_chain` function exist. Transfers store the raw `(protocol, id)` and the resolved `chains.id`, or NULL when unknown. **Wired (A3).** The registry is loaded at startup and refreshed every `CHAIN_REGISTRY_REFRESH_SECS`. Explorer URLs and the `Pending` vs `Detected` decision come from it, so a chain added to the database works without a deploy. `ChainId` only remains as the frozen `/api/v1` response type through the compatibility mapper, until `/api/v1` is retired |
| Protocol core | `BridgeAdapter`, `ProtocolRegistry`, `compose`, pure `SolanaTx` and the Wormhole Token Bridge adapter are **wired into `analyse_tx`** through `AppState.protocols`. Sends and redeems are decoded and stored (A7). A redeem needs the posted VAA account (A1b): the adapter asks for it through `required_accounts` and the fetch step preloads it with `getMultipleAccounts`. The other Wormhole programs (A4) are not decoded yet |
| Read path (`/analyse`) | Working, cache-hit throughput measured (see [Results](#results)) |
| Destination tx resolution | **Fixed (A5b).** Read from the `/operations` payload (`targetChain.transaction.txHash`, legacy paths as fallback), then `/global-tx` through the WormholeScan limiter when the VAA matched but no destination tx is present. `Pending` now means WormholeScan itself has no destination tx |
| Enrichment | **Provider and background pass (A5b).** WormholeScan sits behind the `EnrichmentProvider` trait in `enrichment/`. A supervised worker re-enriches `Detected` and `Pending` rows on a schedule and fills the missing origin tx of completed inbound rows, so statuses converge without anyone calling `/analyse`. The origin tx comes from `/operations` when that record names one, otherwise from the message-key `/vaas` record |
| WebSocket ingestion | **Always on (A8).** The ingester starts with the process, under a supervisor, whether or not anyone is connected to `/stream`. Still **lossy**, and the `reconcile` tool measures it (4a); the baseline run and the fix are Phase 4 |
| CI | `cargo fmt --check`, `clippy -D warnings` and `cargo test` on every PR and on `main` (Phase 0) |
| Deploy | Manual (`git clone` + `cargo run` on EC2, Postgres on RDS). Automated in Phase 2 |

## Principles

1. **The chain is the source of truth.** Detection and decoding come from
   Solana data (Helius), never from an explorer. WormholeScan and any other
   protocol API are enrichment only (destination tx, final status). A
   transaction is persisted as `Detected` from on-chain data alone, even when
   no enrichment exists yet. Enrichment is cross-checked: a WormholeScan VAA is applied only when its emitter
   chain, emitter address and sequence match the message decoded on chain, otherwise
   it is ignored.
2. **Protocol-agnostic core, per-protocol adapters.** Each adapter declares its
   program IDs, classifies and decodes a Solana transaction into a bridge event
   (or rejects it: many transactions that touch a bridge program are admin or
   attest calls, not transfers), and optionally resolves the counterpart.
3. **Composition is real.** Mayan runs on Wormhole and CCTP, and aggregators
   route through other bridges. One transfer can carry an outer protocol and an
   underlying rail. The underlying transfer is indexed once, the router is
   recorded as an attribute (`via`), and nothing is double-counted.
4. **Direction matters.** Solana-outbound sends and Solana-inbound
   redeems/fills (including solver fills for intent bridges) are both indexed.
5. **Unknown is not an error.** An unrecognised chain ID never crashes the
   pipeline or drops a transfer. It is stored as `unresolved(protocol, id)`.

### Protocol coverage

| Protocol | State |
| --- | --- |
| Wormhole Token Bridge (WTT) | Sends are indexed chain-first, WormholeScan only enriches. Redeems into Solana are decoded (A1b) and stored (A7) |
| Wormhole NTT and other Wormhole programs on Solana | Phase A, after program IDs and layouts are verified |
| Mayan (Swift, MCTP), Circle CCTP, LayerZero/Stargate, deBridge, Allbridge, Hyperlane, Chainlink CCIP, Across | Candidates. Inclusion and order come from DefiLlama bridge volume for Solana at the start of each phase. Program IDs and instruction/event layouts are verified against official docs or source before an adapter is written |

### Domain model and chain registry (target)

* A transfer carries `protocol`, an optional `via` (outer router), `direction`,
  source and destination chain references, a protocol-specific message identity
  (Wormhole emitter/sequence, CCTP domain/nonce, LayerZero GUID, deBridge order
  ID, ...) with a unique key per protocol, wallets, token, amount and a status:
  `Detected`, `Pending`, `Completed`, plus `Failed`/`Refunded` where a protocol
  needs them.
* Storage is split into a protocol-agnostic `bridge_transfers` table and one
  detail table per protocol (`wormhole_messages` today). A transfer's identity is
  `UNIQUE (protocol, message_key)`. `observed_tx_hash` is the Solana tx it was decoded
  from and `event_index` its position there, so one tx can hold several transfers.
* Chains live in a DB-backed registry: a canonical `chains` table plus a
  `(protocol, protocol_chain_id) -> chain` mapping table, because every protocol
  numbers chains differently (Wormhole chain IDs, CCTP domains, LayerZero
  endpoint IDs, EVM chain IDs, deBridge IDs). Unknown IDs are persisted as
  `unresolved(protocol, id)`.
* Later upgrade, designed now and built later: an unresolved ID can be looked up
  with a free LLM API. The answer is a proposal, never a fact. It enters the
  registry as `pending_review` and activates automatically only when it can be
  cross-checked against an authoritative source (Wormhole chain list, Circle's
  CCTP domain list, LayerZero endpoint metadata). Lookups are rate-limited and
  cached, one per unresolved `(protocol, id)`, with a negative cache.

Implemented so far:

* `MessageId` is an enum with one variant per protocol (only `Wormhole` today).
  `MessageId::key()` is the canonical string stored as `bridge_transfers.message_key`,
  unique per `(protocol, message_key)`.
* `BridgeAdapter` is pure: `classify(&SolanaTx)` returns either the transfers it
  contains or a `Reject` of kind `not_transfer`, `unsupported` or `malformed`, with a
  reason. `ProtocolRegistry` routes a tx to every adapter whose program appears
  top-level **or in an inner instruction**, so an aggregator and its underlying rail
  both see it, and `compose()` merges them into one transfer with `via` recorded.
* Amounts are `RawAmount`, a decimal string validated for `NUMERIC(78,0)` (fits a `u256`).
* Adapters emit a `RawChainRef` in the protocol's own namespace. `chains::Snapshot`
  resolves it to a canonical chain, or to `ChainRef::Unresolved`, never an error.
  Only `active` rows resolve, in both tables, and a mapping must point at an active
  chain, so a `pending_review` proposal can never label a real transfer.
* The migration seeds the 10 current chains plus the `wormhole` namespace (checked
  against `sdk/vaa/structs.go` in wormhole-foundation/wormhole) and the `evm`
  namespace (checked against ethereum-lists/chains). Each mapping row carries
  `status`, `origin` (`seed`, `manual`, `llm`), `evidence` and `verified_at`, which is
  the provenance the later LLM-proposal upgrade needs.
* `protocols::wormhole::WormholeAdapter` decodes Token Bridge outbound transfers
  (`transfer_native`, `transfer_wrapped` and the with-payload variants) from the tx
  alone. The Token Bridge CPIs the core bridge `PostMessage`: its instruction data
  carries the payload (amount, token, recipient, destination chain), its emitter
  account and the `Sequence: N` log give the message identity. A tx with
  `meta.err` set, a log/instruction mismatch or a bad payload is rejected with a
  reason, never guessed. The amount is the VAA amount (at most 8 decimals).
  Program IDs are from Wormhole's contract address reference, instruction indexes
  and layouts from `solana/modules/token_bridge` and `solana/bridge`.
* Inbound redeems (A1b). `complete_native`, `complete_wrapped` and the with-payload
  variants carry no instruction data: the message they redeem is the posted VAA
  account (the third account of the instruction, owned by the core bridge). Adapters
  declare the accounts they need (`BridgeAdapter::required_accounts`, default none),
  `analyse_tx` reads them with one `getMultipleAccounts` call through the Helius
  limiter into `SolanaTx::accounts`, and the adapter decodes the message identity and
  the transfer from them with the in-house parser in `protocols/wormhole/posted_vaa.rs`
  (no WormholeScan or other external dependency). The adapter checks that the account
  is owned by the core bridge and that the payload is a transfer addressed to Solana.
  A redeem without its account loaded is `Unsupported`, any other mismatch is
  `Malformed`. A redeem decodes to the same `(protocol, message_key)` as the send it
  redeems, with `direction = Inbound` and no source wallet (the sender is on the origin
  chain). `analyse_tx` stores them (A7), see below.
* Inbound rows (A7). A redeem's Solana tx is the destination side of the transfer, so the
  stored row has `direction = 'Inbound'`, `observed_tx_hash` and `destination_tx_hash` both
  the redeem signature, `source_tx_hash` NULL and status `Completed` straight away (the
  adapters reject a failed tx, so a decoded redeem has happened). The origin tx is filled in
  by enrichment: when WormholeScan's operation matches the on-chain message, its
  `sourceChain.transaction.txHash` becomes `source_tx_hash`, and a later analysis that
  could not reach WormholeScan never erases it. `/operations` is keyed by a transaction
  signature, which for a redeem is the destination side, so its response can be empty or
  carry no source tx. The fallback is the message-key record:
  `/api/v1/vaas/{emitterChain}/{emitterAddress}/{sequence}`, whose `data.txHash` is the
  origin tx. EVM hashes arrive as bare hex and get the `0x` prefix from the chain
  registry; other chains keep their encoding. The fallback fills only a missing
  `source_tx_hash` and its explorer URL, never the status or a field that is already set. A
  completed inbound row that still has no origin tx is revisited by the background
  re-enrichment pass (`enrichment::reenrich`) until one of the two routes names it, so a
  re-analysis is not the only way it is filled. A row is one per
  `(protocol, message_key)`, so a redeem is never stored twice.
  `BridgeTransfer::direction()` derives the direction (destination Solana, source not
  Solana), so stored analyses keep their shape. In the
  frozen `/api/v1` response `source_tx_hash` is now nullable, and it is null only for a
  redeem whose origin tx is not known yet. A redeem that still goes through the
  WormholeScan-driven path (its VAA account could not be loaded) is turned around the same
  way (`detect::as_inbound_observation`). The A7 migration
  (`20261024120000_repair_inbound_rows.sql`) repairs rows the old path stored like sends:
  a message emitted by any chain other than Solana and observed on Solana becomes
  `Inbound` with the redeem as destination tx and `Completed`, and its cached analysis is
  dropped so the next read rebuilds it. There is no down migration: snapshot the database
  before deploying.
* The inbound source-wallet repair (`20261030120000_repair_inbound_source_wallet.sql`)
  clears `source_wallet` on `Inbound` rows written by the old WormholeScan fallback, which
  stored the redeem tx's Solana signer there; a redeem's sender is on the origin chain, so
  an inbound row has no Solana source wallet. The cached `analysis` is dropped so the next
  read rebuilds it. Outbound rows and inbound rows that are already NULL are untouched, and
  a second run matches nothing. There is no down migration: snapshot the database before
  deploying.
* The A2 migration (`20261018120000_generalise_bridge_transfers.sql`) rewrites existing
  rows in place: every legacy row becomes `protocol = 'wormhole'`, `direction = 'Outbound'`,
  with the same message key `MessageId::key()` builds. Chains are stored as the protocol's
  raw reference (`*_ref_protocol`, `*_ref_id`) plus `*_chain_id`, which `resolve_chain()`
  fills when the registry knows the chain. An unknown ID stays unresolved, and a partial
  index finds those rows when a mapping is activated later. An amount that is not a plain
  integer becomes NULL (the full value stays in `analysis`). There is no down migration:
  snapshot the database before deploying.
* `services::detect` turns adapter output into the API's `BridgeTransfer` with status
  `Detected`, then `apply_wormholescan` raises it: `Pending` when a matching VAA exists,
  `Completed` when the destination tx is known. No VAA, a failed WormholeScan call or a
  VAA for another message leaves the transfer `Detected`. Token symbol and decimals are
  a best-effort lookup with a 2 s timeout. The upsert never moves a status backwards
  (`Completed` stays, `Pending` is not overwritten by `Detected`), and the cached
  analysis follows the same rule.

## Architecture

### Current (v0)

```mermaid
flowchart LR
    SOL[Solana WS logsSubscribe] --> ING[Solana Ingester]
    ING -->|XADD| STREAM[(Redis Stream: bridge:transactions)]
    STREAM --> W1[Worker 1..5]
    W1 --> ANALYSE[Analyse: Helius + WormholeScan + VAA decode]
    ANALYSE --> DB[(Postgres)]
    W1 -->|on success| BCAST[Broadcast channel]
    BCAST --> WS[WS clients: /api/v1/transactions/stream]
    W1 -->|on failure| DLQ[(Dead-letter stream)]
    RECOVERY[Recovery worker] -->|XAUTOCLAIM| STREAM
```

Every successfully analysed transaction is persisted to Postgres and, in the
same step, broadcast to any connected WebSocket clients — so the live feed and
the durable record come from a single source of truth, not two separate paths
that can drift.

Two properties of v0 that Phase A changes. The first is fixed (A8): the ingester no
longer waits for a `/stream` client, it runs from startup. The second is not yet
fully gone: `analyse_tx` still needs a WormholeScan operation with a VAA before it
will persist anything that does not decode to an outbound transfer.

### Target architecture (Phase A)

Detection and decoding use Solana data only. Enrichment runs after the transfer
is already stored, and a failed or empty enrichment never blocks persistence.

```mermaid
flowchart LR
    SOL["Solana logsSubscribe, one per program ID"] --> ING[Ingester]
    ING -->|XADD| STREAM[(Redis Stream)]
    STREAM --> WK[Workers]
    WK --> FETCH["Fetch tx and adapter accounts: Helius"]
    FETCH --> ROUTE["Route by program ID"]
    ROUTE --> ADP["Protocol adapters: classify and decode, pure"]
    ADP -->|rejected| DROP["Counted by reason, acked"]
    ADP --> COMP["Compose: one transfer per message identity, via recorded"]
    COMP --> CHR["Chain registry: resolve or unresolved"]
    CHR --> DB[(Postgres: Detected)]
    DB --> BCAST[Broadcast channel]
    BCAST --> WSX["WS clients"]
    DB --> ENR["Enrichment providers: WormholeScan, ..."]
    ENR -->|"destination tx, status"| DB
    WK -->|"infra failure"| DLQ[(Dead-letter stream)]
```

### Target architecture (after Phase 2)

Helius rate limits are per key, so the user-facing read path and the ingestion
pipeline must not share one. The same binary runs in two roles with separate
Helius keys, limiters and hostnames. `ROLE=all` stays available for local dev.

```mermaid
flowchart LR
    subgraph api["api.<domain>  (ROLE=api, Helius key A)"]
        H1["GET /analyse"] --> RT[Read-through cache]
        RT --> LA[Limiter A]
    end
    subgraph stream["stream.<domain>  (ROLE=stream, Helius key B)"]
        ING[Ingester + poll] --> RS[(Redis Stream)]
        RS --> WK[Workers]
        WK --> LB[Limiter B]
        WK --> BC[Broadcast]
        BC --> WSX["WS /stream"]
    end
    RT --> PG[(Postgres)]
    WK --> PG
    LA --> HEL[Helius]
    LB --> HEL
    LA --> WHS[WormholeScan]
    LB --> WHS
```

## Project structure

Entries marked *(planned)* arrive with Phase A and later; everything else exists today.

```text
bridgeIndexer/
├── .github/
│   └── workflows/
│       └── ci.yml                      # fmt, clippy, test (Phase 0)
├── .sqlx/                              # offline query data
├── deploy/
│   └── Caddyfile                       # host-installed TLS reverse proxy in front of the API
├── migrations/
│   ├── 20260912071405_create_bridge_transfers.sql
│   ├── 20260912162718_create_indexer_state.sql
│   ├── 20260912232059_fix_indexer_state_key_name.sql
│   ├── 20261002150000_analysis_and_read_indexes.sql
│   ├── 20261017090000_chain_registry.sql
│   ├── 20261018120000_generalise_bridge_transfers.sql
│   ├── 20261024120000_repair_inbound_rows.sql  # redeems stored like sends become inbound (A7)
│   ├── 20261030120000_repair_inbound_source_wallet.sql  # inbound rows lose the Solana source wallet
│   ├── 20261030130000_index_missing_origin.sql  # partial index for the origin-tx backfill (A7)
│   ├── 20261030140000_origin_attempt_marker.sql  # backfill attempt marker and rotated index (A7)
│   └── 20261031120000_list_endpoint_token_index.sql  # token filter index for the list endpoint (3g)
├── docs/
│   ├── 3e-worker-patience.md            # live token wait notes (3e)
│   ├── 4bd-reconnect-verification.md    # 4b + 4d EC2 reconnect/watchdog verification
│   ├── 4c-concurrent-backfill.md        # live/backfill concurrency and the checkpoint invariant (4c)
│   ├── 4f-reconcile-poll.md             # periodic poll: window, checkpoint, metrics (4f)
│   ├── 3g-list-endpoint.md              # list contract, synthetic plans, known limits (3g)
│   └── scaling-log.md                   # measured before/after entries
├── scripts/
│   ├── bench_list.sh                    # first/deep/filtered list benchmark (3g)
│   └── capture_fixtures.sh              # save Helius + WormholeScan fixtures (Phase 0)
├── src/
│   ├── main.rs                         # wiring, supervised workers, HTTP server
│   ├── lib.rs
│   ├── config.rs                       # env config
│   ├── state.rs                        # AppState, limiters, caches
│   ├── error.rs                        # AppError -> HTTP responses
│   ├── shutdown.rs                     # SIGTERM / Ctrl+C
│   ├── bin/
│   │   ├── replay.rs                   # replay the last N signatures, compare with WormholeScan (1e)
│   │   ├── reconcile.rs                # stream recall vs getSignaturesForAddress (Phase 4, 4a)
│   │   └── seed_transfers.rs           # 1M throwaway rows for list plans and benchmarks (3g)
│   ├── domain/                         # types only, no IO
│   │   ├── bridge.rs
│   │   ├── bridge_transfer.rs
│   │   └── transaction.rs
│   ├── vaa/
│   │   └── decode.rs                   # VAA, address and amount decoding (pure)
│   ├── normaliser/                     # raw provider JSON -> domain (pure)
│   │   ├── solana.rs
│   │   └── wormhole.rs
│   ├── clients/                        # provider HTTP + rate limiting
│   │   ├── helius.rs                   # getTransaction, getMultipleAccounts (A1b), getSignaturesForAddress
│   │   ├── wormhole.rs
│   │   └── limiter.rs
│   ├── chain_adapters/                 # token symbol and decimals per chain (destination lookups live in enrichment/)
│   │   ├── mod.rs                      # ChainAdapter trait
│   │   ├── registry.rs
│   │   ├── setup.rs
│   │   └── solana.rs
│   ├── protocols/                      # protocol-agnostic core (Phase A, partly done)
│   │   ├── mod.rs                      # BridgeAdapter trait (classify, required_accounts), Protocol, MessageId, Direction, DetectedTransfer, Reject
│   │   ├── registry.rs                 # program ID -> adapters, required accounts, SOLANA_BRIDGE_PROGRAMS override parser
│   │   ├── compose.rs                  # one transfer per message identity, router recorded as via
│   │   ├── solana_tx.rs                # pure SolanaTx view built from Helius jsonParsed, plus preloaded accounts (A1b)
│   │   ├── amount.rs                   # RawAmount: decimal string for NUMERIC(78,0)
│   │   └── wormhole/                   # Token Bridge adapter, outbound (A1) and inbound (A1b); NTT and other programs planned
│   │       ├── mod.rs
│   │       └── posted_vaa.rs           # in-house parser for the core bridge posted VAA account (A1b)
│   ├── chains/                         # DB-backed chain registry, loaded at startup and refreshed in the background (A3)
│   │   ├── mod.rs                      # RawChainRef, ChainRef, Snapshot, resolve (only active rows resolve)
│   │   └── db.rs                       # load_snapshot from Postgres
│   ├── enrichment/                     # counterpart and status providers (Phase A)
│   │   ├── mod.rs                      # EnrichmentProvider trait, EnrichInput, EnrichOutcome
│   │   ├── wormholescan.rs             # WormholeScan: /operations VAA, /global-tx destination tx, /vaas origin-tx fallback
│   │   └── reenrich.rs                 # background pass over open rows and missing inbound origin txs
│   ├── ingestion/                      # Solana WS listener + backfill, always on (A8)
│   │   ├── solana.rs
│   │   ├── backfill.rs                 # pure backfill planning: checkpoint cut-off, failed txs, cap (A8)
│   │   ├── checkpoint.rs               # per-connection gate: live slots cannot pass an unfinished backfill (4c)
│   │   └── types.rs
│   ├── redis/                          # stream producer, consumer group, recovery
│   │   ├── producer.rs
│   │   └── consumer.rs
│   ├── services/                       # orchestration
│   │   ├── analyzer.rs                 # analyse_tx, preloads adapter accounts (A1b), split into fetch/decode/enrich in Phase 1
│   │   ├── correlator.rs
│   │   ├── token_metadata.rs
│   │   ├── detect.rs                   # chain-first detection and WormholeScan enrichment (pure)
│   │   ├── read_through.rs             # caches, single-flight, stale-while-revalidate
│   │   ├── reconcile.rs                # pure window, diff, recall, miss clustering and lag (4a)
│   │   ├── reconcile_poll.rs           # periodic poll + shared signature scan (4f)
│   │   ├── replay.rs                   # pure compare/summary for the replay tool
│   │   └── transfers.rs                # list query validation, cursor, one keyset page (3g)
│   ├── db/
│   │   ├── connection.rs
│   │   └── repository.rs
│   ├── handler/                        # HTTP and WS handlers
│   │   ├── analyse.rs
│   │   ├── stream.rs
│   │   └── transfers.rs                # GET /api/v2/transfers (3g)
│   └── routes/
│       ├── mod.rs                      # router + CORS
│       ├── v1/
│       │   ├── mod.rs
│       │   └── transactions.rs
│       └── v2/
│           ├── mod.rs                  # /transactions/analyse and /transfers
│           └── transactions.rs
├── tests/
│   ├── bridge_transfers_schema.rs      # A2 schema and migration of existing rows (needs Docker)
│   ├── chain_registry_db.rs            # testcontainers Postgres: seed, resolve, status rules
│   ├── common/                         # shared test helpers (container setup)
│   ├── decode_unit.rs                  # pure decode tests (Phase 0)
│   ├── fixtures_decode.rs              # decoders over captured fixtures (Phase 0)
│   ├── golden_fixtures.rs              # Wormhole adapter over captured fixtures, sends and redeems, checked against WormholeScan (A9, A1b)
│   ├── list_transfers.rs               # keyset paging, filters, cursor/limit validation (3g, needs Docker)
│   ├── persistence_idempotency.rs      # testcontainers Postgres
│   └── fixtures/                       # helius_<name>.json, wormhole_<name>.json, account_<name>_vaa.json (captured, see tests/fixtures)
├── benches/                            # (planned) criterion: pure decode (Phase 1)
├── .env.example                        # (Phase 0)
├── .dockerignore
├── .gitignore
├── Cargo.toml
├── Cargo.lock
├── docker-compose.yml
├── dockerfile
└── README.md
```

### Why Redis Streams, not Kafka

Consumer groups plus `XAUTOCLAIM` give exactly the guarantees this needs —
at-least-once delivery, per-worker ack tracking, automatic redelivery of
stuck messages — without a second infrastructure dependency to operate.
Kafka would be the right call at higher throughput or with multiple
independent consumer applications; for a single-service pipeline at this
scale it's added operational cost for guarantees Redis Streams already
provides.

## Read path: `GET /api/v1/transactions/analyse`

A cold analysis is expensive: Helius and Wormholescan are called in parallel,
the VAA is decoded, and the transfer is correlated with its destination chain
(about 360 ms median, and both providers rate-limit). The read path exists so
that almost no request pays that cost.

```mermaid
flowchart TD
    REQ[GET /analyse] --> VAL{Valid hash and chain?}
    VAL -- no --> E400[400, no cache or upstream touched]
    VAL -- yes --> MEM{"Memory cache: completed, open, negative"}
    MEM -- hit --> RESP[Respond]
    MEM -- miss --> LOCK[Per-key single-flight lock]
    LOCK --> DB{Postgres analysis}
    DB -- fresh --> RESP
    DB -- stale --> RESP
    DB -- stale --> BG[Background refresh]
    DB -- none --> COLD[Cold fetch]
    COLD --> PERM[Upstream permit, 16 concurrent max]
    PERM --> HEL[Helius: limiter, retry]
    PERM --> WH[Wormholescan: limiter, retry]
    HEL --> CORR[Correlate and persist]
    WH --> CORR
    CORR --> RESP
    COLD -- error --> NEG[Negative cache]
    BG --> PERM
```

| Layer | What it does |
| --- | --- |
| Hash validation | Rejects anything that isn't a 64-byte base58 Solana signature, and any source chain other than Solana, with a 400 before any cache or upstream call. |
| Memory cache (moka) | `completed` transfers for 10 min, `open` (Pending/Detected) for 15 s, `not_found` for 30 s, transient upstream and normalisation failures for 10 s (`FAILED_CACHE_TTL_SECS`). |
| Single-flight lock | One request per `(chain, transaction hash)` does the work; concurrent requests for the same key wait, then re-check memory. 50 concurrent requests for one uncached hash cause 1 upstream fetch. |
| Postgres | `bridge_transfers.analysis` holds the last analysis. Completed transfers are always fresh; Pending/Detected ones are fresh for 60 s. |
| Stale-while-revalidate | A stale row is served immediately and refreshed in the background (max 8 refreshes at once, 15 s cooldown per key). A failed refresh only logs; the stale copy keeps being served. |
| Rate limiting | One limiter per provider (`governor`), so retries can't exceed a provider's quota. A call waits at most 2 s for a token, then fails fast with a 503. |
| Retries | 429, timeout and 502 retry up to 3 times with exponential backoff and jitter, each attempt going back through the limiter. |
| Concurrency cap | A semaphore bounds concurrent cold fetches (16); waiting for a permit times out after 2 s with a 503. |

Steady-state upstream load is roughly `hot keys ÷ freshness window`. With the
60 s window, 769 hot Pending transfers need about 13 refreshes per second,
which has to stay under the configured provider quotas.

### Tuning

| Setting | Where | Default |
| --- | --- | --- |
| Freshness window for non-Completed rows | `FRESH_SECS` in `services/read_through.rs` | 60 s |
| Failed-analysis negative cache TTL | `FAILED_CACHE_TTL_SECS` | 10 s |
| Max background refreshes | `MAX_BACKGROUND_REFRESHES` in `services/read_through.rs` | 8 |
| Cache TTLs and capacities | `ReadThrough::new` | see table above |
| Provider quotas | `HELIUS_API_RPS`, `HELIUS_LIVE_RPS` (one per lane, see `Lane` in `config.rs`), `wormhole_limiter` in `AppState::new` | Helius 6/s (API) + 4/s (ingest) to fit the free plan's 10/s while both lanes share one key, WormholeScan 20/s. With a key per lane (`HELIUS_API_URL`, `HELIUS_STREAM_URL`) raise each to its own plan's limit |

### Token metadata

Symbol and decimals come from one Helius `getAsset` per mint, cached per mint (24h resolved, 60s failed or empty). A cache miss uses the shared Helius limiter only if a token is free; otherwise the transfer is processed without metadata and the miss is not cached, so a later request retries. `HELIUS_API_RPS` and `HELIUS_LIVE_RPS` set the quota for the API and live pipeline respectively.

## Reliability properties

* **At-least-once processing, exactly-once persistence.** Redis Streams
  guarantees at-least-once delivery; Postgres upserts on `hash` and
  `(protocol, message_key)` absorb redelivery, so a crash mid-batch can never
  double-write, for every protocol.
* **Dead-letter queue, for permanent failures.** A transaction that fails
  analysis or persistence *and cannot fix itself* (malformed data, a
  normalisation failure, a provider answer that cannot change on retry such as
  invalid params or method not found) moves to `bridge:transactions:failed`
  instead of blocking the stream or being silently dropped.
* **Transient failures stay pending, bounded.** A rate limit, timeout, 5xx,
  limiter shed (`Overloaded`) or database outage neither acks nor dead-letters:
  the message stays in the pending list and recovery redelivers it after its
  idle time. Every delivery is counted (`XPENDING` times-delivered), and past
  `WORKER_MAX_DELIVERIES` (default 10) the message is dead-lettered with the
  reason recorded, so nothing is retried forever.
* **Self-healing recovery.** A dedicated worker reclaims messages abandoned by
  a crashed consumer via `XAUTOCLAIM`, every `WORKER_RECLAIM_INTERVAL_SECS`
  (30) and after `WORKER_RECLAIM_IDLE_MS` (5 min). Recovery runs the same
  handler as first delivery, so it classifies a failure the same way.
* **Graceful shutdown.** SIGTERM/Ctrl+C stops new work being picked up, lets
  in-flight messages finish, and drains open HTTP/WS connections before exit.
* **Upstream protection on the read path.** Per-provider rate limiters, retries
  with backoff and jitter, a bounded concurrency semaphore, and fail-fast 503s
  with `Retry-After` keep a burst of new transactions from turning into a
  provider ban or an unbounded queue.
* **Failures are cached too.** Not-found results (30 s) and transient upstream
  failures (2 s) are remembered, so one bad hash can't make every request redo
  the full upstream attempt or pile up behind its lock.
* **Stale data beats a slow answer.** Pending transfers past their freshness
  window are served from Postgres while a background task refreshes them
  through the same limiters.
* **Enrichment never gates persistence.** A transfer decoded from Solana data is stored
  as `Detected` even if WormholeScan is down, rate-limited or has no VAA yet, and a status
  never moves backwards.
* **Account preloads fail loud on the live path.** The accounts adapters ask for
  (the posted VAA of a redeem) are read in one `getMultipleAccounts` call through
  the Helius limiter. A live message whose preload fails transiently is left
  pending and retried, because a redeem without its VAA account is only reported
  `Unsupported` and then falls onto the WormholeScan path, which can store a row
  with the wrong source wallet (or dead-letter the redeem when WormholeScan is
  down). A permanent preload error, and every preload in the read-only
  `reconcile` tool, stays best effort: the failure is logged and the WormholeScan
  path still runs.
* **Open transfers converge on their own.** A supervised worker re-enriches `Detected`
  and `Pending` rows every `REENRICH_INTERVAL_SECS`, oldest attempt first, through the
  WormholeScan limiter (it never touches Helius). Rows younger than
  `REENRICH_MIN_AGE_SECS` are left to the normal pipeline, a row is not asked again
  within `REENRICH_RETRY_SECS`, and rows older than `REENRICH_MAX_AGE_SECS` are given up
  on. The same pass then backfills the origin tx of `Completed` inbound rows whose
  `source_tx_hash` is still NULL. `REENRICH_ORIGIN_SHARE` (40% by default) reserves that
  share of each `REENRICH_BATCH` for the backfill, so a backlog of open rows cannot
  starve it; `REENRICH_MAX_AGE_SECS` does not apply to those, because the origin tx may
  become known at any time. The backfill records `last_enrich_attempt_at` for every
  attempt, whatever the outcome (including a provider outage), and orders by it NULLS
  FIRST, so the queue rotates instead of re-selecting the same rows forever. A row whose
  cached `analysis` a repair migration dropped is rebuilt from its stored columns before
  enrichment. When `/operations` is empty or carries no source tx, the pass falls back
  to the message-key `/vaas` record (`data.txHash`) and normalises the hash per chain
  (EVM gets `0x`); only the missing origin fields are filled. A pass ends early after
  three provider outages in a row.
* **Helius token wait.** The API sheds a request after 2 s without a token (`Overloaded`, 503). Live workers wait up to `HELIUS_LIVE_TOKEN_WAIT_MS` (20 s) for the first token of a message, but that patience is no longer what keeps a burst out of the dead-letter stream: an `Overloaded` is transient, so the message stays pending and recovery redelivers it. The wait must stay well below the reclaim idle time (5 min), or a message would be reclaimed while it is still waiting.

## API

```
GET /api/v1/transactions/analyse?transaction_hash=<hash>&chain=solana
GET /api/v2/transactions/analyse?transaction_hash=<hash>&chain=solana
GET /api/v2/transfers
GET /api/v2/transfers/live
WS  /api/v1/transactions/stream
```

`chain` is the source chain. Solana is currently the only supported source
chain; anything else returns `422 NORMALISATION_FAILED`.

| Status | Code | Meaning |
| --- | --- | --- |
| 200 | | Analysis returned (from cache, Postgres, or a fresh fetch). |
| 400 | `INVALID_TX_HASH`, `BAD_REQUEST` | Malformed hash or request. |
| 404 | `TX_NOT_FOUND`, `VAA_NOT_AVAILABLE` | Transaction doesn't exist, or its VAA isn't available yet. Cached briefly. |
| 422 | `NORMALISATION_FAILED` | Transaction data couldn't be normalised, including unsupported chains. |
| 429 | `UPSTREAM_RATE_LIMITED` | A provider rate-limited the request after retries. Sends `Retry-After: 1`. |
| 502 / 504 | `UPSTREAM_ERROR`, `UPSTREAM_TIMEOUT` | A provider failed or timed out after retries. |
| 503 | `OVERLOADED` | The service is shedding load (limiter or permit wait exceeded 2 s). Sends `Retry-After: 1`. |
| 500 | `INTERNAL_ERROR` | Unexpected error. |

### List transfers (`GET /api/v2/transfers`)

Lists stored transfers newest first, one page per request. `items` uses the same
transfer object as `/api/v2/transactions/analyse`, so field names are identical.
There is deliberately no total count: it needs a full scan of the filtered set.
Page until `next_cursor` is `null`.

```
GET /api/v2/transfers?limit=50&direction=Inbound&status=Completed&chain=ethereum
```

| Parameter | Meaning | Rules |
| --- | --- | --- |
| `limit` | page size | default 50, 1 to 200 |
| `cursor` | position from the previous `next_cursor` | opaque; strictly after the last returned row |
| `protocol` | exact stored protocol | non-empty |
| `direction` | `Outbound` or `Inbound` | case-sensitive |
| `status` | `Detected`, `Pending`, `Completed`, `Failed`, `Refunded` | — |
| `chain` | canonical chain slug from `chains.slug` | matches source **or** destination |
| `token` | exact stored token address | no token-chain key |
| `from` | inclusive lower bound on `created_at` | RFC3339 |
| `to` | exclusive upper bound on `created_at` | RFC3339; must be after `from` |

Pagination is keyset over `(created_at DESC, id DESC)`: there is no `OFFSET`, and
rows with identical `created_at` are still visited exactly once. `next_cursor`
encodes the last returned row; send it back as `cursor`. The ordering key is the
time the indexer stored the row, not the chain's block time, so a backfilled
transfer appears where it was inserted. `from`/`to` combine with the cursor as
additional bounds.

| Status | Code | Meaning |
| --- | --- | --- |
| 200 | | One page. `next_cursor` is `null` on the last page. |
| 400 | `BAD_REQUEST` | Invalid `limit`, cursor or timestamp; unknown chain, direction or status; `from >= to`. |
| 500 | `INTERNAL_ERROR` | Database or serialization failure. |

There is no cache on this path; every request reads Postgres. Query plans and
the one index it adds are in [docs/3g-list-endpoint.md](docs/3g-list-endpoint.md).

The v2 transfer model still has one `message_id` shape, `wormhole`, so a page
that contains a row for another protocol fails with `422 NORMALISATION_FAILED`.
That matches `/analyse`; it is a model limitation, not a query one, and it goes
away when the adapter for that protocol gets a `MessageId` variant.

CORS is origin-allowlisted via `ALLOWED_ORIGINS` — nothing is permitted by
default.

### Live feed (`GET /api/v2/transfers/live`)

A server-owned ring of the most recent WebSocket frames, so every browser sees
the same recent transactions instead of each tab keeping its own list. The
indexer writes it; readers never touch Postgres.

```
GET /api/v2/transfers/live?limit=15&before=<next_cursor>
```

| Parameter | Meaning | Rules |
| --- | --- | --- |
| `limit` | page size | default 15, clamped to 1..100 |
| `before` | position from the previous `next_cursor` | opaque; strictly older than the last returned item |

The response is `{ "items": [...], "next_cursor": string|null, "total": <ZCARD>, "server_time": <ms> }`.
Each item is the v1 WebSocket frame plus `first_seen_at` and `updated_at`
(milliseconds). `first_seen_at` is the ring position and never changes when an
enrichment update replaces the frame; `updated_at` is the last write. Items are
newest first, and `total` is the current ring size, not the filtered count.

The path reads Redis only (`ZREVRANGEBYSCORE` and `HMGET`). If the ring is empty
after a cold start or Redis restart, one process takes `SET bridge:live:warmup 1
NX EX 30` and fills it from the newest `LIVE_FEED_MAX` stored analyses; concurrent
readers get an empty page until that finishes. The warm-up is the only database
query on this path. When Redis is down the endpoint serves the last in-process
cursorless page for up to 10 seconds, then `503` with an empty-feed body; a
circuit breaker admits at most one Redis probe per 5-second window. Responses
carry `Cache-Control: public, s-maxage=2, stale-while-revalidate=10`.

The endpoint is registered only when `LIVE_FEED_REDIS_ENABLED` is on. When it is
off the route does not exist (404), the WebSocket payload is byte-for-byte
unchanged, and the feed issues no Redis command.

## Running locally

Requires Postgres and Redis reachable at the URLs below.

```bash
cp .env.example .env   # fill in RPC URLs and the bridge program address
cargo run
```

Migrations run on startup. They are embedded at compile time (`sqlx::migrate!`),
so a change that touches only files under `migrations/` now forces a rebuild.

### HTTPS with Caddy (host install)

`deploy/Caddyfile` puts a host-installed Caddy in front of the API: it terminates
TLS, serves HTTP/2, compresses responses, proxies everything to `127.0.0.1:8080`
and writes access logs as JSON. No Docker; it does not touch `docker-compose.yml`.

Install Caddy from the [official apt instructions](https://caddyserver.com/docs/install#debian-ubuntu-raspbian):

```bash
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
  | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install caddy
```

Install the site config, replacing the domain with your own:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo sed -i 's/api\.example\.com/api.yourdomain.com/' /etc/caddy/Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
curl -fsS https://api.yourdomain.com/health
```

`/health` returns 200 normally and 503 on a dependency outage or stale ingest, so
the `curl -fsS` check fails loudly when the stack is not healthy.

Notes:

* Point a DNS `A` record for the site at the box before reloading; Caddy needs it
  to obtain the certificate.
* Security group: allow inbound `80` and `443`, keep `8080` closed. The app binds
  `0.0.0.0`, so Caddy is what makes that port reachable from the internet.
* WebSockets are proxied automatically; `/stream` needs no extra Caddy config.
* CORS stays in the app (`src/routes/mod.rs`). Do not add CORS headers in Caddy,
  or preflight responses will carry duplicates.

### Replay against WormholeScan

```bash
cargo run --bin replay -- --limit 200
```

Replays the last N Token Bridge signatures through the real pipeline and compares
our status with WormholeScan's `global-tx`. Needs the server's env, reads the chain
registry from `DATABASE_URL`, writes nothing. Exits 1 when agreement on transfers
older than `--min-age-secs` (default 900) is below `--min-agreement` (default 99).
`cargo run` alone still starts the server (`default-run = "bridge"`).

### Running reconcile

```bash
cargo run --bin reconcile -- --window-hours 2 --precision-sample 20
```

Measures the WebSocket ingester's recall and the classifier's precision for every
registered program over `--window-hours` (default 2), ending `--end-offset-secs`
(default 600) ago so the ingester had a chance to see the window. Ground truth is
`getSignaturesForAddress`: failed transactions are dropped, signatures already in
`bridge_transfers` are subtracted before any `getTransaction`, and only the
remainder goes through the pipeline's own fetch/preload path and adapter
classifier, offline. It writes nothing to Postgres or Redis.

Per program it prints the missed count and missed %, recall (a lower bound when
`--max-fetch`, default 2000, was hit; the cap counts signatures fetched with
`getTransaction`, not Helius calls, so retries are not counted separately),
ingestion lag p50/p99/max from block time
(`transactions.timestamp`) to `bridge_transfers.created_at`, rejection counts by
reason, and every missed signature flagged `same_slot` (suspect 1), `gap` (no
persisted transfer within `--gap-secs`, suspects 3–5) or `other`.
`--precision-sample N` re-runs up to N persisted transactions through `analyse_tx`
and compares their status with WormholeScan, reusing the replay comparison.
`--json` prints the same report machine-readable. Exits 1 when recall is below
`--min-recall` (default 99.9) and the cap was not hit.

### Periodic reconciliation poll

The `reconcile` bin is a manual measurement; the poll is the always-on safety net.
When ingestion is enabled, the supervised `reconcile-poll` task runs every
`INGEST_POLL_SECS` (default 300) per registered program. Each cycle scans
`getSignaturesForAddress` over `[last_poll_end - INGEST_POLL_OVERLAP_SECS,
now - INGEST_POLL_OFFSET_SECS]`, subtracts what `bridge_transfers` already holds, and
republishes the rest as normal stream candidates, so the consumers classify and
persist them. The Redis dedupe and idempotent upserts make a replay safe. The
per-program checkpoint is stored in `indexer_state` and only moves forward; the
window on a first run starts `INGEST_POLL_OVERLAP_SECS` before now. A cycle publishes
at most `INGEST_POLL_MAX_PUBLISH` signatures and logs when the cap is hit; a capped
cycle does not advance the checkpoint, so the remainder is covered again. It uses the
reconcile Helius lane (`HELIUS_RECONCILE_URL`, `RECONCILE_RPS`). See
[`docs/4f-reconcile-poll.md`](docs/4f-reconcile-poll.md).

### Metrics

Every role serves Prometheus metrics on `0.0.0.0:$METRICS_PORT` (default `9100`)
at `/metrics`, installed once at startup before any work runs. The endpoint is
internal: Caddy does not proxy it, and the security group must keep `9100`
closed, so scrape it only from the private network.

| Metric | Type | Labels |
| --- | --- | --- |
| `cache_lookups_total` | counter | `result`: `memory`, `pg_fresh`, `pg_stale`, `cold` |
| `cache_refresh_skipped_total` | counter | `reason`: `no_token`, `in_flight` |
| `limiter_wait_seconds` | histogram | `provider`: `helius`, `wormhole`; `lane`: `api`, `ingest`, `reconcile`, `shared` |
| `upstream_calls_total` | counter | `provider` and `lane` as above; `outcome`: `ok`, `err`, `timeout` |
| `http_request_duration_seconds` | histogram | `route`: matched route pattern (or `unmatched`); `status_class`: `2xx`, `4xx`, `5xx`, `other` |
| `stream_lag` | gauge | none |
| `stream_pending` | gauge | none |
| `dead_letter_depth` | gauge | none |
| `ingest_age_seconds` | gauge | none |
| `ingest_reconnects_total` | counter | `reason`: `watchdog`, `subscribe_failed`, `closed`, `error` |
| `ingest_backfill_inflight` | gauge | none |
| `ingest_backfill_duration_seconds` | histogram | none |
| `ingest_backfill_published_total` | counter | none |
| `poll_cycles_total` | counter | `program`: registered program ID; `result`: `ok`, `error` |
| `poll_missed_total` | counter | `program`: registered program ID |
| `poll_last_success_timestamp_seconds` | gauge | `program`: registered program ID |

Every label is a static string: no transaction hashes, addresses or user ids are
used as label values. The `program` label is one of the registered program IDs, a
fixed set from the protocol registry. `cache_lookups_total` counts cache lookups, so a v2 request
that misses its own cache also performs the underlying v1 lookups.
`limiter_wait_seconds` includes shed waits (a token timeout is recorded), and
`upstream_calls_total` counts each upstream attempt, retries included. A
WormholeScan call is reported with lane `shared` because one limiter serves every
role.

The four health gauges are set only when `GET /health` is served, from the same
15 s cache the endpoint returns, so they add no Redis or Postgres traffic and
refresh at most every 15 s. A value the probe could not measure (for example
Redis did not answer) is left at its previous reading instead of being reported
as zero. `http_request_duration_seconds` uses the matched route pattern as its
label, so path parameters and unknown URIs cannot create unbound cardinality.

Minimal `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: bridge
    static_configs:
      - targets: ["127.0.0.1:9100"]   # the host that runs the bridge process
```

`METRICS_PORT` is the only setting; set it in `.env` or the service environment.

### Config notes

* Live feed (`LIVE_FEED_REDIS_ENABLED`, default off; `LIVE_FEED_MAX`, default
  1000, clamped 1..10000). When enabled, every frame published to `/stream` is
  also upserted atomically into `bridge:live:index` (ZSET, score
  `first_seen_at_ms`, `ZADD NX`) and `bridge:live:frames` (HASH), which is
  trimmed to `LIVE_FEED_MAX` entries. Enrichment status advances and inbound
  origin-tx resolution publish an update frame as well. A Redis failure only
  logs; the WebSocket broadcast and the database write continue. When disabled
  the endpoint is not registered and no feed Redis command is issued.
* Helius provider lanes (1c). `HELIUS_API_URL` is the API lane's endpoint and
  `HELIUS_STREAM_URL` is the ingest lane's (ingester backfill, consumer workers,
  recovery, re-enrichment); each falls back to `HELIUS_URL` when unset, so a single
  `HELIUS_URL` still works. Each lane has its own limiter: `HELIUS_API_RPS` (6) and
  `HELIUS_LIVE_RPS` (4). While both lanes point at one key those two must add up to the
  plan limit; with a key per lane, give each the limit of its own plan. A process only
  needs its own lane's URL (`ROLE=api` needs the API one, `ROLE=stream` the stream one).
  `HELIUS_LIVE_URL` is the deprecated name of `HELIUS_STREAM_URL`: it is still read when
  `HELIUS_STREAM_URL` is unset and logs a warning at startup.
* `HELIUS_RECONCILE_URL` (optional, falls back to `HELIUS_URL`) and `RECONCILE_RPS`
  (default 3) are the reconcile Helius lane. The `reconcile` bin and the periodic
  reconciliation poll both use this endpoint and quota, each with its own limiter
  built separately from the API's and the live pipeline's, so neither can starve
  ingestion.
* Ingestion is always on (A8), all settings optional: `INGEST_ENABLED` (default on, `0` is a
  kill switch), `LIVE_BACKFILL` (default **on**, was off), `INGEST_BACKFILL_MAX` (5000),
  `INGEST_BACKFILL_PACE_MS` (750), `INGEST_REWIND_SLOTS` (20) and `INGEST_DEDUPE_TTL_SECS`
  (7200, `0` disables). On every (re)connect the backfill runs **concurrently with the
  read loop** (4c), as a task owned by that connection and cancelled with it; it starts
  `INGEST_REWIND_SLOTS` slots before the checkpoint (never below slot 0), so the
  checkpoint's own slot is replayed, and the two-hour dedupe TTL is far longer than that
  rewind window plus the 5 s reconnect delay. While the backfill is unfinished live
  notifications publish normally but do not advance the persisted checkpoint
  (`ingestion::checkpoint`), so a connection that dies mid-backfill leaves the resume
  point unchanged; live messages still refresh the checkpoint row's `updated_at` so
  `/health` does not go stale. See
  [`docs/4c-concurrent-backfill.md`](docs/4c-concurrent-backfill.md).
  Keepalive: `INGEST_PING_SECS` (30) sends a WebSocket ping, and `INGEST_WATCHDOG_SECS` (90,
  always above the ping interval) reconnects when no frame of any kind arrives for that long.
  `LIVE_IDLE_GRACE_SECS` is gone, nothing stops the ingester any more. Backfill is skipped
  while there is no checkpoint (a fresh database), so a first start never walks the whole
  history of the program.
  The pace is 750 ms because one backfilled message costs up to three live Helius calls and
  the live limiter drains 4 tokens/s, so anything faster than about one message per 750 ms
  lets the backfill outrun the limiter (the old 150 ms did). A full 5000-signature run
  therefore takes about 62 minutes instead of about 12, which is also why the dedupe TTL is
  now two hours: a full backfill outlives the old one-hour window.
* Periodic reconciliation poll (4f), all optional and only active when `INGEST_ENABLED`
  is on: `INGEST_POLL_SECS` (300, minimum 30), `INGEST_POLL_OVERLAP_SECS` (600),
  `INGEST_POLL_OFFSET_SECS` (180) and `INGEST_POLL_MAX_PUBLISH` (500). Every cycle, for
  each registered program, it scans `getSignaturesForAddress` over
  `[last_poll_end - overlap, now - offset]`, subtracts signatures already in
  `bridge_transfers`, and republishes the rest through the normal stream; the consumers
  classify and persist them. The per-program checkpoint lives in `indexer_state`
  (no migration) and only moves forward. The offset keeps the window behind the
  ingester, the overlap re-covers what the previous window ended before seeing, and the
  cap bounds the Helius spend after a gap (a capped cycle does not advance the
  checkpoint, so the remainder is re-covered next cycle). Poll cycles use the reconcile lane; their
  metrics are `poll_cycles_total`, `poll_missed_total` and
  `poll_last_success_timestamp_seconds`. Details:
  [`docs/4f-reconcile-poll.md`](docs/4f-reconcile-poll.md).
* Live worker patience and retry budget (all optional): `HELIUS_LIVE_TOKEN_WAIT_MS` (20000)
  is how long a worker waits for the first Helius token of a message;
  `WORKER_MAX_DELIVERIES` (10) is how many times a transiently failing message may be
  redelivered before it is dead-lettered; `WORKER_RECLAIM_IDLE_MS` (300000) is how long a
  message must sit unacknowledged before recovery reclaims it; `WORKER_RECLAIM_INTERVAL_SECS`
  (30) is how often recovery sweeps. The token wait must stay well below the reclaim idle
  time. `CONSUMER_COUNT` (5) is the number of worker tasks, `CONSUMER_READ_COUNT` (10) is
  `XREADGROUP COUNT`, and `DB_POOL_MAX` (20) is the Postgres pool per process. They default
  to the previous hardcoded values; the 3f sweep found no throughput gain above them, so
  raise them only with a measurement (see [docs/3f-write-path.md](docs/3f-write-path.md)).
* `SOLANA_TOKEN_BRIDGE_PROGRAM` is a single address and is still the only program
  setting `config.rs` reads. `protocols::registry` already has a parser for
  `SOLANA_BRIDGE_PROGRAMS` (a `protocol:program_id` CSV), but nothing reads that
  variable yet. A4 wires it and retires the single-address setting.
* `CHAIN_REGISTRY_REFRESH_SECS` (optional, default 60, minimum 5) is how often the chain
  registry reloads from Postgres, so a manual `INSERT` or `UPDATE` takes effect without a restart.
* Re-enrichment (all optional): `REENRICH_ENABLED` (default on, `0` disables),
  `REENRICH_INTERVAL_SECS` (30), `REENRICH_BATCH` (50), `REENRICH_MIN_AGE_SECS` (60),
  `REENRICH_RETRY_SECS` (120), `REENRICH_MAX_AGE_SECS` (7 days) and
  `REENRICH_ORIGIN_SHARE` (40, the percent of each batch reserved for the missing-origin
  backfill; `0` disables the reservation, `100` gives the whole batch to it). The pass
  also fills missing origin txs of completed inbound rows; `REENRICH_MAX_AGE_SECS` does
  not bound those, and the retry window uses `last_enrich_attempt_at`.
* `GET /health` reports `status` (`ok` or `degraded`), `postgres_ok`, `redis_ok`,
  `ingest_age_secs`, `stream_lag` and `stream_pending` (from `XINFO GROUPS
  bridge:transactions`), and `dead_letter_len` (`XLEN bridge:transactions:failed`).
  It never calls Helius or WormholeScan. `ingest_age_secs` is the age of
  `indexer_state.updated_at` for `solana_last_ingested_slot`, which the ingester
  bumps on every parsed notification, transfers or not. The answer is cached in
  memory for 15 s (Upstash bills per command) and concurrent polls share one
  measurement. `HEALTH_MAX_INGEST_AGE_SECS` (optional, default 3600, minimum 1) is
  the age past which the heartbeat is stale; the default is deliberate, quiet
  25-minute gaps have happened in production. The endpoint answers 503 only when
  Postgres or Redis is down, or `ingest_age_secs` is *greater than* the threshold
  (exactly at it is still 200). A missing heartbeat (fresh database, or ingestion
  never started) is `degraded` with 200.
* The database is Postgres 18. Tests and CI run against 18, not older.

## Testing

```bash
cargo test
```

* `tests/decode_unit.rs` — VAA decoding (hand-built VAAs, malformed input),
  address decoding, amount formatting. No network, no Docker.
* `tests/fixtures_decode.rs` — runs the pure decoders over real captured
  provider responses in `tests/fixtures/`. Capture new ones with
  `scripts/capture_fixtures.sh`.
* `tests/golden_fixtures.rs` — runs the protocol registry over the fixture
  pairs and compares the decoded transfers with WormholeScan's decoding. Inbound
  redeems are decoded from the captured posted VAA accounts
  (`tests/fixtures/account_<name>_vaa.json`, a `getAccountInfo` response) and checked
  against WormholeScan (message key, chains, amount, token, destination wallet).
  Without the account they are asserted `Unsupported`.
* Unit tests in `src/ingestion/backfill.rs`: no backfill without a checkpoint, the rewound checkpoint window including the checkpoint's own slot, stop at the rewound cutoff, checkpoint below the rewind floored at 0, failed transactions and entries without a slot or signature skipped, the `INGEST_BACKFILL_MAX` cap, and the empty page. `src/ingestion/checkpoint.rs` covers the 4c invariant: live slots deferred while the backfill runs, a connection dropped mid-backfill leaves the checkpoint unchanged and the next gate starts from it, completion releases the newest live slot and opens the gate, completion without live messages persists nothing, a failed backfill stays closed, and no-backfill connections persist immediately. The Redis dedupe has no automated test (no Redis container in the suite yet).
* A7 in `tests/bridge_transfers_schema.rs`: a redeem is stored as one `Inbound`, `Completed` row with the redeem as destination tx and no origin tx, enrichment fills the origin tx and a later analysis without enrichment keeps it, a completed inbound row whose origin tx is still missing is due for the background backfill while one that learned it (and an outbound send) is not, and the repair migration turns rows stored like sends around (and leaves real sends alone). The source-wallet repair is covered there too: an inbound row with a wallet loses it and its cached analysis, an outbound row and an inbound row that is already NULL are untouched, and a second run is a no-op. In `tests/golden_fixtures.rs`, the captured redeems become `Completed` inbound transfers and the matched WormholeScan operation supplies exactly the origin tx WormholeScan reports. Unit tests in `src/services/detect.rs` cover the mapping, the origin tx rules (only for a matching operation, never overwritten, never for a send, EVM `0x` normalisation) and the legacy-path fix; unit tests in `src/enrichment/wormholescan.rs` cover the `/vaas` fallback (empty operations fill the origin, EVM hashes get `0x`, a VAA 404 leaves the row unchanged, a known origin is not asked for again), and `src/domain/bridge_transfer.rs` the derived direction.
* `tests/persistence_idempotency.rs` — real Postgres 18 container (via `testcontainers`,
  needs Docker): redelivering the same transaction, the scenario `XAUTOCLAIM` recovery
  guarantees will happen, never produces a duplicate row in `transactions`,
  `bridge_transfers` or `wormhole_messages`.
* `tests/list_transfers.rs` — real Postgres 18 container (needs Docker): keyset paging
  visits every row exactly once when `created_at` repeats across page boundaries, every
  filter and filter combination selects the expected rows, `from` is inclusive and `to`
  exclusive, the cursor combines with the range, limit and cursor errors are 400s, the
  last page has no cursor, and an empty database is an empty page. Unit tests in
  `src/services/transfers.rs` cover the opaque cursor round trip, limit bounds, direction
  and status validation, chain slugs, timestamp parsing and empty values without Docker.
* `tests/bridge_transfers_schema.rs` — the A2 schema on Postgres 18: legacy rows are
  staged under the old schema and migrated (known and unknown chains, `u128::MAX`,
  non-integer amounts, analysis kept), new writes land in the protocol-agnostic shape,
  several transfers share one observed tx, the database rejects bad `direction` and
  `status`, and SQL `resolve_chain` agrees with the Rust registry. Set
  `TEST_DATABASE_URL` to use a server you already run instead of Docker.
* Unit tests in `src/services/correlator.rs`: `Pending` only for a destination the registry
  resolves, `Detected` otherwise, `Completed` with an explorer URL from the registry, and no
  explorer URL (but no failure) for an unknown chain.
* Unit tests inside `src/protocols/*` and `src/chains/mod.rs`: routing on inner calls,
  required accounts (sorted, deduplicated, asked only of matching adapters), override
  parsing, compose merging, "only active rows resolve", and the Wormhole adapter
  (outbound decode, with-payload, router-made calls, several transfers per tx, failed tx,
  missing or foreign `Sequence` logs, malformed and truncated data; inbound redeems for
  all four `complete_*` variants, a missing or foreign-owned VAA account, garbage account
  data and a payload addressed to another chain). `protocols/wormhole/posted_vaa.rs`
  covers the account layout parser: identity and payload, wrong magic, truncated and
  oversized accounts, and a lying payload length. No network, no Docker.
* `tests/chain_registry_db.rs` — real Postgres container (needs Docker): the migration
  seeds the ten chains, one chain resolves to one key from every namespace, unknown IDs
  are unresolved, `pending_review` mappings resolve only once activated, the database
  rejects unknown status values, and a failed refresh keeps the previous snapshot.
* Unit tests in `src/clients/wormhole.rs`, `src/clients/helius.rs` and `src/services/detect.rs`:
  transfer mapping, missing metadata, foreign chain namespaces, enrichment (no VAA, matching
  VAA, VAA for another message), destination tx path priority, empty or absent values,
  `Pending` to `Completed` only, `Completed` never rewritten, a matching VAA never lowering a
  status, and the `getMultipleAccounts` response parser (accounts and missing entries in
  order, wrong count, bad base64). `tests/bridge_transfers_schema.rs` also checks that a
  later analysis without enrichment does not downgrade a stored status.
* Unit tests in `src/enrichment/`: the WormholeScan provider on prefetched responses
  (destination tx completes, mismatched VAA and missing VAA change nothing, "not found" is
  an answer while other errors are not, a `Completed` transfer is never moved backwards)
  and the re-enrichment decision (advanced, unchanged but persisted, unreachable provider
  persists nothing). The due-rows queries are covered in `tests/bridge_transfers_schema.rs`:
  a completed inbound row without an origin tx is due, the retry window and a learned
  origin tx take it out of the queue, and an outbound send stays on the open queue. The
  same file covers queue rotation (more due rows than a batch are all attempted by
  successive passes, and an attempted row stays out of the retry window) and a repaired
  row whose cached analysis was dropped being rebuilt from its stored columns.
  `tests/reenrich_backfill.rs` runs the pass against the fake upstream: a twenty-row open
  backlog still leaves half of a six-row batch to the backfill, and the next pass reaches
  the remaining origin rows.

***

# Roadmap

Every checkpoint follows the same loop:

1. Branch: `feat/…` for features and new protocol adapters, `fix/…` for correctness, `perf/…` for speed, `ci/…` / `chore/…` for infra, `docs/…` for docs-only changes. One change per branch.
2. Tests first or alongside. Fixtures for anything that parses provider data.
3. Run the benchmark for that phase, same workload as the previous tag.
4. Add a row to [Results](#results): version, change, before, after.
5. PR, CI green, merge, tag (`v0`, `v1`, …), deploy.
6. Re-run the benchmark on EC2. Numbers in this README are EC2 numbers; laptop numbers are for iteration only.

Rule for the numbers: two runs, warm and cold kept separate, read the status
code distribution (a 4xx/5xx still counts as "success" in most load tools),
record the git SHA and instance type next to every row.

## Phase 0 — foundation

* \[x] GitHub Actions CI: `cargo fmt --check`, `clippy -D warnings`, `cargo test`
* \[x] Unit tests for VAA decode, address decode, amount formatting
* \[x] Fixture capture script and fixture-driven decode tests
* \[x] `.env.example`, README drift fixed
* \[ ] Tag `v0` (baseline numbers below)

Exit: CI green on `main`.

## Phase 1 — protocol-agnostic core (metric: correctness)

Problem 1: the product is a Solana bridge transaction indexer, but the code is a
Wormhole transfer indexer. The domain model, schema, chain handling and
detection are all Wormhole-shaped, and detection is gated on WormholeScan.

Problem 2 (fixed in A5b): every transfer reported `Pending`. The destination tx was read from
`globalTx.destinationTx.txHash` on the `/operations` response, which does not
carry that field, and the old `WormholeScanAdapter::find_transaction` (which hit
`/global-tx/...`) was never called. That adapter is gone (A3), the lookup lives in `enrichment/`.

Delivery is one protocol per phase. Phase A builds the core with Wormhole as the
only real adapter. Phases B and later add one protocol each.

### Phase A — protocol-agnostic core, Wormhole only

* \[x] A1. Adapter trait and registry: each adapter declares its program IDs and classifies/decodes a Solana tx into a bridge event, or rejects it with a reason. Decode is pure (no network, no state). Modelled on the existing `chain_adapters` registry. Done: `BridgeAdapter`, `ProtocolRegistry`, pure `SolanaTx` (#3), Wormhole Token Bridge outbound decode, wiring into `analyse_tx`, and A1b inbound redeems: adapters declare the accounts they need (`required_accounts`), the fetch step preloads them into `SolanaTx::accounts` with `getMultipleAccounts`, and the Token Bridge adapter decodes the redeem from the posted VAA account with an in-house parser
* \[x] A2. Generalised domain model and schema: `protocol`, optional `via`, `direction`, source and destination chain references, a protocol-specific message identity with a unique key per protocol, `Detected`/`Pending`/`Completed` plus `Failed`/`Refunded`, `NUMERIC` amounts. `bridge_transfers` (protocol-agnostic) is split from `wormhole_messages`. Existing rows are migrated in place and tested on staged legacy data
* \[x] A3. DB-backed chain registry: canonical `chains` table plus a `(protocol, protocol_chain_id) -> chain` mapping table, seeded with the current 10 chains, replacing the `ChainId` enum and `from_wormhole_id`. Unknown IDs are stored as `unresolved(protocol, id)`, never dropped and never a crash. The registry carries the status and provenance needed for the later LLM-proposal upgrade, which is not built in this phase... Done: tables, seed, `load_snapshot`, DB tests (#4), SQL `resolve_chain`, and unresolved IDs persisted on transfers (A2). Remaining: move callers off `ChainId` and `from_wormhole_id`, wire the snapshot refresh task
* \[ ] A4. All Wormhole programs on Solana, not only the Token Bridge (Token Bridge, NTT and others, each after its program ID and layouts are verified against official docs or source). One `logsSubscribe` per program ID, since `mentions` accepts a single address, with a checkpoint per program. `SOLANA_TOKEN_BRIDGE_PROGRAM` is replaced by a configured list
* \[x] A5. Chain-first detection: a transfer is persisted as `Detected` from Helius data alone. WormholeScan is an enrichment that fills the status and no longer gates persistence. Done for Wormhole outbound in `analyse_tx` (`services::detect`), cross-checked against the on-chain message ID. Done (A5b): the destination tx fix, the WormholeScan lookups behind an `EnrichmentProvider` in `enrichment/`, and a background re-enrichment pass (`enrichment::reenrich`) that moves `Detected` and `Pending` rows forward and backfills missing inbound origin txs. Several transfers per tx are decoded but only the first is stored until the v2 model
* \[ ] A6. Composition: one transfer per message identity, the outer protocol or router recorded as `via`, and a test that an outer and an underlying adapter seeing the same tx produce one row... Partly done (#3): `compose()` with unit tests. The end-to-end outer+underlying test waits for a second adapter
* \[x] A7. Direction: Solana-outbound sends and Solana-inbound redeems both indexed. Done: `analyse_tx` stores redeems as `Inbound` rows (`Completed` at detection, the redeem tx as destination tx, `source_tx_hash` NULL until WormholeScan's matched operation names it), `source_tx_hash` is nullable in `/api/v1`, and a migration repairs redeems the old path stored like sends. The re-enrichment pass now also revisits completed inbound rows whose origin tx is still NULL, so it is filled once WormholeScan names it without waiting for another `/analyse` (served by the partial index `idx_bt_missing_origin`). The backfill records every attempt in `last_enrich_attempt_at`, orders by it NULLS FIRST, keeps a guaranteed share of each batch (`REENRICH_ORIGIN_SHARE`) and rebuilds rows whose cached analysis a repair migration dropped
* \[x] A8. Ingestion is always on, in this phase and every later one, and no longer tied to `/stream` clients, so the database is a complete record of what the programs emitted. Done: the ingester starts with the process under `spawn_supervised`, `on_demand` and the `/stream` client counter are gone, and the Helius spend is bounded by an ingestion budget: backfill on by default but skipped without a checkpoint, capped at `INGEST_BACKFILL_MAX` signatures and paced by `INGEST_BACKFILL_PACE_MS`, and a Redis `SET NX EX` dedupe so a signature seen by both the backfill and the live feed (or twice after a reconnect) is published once. Workers still read through the live Helius limiter (4/s on the free plan), so a burst queues in the Redis stream instead of being dropped. A real per-key budget comes with the provider lanes (1c)
* \[x] A9. Fixtures captured from real txs (outbound, inbound, admin/attest rejected, unsupported variant) and golden tests per adapter. Inbound fixtures also carry the captured posted VAA account (`account_<name>_vaa.json`), and the golden tests decode redeems from it (A1b)
* \[x] A10. API versioning: `/api/v2` serves the generalised model (protocol, direction, via, chain references, all transfers of a tx). `/api/v1` keeps its current Wormhole-shaped response through a compatibility mapper until it is retired. Amounts are stored as `NUMERIC` so volume can be aggregated. Done: `/api/v2/transactions/analyse` reads every stored transfer of a tx with raw and resolved chain references, and `/api/v1` is the compatibility mapper over the same response, pinned byte for byte by fixture snapshots; the WebSocket stream is unchanged. Every decoded transfer is persisted with its `event_index`

Benchmarks: classification precision on the fixture set; pure classify ns/op (criterion).
Exit: Wormhole transfers are persisted as `Detected` with WormholeScan fully unavailable; every non-transfer fixture is rejected with a reason; the migration is tested on staged existing rows (done in A2).

### Phase B and later — one protocol per phase

Each phase adds one protocol, in descending order of Solana bridge volume, and
follows the same steps:

1. Derive the current ranking from DefiLlama bridge data for Solana. Nothing is hardcoded from memory.
2. Verify the protocol's program IDs and instruction/event layouts against its official docs or source.
3. Capture fixtures from real transactions (`scripts/capture_fixtures.sh`) and write golden tests, including admin/attest transactions that must be rejected.
4. Write the adapter, its enrichment provider if the protocol has an API, and its chain-ID mappings for the registry.
5. Cover composition with any protocol already indexed (Mayan over Wormhole and CCTP, aggregators over everything).

Candidates to evaluate: Mayan (Swift, MCTP), Circle CCTP, LayerZero/Stargate,
deBridge, Allbridge, Hyperlane, Chainlink CCIP, Across. The ranking decides
which of these actually ship and in what order.

### Later upgrade — unknown-chain resolution (design now, build later)

Out of scope for the first delivery. Phase A only ships the registry shape and
the `unresolved` handling. The constraints the design has to satisfy:

* LLM output is a proposal, not a fact. It is written to the registry as `pending_review` and auto-activated only when it can be cross-checked against an authoritative source (Wormhole chain list, Circle's CCTP domain list, LayerZero endpoint metadata). An unverified guess never labels real transfers.
* Lookups are rate-limited and cached, one per unresolved `(protocol, id)`, with a negative cache.
* Activating a mapping resolves the transfers already stored as `unresolved` for that `(protocol, id)`.

### Remaining Phase 1 items

Provider lanes, replay and the decode bench stay in this phase. Items 1a, 1b and
1d are delivered by Phase A (1a by A1, 1b by A5, 1d by A9).

* \[x] 1a. Split `analyse_tx` into fetch (IO), decode (pure, no network or state), enrich (IO). Delivered by A1
* \[x] 1b. Destination tx resolution: operation payload paths first, then the `global-tx` lookup through the WormholeScan limiter. Delivered by A5b, as the WormholeScan enrichment provider
* \[x] 1c. Provider lanes: `Lane::Api` and `Lane::Ingest`, each with its own Helius URL (`HELIUS_API_URL`, `HELIUS_STREAM_URL`, both falling back to `HELIUS_URL`), its own limiter (`HELIUS_API_RPS`, `HELIUS_LIVE_RPS`) and its own token patience. `build_registry` takes a lane, `AppState` carries its lane and endpoint (`state.helius_url`), and `AppState::for_ingest` derives the ingest copy for `ROLE=all`; the `helius_live_url` config field and `AppConfig::for_live` are gone. The `HELIUS_LIVE_URL` variable is still read as a deprecated alias for `HELIUS_STREAM_URL` (with a startup warning) so an existing `.env` keeps its live key. A split role needs only its own lane's URL. The `reconcile` tool keeps its own `HELIUS_RECONCILE_URL` lane
* \[x] 1d. Golden tests per fixture, status matrix test. Delivered by A9, with the matrix redefined for on-chain-first statuses: every outbound fixture is `Detected` from its on-chain decode alone, a matched VAA without a destination tx raises it to `Pending`, and a matched VAA plus its destination tx to `Completed`; no VAA, a VAA for another message (even with a destination tx), or a destination tx with no VAA leaves it `Detected`. Registry membership no longer chooses a status: the same rows produce the same status with an unregistered destination (only resolution and explorer URLs differ). Inbound redeems are `Completed` at detection. The old WormholeScan-driven matrix (known tx → `Completed`, none + registered chain → `Pending`, unregistered → `Detected`) remains only for the fallback path in `services::correlator`, tested there.
* \[x] 1e. `src/bin/replay.rs`: take the last N signatures, run the pipeline, compare our status with WormholeScan's `global-tx`
* \[ ] 1f. Criterion bench for the pure classify/decode path

Benchmarks: classify/decode ns/op (criterion); replay agreement %, Pending %, errors by kind, cold p50/p99.
Exit: ≥ 99% agreement on transfers older than 15 minutes; `Pending` only when WormholeScan itself has no destination tx.

## Phase 2 — deploy pipeline and roles

* \[x] `ROLE=api|stream|all`. `api` serves `/analyse`; `stream` runs ingester, workers, recovery and `/stream`. Shared Postgres and Redis. Deploy exit criteria met: two systemd units on the main EC2 box, `bridge-api` (port 8080, `ROLE=api`) and `bridge-stream` (healthz on 8081, `ROLE=stream`), both reading the same `.env`; the old `bridge` unit is disabled and kept as the rollback. The 3g rerun ran against this deployed service (`docs/scaling-log.md`)
* \[ ] Hostnames `api.<domain>` and `stream.<domain>`. Each process's own Helius key is configurable since 1c (`HELIUS_API_URL`, `HELIUS_STREAM_URL`); what is left is the hostnames and setting the second key on the box
* \[ ] CI builds the release binary; CD over SSH on merge to `main`: deploy key, one systemd unit per role, health check, rollback to previous binary
* \[x] `GET /health`: Postgres and Redis ping, ingest heartbeat age, stream lag and pending, dead-letter depth. 200 normally, 503 on a dependency outage or stale ingest; cached 15 s
* \[ ] Metrics: request latency histogram, cache hit/miss counters, limiter wait time, stream lag
* \[x] Host-installed Caddy in front of the API: TLS, HTTP/2 and compression in `deploy/Caddyfile`; 8080 stays closed and WebSocket and CORS pass through unchanged
* \[ ] Hardening: compose publishes Postgres/Redis on `127.0.0.1` only, no default credentials on a public box, security group allows only 80/443 and SSH from a fixed IP
* \[x] Load generator on a second instance in the same VPC, larger than the `t3.micro` used for v0

Exit: merge to `main` deploys both roles; the API benchmark runs against the deployed service.

## Phase 3 — scale the API (reads and writes)

Measure before changing anything. Each item is one tagged experiment with the same workloads.

* \[ ] 3a. True server ceiling: re-run `R-hit` with a stronger load generator; profile (samply / `perf`, `tokio-console`); explain the 33–45 ms latency cluster
* \[x] 3b. `v1` cache pre-serialized response bytes instead of cloning `NormalisedTransaction` and re-serializing per hit
* \[x] perf-1. mimalloc as the global allocator
* \[x] perf-2. `AppState` behind one `Arc<Inner>`, so clone is one refcount operation
* \[x] 3c. `v2` pre-serialized response body, as 3b did for `v1`. A warm `v2` hit is already a single cache lookup; the per-hit cost was the response clone and JSON serialization
* \[ ] 3d. `v3` two API instances behind a proxy: per-process caches duplicate refreshes; compare a Redis-shared cache against sticky routing
* \[x] 3e. Cold path: fake upstream server replaying fixtures with injected latency and 429s so quota isn't burned; try JSON-RPC batching for Helius, priority for user requests over background refresh, limiters set to the real plan limits
  Helius batching not pursued (free plan, unverified credit accounting, cold path already limiter-bound).
* \[x] 3f. Write path: consumer throughput against the fake upstream. Try larger `XREADGROUP COUNT`, concurrent processing inside a worker, batched upserts per Postgres transaction, pool sizing against RDS limits
* \[x] 3g. List endpoint using the `idx_bt_*` indexes with keyset pagination, benchmarked on ~1M seeded rows with `EXPLAIN ANALYZE`

Exit: documented ceiling for each workload and what limits it (CPU, Postgres, provider quota).

## Phase 4 — WebSocket stream (metric: recall and precision)

First build `src/bin/reconcile.rs`. Ground truth is per program: for each
subscribed program ID and a time window, take `getSignaturesForAddress` as the
set of transactions that touched the program, run the adapter's pure classifier
over them offline, and diff the ones classified as transfers against the
database. Report, per program: missed count, missed %, and ingestion lag (block
time → published).

Recall alone is not enough, because many transactions that touch a bridge
program are admin or attest calls. The same run also reports classification
precision: of the transactions we persisted as transfers, the share confirmed
by an independent source (the protocol's own API, or a hand-labelled sample),
plus the rejection counts by reason. That is the stream's `v0`.

Known suspects, most likely first:

1. \~~Checkpoint is slot-granular and backfill stops at `slot <= checkpoint`, so other transactions in the checkpoint's slot are lost on any reconnect~~ — fixed in 4b: backfill starts `INGEST_REWIND_SLOTS` (20) before the checkpoint, floored at slot 0, and the Redis dedupe TTL (2 h) covers the replay
2. \~~Backfill runs before the read loop with a 150 ms sleep per signature, so live notifications queue up behind it~~ — fixed in 4c: the backfill is a per-connection task running next to the read loop, and a checkpoint gate keeps live slots from passing an unfinished backfill
3. \~~No keepalive or watchdog: a silently dead socket produces neither errors nor data~~ — fixed in 4d: a ping every `INGEST_PING_SECS` (30) and a no-frame watchdog at `INGEST_WATCHDOG_SECS` (90) that forces a reconnect
4. \~~A rejected `logsSubscribe` is swallowed (the check is unreachable)~~ — fixed in 4e: the reply is awaited before the read loop and a rejection or timeout is a failed connection. Any publish error still tears down the connection
5. \~~The ingester is not under `spawn_supervised`, so a panic stops it permanently~~ — already handled by A8; the ingester starts under `spawn_supervised`
6. \~~The checkpoint is written on every message and can move backwards on out-of-order slots~~ — fixed in 4e: the upsert uses `GREATEST`, so it only moves forward

Work items:

* \[x] 4a. `reconcile` bin: per-program recall, classification precision, and baseline over 24 h. The bin and its pure tests are built; the 24 h baseline is recorded in [Phase 4 — stream](#phase-4--stream)
* \[x] 4b. Rewind the checkpoint a few slots on reconnect and dedupe by signature (persistence is already idempotent). Done: backfill starts `INGEST_REWIND_SLOTS` slots before the checkpoint (default 20, floored at 0), still inside `INGEST_BACKFILL_MAX` and `INGEST_BACKFILL_PACE_MS`, and the Redis dedupe drops anything already published
* \[x] 4c. Backfill as a separate task, concurrent with live reads. Done: after `logsSubscribe` is confirmed the backfill is spawned as a child of the connection and the read loop starts immediately; the task is cancelled and awaited when the connection ends, so a reconnect starts one fresh backfill from the checkpoint. A pure per-connection `CheckpointGate` keeps live notifications from advancing the persisted checkpoint until the backfill finishes (failure or panic leaves it closed, and the next reconnect retries), while `ingest_backfill_inflight`, `ingest_backfill_duration_seconds` and `ingest_backfill_published_total` report the task. Notes: [`docs/4c-concurrent-backfill.md`](docs/4c-concurrent-backfill.md)
* \[x] 4d. Ping/pong keepalive and a no-message watchdog that forces a reconnect. Done: a ping every `INGEST_PING_SECS` (default 30), the watchdog resets on every frame and reconnects after `INGEST_WATCHDOG_SECS` (default 90) of silence, and `ingest_reconnects_total{reason}` counts each ended connection
* \[x] 4e. Surface subscribe errors, supervise the ingester, `GREATEST` on the checkpoint. Supervision was already in place from A8; subscribe replies are awaited and surfaced, and the checkpoint upsert is monotonic
* \[x] 4f. Periodic reconciliation poll as a completeness guarantee: WebSocket is the low-latency path, polling is the safety net. Done: the supervised `reconcile-poll` task runs every `INGEST_POLL_SECS` (default 300, minimum 30) per registered program over `[last_poll_end - INGEST_POLL_OVERLAP_SECS, now - INGEST_POLL_OFFSET_SECS]`, subtracts signatures already in `bridge_transfers`, and republishes the rest through the normal stream so the consumers classify and persist them. The per-program checkpoint is monotonic in `indexer_state`, each cycle is capped at `INGEST_POLL_MAX_PUBLISH` (500), and the poll has its own reconcile Helius lane. Notes: [`docs/4f-reconcile-poll.md`](docs/4f-reconcile-poll.md)
* \[x] 4g. Evaluate Helius' other streaming options against current docs before building anything custom. Done: Enhanced WebSockets need Developer+, LaserStream gRPC mainnet needs Business+, webhooks are limited to 1 on Free and need a public endpoint. Decision: keep `logsSubscribe` and build 4f as the safety net. Notes: [`docs/4g-helius-streaming-options.md`](docs/4g-helius-streaming-options.md)

Exit: recall ≥ 99.9% for every indexed program over 24 h, classification precision and p99 ingestion lag recorded.

## Out of scope

Ingesting bridge transactions that happen only on other chains. The indexer
decodes what happens on Solana. Other chains appear as counterparties: through
the chain registry and the enrichment providers that resolve the other leg, never
as ingestion targets. This replaces the earlier multi-chain ingestion phase
(Ethereum, BSC and other EVM listeners).

***

# Benchmarks

## Workloads

| ID | What | Tool |
| --- | --- | --- |
| `R-hit` | 769 distinct signatures, warm cache, 50 connections, 30 s | `oha` |
| `R-mix` | 90% hot / 10% cold keys against the fake upstream | `oha` |
| `R-cold` | all-unique signatures against the fake upstream (50 ms latency) | `oha` |
| `R-junk` | invalid hashes, must never reach an upstream | `oha` |
| `W-ingest` | 100k candidates `XADD`ed, fake upstream, measure persisted tx/s and stream lag | custom bin |
| `S-recall` | per-program 24 h reconciliation against `getSignaturesForAddress` | `reconcile` bin |
| `S-precision` | of transactions persisted as transfers, share confirmed by an independent source, plus rejections by reason | `reconcile` bin |
| `D-decode` | pure classify/decode path, ns per operation | `criterion` |
| `C-agree` | status agreement with WormholeScan over the last N signatures | `replay` bin |
| `L-first` | `GET /api/v2/transfers?limit=50`, 1M seeded rows | `bench_list.sh` |
| `L-deep` | the same at about 80% depth with the cursor | `bench_list.sh` |
| `L-filtered` | `direction=Inbound&status=Completed&chain=ethereum`, limit 50 | `bench_list.sh` |

Reproducing `R-hit`: build a file with one URL per line (valid Solana
signatures only), then run it twice. The first run fills the cache, the second
is the warm number.

```bash
oha -z 30s -c 50 --urls-from-file urls.txt
```

Reproducing the list workloads: seed a throwaway database with
`src/bin/seed_transfers.rs`, build the deep cursor from it, then run
`scripts/bench_list.sh`. The steps, including the cursor derivation, are under
"How to run the 3g list benchmarks" in [docs/scaling-log.md](docs/scaling-log.md).
Each workload is run twice and kept separate, and every row records the
server SHA and instance type. The measured run used `cold_start=no`, so run 1
is a first run, not a restart-cold run.

## Results

Server: `m7i-flex.large` (2 vCPU). Load generator: `t3.micro` in the same VPC,
the first thing to hit 100% CPU. 30 s runs, 50 connections, 769 distinct
transactions, 1.2 KiB responses.

### Baseline (`v0`)

| Workload | Result |
| --- | --- |
| `R-hit` | 36.5k and 37.1k req/s over two runs, 1.1M requests each, every response a 200 |
| `R-hit` latency | about 0.8 ms median, p99 about 33 ms |
| `R-hit`, 200 connections | 33.9k req/s, load generator CPU at 100% |
| Cold burst, 50 concurrent requests for one hash | 1 upstream fetch, all 50 served in a 28 ms window |
| `R-junk` | about 55k req/s of cheap 400s, zero upstream calls |

What these do and don't mean:

* Cache-hit throughput is a lower bound: at 200 connections the load generator was CPU-bound, so the server's ceiling hasn't been measured.
* Uncached transactions are limited by the providers. With the default quotas (6/s Helius for the API, 20/s WormholeScan) that is roughly 6 new lookups per second on the free plan; overflow gets a 503 after 2 s.
* There is a cluster of 33–45 ms requests that hasn't been explained yet.
* The cache lives in one process, so several instances would each refresh their own copies.

### Re-baseline (4ffb783)

New hardware, before the Arc AppState change: `c7i.xlarge` load generator,
server CPU-bound, generator about 72% idle. 30 s runs, 50 connections, the same
769 transactions.

| Workload | Result |
| --- | --- |
| `R-hit` | 77,275 and 77,302 req/s over two runs, 50 connections, every response a 200 |
| `R-hit` latency | 0.64 ms median, p99 1.05 ms |
| `R-hit`, 200 connections | 75,118 req/s, server CPU-bound |

v0's ~33 ms p99 was measured with the `t3.micro` generator at 100% CPU and is
not comparable to the re-baseline's 1.05 ms. The latency difference is not
attributable to the Arc change.

### 3b — pre-serialized `/api/v1` cache bodies (03bbb0d)

Against `35d3e1b` on the same hardware: 86,115 → 132,082 req/s at 50
connections (+53.4%), 83,293 → 124,969 at 200 (+50.0%) and 81,711 → 124,706 at
400 (+52.6%). At 50 connections median latency went from 0.58 to 0.38 ms and
p99 from 0.94 to 0.59 ms. These are the means of the initial and repeated
rounds for each build.

The API ran as a deployed instance with ingestion off, against 769 URLs, 30 s
runs, warm-up run excluded. Server CPU-bound; the load generator was about
66.5–78.6% idle in the old runs and 66.5–69.5% idle in the new ones, so these
figures are not capacity guarantees. Every run reported 100% success and zero
non-200 responses.

The change removes three costs per warm hit: the deep clone of the cached
entry, the v1 remap, and per-hit serialization. The numbers are their combined
effect and do not isolate any one of them. Methodology and per-run numbers:
[docs/scaling-log.md](docs/scaling-log.md).

### 3e-2 — Live workers wait for Helius tokens

Live workers, recovery, and backfill now wait up to 20 seconds for a Helius
rate-limit token, while API requests retain the existing 2-second wait. This
lets queued transactions tolerate temporary rate-limit pressure instead of
unnecessarily entering the dead-letter stream.

* **Before:** 197 `service overloaded` dead-letter entries initially; the count continued growing during redrive, reaching 237.
* **After:** No new dead-letter entries or overload log lines observed after deployment. The 7 remaining entries were successfully redriven, leaving 0.
* **Rate limit and sizing:** Approximately 4 tokens/s. Five workers with `XREADGROUP COUNT 10` can have up to 50 messages waiting for tokens.
* **Validation limitation:** Only a few messages arrived after deployment. This is evidence of normal-load behavior, not proof that a large burst or 400-entry redrive will succeed.
* 4b + 4d: a real WebSocket read error recovered in 6 s with the checkpoint rewound by 20 slots (3 h reconcile 97/97, 0 missed), and a blackholed stream tripped the 90 s watchdog, reconnected and backfilled the same way. The 24 h clean-window reconcile on this build (2026-11-05T18:01Z to 2026-11-06T18:01Z) passed: 594/594 persisted, 0 missed, lag p50 1 s / p99 3 s / max 11 s, precision 20/20. Link to the detailed notes: [`docs/4bd-reconnect-verification.md`](docs/4bd-reconnect-verification.md).

Link to the detailed notes: [`docs/3e-worker-patience.md`](docs/3e-worker-patience.md).

**Postscript.** The 20 s wait only covered the first Helius token of a message;
every retry and the account preload still shed after a hardcoded 2 s. On the
29 Oct restart backfill that was not enough — workers logged `error=Overloaded`
and dead-lettered 41 of about 467 backfilled transactions. The wait is now
`HELIUS_LIVE_TOKEN_WAIT_MS`, and `Overloaded` is no longer a dead-letter reason at
all: it is transient, so the message stays pending and recovery redelivers it,
bounded by the delivery budget (`WORKER_MAX_DELIVERIES`). Backfill also publishes
no faster than the live limiter drains (`INGEST_BACKFILL_PACE_MS`, now 750 ms).

### 3f — Write path: consumer throughput against the fake upstream

5000 pre-filled signatures, five workers, production defaults, fake upstream at
zero latency. With the Helius lane raised to 100000 tokens/s the write path
sustains **about 0.9k messages/s on EC2** (RDS, local Redis; 2.35k on the laptop)
with 5000/5000 rows and no dead letters; with the real 4/s quota it is
quota-bound at 2.3–2.4 messages/s,with Helius limiter wait averaging 4.0 s per call. Larger `XREADGROUP COUNT`,
bounded per-worker concurrency, batched upserts and a bigger pool were flat or
worse, so no behavioural change was kept. The isolated ceiling is Postgres work
per message; real traffic has distinct message keys, whereas the six fake
fixtures collapse onto one hot `bridge_transfers` row.

Method, both configurations, every before/after row and the RDS connection
assumption: [`docs/3f-write-path.md`](docs/3f-write-path.md).

### perf-1: mimalloc allocator

Against `16018712` on the same `m7i-flex.large` server and `c7i.xlarge`
generator: 130,817 → 139,345 req/s at 50 connections (+6.5%), 125,318 →
134,973 at 200 (+7.7%) and 123,239 → 131,172 at 400 (+6.4%). At 50 connections
median latency went from 0.38 to 0.35 ms and p99 from 0.59 to 0.57 ms. The 50
connection figures are the means of two runs per build; 200 and 400 are single
runs each. Server CPU-bound, the load generator about 70.4% idle in the
mimalloc runs, and every run reported 100% success with zero non-200 responses.

| Connections | `16018712` (glibc), req/s | `59fe708` (mimalloc), req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 130,817 (131,033 and 130,601) | 139,345 (139,755 and 138,936) | +6.5% |
| 200 | 125,318 | 134,973 | +7.7% |
| 400 | 123,239 | 131,172 | +6.4% |

The reason for the change: a profile at `16018712` put about 12.5% of server
time in glibc allocation (`malloc`, `_int_malloc`, `_int_free`,
`malloc_consolidate`, `realloc`, `cfree`). During the mimalloc runs the server
was at 50.1 user, 23.1 system and 27.1 idle CPU, against about 57% user CPU
with glibc.

These are single comparison runs per connection count (two at 50), the glibc
runs were not interleaved with the mimalloc runs, and they cover `R-hit` only:
nothing is known about the cold path or writes, and memory use under mimalloc
was not measured. Methodology and per-run numbers:
[docs/scaling-log.md](docs/scaling-log.md).

### perf-2: AppState behind one Arc

Against `06b891a` on the same `m7i-flex.large` server and `c7i.xlarge`
generator: 142,095 → 156,819 req/s at 50 connections (+10.4%), 134,784 →
147,438 at 200 (+9.4%) and 130,376 → 145,689 at 400 (+11.7%). At 50 connections
median latency went from about 0.35 to 0.31 ms and p99 from about 0.55 to
0.50 ms. The runs were interleaved old/new/old/new; the 50 connection figures
are the means of four runs per build and 200 and 400 are means of two runs
each. Server CPU-bound, and every run reported 100% success with zero non-200
responses.

| Connections | `06b891a` (before), req/s | `0144ea9` (after), req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 142,095 (141,846, 140,579, 142,260 and 143,697) | 156,819 (155,643, 156,057, 156,984 and 158,590) | +10.4% |
| 200 | 134,784 (133,581 and 135,986) | 147,438 (145,878 and 148,998) | +9.4% |
| 400 | 130,376 (129,347 and 131,404) | 145,689 (146,457 and 144,921) | +11.7% |

The reason for the change: a profile at `16018712` put about 4% of server time
in `drop_glue<AppState>` and `FromRef::from_ref`, and the old `AppState` clone
took about 8 refcount increments per request against 1 now.

The gain is larger than that 4%. A likely explanation, stated as a hypothesis
rather than a measured fact: the removed atomic refcount operations probably
also cause cache-line contention between cores, which a CPU profile does not
attribute cleanly.

The 50 connection ranges do not overlap (old 140.6k–143.7k, new
155.6k–158.6k), and the old runs agree with each other within about 1.3%.
Caveats: the runs were interleaved old/new/old/new; 200 and 400 connections
have two runs per version only; this covers `R-hit` only, so nothing is known
about the cold path or writes; server and load generator CPU were not recorded
for these rounds; the old baseline already includes mimalloc; and the tradeoff
is one extra pointer hop per field access through `Deref`. Methodology and
per-run numbers: [docs/scaling-log.md](docs/scaling-log.md).

| — | 3c: v2 pre-serialized body | — | — | — | — | — |

### 3c: v2 pre-serialized body

The `/api/v2` hit path already costs one cache lookup (`v2_completed`, then
`v2_open` on a miss), so the roadmap's "one lookup instead of four" no longer
applies. The per-hit cost was the deep clone of the cached
`TransactionResponse` and its JSON serialization on every request. `V2Analysis`
now serializes the v2 body once at fill time and a hit serves a `Bytes` handle,
as 3b did for `v1`. Status, headers and body bytes are pinned against the old
`Json` path by a test.

Tradeoffs: each cached entry holds one more serialized body in memory (up to 50k
completed entries), and a v2-only miss pays one more serialization on the cold
path.

Against `5a2fce3`, on `/api/v2`: 86,660 → 128,872 req/s at 50 connections
(+48.7%), 82,852 → 127,498 at 200 (+53.9%) and 80,290 → 121,888 at 400
(+51.8%). At 50 connections median latency went from 0.58 to 0.39 ms and p99
from 0.82 to 0.53 ms.

| Connections | `5a2fce3` (before), req/s | `0ed3edb` (after), req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 86,660 (mean of 4) | 128,872 (mean of 4) | +48.7% |
| 200 | 82,852 | 127,498 | +53.9% |
| 400 | 80,290 | 121,888 | +51.8% |

The runs were interleaved old/new/old/new. A `/api/v1` control pass on the same
day, at 50 connections, gave 127.9k req/s on the old build and 128.2k on the
new, so there is no `v1` regression. Server CPU is not comparable: the old1
sampler ran too long, so no CPU figures are reported.

Caveats: 200 and 400 connections have two runs per version; this covers
`R-hit` only, so nothing is known about the cold path or writes; the `/api/v1`
control is one pass each. Methodology:
[docs/scaling-log.md](docs/scaling-log.md).

### 3g — list endpoint

Version: branch `feat/3g-list-endpoint` (2026-10-31), measured at `f5813d1`.
Change: `GET /api/v2/transfers`, keyset pagination over
`(created_at DESC, id DESC)`, one added index (`idx_bt_token_created`), no
cache. Plans: [docs/3g-list-endpoint.md](docs/3g-list-endpoint.md).

Exit criteria, for every workload: 300 req/s and p99 < 150 ms on the EC2 box,
2xx only counted as success. Server: `m7i-flex.large`, 50 connections, 30 s per
run, about 7,050 rows in the table. `cold_start=no`: run 1 is the first run,
not a restart-cold run. Results: `bench-results/20261104T170246Z`.

| Workload | Run | req/s | p99 (ms) | 2xx | non-2xx | Result |
| --- | --- | ---: | ---: | --- | ---: | --- |
| `L-first` | 1 | 1,574 | 66 | all | 0 | met |
| `L-first` | 2 | 1,571 | 66 | all | 0 | met |
| `L-deep` | 1 | 1,463 | 90 | all | 0 | met |
| `L-deep` | 2 | 1,508 | 74 | all | 0 | met |
| `L-filtered` | 1 | 1,272 | 85 | all | 0 | met |
| `L-filtered` | 2 | 1,295 | 80 | all | 0 | met |

All six runs returned only 2xx responses; there were zero non-2xx responses.
The 2 s dry-run filtered p99 outlier was warmup noise and is not part of these
runs. There was no endpoint before, so there is no before number. Methodology:
[docs/scaling-log.md](docs/scaling-log.md).

### Phase 1 — correctness

| Version | Change | `C-agree` | Pending % | `D-decode` ns/op | Cold p50 / p99 |
| --- | --- | --- | --- | --- | --- |
| v0 | baseline | — | ~100% (bug) | — | ~360 ms median |
| v1 | | — | — | — | — |

### Phase 2 — deploy pipeline

| Version | Change | Merge → live | Rollback | Health check | Roles deployed |
| --- | --- | --- | --- | --- | --- |
| v0 | manual `git clone` + `cargo run` | — | — | none | 1 (all in one) |
| | | | | | |

### Phase 3 — API reads and writes

| Version | Change | `R-hit` req/s | `R-hit` p99 | `R-cold` req/s | `W-ingest` tx/s | Limited by |
| --- | --- | --- | --- | --- | --- | --- |
| v0 | baseline | 36.5k | ~33 ms | ~20 (quota) | — | load generator |
| 4ffb783 | re-baseline, new load generator | 77.3k | ~1.05 ms | ~20 (quota) | — | server CPU |
| 03bbb0d (3b) | `/api/v1` cache stores the serialized body; a hit returns it directly | 132.1k | ~0.59 ms | — | — | server CPU |
| perf-1 | global allocator switched to mimalloc | 139.3k | ~0.57 ms | — | — | server CPU |
| perf-2 | `AppState` clone behind a single `Arc` | 156.8k | ~0.50 ms | — | — | server CPU |
| 3f | write path measured; no default changed (larger `COUNT`, bounded concurrency, batched upserts and pool sweeps were flat or worse); worker count, read count and pool size made env-configurable | — | — | — | 0.9k (EC2, isolated); 2.35k (laptop, isolated); 2.3–2.4 at 4/s | Postgres on RDS (isolated), provider quota (real) |
| | | | | | | |

### Phase 4 — stream

| Version | Change | Program | `S-recall` | `S-precision` | Missed | p99 lag |
| --- | --- | --- | --- | --- | --- | --- |
| v0 | baseline | — | — | — | — | — |
| ee1a2a6 | 24 h baseline | Token Bridge | 100% (1101/1101) | 50/50 (39 `both_completed`, 11 `both_open`) | 0 | 5092 s |
| — | reconcile 24 h re-run on EC2 (clean window, 4b + 4d build): recall 100.00% (594/594), 0 missed, lag p50 1 s / p99 3 s / max 11 s, precision 20/20 | — | — | — | — | — |

**24 h baseline, `ee1a2a6`.** Window `2026-10-29T10:06Z` to `2026-10-30T09:06Z`
(23 h) on EC2, Token Bridge program: 1101 of 1101 successful signatures
persisted, 0 missed (recall 100%). 45 failed signatures were excluded. Precision
was 50/50 against WormholeScan. Ingestion lag p50 1 s, p99 5092 s, max 5463 s.

The p99 comes from a planned 91-minute service stop (07:13 to 08:44 UTC on
Oct 30) that the restart's backfill recovered, which is why it is not counted as
missed. Every other hour of the window was fully live: rows were stored under
60 s after block time.
