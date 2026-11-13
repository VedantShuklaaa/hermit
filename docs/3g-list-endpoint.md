# 3g: `GET /api/v2/transfers` and its query plans

The endpoint lists transfers newest first with keyset pagination over
`(created_at DESC, id DESC)`. This document records the contract, the synthetic
seed used for the plans, and the `EXPLAIN (ANALYZE, BUFFERS)` evidence behind the
single index the item adds.

**The plans below are synthetic.** They were taken on a local Docker Postgres 18
with one million seeded rows. They say which index and plan shape the endpoint
gets and how the plans scale with table size; they are not production numbers
and not EC2 numbers. The benchmark with the 300 req/s and p99 < 150 ms target is
run by the maintainer on EC2 and recorded in `docs/scaling-log.md`.

## Contract

```
GET /api/v2/transfers
```

| Parameter | Meaning | Rules |
| --- | --- | --- |
| `limit` | page size | default 50, 1 to 200; anything else is 400 |
| `cursor` | opaque position from the previous page's `next_cursor` | strictly after the last returned row; invalid is 400 |
| `protocol` | exact `bridge_transfers.protocol` | `wormhole` or `cctp`; anything else is 400 |
| `direction` | `Outbound` or `Inbound` | case-sensitive; anything else is 400 |
| `status` | `Detected`, `Pending`, `Completed`, `Failed`, `Refunded` | anything else is 400 |
| `chain` | chain slug from `chains.slug` | matches source **or** destination; unknown slug is 400 |
| `token` | exact `bridge_transfers.token` | non-empty; no token-chain key |
| `from` | inclusive lower bound on `created_at`, RFC3339 | invalid is 400 |
| `to` | exclusive upper bound on `created_at`, RFC3339 | invalid is 400; `from >= to` is 400 |

Response:

```json
{
  "items": [
    {
      "protocol": "wormhole",
      "message_id": { "protocol": "wormhole", "emitter_chain": 1, "emitter_address": "…", "sequence": "…" },
      "direction": "Outbound",
      "source_chain": "solana",
      "destination_chain": "ethereum",
      "token": "…",
      "amount": "1000000",
      "status": "Completed",
      "updated_at": "2026-10-31T00:00:00Z",
      "…": "same fields as the analyse response"
    }
  ],
  "next_cursor": "eyJjcmVhdGVkX2F0Ijoi…"
}
```

* `items` are rendered by the same `transfer_view` the analyse endpoint uses, so
  the two responses cannot drift.
* `next_cursor` is `null` on the last page. It is opaque: clients echo it back as
  `cursor` and must not parse it.
* There is deliberately no total count. Counting the filtered set requires a full
  scan and is not cheap enough to expose on every page. Clients detect the end by
  `next_cursor: null`.

### Pagination semantics

The cursor encodes the last returned `(created_at, id)`. The next page selects
rows with `(created_at, id) < (cursor_created_at, cursor_id)` in descending
order. Because `id` is the tiebreaker, rows that share the same `created_at` are
still visited exactly once and in a stable order. `from`/`to` combine with the
cursor as additional bounds; they do not reset it.

The ordering key is the row's `created_at` column, which is the time the row was
written by the indexer. It is not the chain's block time. A transfer backfilled
later appears at the position of its insertion, which can be far from the
position of the on-chain event.

## Seed

`src/bin/seed_transfers.rs` writes realistic rows into a throwaway database. It
does not read `.env`, refuses when `SEED_DATABASE_URL` equals `DATABASE_URL`,
refuses without `--yes`, and truncates the three tables it owns first.

```bash
docker run --rm -d --name bridge-seed-3g \
  -e POSTGRES_USER=bridge -e POSTGRES_PASSWORD=bridge \
  -e POSTGRES_DB=bridge_seed -p 55432:5432 postgres:18

SEED_DATABASE_URL='postgres://bridge:bridge@127.0.0.1:55432/bridge_seed' \
  cargo run --release --bin seed_transfers -- --yes

# fresh optimiser statistics before EXPLAIN
docker exec bridge-seed-3g psql -U bridge -d bridge_seed -c 'ANALYZE'
```

The default is 1,000,000 rows. `--rows N` changes it. The seed applies the
migrations itself. It mixes direction (35% Inbound), status, chains, tokens and
`via`, and gives every `created_at` to a group of four rows so the id tiebreaker
is exercised. It writes only `protocol = 'wormhole'`: the v2 serializer (which
3g does not change) only models the Wormhole message identity, and a row for
another protocol would make the list endpoint answer 422, which is not a success
under the benchmark rules. Filter plans for another protocol were taken on a
separate throwaway seed where 10% of rows were `cctp`.

This seed is not production data, and the row distribution is not a sample of
mainnet traffic. Query costs on a real table depend on its actual value
distribution and vacuum state.

## Plans

Postgres 18, Docker, 1,000,000 `bridge_transfers` rows, `ANALYZE` run, one warm
pass before capture. Every plan is the SQL `repository::list_transfers` builds,
including the `wormhole_messages` left join and the same projection, with
literal values where the endpoint binds parameters. Times are single-run
`EXPLAIN ANALYZE` values on a table whose pages are partly cached; the
sequential scans below move between runs with the cache state. They are not
benchmarks.

| # | Query | Plan | Served by | Sort node | Execution (ms) |
| --- | --- | --- | --- | --- | ---: |
| A | first page, no filter | Nested Loop Left Join → Index Scan | `idx_bt_created` + `wormhole_messages_pkey` | no | 1.341 |
| B | deep cursor (`created_at, id` at about 80% depth) | Index Scan | `idx_bt_created` | no | 1.004 |
| C | `direction=Inbound&status=Completed&chain=ethereum` | Index Scan + Filter | `idx_bt_status_created` | no | 1.556 |
| D | `protocol=wormhole` (all rows) | Index Scan + Filter | `idx_bt_created` | no | 0.863 |
| E | `direction=Inbound` | Index Scan + Filter | `idx_bt_created` | no | 0.146 |
| F | `status=Completed` | Index Scan | `idx_bt_status_created` | no | 0.134 |
| G | `chain=ethereum` (OR) | Index Scan + Filter | `idx_bt_created` | no | 0.405 |
| H | `token=USDC` | Index Scan | `idx_bt_token_created` | no | 0.143 |
| I | `from=2026-10-01&to=2026-10-15` | Index Scan | `idx_bt_created` | no | 0.643 |
| J | `direction=Inbound&status=Completed` | Index Scan + Filter | `idx_bt_status_created` | no | 0.084 |
| K | `status=Completed&chain=ethereum` | Index Scan + Filter | `idx_bt_status_created` | no | 0.253 |
| L | `token=USDC&status=Completed` | Index Scan + Filter | `idx_bt_token_created` | no | 0.073 |
| M | `token=no-such-token` | Index Scan | `idx_bt_token_created` | no | 0.047 |
| N | `chain=999` (no rows) | Parallel Seq Scan + Sort | none | **yes** | 113.590 |
| O | `direction=Inbound&status=Refunded&chain=bsc` (sparse) | Index Scan + Filter, 20 000 rows removed | `idx_bt_status_created` | no | 74.255 |
| P | `protocol=cctp` (no rows) | Index Scan + Sort over 0 rows | `bridge_transfers_message_unique` | **yes** | 0.031 |

Rows C and O are the same shape: the ordered status index fills the page, and
the remaining predicates are filters. C removes 872 rows to fill 51; O removes
all 20 000 `Refunded` rows and returns an empty page. Dense filter values are
cheap; a combination of rare values is bounded by the number of rows the leading
index predicate admits, not by `LIMIT`.

### A: unfiltered first page

```
 Limit  (cost=0.85..37.08 rows=51 width=386) (actual time=0.271..1.266 rows=51.00 loops=1)
   Buffers: shared hit=231 read=22
   ->  Nested Loop Left Join  (cost=0.85..710394.83 rows=1000000 width=386) (actual time=0.270..1.256 rows=51.00 loops=1)
         Buffers: shared hit=231 read=22
         ->  Index Scan using idx_bt_created on bridge_transfers b  (cost=0.42..213078.89 rows=1000000 width=306) (actual time=0.049..0.228 rows=51.00 loops=1)
               Index Searches: 1
               Buffers: shared hit=49
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..0.49 rows=1 width=36) (actual time=0.019..0.019 rows=1.00 loops=51)
               Index Cond: (transfer_id = b.id)
               Index Searches: 51
               Buffers: shared hit=182 read=22
 Planning Time: 2.402 ms
 Execution Time: 1.341 ms
```

### B: deep cursor page

```
 Limit  (cost=0.85..80.87 rows=51 width=386) (actual time=0.186..0.923 rows=51.00 loops=1)
   Buffers: shared hit=236 read=20 dirtied=3
   ->  Nested Loop Left Join  (cost=0.85..320621.77 rows=204332 width=386) (actual time=0.186..0.918 rows=51.00 loops=1)
         Buffers: shared hit=236 read=20 dirtied=3
         ->  Index Scan using idx_bt_created on bridge_transfers b  (cost=0.42..183345.54 rows=204332 width=306) (actual time=0.108..0.327 rows=51.00 loops=1)
               Index Cond: (ROW(created_at, id) < ROW('2026-09-14 13:11:22.517039+00'::timestamp with time zone, 975104))
               Index Searches: 1
               Buffers: shared hit=49 read=3 dirtied=3
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..0.66 rows=1 width=36) (actual time=0.011..0.011 rows=1.00 loops=51)
               Index Cond: (transfer_id = b.id)
               Index Searches: 51
               Buffers: shared hit=187 read=17
 Planning Time: 0.230 ms
 Execution Time: 1.004 ms
```

The cursor page is not slower than the first page: the row comparison is an
index condition, so Postgres seeks directly to the cursor position and does not
walk the preceding pages. There is no `OFFSET`.

### C: filtered page (`Inbound` + `Completed` + `ethereum`)

```
 Limit  (cost=0.85..487.53 rows=51 width=386) (actual time=0.058..1.528 rows=51.00 loops=1)
   Buffers: shared hit=1016 read=10
   ->  Nested Loop Left Join  (cost=0.85..251558.65 rows=26361 width=386) (actual time=0.057..1.522 rows=51.00 loops=1)
         Buffers: shared hit=1016 read=10
         ->  Index Scan using idx_bt_status_created on bridge_transfers b  (cost=0.42..194814.30 rows=26361 width=306) (actual time=0.052..1.149 rows=51.00 loops=1)
               Index Cond: (status = 'Completed'::text)
               Filter: ((direction = 'Inbound'::text) AND ((source_chain_id = 2) OR (destination_chain_id = 2)))
               Rows Removed by Filter: 872
               Index Searches: 1
               Buffers: shared hit=822
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..2.14 rows=1 width=36) (actual time=0.006..0.006 rows=1.00 loops=51)
               Index Cond: (transfer_id = b.id)
               Index Searches: 51
               Buffers: shared hit=194 read=10
 Planning Time: 0.240 ms
 Execution Time: 1.556 ms
```

### M: token with no rows, after `idx_bt_token_created`

```
 Limit  (cost=0.98..16.77 rows=1 width=386) (actual time=0.041..0.041 rows=0.00 loops=1)
   ->  Nested Loop Left Join  (cost=0.98..16.77 rows=1 width=386) (actual time=0.041..0.041 rows=0.00 loops=1)
         ->  Index Scan using idx_bt_token_created on bridge_transfers b  (cost=0.55..8.32 rows=1 width=307) (actual time=0.040..0.040 rows=0.00 loops=1)
               Index Cond: (token = 'no-such-token'::text)
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..8.44 rows=1 width=36) (never executed)
               Index Cond: (transfer_id = b.id)
 Execution Time: 0.047 ms
```

### N: chain with no rows (known limit)

```
 Limit  (cost=51275.49..51275.49 rows=1 width=386) (actual time=111.245..113.560 rows=0.00 loops=1)
   Buffers: shared hit=10701 read=33329
   ->  Sort  (cost=51275.49..51275.49 rows=1 width=386) (actual time=111.244..113.559 rows=0.00 loops=1)
         Sort Key: b.created_at DESC, b.id DESC
         Sort Method: quicksort  Memory: 25kB
         ->  Nested Loop Left Join  (cost=1000.42..51275.48 rows=1 width=386) (actual time=111.220..113.535 rows=0.00 loops=1)
               ->  Gather  (cost=1000.00..51267.03 rows=1 width=307) (actual time=111.220..113.534 rows=0.00 loops=1)
                     Workers Planned: 2
                     Workers Launched: 2
                     ->  Parallel Seq Scan on bridge_transfers b  (cost=0.00..50266.93 rows=1 width=307) (actual time=107.752..107.753 rows=0.00 loops=3)
                           Filter: ((source_chain_id = 999) OR (destination_chain_id = 999))
                           Rows Removed by Filter: 333333
               ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..8.44 rows=1 width=36) (never executed)
                     Index Cond: (transfer_id = b.id)
 Execution Time: 113.590 ms
```

### O: sparse filter combination (known limit)

```
 Limit  (cost=0.85..4269.67 rows=51 width=386) (actual time=74.243..74.243 rows=0.00 loops=1)
   Buffers: shared hit=4914 read=13940 written=4
   ->  Nested Loop Left Join  (cost=0.85..60099.13 rows=718 width=386) (actual time=74.242..74.243 rows=0.00 loops=1)
         ->  Index Scan using idx_bt_status_created on bridge_transfers b  (cost=0.42..54470.67 rows=717 width=307) (actual time=74.242..74.242 rows=0.00 loops=1)
               Index Cond: (status = 'Refunded'::text)
               Filter: ((direction = 'Inbound'::text) AND ((source_chain_id = 6) OR (destination_chain_id = 6)))
               Rows Removed by Filter: 20000
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..7.84 rows=1 width=36) (never executed)
               Index Cond: (transfer_id = b.id)
 Execution Time: 74.255 ms
```

### P: protocol with no rows

```
 Limit  (cost=14.66..14.66 rows=1 width=386) (actual time=0.017..0.017 rows=0.00 loops=1)
   ->  Sort  (cost=14.66..14.66 rows=1 width=386) (actual time=0.016..0.016 rows=0.00 loops=1)
         Sort Key: b.created_at DESC, b.id DESC
         Sort Method: quicksort  Memory: 25kB
         ->  Nested Loop Left Join  (cost=0.85..14.65 rows=1 width=386) (actual time=0.013..0.013 rows=0.00 loops=1)
               ->  Index Scan using bridge_transfers_message_unique on bridge_transfers b  (cost=0.42..6.19 rows=1 width=307) (actual time=0.013..0.013 rows=0.00 loops=1)
                     Index Cond: (protocol = 'cctp'::text)
               ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..8.44 rows=1 width=36) (never executed)
                     Index Cond: (transfer_id = b.id)
 Execution Time: 0.031 ms
```

## The one index

Existing indexes serve every filter except a token that has few or no rows:

* `idx_bt_created (created_at DESC, id DESC)` serves the unfiltered page, the
  cursor, `direction`, `chain`, and the `from`/`to` range.
* `idx_bt_status_created (status, created_at DESC, id DESC)` serves `status`,
  alone or leading a combination.
* `bridge_transfers_message_unique (protocol, message_key)` serves an empty
  `protocol`; a dense protocol uses `idx_bt_created` and filters.

For a token whose rows cannot fill a page, `idx_bt_created` has to walk the
whole index looking for matches and Postgres falls back to a sequential scan:

Before (`token=no-such-token`):

```
 Limit  (cost=50235.00..50235.01 rows=1 width=386) (actual time=45.308..47.081 rows=0.00 loops=1)
   Buffers: shared hit=15270 read=28760
   ->  Sort  (cost=50235.00..50235.01 rows=1 width=386) (actual time=45.308..47.080 rows=0.00 loops=1)
         Sort Key: b.created_at DESC, b.id DESC
         Sort Method: quicksort  Memory: 25kB
         ->  Nested Loop Left Join  (cost=1000.42..50234.99 rows=1 width=386) (actual time=45.281..47.053 rows=0.00 loops=1)
               ->  Gather  (cost=1000.00..50226.54 rows=1 width=307) (actual time=45.280..47.052 rows=0.00 loops=1)
                     Workers Planned: 2
                     Workers Launched: 2
                     ->  Parallel Seq Scan on bridge_transfers b  (cost=0.00..49226.44 rows=1 width=307) (actual time=44.106..44.107 rows=0.00 loops=3)
                           Filter: (token = 'no-such-token'::text)
                           Rows Removed by Filter: 333333
               ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..8.44 rows=1 width=36) (never executed)
                     Index Cond: (transfer_id = b.id)
 Execution Time: 47.102 ms
```

After (`migrations/20261031120000_list_endpoint_token_index.sql`,
`idx_bt_token_created (token, created_at DESC, id DESC)`):

```
 Limit  (cost=0.98..16.76 rows=1 width=386) (actual time=0.010..0.010 rows=0.00 loops=1)
   Buffers: shared hit=2 read=2
   ->  Nested Loop Left Join  (cost=0.98..16.76 rows=1 width=386) (actual time=0.010..0.010 rows=0.00 loops=1)
         ->  Index Scan using idx_bt_token_created on bridge_transfers b  (cost=0.55..8.31 rows=1 width=307) (actual time=0.010..0.010 rows=0.00 loops=1)
               Index Cond: (token = 'no-such-token'::text)
         ->  Index Scan using wormhole_messages_pkey on wormhole_messages w  (cost=0.42..8.44 rows=1 width=36) (never executed)
               Index Cond: (transfer_id = b.id)
 Execution Time: 0.016 ms
```

The same plan shape was measured at 122.3 ms before the index in an earlier run
on a colder cache, and at 47.1 ms before / 0.047 ms after in the all-filter run
above; the sequential scan's time moves with how much of the table the page
cache holds. The plan shape is the point, not the exact millisecond.

Dense token filters also improve and stay index-ordered: `token=USDC` went from
an `idx_bt_created` scan with 151 rows filtered (0.209 ms) to the new index
(0.118 ms), and `token=USDC&status=Completed` went from `idx_bt_status_created`
with 147 filtered (0.140 ms) to the new index with 14 filtered (0.071 ms). The
unfiltered page is unchanged (`idx_bt_created`, 0.083 ms before and 0.069 ms
after the build).

No other index is added. `direction`, `chain` and `protocol` were considered:
the values that fill a page are served by `idx_bt_created` in order, so a new
index would only help the rare-value case (N, O below), which is measured and
documented rather than paid for on every ingest write. The task's
`idx_bt_missing_origin` was not touched.

The migration uses plain `CREATE INDEX`, like the earlier index migrations:
sqlx runs migrations inside a transaction and `CONCURRENTLY` cannot. The build
takes a write-blocking lock while it runs; size it against the production row
count before deploying.

## Known limits

* A chain slug with no rows (N) reads the whole table: `source_chain_id = $1 OR
  destination_chain_id = $1` has no index that can answer both sides while
  preserving the order. On 1M synthetic rows this measured 76.7 to 113.6 ms for
  an empty page, depending on the page cache. A production-sized table will be
  slower. A fix is the UNION ALL / MergeAppend
  over `(source_chain_id, created_at DESC, id DESC)` and `(destination_chain_id,
  created_at DESC, id DESC)` the maintainer allowed only if the BitmapOr + Sort
  plan proved too slow. It was not built: for values present in the data the
  ordered scan fills a page in under 1 ms, so the cost would be paid on ingest
  for a rare empty-result case.
* A sparse combination of an indexed status with a rare direction and chain (O)
  scans every row the status admits. Cost grows with the number of rows sharing
  the status; `LIMIT` does not bound it because no matching row exists.
* Filter values that are absent from the data are cheap only when an index can
  prove absence. `protocol` (P) has a tiny sort; `chain` (N) does not.
* The seed writes one protocol. Plans for a second protocol were measured on a
  separate 1M-row throwaway seed (10% `cctp`), not on the committed seed.
* `TransferView` can represent only `wormhole` rows today, because `MessageId`
  has one variant. A page that contains a row for another protocol returns 422,
  exactly as `/analyse` does. The serializer was not changed by 3g, and the
  `protocol` filter cannot make an unsupported row serializable.
* No total count is available by design.
* The plan numbers here are synthetic, local, single-run, and not a benchmark.
  The EC2 benchmark, its status-code distribution and the warm/cold split go in
  `docs/scaling-log.md` next to this document.

## Reproducing

```bash
# seed (see above), then psql into the container:
docker exec -it bridge-seed-3g psql -U bridge -d bridge_seed
```

The SQL behind every plan is `LIST_SELECT` in `src/db/repository.rs` plus the
cursor and filter clauses from `list_transfers`, with the page's `LIMIT` set to
`limit + 1`. Paste one of the filter clauses into that statement and run it under
`EXPLAIN (ANALYZE, BUFFERS)`.
