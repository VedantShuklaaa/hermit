# Scaling log

Every performance claim in this repository needs measured before/after numbers
(CLAUDE.md). Add an entry only after the runs exist; never fill a number in from
an estimate.

Results go in `bench-results/<timestamp>/` and are kept out of git through
`.git/info/exclude`.

## How to run R-hit

Run the steps in this order. The first 50-connection run fills the cache, spends
Helius quota, and is excluded from the summary table.

1. Start the API server with ingestion off and warning-level logs. This is the
   deployed API server for the test, not a shared or production URL.

   ```bash
   INGEST_ENABLED=0 RUST_LOG=warn ./target/release/bridge
   ```

   `INGEST_ENABLED` is only disabled by the exact value `0`. Any other value
   (including `false`) leaves ingestion on.

2. On the load generator, build the URL list from the deployed database.

   ```bash
   export DATABASE_URL='postgres://user:password@host:5432/bridge'
   export BASE_URL='http://172.31.38.190:8080'

   scripts/make_urls.sh --print-sql          # inspect the query first
   scripts/make_urls.sh v1 urls.txt          # 769 URLs, newest first
   ```

   Use `scripts/make_urls.sh v2 urls-v2.txt` or
   `--api-version v2` to target `/api/v2`.

3. Start CPU sampling on both the API server and the load generator, before the
   series starts.

   ```bash
   ssh api-host 'scripts/cpu_sample.sh /tmp/rhit-server-cpu.log'
   scripts/cpu_sample.sh /tmp/rhit-loadgen-cpu.log
   ```

4. Run the series. The optional second positional argument is recorded with the
   date and oha version in `bench-results/<timestamp>/meta.txt`.

   ```bash
   BASE_URL="$BASE_URL" \
     scripts/bench_rhit.sh urls.txt "$(git rev-parse HEAD)"
   ```

5. Ctrl+C both `cpu_sample.sh` processes. Read the table printed by
   `bench_rhit.sh`, then fill in one entry below.

R-hit uses oha with `-z 30s`, `--urls-from-file`, and `--output-format json`.
The script raises the open-file limit to 65535 before the first run and stops
with a clear message if that fails.

## Entry template

Copy this block for each experiment. Leave every result row blank until the runs
have been made.

```markdown
## <experiment> (<date>)

### Hypothesis

### Workload

### Setup

### Before

### After

### Delta

### Runs

| Run | Connections | req/s | median latency (ms) | p99 latency (ms) | success rate | non-200 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |

Median and min–max across the repeated runs:

| Metric | Before (median, min–max) | After (median, min–max) | Delta |
| --- | --- | --- | --- |
|  |  |  |  |

### Limited by

### Notes
```

## Arc AppState, 35d3e1b vs 4ffb783 (2026-10-26)

### Setup

m7i-flex.large server, c7i.xlarge load generator. Limited by server CPU. Load
generator about 72% idle. Zero non-200s in every run.

### Workload

`R-hit`, single warm key set. Says nothing about the cold path or writes.

### Reason for change

Profile showed `AppState::clone` plus drops at about 4% and `String::clone` at
1.35%.

### Control

Old commit 4ffb783 gave 77,275 and 77,302 req/s at 08:40 and 76,954 and 77,329
at 11:24. Within about 0.5%, so there was no drift.

### Runs

req/s:

| Connections | 4ffb783 (08:40) | 4ffb783 (11:24) | 35d3e1b (11:11) | Change vs same-day old |
| --- | ---: | ---: | ---: | ---: |
| 50, run 1 | 77,275 | 76,954 | 84,995 | |
| 50, run 2 | 77,302 | 77,329 | 84,391 | |
| 50, mean | 77,289 | 77,141 | 84,693 | +9.8% |
| 50, median / p99 (ms) | 0.64 / 1.05 | 0.645 / 1.055 | 0.59 / 0.955 | -8.5% / -9.5% |
| 200 | 75,118 | 74,729 | 80,754 | +8.1% |
| 400 | 74,431 | 73,604 | 81,173 | +10.3% |

### Caveats

200 and 400 are single runs per commit. Runs were not interleaved (new at 11:11,
old at 11:24). One workload only.

## 3b — pre-serialized `/api/v1` cache bodies (2026-10-26)

### Hypothesis

A warm `/api/v1` hit does three avoidable things per request today: moka
returns a deep clone of the cached entry, the handler rebuilds a
`NormalisedTransaction` through the v1 compatibility mapper, and axum
serializes it. Storing the serialized body in the cache and returning it
directly should raise `R-hit` throughput and cut latency.

### Workload

`R-hit` (v1), warm cache, 30 s runs at 50, 200 and 400 connections, run per the
procedure above. Says nothing about the cold path or writes.

### Setup

Same server (`m7i-flex.large`) and load generator (`c7i.xlarge`) as the Arc
AppState entry. The benchmark ran against the deployed API instance with
ingestion off, using the 769-URL list from the deployed database; the warm-up
run is excluded. Old build `35d3e1b`, optimized build `03bbb0d` (merged as
`05d3192`, PR #29). Four rounds: old, new, old again, new again. The repeats
check for drift; the comparison uses the mean of the initial and repeat rounds
for each build (four samples at 50 connections, two at 200 and 400).

### Before

`35d3e1b`: 86,115 req/s at 50 connections (mean of four runs), 0.58 ms median,
0.94 ms p99; 83,293 req/s at 200; 81,711 req/s at 400.

### After

`03bbb0d`: 132,082 req/s at 50 connections (mean of four runs), 0.38 ms median,
0.59 ms p99; 124,969 req/s at 200; 124,706 req/s at 400.

### Delta

+53.4% at 50 connections, +50.0% at 200 and +52.6% at 400. At 50 connections
the median fell from 0.58 to 0.38 ms and p99 from 0.94 to 0.59 ms. Every run
reported 100% success and zero non-200 responses.

### Caveat

The change bundles three effects: the deep clone of the cached entry is gone,
the v1 remap (and its string allocations) is gone, and per-hit serialization is
gone. Any gain is not attributable to serialization alone. The baseline
already contains the Arc AppState change, so the delta is relative to
`35d3e1b`, not `4ffb783` or `v0`.

### Runs

All runs reported 100% success and zero non-200s.

| Run | Commit | Connections | req/s | median latency (ms) | p99 latency (ms) | success rate | non-200 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline, initial | 35d3e1b | 50 | 85,346.6 | 0.59 | 0.94 | 100% | 0 |
| baseline, initial | 35d3e1b | 50 | 85,026.4 | 0.59 | 0.96 | 100% | 0 |
| baseline, initial | 35d3e1b | 200 | 83,189.4 | 2.38 | 4.01 | 100% | 0 |
| baseline, initial | 35d3e1b | 400 | 81,274.6 | 4.40 | 13.96 | 100% | 0 |
| optimized, initial | 03bbb0d | 50 | 132,588.2 | 0.38 | 0.59 | 100% | 0 |
| optimized, initial | 03bbb0d | 50 | 130,422.7 | 0.39 | 0.60 | 100% | 0 |
| optimized, initial | 03bbb0d | 200 | 125,208.5 | 1.57 | 2.74 | 100% | 0 |
| optimized, initial | 03bbb0d | 400 | 125,441.1 | 2.72 | 12.36 | 100% | 0 |
| baseline, repeat | 35d3e1b | 50 | 87,300.9 | 0.57 | 0.92 | 100% | 0 |
| baseline, repeat | 35d3e1b | 50 | 86,784.9 | 0.58 | 0.93 | 100% | 0 |
| baseline, repeat | 35d3e1b | 200 | 83,396.9 | 2.37 | 4.04 | 100% | 0 |
| baseline, repeat | 35d3e1b | 400 | 82,146.5 | 4.38 | 14.25 | 100% | 0 |
| optimized, repeat | 03bbb0d | 50 | 132,899.5 | 0.38 | 0.59 | 100% | 0 |
| optimized, repeat | 03bbb0d | 50 | 132,417.7 | 0.38 | 0.59 | 100% | 0 |
| optimized, repeat | 03bbb0d | 200 | 124,729.0 | 1.59 | 2.69 | 100% | 0 |
| optimized, repeat | 03bbb0d | 400 | 123,970.3 | 2.76 | 12.33 | 100% | 0 |

Mean and min–max across the repeated rounds of each build. These means are the
comparison:

| Metric | Before, `35d3e1b` (mean, min–max) | After, `03bbb0d` (mean, min–max) | Delta |
| --- | --- | --- | --- |
| 50 connections, req/s | 86,115 (85,026–87,301) | 132,082 (130,423–132,900) | +53.4% |
| 50 connections, median latency (ms) | 0.58 (0.57–0.59) | 0.38 (0.38–0.39) | −34.5% |
| 50 connections, p99 latency (ms) | 0.94 (0.92–0.96) | 0.59 (0.59–0.60) | −37.2% |
| 200 connections, req/s | 83,293 (83,189–83,397) | 124,969 (124,729–125,209) | +50.0% |
| 200 connections, median latency (ms) | 2.38 (2.37–2.38) | 1.58 (1.57–1.59) | −33.6% |
| 200 connections, p99 latency (ms) | 4.03 (4.01–4.04) | 2.72 (2.69–2.74) | −32.5% |
| 400 connections, req/s | 81,711 (81,275–82,147) | 124,706 (123,970–125,441) | +52.6% |
| 400 connections, median latency (ms) | 4.39 (4.38–4.40) | 2.74 (2.72–2.76) | −37.6% |
| 400 connections, p99 latency (ms) | 14.11 (13.96–14.25) | 12.35 (12.33–12.36) | −12.5% |

### Limited by

Server CPU. The load generator was about 66.5–78.6% idle in the old runs and
66.5–69.5% idle in the optimized runs, so it was not the bottleneck. Throughput
depends on this hardware and this workload; these numbers are not capacity
guarantees.

### Notes

The repeat rounds agree with the initial round within about 2% for the old
build and 1% for the new one, so no drift was measured. One deployed API
instance and one warm key set only.

## perf-1 — mimalloc global allocator (2026-10-27)

### Hypothesis

Warm `R-hit` throughput is server-CPU-bound. Replacing the default system
allocator with mimalloc (`mimalloc = "0.1.52"`, `#[global_allocator]` in
`src/main.rs`) may reduce allocator time and raise throughput.

### Workload

`R-hit` (v1), warm cache, 30 s runs at 50, 200 and 400 connections, run per the
procedure above. Says nothing about the cold path or writes.

### Setup

Baseline `16018712` (glibc `malloc`), after `59fe708` (mimalloc, full SHA
`59fe7086145948617e395b425e605108bdd07129`), on the same `m7i-flex.large`
server with a `c7i.xlarge` load generator. The runs were not interleaved: the
glibc runs were made before the mimalloc runs. The only change is the global
allocator. The allocator is declared in the `bridge` binary only, so
integration-test binaries still use the system allocator and benchmarks must
run the `bridge` binary.

### Before

`16018712` (glibc `malloc`): 131,033 and 130,601 req/s at 50 connections (mean
130,817), median 0.38 ms and p99 0.59 ms; 125,318 req/s at 200 connections and
123,239 at 400.

### After

`59fe708` (mimalloc): 139,755 and 138,936 req/s at 50 connections (mean
139,345), median 0.35 ms and p99 0.57 ms (runs 0.56 and 0.58); 134,973 req/s at
200 connections and 131,172 at 400.

### Delta

| Connections | `16018712` (glibc), req/s | `59fe708` (mimalloc), req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 130,817 (runs 131,033 and 130,601) | 139,345 (runs 139,755 and 138,936) | +6.5% |
| 200 | 125,318 | 134,973 | +7.7% |
| 400 | 123,239 | 131,172 | +6.4% |

At 50 connections median latency went from 0.38 to 0.35 ms and p99 from 0.59 to
0.57 ms.

### Runs

| Run | Connections | req/s | median latency (ms) | p99 latency (ms) | success rate | non-200 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `16018712` (glibc), run 1 | 50 | 131,033 | — | — | 100% | 0 |
| `16018712` (glibc), run 2 | 50 | 130,601 | — | — | 100% | 0 |
| `16018712` (glibc) | 200 | 125,318 | — | — | 100% | 0 |
| `16018712` (glibc) | 400 | 123,239 | — | — | 100% | 0 |
| `59fe708` (mimalloc), run 1 | 50 | 139,755 | — | 0.56 | 100% | 0 |
| `59fe708` (mimalloc), run 2 | 50 | 138,936 | — | 0.58 | 100% | 0 |
| `59fe708` (mimalloc) | 200 | 134,973 | — | — | 100% | 0 |
| `59fe708` (mimalloc) | 400 | 131,172 | — | — | 100% | 0 |

`—` means not recorded. The 50-connection medians above are per-build figures
(0.38 ms for `16018712`, 0.35 ms for `59fe708`), not per-run values.

### Limited by

Server CPU. During the mimalloc run the server reported us 50.1, sy 23.1 and
id 27.1; the load generator was 70.4% idle, so it was not the bottleneck. With
glibc, server user CPU was about 57%.

### Notes

A profile at `16018712` put about 12.5% of server time in glibc allocation
(`malloc`, `_int_malloc`, `_int_free`, `malloc_consolidate`, `realloc`,
`cfree`), which is why the allocator was swapped. Caveats: one comparison run
per connection count (two at 50 connections); the glibc runs were not
interleaved with the mimalloc runs; `R-hit` only, so nothing is known about the
cold path or writes; memory use under mimalloc was not measured. Zero non-200
responses in every run.

## perf-2 — `AppState` clone behind one `Arc` (2026-10-27)

### Hypothesis

`AppState` is cloned for every axum state extraction and for each spawned
worker task. All of its fields were already cheap to clone, but each clone
still copied the whole struct and took one refcount per `Arc`-backed field.
Holding the fields in one `Inner` behind a single `Arc` makes a clone one
refcount bump.

### Workload

`R-hit` (v1), warm cache, same procedure and connection counts as perf-1. Says
nothing about the cold path or writes.

### Setup

Baseline `06b891ac4c157e4cff4881174dd84b884988b3bf` (includes mimalloc), after
`0144ea90ea3088bbcba76a0eecb6da0ebe17ebe0` (`AppState { inner: Arc<Inner> }`),
on the same `m7i-flex.large` server with a `c7i.xlarge` load generator. The
runs were interleaved in the order old, new, old, new. The only change is how
`AppState` is cloned.

### Before

`06b891a`: 142,095 req/s at 50 connections (mean of four runs: 141,846,
140,579, 142,260 and 143,697), median about 0.35 ms and p99 about 0.55 ms;
134,784 req/s at 200 connections (mean of two: 133,581 and 135,986) and 130,376
at 400 (mean of two: 129,347 and 131,404).

### After

`0144ea9`: 156,819 req/s at 50 connections (mean of four runs: 155,643,
156,057, 156,984 and 158,590), median about 0.31 ms and p99 about 0.50 ms;
147,438 req/s at 200 connections (mean of two: 145,878 and 148,998) and 145,689
at 400 (mean of two: 146,457 and 144,921).

### Delta

| Connections | `06b891a`, req/s | `0144ea9`, req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 142,095 (141,846, 140,579, 142,260 and 143,697) | 156,819 (155,643, 156,057, 156,984 and 158,590) | +10.4% |
| 200 | 134,784 (133,581 and 135,986) | 147,438 (145,878 and 148,998) | +9.4% |
| 400 | 130,376 (129,347 and 131,404) | 145,689 (146,457 and 144,921) | +11.7% |

### Runs

| Run | Connections | req/s | median latency (ms) | p99 latency (ms) | success rate | non-200 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `06b891a`, run 1 | 50 | 141,846 | — | — | 100% | 0 |
| `0144ea9`, run 1 | 50 | 155,643 | — | — | 100% | 0 |
| `06b891a`, run 2 | 50 | 140,579 | — | — | 100% | 0 |
| `0144ea9`, run 2 | 50 | 156,057 | — | — | 100% | 0 |
| `06b891a`, run 3 | 50 | 142,260 | — | — | 100% | 0 |
| `0144ea9`, run 3 | 50 | 156,984 | — | — | 100% | 0 |
| `06b891a`, run 4 | 50 | 143,697 | — | — | 100% | 0 |
| `0144ea9`, run 4 | 50 | 158,590 | — | — | 100% | 0 |
| `06b891a`, run 1 | 200 | 133,581 | — | — | 100% | 0 |
| `0144ea9`, run 1 | 200 | 145,878 | — | — | 100% | 0 |
| `06b891a`, run 2 | 200 | 135,986 | — | — | 100% | 0 |
| `0144ea9`, run 2 | 200 | 148,998 | — | — | 100% | 0 |
| `06b891a`, run 1 | 400 | 129,347 | — | — | 100% | 0 |
| `0144ea9`, run 1 | 400 | 146,457 | — | — | 100% | 0 |
| `06b891a`, run 2 | 400 | 131,404 | — | — | 100% | 0 |
| `0144ea9`, run 2 | 400 | 144,921 | — | — | 100% | 0 |

`—` means not recorded. The 50-connection medians and p99s above are per-build
approximate values (about 0.35 and 0.55 ms for `06b891a`; about 0.31 and
0.50 ms for `0144ea9`), not per-run values.

### Limited by

Server CPU. Server and load generator CPU percentages were not recorded for
these rounds.

### Notes

The reason for the change: a profile at `16018712` showed about 4% of server
time in `drop_glue<AppState>` and `FromRef::from_ref`. The old `AppState` clone
did about 8 refcount increments per request; the new one does 1.

The measured gain (+9.4% to +11.7%) is larger than that 4%. A likely
explanation, stated as a hypothesis rather than a measured fact: the removed
atomic refcount operations probably also cause cache-line contention between
cores, which a CPU profile does not attribute cleanly.

Caveats: the runs were interleaved old/new/old/new; 200 and 400 connections
have two runs per version; `R-hit` only, so nothing is known about the cold
path or writes; server and load generator CPU were not recorded for these
rounds; the old baseline already includes mimalloc; the tradeoff is one extra
pointer hop per field access through `Deref`. At 50 connections the old and new
ranges do not overlap (140.6k–143.7k against 155.6k–158.6k), and the old runs
agree with each other within about 1.3%. Zero non-200 responses in every run.

## 3c — pre-serialized `/api/v2` response bodies (2026-10-27)

### Hypothesis

A warm `/api/v2` hit already costs one cache lookup (`v2_completed`, then
`v2_open` on a miss), so the roadmap's "one lookup instead of four" no longer
applies. The remaining per-hit cost was the deep clone of the cached
`TransactionResponse` and its JSON serialization on every request. Storing the
serialized body in the cache and returning it directly, as 3b did for
`/api/v1`, should raise `R-hit` throughput and cut latency.

### Workload

`R-hit` (`v2`), warm cache, 30 s runs at 50, 200 and 400 connections, run per
the procedure above. One `/api/v1` pass on each build, at 50 connections,
served as a control. Says nothing about the cold path or writes.

### Setup

Old build `5a2fce3`, new build `0ed3edb`. The runs were interleaved in the
order old, new, old, new.

### Before

`5a2fce3`: 86,660 req/s at 50 connections (mean of four runs), 0.58 ms median,
0.82 ms p99; 82,852 req/s at 200 connections and 80,290 at 400.

### After

`0ed3edb`: 128,872 req/s at 50 connections (mean of four runs), 0.39 ms median,
0.53 ms p99; 127,498 req/s at 200 connections and 121,888 at 400.

### Delta

| Connections | `5a2fce3`, req/s | `0ed3edb`, req/s | Change |
| ---: | ---: | ---: | ---: |
| 50 | 86,660 (mean of 4) | 128,872 (mean of 4) | +48.7% |
| 200 | 82,852 | 127,498 | +53.9% |
| 400 | 80,290 | 121,888 | +51.8% |

At 50 connections the median fell from 0.58 to 0.39 ms and p99 from 0.82 to
0.53 ms.

### Control

A `/api/v1` pass on each build on the same day, at 50 connections, gave
127.9k req/s on `5a2fce3` and 128.2k on `0ed3edb`, so there is no `v1`
regression from this change. The control is one pass per build.

### Limited by

CPU is not comparable: the old1 sampler ran too long. No CPU numbers are
reported.

### Notes

Caveats: the runs were interleaved old/new/old/new; 200 and 400 connections
have two runs per version; `R-hit` only, so nothing is known about the cold
path or writes; the `/api/v1` control is one pass per build. The tradeoff is
one more serialized body per cached entry in memory (up to 50k completed
entries), plus one more serialization on a v2-only miss.

## 3e experiment 1 — take the Helius token before the Wormholescan call (2026-10-28)

### Finding

A request that was shed on the Helius limiter had already started its
Wormholescan call, whose result was then discarded. In config A `R-cold` this
showed up as 2.19 to 2.23 operations calls per `getTransaction`. Taking the
Helius token before starting either call brings the ratio to 1.000.

### Workload

`R-cold` (all-unique signatures) and, for config A, `R-mix` (90% hot / 10%
cold), both against the fake upstream with 50 ms latency, `/api/v2`, per
`scripts/bench_cold.sh`. Says nothing about writes and nothing about the real
upstream's own behaviour.

Config A: `HELIUS_API_RPS=6 HELIUS_LIVE_RPS=4 WORMHOLE_RPS=20
UPSTREAM_PERMITS=16`, where the Helius limiter binds. Config B: all limiters
`100000`, `UPSTREAM_PERMITS=16`, where the limiters should not bind.

### Setup

Old build `e9708ee` (main), new build `cdb471f`. The baseline was measured on
the same app code as `e9708ee`. The runs were interleaved in the order old,
new, old, new, with two pairs per config. The benchmark ran with
`FAKE_429_RATE=0`.

### Before

Baseline, before the change, fake upstream, two runs each:

- Config A `R-cold`: 18.3 and 18.4 req/s; about 28% of requests answered 200 and
  the rest 503 from the Helius limiter.
- Config B `R-cold`: 130.5 and 130.8 req/s, median 373 ms, p99 533 ms.

`e9708ee` under config A paid 2.186 and 2.232 Wormholescan operations per
`getTransaction` on `R-cold` (365 and 366 operations calls in 30 s) and 2.193
and 2.178 on `R-mix`. The 200 responses were 164 and 160 in the `R-cold` runs.

### After

`cdb471f` under config A paid 1.000 and 1.000 operations per `getTransaction`
on `R-cold` (169 and 162 operations calls in 30 s) and 1.006 and 1.000 on
`R-mix`. The 200 responses were 168 and 159 in the `R-cold` runs.

Under config B, `R-cold` gave 130.8 and 129.7 req/s on the new build against
131.3 and 130.6 on the old, with a median of 372 to 377 ms on both. `R-mix`
was within noise.

### Delta

| Config | Workload | Old, operations per `getTransaction` | New, operations per `getTransaction` |
| --- | --- | ---: | ---: |
| A | `R-cold` | 2.186, 2.232 | 1.000, 1.000 |
| A | `R-mix` | 2.193, 2.178 | 1.006, 1.000 |

Under config B the old build did 131.3 and 130.6 req/s and the new build 130.8
and 129.7, so there is no regression.

| Config | Build | Run | Operations calls in 30 s | 200 responses | req/s |
| --- | --- | ---: | ---: | ---: | ---: |
| A `R-cold` | `e9708ee` | 1 | 365 | 164 | — |
| A `R-cold` | `e9708ee` | 2 | 366 | 160 | — |
| A `R-cold` | `cdb471f` | 1 | 169 | 168 | — |
| A `R-cold` | `cdb471f` | 2 | 162 | 159 | — |
| B `R-cold` | `e9708ee` | 1 | — | — | 131.3 |
| B `R-cold` | `e9708ee` | 2 | — | — | 130.6 |
| B `R-cold` | `cdb471f` | 1 | — | — | 130.8 |
| B `R-cold` | `cdb471f` | 2 | — | — | 129.7 |

Under config A the number of 200 responses did not change: the run is
limiter-bound by design, so no throughput change was expected. Under config B
there is no regression.

### Limited by

Config A is bound by the Helius limiter, not by the server or the upstream
latency. Config B is not limiter-bound.

### Caveats

Two runs per build per config. The database was not reset between the old and
new sub-runs inside a series. The benchmark ran with `FAKE_429_RATE=0`;
retry-through-the-limiter is covered by a unit test rather than by these runs.
`R-mix` numbers use `--disable-keepalive` and are not comparable with the
keep-alive `R-hit` numbers.

### Next

Findings, not commitments:

- `getAsset` (token metadata) hits the Helius URL, is called twice per detected
  token, and is outside both the limiter and the permit pool.
- In config A `R-mix` the 503 count (about 300) is roughly double the
  cold-request count (about 150), so hot-set requests are probably also
  returning 503; the cause is not yet established.

## 3e R-mix hot-set correction and re-baseline (2026-10-29)

### Correction

The earlier `R-mix` runs used a fixed hot set chosen without regard to the
fixture a signature maps to. 264 of the 769 hot keys mapped to fixtures whose
analysis is `Detected` or `Pending`; those keys expired from the 15 s open cache
during a run and were fetched cold again. `R-mix` absolute req/s and 503 counts
from before this change are therefore not comparable with later runs. The
experiment 1 and 2 ratios are unaffected: they compare upstream calls per cold
request, and the old hot-set leak added cold requests to both sides of those
comparisons.

The hot set now contains only signatures whose fixture resolves to `Completed`,
so all 769 keys stay in the 600 s completed cache. After warm-up, a sequential
pass over the new set returned 769/769 `200`, p50 0.29 ms, p99 0.78 ms, no
request above 10 ms, and the fake upstream saw no `getTransaction` calls. A
per-kind labelled R-mix probe on config A recorded 0 503s for hot requests
(4,718 hot requests, every one `200`) against 370 503s for cold requests.

### Workload and setup

`R-mix` (90% hot / 10% cold), fake upstream with `FAKE_LATENCY_MS=50` and
`FAKE_429_RATE=0`, `/api/v2`, 50 connections, 30 s per run, two runs per config.
Local bench Postgres and Redis; the database was reset and the app restarted
before each run. Config A: `HELIUS_API_RPS=6 HELIUS_LIVE_RPS=4 WORMHOLE_RPS=20
UPSTREAM_PERMITS=16`. Config B: all limiters `100000`, `UPSTREAM_PERMITS=16`.

### Results

| Config | Run | req/s | median (ms) | p99 (ms) | requests | 200 | 503 | cold requests | getTransaction |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 1 | 189.6 | 0.30 | 3997.78 | 5689 | 5294 | 345 | 557 | 166 |
| A | 2 | 182.5 | 0.31 | 3995.89 | 5475 | 5071 | 354 | 560 | 157 |
| B | 1 | 1816.8 | 0.30 | 312.82 | 54513 | 54463 | 0 | 5508 | 5490 |
| B | 2 | 1862.5 | 0.29 | 313.70 | 55879 | 55829 | 0 | 5507 | 5477 |

Every run also had 50 transport errors from oha's per-connection deadline
abort. There were no 429s.

### Limited by

Config A is bound by the Helius limiter: 557 to 560 cold requests produced 157
to 166 `getTransaction` calls, and the rest were shed before reaching the fake.
Config B is not limiter-bound; `getTransaction` tracks cold requests one-for-one.

### Caveats

Two runs per config. Fixtures are synthetic, so absolute numbers describe the
fake upstream, not Helius. `R-mix` uses `--disable-keepalive` and is not
comparable with `R-hit`. Earlier `R-mix` numbers are not comparable with these.


## Cold-path quota fixes (2026-11-04)

Two merged PRs (#60, #61), with correctness tests rather than a throughput
experiment. Each change removes a way a cold request could spend provider quota
(it shares the Helius free plan's ~6 uncached lookups/s) without doing useful
work.

- Unknown `chain` is rejected with a 400 before any cache or upstream call.
  Previously a non-Solana chain still fired a WormholeScan request on every
  request, because the Normalisation error was not negative-cached.
- One normalised key (lowercased chain + hash) is used for single-flight, the
  15 s refresh cooldown and the not-found cache. The same hash under two chain
  spellings could previously bypass dedupe and spawn a second refresh.
- The live lane uses `HELIUS_LIVE_URL`. `live_config` never copied
  `helius_live_url` into `helius_url`, so live `getTransaction` and
  `getMultipleAccounts` calls still spent the API key.
- Normalisation errors are negative-cached, and the failed-fetch TTL is raised
  from 2 s to 10 s (`FAILED_CACHE_TTL_SECS`).
- Background refresh admission is atomic: a key is claimed with an
  insert-if-absent guard released by RAII, so concurrent hits on one stale entry
  start at most one refresh.
- A refresh never waits on quota. It try-acquires a background slot, a shared
  upstream permit and one Helius and WormholeScan token; if any is busy it is
  skipped and does not set the cooldown, leaving the quota to user cold lookups.
  Spawned and skipped refreshes are counted.


## How to run the 3g list benchmarks

`GET /api/v2/transfers` has no cache: every request goes to Postgres. The three
workloads are first page, deep cursor page and filtered page. Each is run twice
(run 1 cold, run 2 warm) and the two are kept separate; they are not averaged.

1. Seed a throwaway database with one million rows. Never use production.

   ```bash
   docker run --rm -d --name bridge-seed-3g \
     -e POSTGRES_USER=bridge -e POSTGRES_PASSWORD=bridge \
     -e POSTGRES_DB=bridge_seed -p 55432:5432 postgres:18

   SEED_DATABASE_URL='postgres://bridge:bridge@127.0.0.1:55432/bridge_seed' \
     cargo run --release --bin seed_transfers -- --yes
   ```

   `src/bin/seed_transfers.rs` refuses without `--yes` and refuses when
   `SEED_DATABASE_URL` equals `DATABASE_URL`. Here the database is local; for a
   remote seed, change the host in `SEED_DATABASE_URL` only.

2. Point the API server at the seeded database. On the API host:

   ```bash
   DATABASE_URL='postgres://bridge:bridge@127.0.0.1:55432/bridge_seed' \
     INGEST_ENABLED=0 RUST_LOG=warn ./target/release/bridge
   ```

   `INGEST_ENABLED=0` stops the indexer from writing while the run is in flight.

3. Build the deep cursor at about 80% depth. The cursor is the URL-safe,
   unpadded base64 of `{"created_at":"<RFC3339>","id":<i64>}`. Run this on the
   seeded database host:

   ```bash
   psql "$SEED_DATABASE_URL" -At -c \
     "SELECT to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US'), id
      FROM bridge_transfers
      ORDER BY created_at DESC, id DESC OFFSET 800000 LIMIT 1" |
   python3 -c '
   import sys, json, base64
   ts, row_id = sys.stdin.read().strip().split("|")
   print(base64.urlsafe_b64encode(
       json.dumps({"created_at": ts + "Z", "id": int(row_id)}).encode()
   ).decode().rstrip("="))'
   ```

   Use the printed value as `DEEP_CURSOR`. A deep cursor page that returns zero
   items is rejected by the script: pick an offset that leaves rows below it.

4. Run the benchmark from the load generator:

   ```bash
   BASE_URL='http://<api-host>:8080' \
   SERVER_SHA="$(git rev-parse --short HEAD)" \
   INSTANCE_TYPE='m7i-flex.large' \
   LOADGEN_INSTANCE='c7i.xlarge' \
   DEEP_CURSOR='<from step 3>' \
   COLD_START=yes \
     scripts/bench_list.sh
   ```

   The script pre-checks all three URLs with curl, runs 50 connections for 30 s
   per workload, and writes one JSON file per workload and run under
   `bench-results/<timestamp>/`. It prints one markdown row per run with
   req/s, median, p99 and the 2xx/4xx/5xx split. Success is 2xx only.

   `COLD_START=yes` records that you made run 1 cold (API and Postgres restarted
   or the database page cache dropped). If you did not, use `COLD_START=no` and
   label the entry accordingly: without a restart, run 1 is not cold, and the
   entry must say so.

## 3g — `GET /api/v2/transfers` (2026-11-04)

Results: `bench-results/20261104T170246Z`, server git `f5813d1`.

### Hypothesis

Keyset pagination over `(created_at DESC, id DESC)` with at most one extra
index keeps all three workloads inside 300 req/s and p99 < 150 ms, limited by
the server's Postgres round trips rather than by sorting or a sequential scan.

### Workload

| Name | Request |
| --- | --- |
| list-first | `GET /api/v2/transfers?limit=50` |
| list-deep | `GET /api/v2/transfers?limit=50&cursor=<about 80% depth>` |
| list-filtered | `GET /api/v2/transfers?direction=Inbound&status=Completed&chain=ethereum&limit=50` |

### Setup

- Server: EC2 `m7i-flex.large`, git `f5813d1`.
- Load generator: not recorded in this entry.
- Database: about 7,050 rows in the table.
- oha, 50 connections, 30 s per run, keep-alive on, two runs per workload kept
  separate. `cold_start=no`: run 1 is a first run, not a restart-cold run.
- No cache on the list path; every request reads Postgres.

### Before

There was no list endpoint, so there is no before measurement. The comparison
is against the targets below, and against the query plans in
`docs/3g-list-endpoint.md`.

### After

| Workload | Run | Connections | req/s | median (ms) | p99 (ms) | 2xx | 4xx | 5xx | non-2xx |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| list-first | 1 | 50 | 1,574 | — | 66 | all | 0 | 0 | 0 |
| list-first | 2 | 50 | 1,571 | — | 66 | all | 0 | 0 | 0 |
| list-deep | 1 | 50 | 1,463 | — | 90 | all | 0 | 0 | 0 |
| list-deep | 2 | 50 | 1,508 | — | 74 | all | 0 | 0 | 0 |
| list-filtered | 1 | 50 | 1,272 | — | 85 | all | 0 | 0 | 0 |
| list-filtered | 2 | 50 | 1,295 | — | 80 | all | 0 | 0 | 0 |

Targets, applied to every row: 300 req/s, p99 < 150 ms, 2xx only counted as
success.

### Delta

| Workload | Target req/s | Achieved req/s | Target p99 (ms) | Achieved p99 (ms) | Met |
| --- | ---: | ---: | ---: | ---: | --- |
| list-first | 300 | 1,574 / 1,571 | 150 | 66 / 66 | yes |
| list-deep | 300 | 1,463 / 1,508 | 150 | 90 / 74 | yes |
| list-filtered | 300 | 1,272 / 1,295 | 150 | 85 / 80 | yes |

Achieved values are run 1 / run 2.

### Runs

- `bench-results/20261104T170246Z/meta.txt` (SHA, instance type, cold_start)
- `bench-results/20261104T170246Z/01-first-run1.json` … `06-filtered-run2.json`
- `commands.txt` holds the exact oha invocations.

### Limited by

Not measured in these runs. All three workloads met the target, so no limiting
resource was identified.

### Notes

- `cold_start=no`: run 1 is a first run, not a restart-cold run.
- About 7,050 rows in the table during the run.
- All six runs returned 2xx only; zero non-2xx.
- The 2 s dry-run filtered p99 outlier was warmup noise and is not part of these
  runs.
- The plans in `docs/3g-list-endpoint.md` are synthetic and local; their
  millisecond values are not results.

## ROLE split (api/stream) — 3g rerun (2026-11-04)

Results: `bench-results/20261104T181215Z`, server git `1dc62db`.

### Hypothesis

Serving the API from a dedicated `ROLE=api` process, with ingester, workers,
recovery and the WebSocket stream in a `ROLE=stream` process, does not regress
the three 3g list workloads.

### Workload

Same as the 3g entry: `list-first`, `list-deep` and `list-filtered` against
`GET /api/v2/transfers`, 50 connections, 30 s per run, two runs per workload.

### Setup

- Server: EC2 `m7i-flex.large`, git `1dc62db`.
- Two systemd units on the same box: `bridge-api` (port 8080, `ROLE=api`) and
  `bridge-stream` (healthz on 8081, `ROLE=stream`), both reading the same
  `.env`. The old `bridge` unit is disabled and kept as the rollback.
- `cold_start=no`: run 1 is a first run, not a restart-cold run.
- No cache on the list path; every request reads Postgres.

### After

| Workload | Run | Connections | req/s | median (ms) | p99 (ms) | non-2xx |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| list-first | 1 | 50 | 1,602 | — | 62 | 0 |
| list-first | 2 | 50 | 1,576 | — | 73 | 0 |
| list-deep | 1 | 50 | 1,535 | — | 65 | 0 |
| list-deep | 2 | 50 | 1,465 | — | 101 | 0 |
| list-filtered | 1 | 50 | 1,290 | — | 74 | 0 |
| list-filtered | 2 | 50 | 1,212 | — | 104 | 0 |

### Delta

| Workload | 3g req/s | Role split req/s | 3g p99 (ms) | Role split p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| list-first | 1,574 / 1,571 | 1,602 / 1,576 | 66 / 66 | 62 / 73 |
| list-deep | 1,463 / 1,508 | 1,535 / 1,465 | 90 / 74 | 65 / 101 |
| list-filtered | 1,272 / 1,295 | 1,290 / 1,212 | 85 / 80 | 74 / 104 |

Run 1 / run 2. This is **no regression, within run-to-run noise**, not an
improvement. The targets (300 req/s, p99 < 150 ms) are met on every row.

### Runs

- `bench-results/20261104T181215Z/meta.txt` (SHA, instance type, cold_start)
- One JSON file per workload and run in the same directory.

### Limited by

Not measured in these runs.

### Notes

- `cold_start=no`: run 1 is a first run, not a restart-cold run.
- Zero non-2xx across all six runs.
- The `bridge-stream` process still shares the box's CPU with the API, so this
  run does not isolate the API from ingest load.

## How to run the EVM RPC probes

E0 measures two facts per endpoint before the multi-endpoint design is chosen:
the largest `eth_getLogs` block range the endpoint accepts, and how far the
`finalized` and `safe` tags lag the head. Neither script writes to the
repository or prints the URL, so a provider key in the URL stays out of the
terminal and the logs.

1. Range probe. Run it for the primary and, when one is configured, the
   fallback:

   ```bash
   EVM_RPC_ETHEREUM=<url> scripts/probe_evm_range.sh ethereum
   EVM_RPC_ETHEREUM_FALLBACK=<url> scripts/probe_evm_range.sh ethereum --fallback
   ```

   Without `--address` the probe sends the two Wormhole topic0 hashes and no
   address filter. That keeps the result set small while the block range is what
   is measured. Once a chain's Core Bridge and Token Bridge addresses are
   verified, pass `--address <core>,<token>`; a cap that depends on the matched
   logs then shows up as well. The probe assumes the endpoint rejects a range
   monotonically: doubling finds an upper bound, and binary search finds the
   boundary. A true 1-block rejection or a non-range error stops the run instead
   of being reported as a cap. If nothing is rejected up to `--max`, the result
   carries `no_rejection_up_to` rather than claiming a cap.

   Both scripts call `eth_chainId` first and exit if it does not match the
   slug's chain ID (Ethereum 1, Polygon 137, Avalanche 43114, Arbitrum 42161,
   Optimism 10, Base 8453). This catches a URL copied for the wrong chain;
   dRPC serves every chain from the same host.

2. Finality probe:

   ```bash
   EVM_RPC_BASE=<url> scripts/probe_evm_finality.sh base
   ```

   A tag the endpoint does not support is printed as `unsupported`. That is a
   result: the watcher currently falls back to `head - finality_depth`.

3. Copy one row per endpoint into the table below. Add the date, provider and
   plan in the note. Leave a cell blank if it was not measured; do not fill it
   from an estimate.

## EVM RPC capabilities

| Date | Chain | Endpoint (host) | Role | Max `getLogs` range | `finalized` lag (blocks) | `finalized` lag (s) | `safe` lag (blocks) | `safe` lag (s) | Note |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-11-11 | ethereum | eth-mainnet.g.alchemy.com | primary | 10 (11 rejected) | 84 | 1008 | 52 | 624 | Alchemy free plan; `finalized` supported; 12 s blocks |
| 2026-11-11 | ethereum | lb.drpc.live | fallback | 101 (102 rejected) | 86 | 1044 | 55 | 660 | dRPC free plan; message claims 10,000 but measured 101; 12 s blocks |
| 2026-11-11 | base | lb.drpc.live | primary | 101 (102 rejected) | 5895 | 1179 | 946 | 189 | dRPC free plan; 0.2 s blocks; `finalized` lags about 19.7 min |
| 2026-11-11 | base | base-mainnet.g.alchemy.com | fallback | 10 (11 rejected) | 5875 | 1175 | 358 | 72 | Alchemy free plan; 10-block cap; 0.2 s blocks |
| 2026-11-11 | arbitrum | lb.drpc.live | primary | 101 (102 rejected) | 4338 | 1236 | 3110 | 876 | dRPC free plan; 0.285 s blocks (about 3.5 blocks/s) |
| 2026-11-11 | arbitrum | arb-mainnet.g.alchemy.com | fallback | 10 (11 rejected) | 3791 | 1067 | 2093 | 587 | Alchemy free plan; 10-block cap; 0.281 s blocks |
| 2026-11-11 | avalanche | rpc.ankr.com | primary | no rejection to 100,000 | 5 | 3 | 6 | 4 | Ankr public; `finalized` one block ahead of `safe` in both samples; about 0.6 s blocks |
| 2026-11-11 | avalanche | avax-mainnet.g.alchemy.com | fallback | 10 (11 rejected) | 4 | 3 | 5 | 5 | Alchemy free plan; 10-block cap; `finalized` one block ahead of `safe`; 0.75-1.0 s blocks |

**Findings (2026-11-11).**

- Free-tier `eth_getLogs` caps are plan-level, not chain-level. Alchemy rejected
  11 blocks on all four chains. dRPC rejected 102 on Ethereum, Base and
  Arbitrum, so 101 is the accepted maximum despite its message claiming 10,000.
  The Ankr public Avalanche endpoint rejected no range up to the 100,000 tested.
- `finalized` is supported on all eight endpoints. The lag is why finality must
  be per chain: Ethereum 84 blocks (about 17 min), Base about 5,880 blocks
  (about 19.6 min), Arbitrum 3,791 to 4,338 blocks (about 18 to 21 min),
  Avalanche 4 to 5 blocks (about 3 to 5 s).
- Avalanche reports `finalized` one block ahead of `safe` on both Ankr and
  Alchemy. Do not assume `finalized <= safe` when validating tags.
- dRPC's error text ("ranges over 10000 blocks are not supported on free plan")
  is not matched by `is_range_too_large_error` in `src/evm/client.rs`, which
  looks for "block range" or "eth_getlogs". Configure dRPC chains at 101 or
  lower, and fix the classifier in E1.
- All range runs used the default filter: the two Wormhole topic0 hashes and no
  address filter.
- The Ankr Avalanche endpoint is public; its rate limits were not measured.
