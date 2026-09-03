# crypto-ticks-refinery-glue-dynamo

A ten-step statistical refinery applied to Binance spot tick data — 340,971,834 trades across
BTCUSDT, ETHUSDT and SOLUSDT for January 2025, aggregated into 5-second OHLCV bars.

The point of the project is the refinery's structure, not the bars:

> **Steps 1–3 are a shared entryway. Steps 4–10 are three different refineries.**

Steps 1–3 are *path-blind* — they run identically regardless of what follows, because they finish
before the pipeline knows what kind of target it is handling. The moment data exits Step 3 the
pipeline reads the geometry of `y` and forks into three path-isolated sub-refineries that share
step *numbers* and little else. On the bandit path three of the seven steps are **forbidden**, and
each ban exists because a named downstream engine would break if the step ran.

`glue-ingest-bars.py` is the shared entryway. `refinery-walkthrough.ipynb` is the whole thing —
entryway, fork, and all three paths — executed end to end on a committed sample.

## Architecture

```
 S3 raw/                          AWS Glue 4.0                      S3 curated/
 BTCUSDT-trades-2025-01.csv  ->   glue-ingest-bars.py           ->  bars/ Parquet
 ETHUSDT-trades-2025-01.csv       Steps 1-3, path-blind             1,607,040 x 18
 SOLUSDT-trades-2025-01.csv       ticks -> 5s OHLCV bars                  |
 (340,971,834 rows)                                                      |
                                  ================ THE FORK ==============
                                                                         |
                                  +--------------------+-----------------+
                                  |                    |                 |
                          glue-refinery-       glue-refinery-    glue-refinery-
                            path1.py             path2.py          path3.py
                          continuous           categorical         bandit
                          next-bar return      up/flat/down        Thompson Sampling
                                  |                    |                 |
                                  +--------------------+-----------------+
                                                       |
                                              glue-dynamo.py (Python shell)
                                                       |
                                                  DynamoDB
```

Airflow submits each Glue job with boto3 and polls `JobRunState`, following
`Lab2-Airflow-Spark-Dynamo`.

## Status

| File | State |
| --- | --- |
| `glue-ingest-bars.py` | **done** — verified at both scales, see below |
| `refinery-walkthrough.ipynb` | **done** — 119 cells, executed, all three paths |
| `test_ingest_bars.py` | **done** — every bar checked against a `Decimal` reference |
| `glue-refinery-path{1,2,3}.py` | not written — prototyped in the notebook |
| `glue-dynamo.py` | not written |
| `dag-glue-workflow.py` | not written |
| `local-docker-development.sh` | not written |

## The data

Binance publishes monthly spot trade archives, free and unauthenticated:

```
https://data.binance.vision/data/spot/monthly/trades/<SYMBOL>/<SYMBOL>-trades-2025-01.zip
```

The CSVs inside are **headerless**, seven columns:

| # | Column | Type | Notes |
| --- | --- | --- | --- |
| 1 | `trade_id` | `bigint` | per-symbol sequence; BTC reaches 4,495,881,900, so not `int` |
| 2 | `price` | `decimal(18,8)` | |
| 3 | `qty` | `decimal(18,8)` | base asset |
| 4 | `quote_qty` | `decimal(18,8)` | quote asset; exactly `price * qty` on all three symbols |
| 5 | `time` | `bigint` | **epoch MICROseconds** — see below |
| 6 | `is_buyer_maker` | `boolean` | literal `True`/`False` |
| 7 | `is_best_match` | `boolean` | constant `True` across all 341M rows |

**`time` is microseconds, not milliseconds.** Sixteen digits. Reading it as milliseconds produces
a valid-looking timestamp in the year 56971 and raises nothing. Binance moved spot trade archives
to microsecond resolution for 2025 data; the download page does not say so. This is the single
biggest trap in the reader, and the schema names the column `event_time_us` specifically to make
the wrong reflex look wrong.

`data/raw/` and `data/unzipped/` are gitignored — 2.85 GB zipped, 25.6 GB extracted.
`data/sample/` holds a committed two-hour slice (2.9 MB gzipped, 356,201 real ticks) so the
project runs straight after a clone.

**Spark cannot read `.zip`.** It does not error either — it decodes the archive bytes as UTF-8 and
returns rows beginning `PK\x03\x04`. Extract before running:

```bash
unzip 'data/raw/*.zip' -d data/unzipped/
```

Extract to plain `.csv`, not `.csv.gz`: gzip is not splittable, so a 10 GB `.csv.gz` is handed to
a single task. The committed sample is gzipped only because at 1.3 MB it fits in one task anyway.

## Running the entryway locally

Needs Python 3.10+ and `pyspark==3.5.5`, plus a Java 11 or 17 JDK on `JAVA_HOME` — Spark does not
support Java 21+. From the project root:

```bash
python glue-ingest-bars.py --local \
    --input data/sample \
    --bars-output _localrun/bars \
    --bar-interval 5s \
    --calendar-start 2025-01-01T00:00:00 \
    --calendar-end   2025-01-01T02:00:00
```

The calendar overrides are what make a sample run meaningful. The bar grid is a **declared**
calendar, never derived from the observed data — an observed-range grid reports zero gaps even
when the last three days of a download failed, which would make Step 3 structurally unable to find
anything. Run the two-hour sample against the default month-wide grid and Step 3 truthfully
reports 1,606,390 of 1,607,040 bars missing: correct, and a measurement of the extract rather than
of the data. Declaring the narrower window keeps the check meaningful at both scales.

The full month needs neither override:

```bash
python glue-ingest-bars.py --local \
    --input data/unzipped \
    --bars-output _localrun/bars \
    --month 2025-01 --bar-interval 5s
```

`--local` supplies `master local[*]`; without it the session is built with no master so Glue
supplies one. `--shuffle-partitions 8` suits the sample; leave it unset for the full month so
Spark's default of 200 applies — the Step 1 dedup groups 341M ticks by `(symbol, trade_id)`, which
is essentially one group per row, and eight reducers for that spill relentlessly.

### Acceptance check

```bash
python test_ingest_bars.py
```

Runs the job on the committed sample, then rebuilds all 4,320 bars from the raw CSVs in pure
`decimal.Decimal` — no Spark, no float — and compares field for field. It also asserts that at
least 100 bars have a close strictly inside `(low, high)` and different from open, so the check
cannot silently go blind to an open/close ordering bug.

```
OK  4312 non-empty bars match the decimal reference field for field
OK  8 empty bars flagged and left unfilled
OK  916 bars would have caught an open/close ordering bug
```

## Verified run

Both runs are real. Sample figures come from the committed data; month figures from the full
extract on an NVMe SSD, 24 threads, `--driver-memory 10g`.

| | committed sample | full month |
| --- | --- | --- |
| input | 356,201 ticks | 340,971,834 ticks |
| window | 2 h | 31 days |
| bars at 5s | 4,320 | 1,607,040 |
| bars per symbol | 1,440 | 535,680 |
| empty bars | 8 (0.185%) | 613 (0.038%) |
| duplicate `(symbol, trade_id)` | 0 | 0 |
| `is_best_match` False | 0 | 0 |
| output | 0.4 MB Parquet | 125 MB Parquet, 200 files |
| wall clock | ~50 s | **6 min 56 s** |

The tick-accounting identity holds exactly at both scales: `sum(n_ticks)` over the bars equals the
tick count read. That single assertion is what proves the granularity change neither lost nor
invented a trade, and it is what catches a dropped file split — the most likely silent failure.

Verdict ledger from the full-month run, verbatim:

```
APPLIES  -- Step 1 ingest: 3 files -> one frame keyed (symbol, trade_id), symbols BTCUSDT, ETHUSDT, SOLUSDT
BANNED   -- Step 1 temporal join: aligning symbols on event_time fabricates phantom rows -- BTC/ETH/SOL routinely share a microsecond
N/A      -- Step 1 dedup: 0 duplicate (symbol, trade_id) keys in 340,971,834 rows
APPLIES  -- Step 2 schema: 7 headerless columns named and typed by position -- 3 decimal(18,8), 2 bigint, 2 boolean; inferSchema not used
APPLIES  -- Step 2 timestamp: event_time_us (epoch microseconds) -> event_time via timestamp_micros, session timezone pinned to UTC
N/A      -- Step 2 text: 0 string columns survive the schema -- trim/lower has nothing to normalise
BANNED   -- Step 2 prune: is_best_match is a variance question and VarianceThreshold is Step 8 -- path-isolated, BANNED on Path 3
APPLIES  -- Step 2.5 granularity: 340,971,834 ticks -> OHLCV bars at 5s (open/close by min_by/max_by on trade_id, never first/last)
APPLIES  -- Step 3 bars: 613/1,607,040 empty at 5s (0.038%)
APPLIES  -- Step 3 window edges: is_first_bar / is_last_bar flagged
BANNED   -- Step 3 fill: forward-fill / coalesce(volume, 0) / row-drop are Step 4 -- path-isolated, decided after the fork, not here
BANNED   -- Step 3 target: next-bar return via lead() is a post-fork feature-layer construct
N/A      -- Step 3 ticks: 0 nulls, 0 zero-qty, 0 zero-price in 340,971,834 ticks
N/A      -- Step 2 prune evidence: is_best_match is False in 0/340,971,834 ticks
```

Two of those `N/A`s are the interesting ones. Step 1 finds no duplicates because `trade_id` is
contiguous and strictly increasing on all three symbols; Step 3 finds almost nothing because these
markets never stop trading. **An `N/A` that was measured is a result.** Every one above carries the
number it was measured from, and none is hardcoded — the first month Binance ships a gap, they
flip on their own.

## Why 5-second bars

Measured over the full month at every candidate interval. The binding constraint is Path 2: its
`k=3` target needs a *flat* class that actually exists.

| interval | bars/month | empty bars | flat class (SOL / ETH / BTC) |
| --- | --- | --- | --- |
| 1s | 8,035,200 | 658,313 | 30.5 / 29.1 / 35.8 % |
| **5s** | **1,607,040** | **613** | **14.4 / 13.3 / 20.0 %** |
| 15s | 535,680 | 0 | 6.4 / 4.4 / 9.0 % |
| 1m | 133,920 | 0 | 2.3 / 0.6 / 1.7 % |
| 5m | 26,784 | 0 | 1.0 / 0.2 / 0.1 % |

At 1m, ETH's flat class is 285 bars out of 44,640 — declaring `k=3` there would be a fiction. At
15s and coarser Step 3 finds literally nothing. At 1s the bars degenerate: a quarter have
`high == low` and up to 14% contain a single tick, and missingness crosses the framework's own 5%
Path 1 override threshold on SOL and ETH but not BTC, which would silently run complete-case on
two symbols and median-impute on the third.

5s is the only width where the flat class is material, Step 3 has something honest to flag, and
the bars are still bars (median 98–129 ticks each). `1s`, `15s`, `1m` and `5m` all still work;
`--bar-interval 1s` is the documented missingness stress run.

## Bars schema

One row per `(symbol, bar)`, dense over the declared calendar.

| # | Column | Type | Meaning |
| --- | --- | --- | --- |
| 1 | `symbol` | `string` | `BTCUSDT` / `ETHUSDT` / `SOLUSDT` |
| 2 | `bar_us` | `bigint` | bucket start, epoch microseconds — timezone-proof join key |
| 3 | `bar_open_time_utc` | `timestamp` | same instant, `TIMESTAMP_MICROS` not INT96 |
| 4–7 | `open` `high` `low` `close` | `decimal(18,8)` | open/close by `min_by`/`max_by` on `trade_id` |
| 8 | `volume` | `decimal(28,8)` | `sum(qty)` |
| 9 | `quote_volume` | `decimal(28,8)` | `sum(quote_qty)`, the exchange's own notional |
| 10 | `n_ticks` | `bigint` | `0` for an empty bar — an observation, not a fill |
| 11–12 | `first_trade_id` `last_trade_id` | `bigint` | `NULL` on an empty bar |
| 13–14 | `taker_buy_qty` `taker_buy_quote_qty` | `decimal(28,8)` | `is_buyer_maker == False` |
| 15 | `all_best_match` | `boolean` | carried, not pruned — pruning is Step 8 |
| 16 | `is_missing_bar` | `int` | Step 3's flag. Flagged, never filled |
| 17–18 | `is_first_bar` `is_last_bar` | `int` | no predecessor / no successor |

Empty bars carry `NULL` OHLCV. That is deliberate: filling them is Step 4, which is path-isolated
and decided after the fork — and Path 3's Step 4 override keeps native missingness as a latent
state, so an entryway that forward-filled would have destroyed Path 3's input before the router
ever ran.

## The notebook

`refinery-walkthrough.ipynb` — 119 cells, executed, outputs committed.

| Part | Contents |
| --- | --- |
| 1 | Steps 1–3, the shared entryway — mirrors `glue-ingest-bars.py` operation for operation |
| — | **The Architectural Fork** |
| 2 | Path 1 — continuous, next-bar log return |
| 3 | Path 2 — categorical, direction `k=3` |
| 4 | Path 3 — bandit, Thompson Sampling, and three banned steps |

Launch Jupyter from the project root; every path is repo-relative. It runs entirely on the
committed sample, and its first cell builds the bars by invoking `glue-ingest-bars.py` itself — so
Run All works on a fresh clone, and the notebook cannot drift from the production job. It also
imports `verdict()` from the job rather than redefining it.

Numbers printed in the notebook are the **sample's**. The sample is a quiet window (00:00–02:00
UTC on New Year's Day), so its flat class runs 18.8–27.6% against the month's 13.3–20.0%. Where a
month figure matters it is quoted as prose and labelled as measured separately.

### Steps 4–10 × 3 paths

| Step | Path 1 — Continuous | Path 2 — Categorical | Path 3 — Bandit |
| --- | --- | --- | --- |
| 4 Imputation | **OVERRIDE** >5% missing → complete-case | median; missing cats → `SYSTEM_STATE_UNKNOWN` | **OVERRIDE** keep native missingness |
| 5 Diagnostics | univariate density; t-SNE sandbox | class balance; interaction correlation | stream diagnostics |
| 6 Topology | global cross-correlation | cross-correlation + cyclical sin/cos | cyclical time coordinates |
| 7 Feature Eng | deterministic cross-products | cyclical + interaction cross-products | interaction frame prep |
| 8 Pruning | VarianceThreshold | **ENFORCE** one-hot *before* pruning | **BANNED** |
| 9 Regularisation | CV Elastic Net | CV Elastic Net | **BANNED** |
| 10 Scaling | **LIMIT** linear only | quantile search | **BANNED** |

Path 3's three bans are the substance, and the notebook demonstrates each rather than asserting
it:

- **Step 8** — Apriori/ECLAT consume raw categorical tokens. One-hot dissolves a token into
  indicator columns and there is no itemset left to mine.
- **Step 9** — Elastic Net's L1 term zeroes low-variance columns, and a rare-but-high-lift itemset
  *is* a low-variance column. Regularisation deletes the discoveries the path exists to make.
- **Step 10** — Thompson Sampling updates `α ← γα + x`, `β ← γβ + (1−x)`. α and β are counts.
  Standardising them yields negative shape parameters, for which no Beta density exists.

### Two things the notebook is careful about

**Path 3 is a replay, not a stream.** Monthly archives are replayed in timestamp order to drive
the γ=0.97 conjugate updates. That is an honest reconstruction of a bandit's arithmetic, but a
replay is not a live stream — the file holds every arm's reward at every slot, and a live bandit
never sees the counterfactual.

**The reward definition inverts the arm ranking.** Reward = "bar closed up" scores a flat bar as a
failure, so each arm's rate is contaminated by its flat share. Measured on the full month at 5s:

| symbol | reward `close > prev` | reward `close >= prev` |
| --- | --- | --- |
| SOL | 42.91 | 57.28 |
| ETH | **43.47** (best) | 56.72 (worst) |
| BTC | 39.69 (worst) | **59.73** (best) |

The ranking flips completely, and the ETH–BTC gap under `>` is roughly half their difference in
flat share. Up and down are symmetric to within 0.6 pp on every symbol at every interval. This
bandit is learning tick-flatness, not alpha, and the notebook says so rather than presenting a
confident spurious result.

## Deploying to AWS Glue 4.0

Glue 4.0 is **Spark 3.3.0 / Python 3.10 / Java 8**. The job is written to that API surface, which
is narrower than the local Spark 3.5.5 in three places that matter:

| Wanted | Why it fails on Glue | Used instead |
| --- | --- | --- |
| `func.pmod(...)` | Python wrapper is 3.4.0 | `func.expr("pmod(...)")` |
| `func.timestamp_micros(...)` | Python wrapper is 3.5.0 | `func.expr("timestamp_micros(...)")` |
| `func.bool_and(...)` | Python wrapper is 3.5.0 | `func.expr("bool_and(...)")` |

In each case the **SQL name exists in 3.3.0** and only the Python wrapper is missing, so `expr()`
reaches it. `min_by`/`max_by` are genuinely 3.3.0 and are called directly. This is verified by
execution, not by reading release notes: stripping all 173 post-3.3.0 wrappers from
`pyspark.sql.functions` and re-running produces byte-identical output.

Upload and create the job:

```bash
aws s3 cp glue-ingest-bars.py s3://<bucket>/crypto_ticks/scripts/
```

Create a Glue job of type Spark, Glue version 4.0, pointing at that script, with an IAM role
holding read/write on the bucket. The job takes plain `argparse` arguments and parses them with
`parse_known_args`, because Glue appends `--JOB_NAME`, `--TempDir` and friends to `sys.argv` on
every run and a strict parser would exit 2 before Spark ever starts. No `--additional-python-modules`
is needed: the job is pure PySpark, no pandas, no boto3, no `awsglue`.

```bash
aws glue start-job-run --job-name crypto-ticks-ingest-bars \
  --arguments '{
    "--input":"s3://<bucket>/crypto_ticks/unzipped/",
    "--bars-output":"s3://<bucket>/crypto_ticks/curated/bars/",
    "--month":"2025-01",
    "--bar-interval":"5s"
  }'
```

Note the absence of `--local`: on Glue the master comes from the cluster.

## Repository layout

```
glue-ingest-bars.py            the Glue 4.0 entrypoint, Steps 1-3
refinery-walkthrough.ipynb     119 cells, executed: entryway, fork, all three paths
test_ingest_bars.py            every bar vs a pure-Decimal reference
data/sample/                   2.9 MB, 356,201 real ticks, committed
data/raw/                      gitignored -- the three source zips
data/unzipped/                 gitignored -- 25.6 GB extracted
```

## Notes and known gaps

Five things in the job are load-bearing and easy to break.

- **`event_time_us` is microseconds.** Every wrong conversion is silent. `timestamp_millis` and
  `to_timestamp(v/1000)` both return a valid timestamp in the year 56971 without raising.
- **The session timezone is pinned to UTC unconditionally**, not behind `--local`. Left to the
  default it takes the machine's zone; on the box this was built on that is UTC+2, under which the
  last bar of January renders as 1 February and `to_date()` drops 120 January bars per symbol into
  a February partition. Glue defaults to UTC, so without the pin the same job emits different
  partitions in the two places — invisible in a diff.
- **Money columns are `DecimalType`, never `Double`.** Summing 2M real `quote_qty` values under
  3 / 11 / 29 shuffle partitions gives three different doubles and one identical decimal. Float
  addition is not associative and Spark does not promise a stable merge order, so a Double
  pipeline is not idempotent — the same job over byte-identical input emits different bars. Cast
  to double only at the feature boundary, for `log()` and `pyspark.ml`.
- **`open`/`close` come from `min_by`/`max_by` on `trade_id`**, not `first()`/`last()` and not an
  ordering on `event_time`. `first()`/`last()` over a `groupBy` return whatever reached the
  partition first. `event_time` is only non-decreasing: 75% of ticks share a microsecond with
  their predecessor, up to 1,110 in a single microsecond, and 24,851 of those tie groups have a
  non-constant price — so an `event_time` ordering leaves open and close genuinely undefined on a
  large fraction of bars.
- **`is_buyer_maker == True` means the buyer was the *maker***, so the trade is an aggressive
  **sell**. `taker_buy` is therefore `is_buyer_maker == False`. Verified on 2M ticks:
  P(next tick up | `False`) = 0.996 versus 0.003 for `True`, and `corr(imbalance, return)` is
  +0.51 with this convention and exactly −0.51 inverted. The global buy/sell ratio is ~50/50 on
  every symbol, so no ratio sanity-check would catch the inversion.

Known gaps, stated plainly:

- **Nothing here has been run on AWS.** Glue 4.0 compatibility is verified by simulating the
  Spark 3.3.0 API surface locally, which catches missing Python wrappers but not runtime or IAM
  behaviour. The `--month` argument makes the job single-month; a backfill loops it.
- **Targets are not computed by the entryway**, deliberately. A next-bar return computed per month
  freezes a `NULL` into the last bar of every month, so the target is not append-only and
  January's edge needs recomputing when February lands. Targets belong to the feature layer, over
  the concatenated series.
- **The three path jobs and the DAG are not written.** They are prototyped in the notebook, which
  is where their design decisions and their measured numbers live.
- **The Step 1 dedup is one full shuffle over 341M rows to print a line expected to read `N/A`.**
  Kept deliberately — an `N/A` the job did not measure is a lie, and Step 1's whole claim is that
  the merge is deterministic. It is the job's dominant cost. Contiguity alone cannot replace it:
  `{1,2,2,4}` has `max-min+1 == count` and still contains a duplicate.
