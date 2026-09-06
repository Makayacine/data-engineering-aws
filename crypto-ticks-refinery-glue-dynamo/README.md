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
| `refinery_common.py` | **done** — `verdict()`, `build_session()`, `load_bars()` |
| `refinery-walkthrough.ipynb` | **done** — 119 cells, executed, all three paths |
| `test_ingest_bars.py` | **done** — every bar checked against a `Decimal` reference |
| `glue-refinery-path3.py` | **done** — Steps 4–10 of the bandit path, reproduces the notebook's numbers exactly |
| `glue-refinery-path1.py` | **done** — Steps 4-10 of the continuous path, reproduces the notebook's numbers exactly |
| `glue-refinery-path2.py` | **done** — Steps 4-10 of the categorical path, reproduces the notebook's numbers exactly |
| `glue-dynamo.py` | **done** — Python shell job, 96 items; designed rather than lifted, so see *What "verified" means here* |
| `dag-glue-workflow.py` | **done** — monthly Glue workflow, DagBag-verified on Airflow 2.9.3 |
| `test_dag_workflow.py` | **done** — the DAG's graph and two provider assumptions, no AWS |
| `local-docker-development.sh` | **done** — the whole chain inside the Glue 4.0 image, one stage per argument |

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
python glue-jobs/glue-ingest-bars.py --local \
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
python glue-jobs/glue-ingest-bars.py --local \
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
python tests/test_ingest_bars.py
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

## Running Path 1 locally

Like Path 3, Path 1 consumes **bars**, so the entryway runs first:

```bash
python glue-jobs/glue-refinery-path1.py --local \
    --input _localrun/bars \
    --output _localrun/path1
```

| Prefix | Rows on the sample | What it is |
| --- | --- | --- |
| `features/` | 4,301 x 11 | the Step 10 scaled matrix — what Part II consumes |
| `topology/` | 289 x 3 | Step 6's dependency schema, long-format, exported and never read back |
| `coefficients/` | 18 x 12 | the Step 9 + Step 10 ledger, and `glue-dynamo.py`'s input |

`features/` expands the scaled vector into named double columns rather than writing a
`VectorUDT`, which round-trips through Parquet as an opaque struct only Spark understands.
`coefficients/` carries a row for **every** column Step 8 kept, including the twelve Step 9
zeroed — the ledger is more useful as "what happened to each candidate" than as a list of
winners.

```bash
python glue-jobs/glue-refinery-path1.py --self-check
```

No Spark. It pins the three pure decisions that would go wrong silently: the **strict** 5%
firewall boundary (`>` quietly becoming `>=` swaps the entire Step 4 branch without changing a
row count or raising anything), the inclusive fold width (drop the `+1` and the last bar lands
in fold `K` so the `least()` clamp fires on every run, silently making the final block one row
wide), and the basis-point back-translation that is the whole justification for Step 10's LIMIT.

### What Path 1 does with each step

| Step | Badge | What the job does |
| --- | --- | --- |
| 4 Imputation | `OVERRIDE` above 5% | measures target missingness; **both branches are implemented** |
| 5 Diagnostics | `APPLIES` | univariate density profile — read-only, and asserted so |
| 6 Topology | `APPLIES` | 17x17 Pearson matrix — read-only, and asserted so |
| 7 Feature Eng | `APPLIES` | four named deterministic cross-products, 16 → 20 columns |
| 8 Pruning | `APPLIES` | `VarianceThreshold(0.0)`; `symbol` deleted, never encoded |
| 9 Regularisation | `APPLIES` | CV Elastic Net over 5 contiguous time-blocked folds |
| 10 Scaling | `LIMIT` | `StandardScaler` only; mu and sigma retained |

**Both Step 4 branches are implemented, and the measured rate takes the permissive one.** The
framework demonstrates only the override and merely *states* the sub-5% branch, so a job that
implemented what was demonstrated would do nothing at all on its own data. The threshold is
strict in both sources (`>5%`, "exceeds the 5% firewall"), so 5.0% exactly keeps the default.

**The target is manufactured.** `log(lead(close) / close)` over a window partitioned by symbol
and ordered by `bar_us`. The framework gives no rule for manufacturing a target — NexusMart
arrived with three genuine ones and a bar file arrives with none — so every Path 1 verdict is
conditional on that nomination, and the job says so through `verdict()` at the fork rather than
leaving it implied.

**The cross-validation is time-blocked, not a true expanding window.** Spark's `CrossValidator`
uses random folds by default; on ordered bars that trains on the future, and adjacent bars are
near-duplicates so a random split puts a row and its own neighbours on both sides of the
boundary. `foldCol` with contiguous `bar_us` blocks — keyed on the timestamp so all three
symbols of one instant stay together — fixes the duplicate leak but **not** the direction:
block 0 is still validated against a model trained on blocks 1-4, which are later.

**Step 3's missingness flag cannot survive Path 1's Step 4, and the framework says it should.**
`is_missing_bar` is 1 exactly when `close` is NULL, a NULL close makes the target NULL, and
every NULL-target row is evacuated by the complete-case cut — so the column is a flat zero by
the time Step 8 measures it, and Step 8 deletes it for having no variance. That is structural,
not a property of one month. The LaTeX trace quietly loses the same column between Step 7 and
Step 8 without comment. The job reports the collision instead of dropping the column silently.

### Verified Path 1 run

Against the committed sample, reproducing `refinery-walkthrough.ipynb` Part 2 to the last digit:

```
N/A      -- Step 4 firewall: target missingness 0.440% vs the strict 5% threshold
APPLIES  -- Step 4 Path 1: complete-case on y (4,320 -> 4,301 rows), median impute on X (11 cells)
APPLIES  -- Step 6 topology: 17x17 Pearson matrix, 8 pairs at |r| >= 0.90; no value changed
APPLIES  -- Step 8 Path 1: VarianceThreshold(0.0) removed 2 columns [all_best_match, is_missing_bar]
APPLIES  -- Step 9 Path 1: CV Elastic Net kept 6/18 columns; 12 shrunk to exactly zero
LIMIT    -- Step 10 Path 1 LIMIT: StandardScaler over 6 columns; mu and sigma retained

stage                                 rows  X cols     selected  regParam 1e-06, pure L1
post-fork, y + base features         4,320      16     CV RMSE   0.00016487
4  imputation                        4,301      16     target sd 0.00016641
7  cross-products                    4,301      20
8  VarianceThreshold(0.0)            4,301      18     back-translation, bp of next-bar return
9  CV Elastic Net                    4,301       6       +1 sd imbalance -> +0.1196 bp
10 StandardScaler (LIMIT)            4,301       6       +1 sd body_bp   -> +0.0440 bp
```

The CV RMSE of 0.00016487 against a target sd of 0.00016641 is a 0.9% improvement on predicting
the mean. That is the honest read of it, and it is roughly what a next-bar return on 5-second
crypto bars should look like.

Glue 4.0 compatibility is checked by the strip test described under **Deploying**, **extended
to `pyspark.ml`** because Path 1 is the first path that uses it: 174 names deleted, control and
treatment through the same harness, then diffed. Output identical — 123 log lines and all three
Parquet payloads byte for byte.

## Running Path 2 locally

Like the other two, Path 2 consumes **bars**, so the entryway runs first:

```bash
python glue-jobs/glue-refinery-path2.py --local \
    --input _localrun/bars \
    --output _localrun/path2
```

| Prefix | Rows on the sample | What it is |
| --- | --- | --- |
| `features/` | 4,301 x 29 | the scaled matrix Part II consumes |
| `topology/` | 144 x 4 | Step 6's dependency schema — pooled **and** per symbol |
| `coefficients/` | 72 x 8 | the k x p Step 9 matrix in long form, `glue-dynamo.py`'s input |
| `scaling_search/` | 3 x 5 | the Step 10 evidence: every candidate, its holdout accuracy, and whether Spark actually ships it |

`scaling_search/` exists because the framework mandates a *search* and never states its
criterion. The criterion used here is **held-out accuracy on the last 20% of the window** — the
only block no candidate was fitted on — and persisting the losing candidates alongside the
winner is what makes that claim checkable rather than asserted.

```bash
python glue-jobs/glue-refinery-path2.py --self-check
```

No Spark. It pins the class-label bijection (get it wrong and every coefficient row is
attributed to the wrong class — the accuracy is unchanged and only the interpretation is
destroyed) and the fold geometry: `fold_cut` monotone over four spans, and three expanding fold
blocks that all end at or before the dev/holdout boundary. That last assert is the one that
matters — edit `FOLD_BLOCKS` to `(0.8, 0.9)` and Step 9 would validate on the block Step 10
later scores on, so both numbers would be optimistic and neither would look wrong.

### What Path 2 does with each step

| Step | Badge | What the job does |
| --- | --- | --- |
| 4 Imputation | `APPLIES` | median impute **per symbol**; the missing category gets a name |
| 5 Diagnostics | `APPLIES` | class balance — read-only, and asserted so |
| 6 Topology | `APPLIES` | cross-correlation **and** cyclical sin/cos coordinates |
| 7 Feature Eng | `APPLIES` | numeric cross-products **and** a categorical x categorical product |
| 8 Pruning | `ENFORCE` | one-hot encoding **before** variance pruning |
| 9 Regularisation | `APPLIES` | multinomial Elastic Net over expanding time-blocked folds |
| 10 Scaling | `APPLIES` | quantile search across three candidate transforms |

**Step 4's grouping is the trap.** A single median over the frame blends three price scales
(BTC ~ $94k, ETH ~ $3.3k, SOL ~ $190) and would hand an empty ETH bar a five-figure close. The
framework's NexusMart matrix has one cohort per run and never has to state this; bar data does.
Note also that Path 2 has **no** 5% firewall — that is a Path 1 rule, and the framework runs
the Path 2 branch at 3.1% missing with no threshold test at all — so the rate is reported and
not acted on.

**Step 6 is read-only on Path 1 but not on Path 2.** The same grid cell also mandates cyclical
coordinate spaces, and those are columns. The correlation matrix is computed **pooled and per
symbol**, because a "global" matrix over a frame holding three symbols is largely a matrix of
*between-symbol scale differences*: 10 pair-symbol combinations flip sign against the pooled
figure, and 5 per-symbol cells are undefined outright because a symbol with no empty bars has a
constant `is_missing_bar` and therefore a zero denominator. The pooled matrix conceals that by
borrowing the other symbols' variance.

**Step 8's ENFORCE is about order, and the order is priced.** Both operations happen either
way; what changes is the granularity the variance filter can act at. Encode-first prunes at
**level** granularity, so `ETHUSDT__SYSTEM_STATE_UNKNOWN` lives or dies on its own. Prune-first
prunes at **column** granularity — and what it measures to decide is the variance of index
*codes*, which the job measures both ways on identical data: **0.6765** under the default
`frequencyDesc` ordering and **2.6664** under `alphabetAsc`. Same 4,320 rows, different
numbering, different keep/drop decision. That counterfactual is read-only; the job never runs
the banned order.

**The honest bookend on the ENFORCE.** On this data the preserved level cannot pay off. The
empty bars are precisely the rows whose target is unobservable, so the `SYSTEM_STATE_UNKNOWN`
dummies are all-zero on the labelled frame Step 9 trains against, and Elastic Net zeroes both.
The mandate is still right — it is what keeps the level addressable — but a subgroup finding
was not measured here and is not claimed.

### Verified Path 2 run

Against the committed sample, reproducing `refinery-walkthrough.ipynb` Part 3 to the last digit:

```
APPLIES  -- Step 4 numerics: 64 nulls across 8 columns median-imputed per symbol, 0 remain
APPLIES  -- Step 4 categoricals: SYSTEM_STATE_UNKNOWN assigned to 8 rows, 0 dropped
APPLIES  -- Step 5 class balance: k=3, pooled flat share 21.83% of 4,301 labelled rows
ENFORCE  -- Step 8: prune-first's criterion moves 0.6765 -> 2.6664 on identical data
APPLIES  -- Step 9 CV: random folds score higher on 4 of 4 grid points -- the leak, not a better model
APPLIES  -- Step 10: PowerTransformer (signed log1p) wins at 0.4042 over 3 candidates

  regParam  elasticNet   time-ordered CV   random-fold CV      gap
      0.01         0.5            0.4102           0.4448  +0.0346
      0.01         1.0            0.4079           0.4433  +0.0355
       0.1         0.5            0.3598           0.4058  +0.0460
       0.1         1.0            0.3598           0.4058  +0.0460

candidate                                 holdout accuracy   vs baseline
PowerTransformer (signed log1p)                     0.4042       +0.0502
StandardScaler                                      0.4007       +0.0467
QuantileTransformer (percent_rank)                  0.3972       +0.0432
(majority-class baseline)                           0.3540
```

**The random folds beat the time-ordered folds on every single grid point.** That gap — +0.035
to +0.046 — is the leak, measured rather than asserted. Spark's `CrossValidator` builds random
folds by default, and on ordered bars that trains on the future *and* splits near-duplicate
adjacent bars across the boundary. The framework says "cross-validated" and never mentions
temporal ordering, because NexusMart's rows are exchangeable sessions and bars are not.

**Two of the three Step 10 legs are substitutions, and `scaling_search/` says which.** Spark
ships neither `PowerTransformer` (no Yeo-Johnson, no Box-Cox) nor `QuantileTransformer` (no
rank-to-uniform map). The stand-ins are a signed `log1p` — monotone, sign-preserving,
Yeo-Johnson at lambda=0 with no lambda search — and `percent_rank` over an unpartitioned
window. The second also cannot carry the training quantiles across to the holdout the way a
real `QuantileTransformer` would, and it collapses to one executor per column, so above
`MAX_QUANTILE_ROWS` (250,000) the job **skips that leg and says so** rather than stalling. The
winning transform is *not* the one written to `features/`: `StandardScaler` is, because it is
the only leg that can be refitted reproducibly on next month's bars. The search result is
persisted next to it so the choice is visible rather than silently overridden.

**Cost.** Step 9 fits `len(REG_PARAMS) x len(ELASTIC_NET_PARAMS) x (3 ordered + 3 random)` = 24
logistic regressions at `maxIter=50`, and Step 10 fits 3 more. On the committed sample the whole
job is about 2.5 minutes; the fit count, not the row count, is what dominates.

## Running Path 3 locally

Path 3 consumes **bars**, not ticks, so the entryway runs first and Path 3 reads its output:

```bash
python glue-jobs/glue-refinery-path3.py --local     --input _localrun/bars     --output _localrun/path3
```

Three artifacts are written beneath `--output`, one per thing the path produces:

| Prefix | Rows on the sample | What it is |
| --- | --- | --- |
| `features/` | 4,320 x 20 | the per-bar frame — Steps 5 and 6, auditable against `bars` |
| `frame/` | 1,440 x 2 | the interaction frame — Step 7, the raw-token baskets the bans protect |
| `arms/` | 3 x 16 | the bandit posterior — the engine's result, and `glue-dynamo.py`'s input |

The arms and the bar width are **derived from the input**, not passed as flags. Both are
properties of the frame that was actually written, and a flag is a second place for them to be
wrong: a symbol list that disagrees with the data silently drops an arm from the pivot, and a
declared interval that disagrees turns Step 5's stream diagnostic into a statement about
nothing. Deriving them is also what makes the grid checks possible — the job refuses an input
whose slots are off-grid or not dense across every symbol, because `lag()` over a sparse grid
compares a bar against a non-adjacent one and leaves no null to notice.

```bash
python glue-jobs/glue-refinery-path3.py --self-check
```

No Spark, no input, no output: it asserts the conjugate loop against known answers. `replay()`
is the only non-trivial arithmetic in the job and every way it can be wrong is quiet — a wrong
decay, an imputed `x = 0` for an unobserved reward, and ageing the arms that were not pulled all
return a valid Beta and a plausible winner. The check pins the decay-without-increment rule, the
exact `alpha <- gamma*alpha + x` recurrence, saturation at `1/(1-gamma)`, and seeded
reproducibility.

### What Path 3 does with each step

| Step | Badge | What the job does |
| --- | --- | --- |
| 4 Imputation | `OVERRIDE` | carries the 8 empty bars forward as unobserved latent states; reports the five standard OHLC fills as `BANNED` by name |
| 5 Diagnostics | `APPLIES` | stream diagnostics (19 of 4,320 arm-slots have an unobserved reward), then the reward **definition** |
| 6 Topology | `APPLIES` | hour-of-day and minute-of-hour as sin/cos pairs |
| 7 Feature Eng | `APPLIES` | 1,440 variable-length baskets over 28 raw string tokens |
| 8 Pruning | `BANNED` | one-hot + VarianceThreshold prunes in ascending order of token rarity |
| 9 Regularisation | `BANNED` | L1 zeroes the low-support/high-lift tail — and there is no `y` to regress against |
| 10 Scaling | `BANNED` | standardising alpha/beta gives 3 of 6 shape parameters `<= 0` |

A banned step is **reported, never skipped**. A step that leaves no line in the log is
indistinguishable from a step nobody thought of, and this path's whole claim is that its three
bans are load-bearing. The notebook runs each banned operation once on a copy to measure what it
would cost; that demonstration is the notebook's job, and a production job that ran them would be
doing the thing it forbids.

`is_best_match` is settled here, as the entryway said Path 3 would have to. It is resolved at
**Step 7 as a frame-construction rule, not as the banned Step 8 variance prune** — the job emits
`<SYM>_NOT_BEST_MATCH` only for the column's *minority* state, so a token that sits in 100% of
baskets never enters the vocabulary (it would add a constant to every support count and drag
every lift toward 1), while the moment the column varies its rare state becomes exactly the kind
of high-lift token the path exists to keep. A variance filter deletes a column because it does
not vary; this rule would keep it.

### Verified Path 3 run

Against the committed sample, reproducing `refinery-walkthrough.ipynb` Part 4 to the last digit:

```
OVERRIDE -- Step 4 native missingness: 8 empty bars carried forward as unobserved latent states
APPLIES  -- Step 5 stream diagnostics: 19 of 4,320 arm-slots have an unobserved reward
APPLIES  -- Step 5 reward: '>' picks SOLUSDT, '>=' picks BTCUSDT -- the ranking inverts
APPLIES  -- Step 7 interaction frame: 1,440 token sets, 28 distinct raw string tokens
BANNED   -- Step 10 domain: 3 of 6 standardised shape parameters are <= 0
APPLIES  -- Path 3 engine: 1,440 pulls, budget concentrates on SOLUSDT (41.8% over 200 seeds)

arm        up%   flat%   down% | full-info rate | budget share | won the run
BTCUSDT  36.34   27.59   36.07 |     0.3636     |    20.3%     |     5.5%
ETHUSDT  40.24   19.03   40.73 |     0.4025     |    37.9%     |    60.0%
SOLUSDT  41.10   18.84   40.06 |     0.4111     |    41.8%     |    34.5%
```

Glue 4.0 compatibility is checked by the strip test described under **Deploying**: 174
post-3.3.0 names deleted, control and treatment through the same harness, then diffed. Output is
identical — 61 log lines and all three Parquet payloads byte for byte. `pmod` is among the
stripped names, and the job reaches it through `func.expr` for the same reason the entryway
does.

**Cost of `--seed-replicates`.** Measured: 5.4 ms per replay of 1,440 slots x 3 arms, about
266,000 slot-steps per second on one core. The default of 200 replicates costs ~1.1 s on the
sample and projects to **~6.7 minutes on a full month at 5s** (535,680 slots). The replay matrix
is collected to the driver, so the job refuses more than 1,000,000 slots outright rather than
dying in an OOM twenty minutes in — a full month at 1s is 2,678,400 slots and is refused by that
cap, not by the timing.

## Loading DynamoDB

`glue-dynamo.py` publishes the three path ledgers to one DynamoDB table. It is a Glue **Python
shell** job, not Spark: the three artifacts are 18, 72 and 3 rows — about 12 KB of Parquet
between them — and a Spark job would spend two and a half minutes starting a cluster to move
them. Nothing in the file imports pyspark, so the Glue 4.0 strip test that gates the three path
jobs does not apply to it.

The item count is set by the *shape* of the refinery, not by how much data went through it: it is
one item per surviving feature, per (feature, class) cell and per arm, so it is the same order of
magnitude for the committed two-hour sample as for the full 340,971,834-tick month. Only the
values change.

```bash
python glue-jobs/glue-dynamo.py --self-check
```

```bash
python glue-jobs/glue-dynamo.py --dry-run --run-id 2025-01-sample \
    --path1 _localrun/path1 --path2 _localrun/path2 --path3 _localrun/path3
```

Both run with **no AWS account, no credentials and no region configured**. Each `--path*` is that
path job's `--output` prefix; the loader appends the artifact name it knows that path writes.
Paths are independent, so a single path that was re-run can be re-published on its own.

### The table

| | |
| --- | --- |
| partition key | `pk` (S) — `<run-id>#<path>`, e.g. `2025-01#path1` |
| sort key | `sk` (S) — the grain within that path |

| Source | Rows | Items | Sort key | Example |
| --- | --- | --- | --- | --- |
| `path1/coefficients` | 18 | 1 + 18 | `feature#<name>` | `feature#imbalance` |
| `path2/coefficients` | 72 | 1 + 72 | `feature#<name>#class_name#<class>` | `feature#imbalance#class_name#down` |
| `path3/arms` | 3 | 1 + 3 | `symbol#<symbol>` | `symbol#BTCUSDT` |

96 items on the sample. One partition is exactly one path's result from exactly one run, so
*give me Path 1's ledger for 2025-01* is a single `Query` on the partition key, and
`begins_with(sk, "feature#imbalance#")` is one Path 2 feature's three class rows. The sort key
alternates the artifact's own column names with their values, so it reads back against the
Parquet it came from without a translation table. The `#model` header item sorts before every
grain key — `#` is `0x23` and every grain prefix starts with a letter — so a Query returns it
first without being asked to.

The separator is safe **by measurement**: all 47 distinct grain tokens across the three artifacts
are `[A-Za-z0-9_]`, none contains a `#`, and the longest is 47 characters. The loader raises on a
grain value containing one rather than emitting a key that would silently collide.

Three parts of that are judgement calls rather than deductions:

- **The run id is in the partition key.** A rerun of the same run is idempotent — same keys, same
  overwrite — while a new run writes a new partition instead of mutating the old one. The
  alternative, a table holding only "current" with history left in S3, is smaller and defensible;
  it was rejected because a month whose Step 8 keeps fewer features than the last one leaves the
  dropped features behind as items that still claim to be current. The key alone does not fix
  that — `put_item` cannot delete — so after writing each partition the job **reads it back and
  removes whatever this run did not produce**, logging every deletion by name. An earlier draft
  only counted and logged a warning, which is the shape of fix that reads as diligence and
  changes nothing: the job still exits 0, Airflow still goes green, and the stale item is still
  served. Deleting is safe here precisely because the partition is keyed `<run-id>#<path>`, so
  everything in it was written by a previous run of this job for this run id and this path.
- **The run id is supplied, not derived.** Path 3 derives its arms and its bar width from the
  input on the grounds that a flag is a second place for them to be wrong. That argument cannot
  be made here: the three artifacts carry no month, no calendar and no run identity of any kind,
  so there is nothing to derive one from. `--run-id` is therefore required and has no default,
  which at least makes the second place a visible one.
- **The model-level columns are lifted onto a header item.** `reg_param`, `elastic_net_param`,
  `intercept`, `cv_rmse`, `target_sd` and `n_rows` are identical on all 18 Path 1 rows, and
  `reg_param`, `elastic_net_param` and `baseline_accuracy` on all 72 Path 2 rows. Parquet repeats
  them because Parquet is rectangular and has nowhere else to put them; DynamoDB is not and does.
  The job **checks** they are constant before lifting them, rather than trusting the list: if a
  future run made one per-row, lifting it would publish one row's value as the model's and delete
  the other seventeen without a word.

There is no `LATEST` pointer item, no GSI and no run manifest. *Give me the current model*
therefore requires the caller to know the run id, and *how did this coefficient move over twelve
months* is twelve Queries. Both are the right upgrade the day something downstream actually asks
— Part II and the gates were never written, so today they would be structure built for a consumer
that does not exist. A `#complete` manifest item was considered for the same reason and dropped
for a sharper one: Glue's own `JobRunState` already tells Airflow whether the load finished, and
with the sweep in place a load interrupted half way is repaired by the rerun rather than needing
to be detected first.

### DynamoDB rejects `float`, and rejects it at write time

The same shape of trap as the `numpy.float64` that killed Path 2's first write, and the same
cost: the type error is raised after every step of the refinery has already been paid for.
Measured against boto3 1.39.11's `TypeSerializer`:

| Value | Result |
| --- | --- |
| `0.1` | `TypeError` — "Float types are not supported. Use Decimal types instead" |
| `Decimal(0.1)` | `decimal.Inexact` — the exact binary value is 55 significant digits and `DYNAMODB_CONTEXT` has `prec=38` |
| `Decimal(str(0.1))` | `{'N': '0.1'}` — correct |
| `numpy.float64(0.1)` | `TypeError` — a `float` subclass, caught by the float check |
| `numpy.int64(212)` | `TypeError` — "Unsupported type" |
| `numpy.bool_(True)` | `TypeError` — "Unsupported type"; numpy 2.x `bool_` is **not** a `bool` subclass |
| `Decimal(str(float("nan")))` | `TypeError` — "Infinity and NaN not supported" |
| `Decimal("1E-131")`, `Decimal("1E126")` | **encoded without complaint** — and DynamoDB rejects both |

That last row is the one that matters to the verification argument. DynamoDB's number range is
`1E-130` to `9.9999…E125`; boto3's serializer does not enforce it, so there is a band where
`--dry-run` comes out green and the real write fails. `attribute()` therefore checks the
magnitude itself. It is unreachable on today's artifacts — they span `1.36e-08` to `4301.0` —
but "if it would be rejected, it is rejected on a laptop" is either true or it is not.
(`5e-324` is *not* in that band: boto3 raises `Underflow`, because a denormal's exponent falls
below `DYNAMODB_CONTEXT`'s `Etiny`.)

Two consequences run through the whole file.

**The Parquet is read with pyarrow, not pandas.** `pyarrow.Table.to_pylist()` returns native
`str` / `int` / `float` / `bool` and `None` for a null. `pandas.read_parquet` returns numpy
scalars — every one of which the table above rejects — and turns a null `float64` into `NaN`,
which is a *different* rejection with a different message. Path 1's `mu`, `sigma` and `bp_per_sd`
are null on the 12 features Step 9 zeroed, so the pandas route hits that on the first artifact.
`Decimal(str(x))` loses nothing: Python's float `repr` has been the shortest round-tripping
string since 3.1, and all 543 float cells in the three artifacts satisfy
`float(Decimal(str(x))) == x`.

**A null becomes an absent attribute**, not a `NULL` and not a zero. The feature has no `sigma`
because Step 9 zeroed it and Step 10 never scaled it; absent is the honest encoding and the one
that costs nothing to store.

### What "verified" means here

The three path jobs each reproduce a notebook Part to the last digit. `refinery-walkthrough.ipynb`
stops at the three paths, so **this job has no prototype and no oracle**. What replaces one is
narrower, and worth naming exactly:

1. **Every item is encoded by the real encoder before anything is sent.** `--dry-run` builds
   every item from the real artifacts and passes each through
   `boto3.dynamodb.types.TypeSerializer` — not a mock and not a re-implementation, but the exact
   code the DynamoDB client runs on the way to the wire. An item that would be rejected for its
   types is rejected on a laptop, before the refinery is paid for.
2. **The keys are proved unique at build time.** A duplicate `(pk, sk)` is the one failure here
   that destroys data without raising: `batch_writer` de-duplicates nothing, a repeated key
   inside one batch is a `ValidationException`, and a repeated key *across* batches is a silent
   overwrite that would turn 72 rows into 24 items with no error and no warning. Not
   hypothetical: **`imbalance` is a feature name in Path 1's ledger and in Path 2's** — the only
   name the two share — so a key built from the feature name alone would have those two rows
   fighting over one item today. Putting the path in the *partition* key is what keeps them
   apart, which is the reason for it rather than a pleasant side effect.
3. **`--self-check` pins the pure decisions** — `bool` checked before `int` (`isinstance(True,
   int)` is `True`, so the wrong order writes `survived_step9` as the number `1`, and `== True`
   cannot catch it because `1 == True`), the `Decimal(str(x))` round trip, `NaN`/`inf` refused
   with the column name attached, nulls omitted, Path 2's two-part grain, the `#` guard, and the
   constant-column proof. No AWS, no network, no input.

What sits on the other side of that line belongs to the deployment rather than to the code:
the table and its key schema, the IAM role's write permission, the region and the provisioned
throughput. The key schema is the one to get right, and it is stated exactly in *The table*
above so it can be created to match.

**Why not moto or localstack.** Both were considered and neither is used. moto reimplements
DynamoDB in Python, so a green moto run is evidence about moto; the parts it would add on top of
the serializer — does the table exist, is its key schema the one this job assumes — are exactly
the parts a fake table cannot vouch for, because the test creates the fake table. That is a new
test dependency bought for a weaker guarantee than the one boto3 already ships.

One thing this job found that nothing upstream could. It is the first place all three artifacts
meet, and when it was written they disagreed: `survived_step9` was a genuine boolean on Path 1
and the strings `"True"`/`"False"` on Path 2. The loader reports a type disagreement rather than
repairing one — retyping here would make the table disagree with the Parquet it came from — so
the fix went where it belonged, `StringType` → `BooleanType` in `glue-refinery-path2.py`'s
`COEFFICIENT_SCHEMA`. Path 2 was re-run to confirm the change was isolated: `coefficients/` is
still 72 × 8 and every one of the 72 differing cells is that column, same truth value, while
`features/`, `topology/` and `scaling_search/` came back identical in schema and payload. The
check now reports nothing, which is the honest state for it rather than a reason to remove it —
a fourth path, or a schema edit to any of the three, has nowhere else to be caught.

## The Airflow DAG

`dag-glue-workflow.py` runs the whole pipeline once a month:

```
validate_raw_ticks >> check_validation >> [ingest_bars, end_dag]
ingest_bars >> [refine_path1, refine_path2, refine_path3] >> load_dynamo
```

The three path tasks are parallel, and that is not a scheduling optimisation. The fork is the
project's thesis: the paths share step numbers and nothing else, so a DAG that chained them
would assert a dependency the code deliberately does not have.

**The month is supplied once and reaches three places.** `glue-dynamo.py` says in its own
docstring that its `--run-id` is supplied rather than derived, because the artifacts carry no
month to derive it from. This is where it comes from — `data_interval_start` on an `@monthly`
schedule, formatted once and passed to the entryway's `--month`, into the S3 prefix every path
job writes under, and into the loader's `--run-id`. Those three *have* to agree: every job
writes `mode("overwrite")` and none of them partitions, so a second month pointed at the same
prefix destroys the first, and DynamoDB would hold a run-scoped history whose S3 artifacts no
longer exist. Month-scoping the prefixes in the DAG is what keeps the table's key and the bucket
telling the same story. `test_dag_workflow.py` asserts the agreement rather than trusting it.

### What the reference lab's DAG does differently

`Lab2-Airflow-Spark-Dynamo/dag-glue-workflow.py` hand-rolls its Glue polling with boto3, and
that poller has two bugs worth naming because both are silent:

- **It passes on failure.** The loop exits once the state is no longer
  `RUNNING`/`STARTING`/`STOPPING` and then logs that the job "has finished" — so `FAILED`,
  `TIMEOUT` and `STOPPED` all leave the loop and the task goes green. A Glue job that died hands
  a green task to the next one, which runs against last month's artifacts.
- **It polls the wrong run.** `get_job_runs(JobName=..., MaxResults=1)` returns the most recent
  run of that job *name*; the `JobRunId` that `start_job_run` returned is thrown away.

`GlueJobOperator` from `apache-airflow-providers-amazon` keeps the run id it started, polls that
one, and raises on a terminal state that is not `SUCCEEDED`. That provider is already a
dependency of this repository — the sibling `bank-marketing` DAG uses
`EmrServerlessStartJobOperator` — so preferring the operator over hand-rolled boto3 is this
repo's existing precedent, not a new one.

**A correction, since this repo repeats the claim elsewhere.** The widely-repeated line that
`airflow.operators.dummy_operator` and `airflow.operators.python_operator` *break DAG parsing*
on Airflow 2.4+ is **wrong**, and it was worth measuring rather than repeating. On Airflow 2.9.3
all of `airflow.operators.dummy_operator`, `airflow.operators.dummy`,
`airflow.operators.python_operator`, `airflow.hooks.postgres_hook` and `airflow.utils.dates.days_ago`
import fine, and Lab 2's DAG loads into a DagBag with **no import errors at all** — each just
emits a `DeprecationWarning`, and `provide_context=True` a `RemovedInAirflow3Warning` that is
otherwise ignored. They are shims scheduled for removal in Airflow 3. This DAG uses the modern
spellings because the old ones are deprecated, not because they are broken today.

`days_ago(1)` is dropped for a second reason as well: a *dynamic* `start_date` moves every time
the scheduler re-parses the file, and this DAG derives its S3 prefixes and its DynamoDB
partition key from the run's data interval. A key that depends on when a file was last parsed is
not a key. `start_date` is a fixed `datetime(2025, 1, 1)`.

### Verifying the DAG

A DAG has no `--self-check`: it is not a program that runs, it is a graph a scheduler parses, and
every way it can be wrong is a parse or a wiring error. `test_dag_workflow.py` is the check —
no AWS, no network, no Airflow metadata database:

```bash
docker run --rm -v "${PWD}:/opt/airflow/proj" -w /opt/airflow/proj \
  apache/airflow:2.9.3-python3.11 python tests/test_dag_workflow.py
```

It asserts that the file parses into a DagBag with zero import errors, that the task graph is
the one drawn above, that the three path tasks are wired to each other in neither direction,
that all five Glue tasks have `wait_for_completion=True` — the reference lab's bug, asserted
away — that `--local` reaches none of them, and that `script_args` is a template field, which is
the one assumption in the DAG about the provider rather than about this repository. Without
that last one the jobs would receive the literal string
`{{ data_interval_start.strftime('%Y-%m') }}` as their `--month` and the entryway would build a
bar calendar for a month of that name.

Verified against **Airflow 2.9.3 / `apache-airflow-providers-amazon` 8.25.0** in the official
image. That covers the DAG itself: it parses, its graph is the one drawn above, every Glue task
waits for completion, and `script_args` is a template field — which is what lets `MONTH` reach
the jobs at all. Whether the Glue jobs exist under those names, and whether the scheduler's role
may start them, is settled when they are created.

## Running the whole chain in the Glue 4.0 image

`local-docker-development.sh` runs the same five scripts inside
`amazon/aws-glue-libs:glue_libs_4.0.0_image_01` — the image AWS ships for Glue 4.0, so
**Spark 3.3.0 / Python 3.10 / Java 8** instead of the local Spark 3.5.5. It takes a stage:

```bash
./local-docker-development.sh              # the whole chain
./local-docker-development.sh selfcheck    # the four --self-check modes only
./local-docker-development.sh path2        # one leg, if an earlier run left the bars
```

Output goes to `_localrun/docker/`, never `_localrun/`, so a container run cannot overwrite what
a native run produced. Nothing is installed: the image already carries pyspark 3.3.0+amzn.1,
boto3 1.24.70, pyarrow 10.0.0 and numpy 1.23.5, which is every import these jobs make.

Three things make it longer than the reference lab's six lines.

**The five scripts are a pipeline, not a menu.** One `SCRIPT_FILE_NAME` variable can name one
script; it cannot say that the three paths consume the entryway's bars and the loader consumes
all three. So the default is the whole chain and a stage argument runs one leg.

**They are not all Spark.** `glue-dynamo.py` is a Glue *Python shell* job, run with `python3` and
never `spark-submit` — the same split the deployed job definitions make.

**The image's ENTRYPOINT is `bash -l`, which is not an exec.** The container command is handed to
a login shell as the name of a *script file*, so `docker run <image> python3 job.py` asks bash to
interpret an ELF binary and exits 126 with `cannot execute binary file`; `spark-submit` survives
only because it happens to be a shell script itself. Both therefore go through `-c`, which is
also the form AWS's own documentation uses.

Two of the lab's flags are dropped on purpose. The `~/.aws` mount and `AWS_PROFILE` buy nothing —
every stage reads the committed sample and writes into the workspace — and on a host without a
`~/.aws` the mount's only effect is to create one, owned by root. The Spark UI is not published
either: these are batch jobs that exit on their own, and `-p 4040:4040` would abort the whole
chain whenever a native `--local` run, or an orphaned JVM from a killed one, already held the
host port. `DISABLE_SSL=true` is kept, and is not cargo cult — without it the login profile
generates a self-signed keystore on every container start.

### What the container run measured

Docker Desktop gives the container **8 CPUs and 7.8 GiB** against the host's 24 cores, so the
chain is slower than the native runs documented above, and the gap is widest on Path 2 — the
stage whose cost is its **fit count** (24 logistic regressions at Step 9, three more at Step 10)
rather than its row count.

| Stage | Container | Native, for comparison |
| --- | --- | --- |
| `selfcheck` | 24 s | no Spark job runs |
| `ingest` | 46 s | ~1 min |
| `path1` | 3 min 43 s | ~3 min |
| `path2` | 6 min 52 s | ~2.5 min |
| `path3` | 1 min 33 s | ~53 s |
| `dynamo` | 5 s | ~5 s |

Run as `./local-docker-development.sh` with no argument, end to end, the whole chain measured
**12 min 15 s** — less than the stages sum to above, because those were timed one container at a
time from cold page cache. The native chain is ~7.5 min.

Every number the jobs logged is the one this README already records from the native runs: 8 of
4,320 bars empty at 5s, Path 1's folds 864/860/861/860/856 and CV RMSE 0.00016487 against a
target sd of 0.00016641, Path 2's 10 of 24 coefficients zeroed and PowerTransformer at 0.4042
over a 0.3540 majority baseline, Path 3's budget concentrating on SOLUSDT at 41.8% mean share
while the final argmax is ETHUSDT in 60.0% of replays, and 96 items encoded by the loader.

Comparing the **artifacts** rather than the logs is the stronger check, and it separates things
the logs cannot:

| Artifact | Rows | Container vs native |
| --- | --- | --- |
| `bars/` | 4,320 | **identical** |
| `path1/topology/` | 289 | **identical** |
| `path2/topology/` | 144 | **identical** |
| `path2/scaling_search/` | 3 | **identical** |
| `path3/arms/` | 3 | **identical** |
| `path1/coefficients/` | 18 | all 18 differ, worst relative 9.2e-14 |
| `path1/features/` | 4,301 | all differ, worst 1.6e-13 |
| `path2/coefficients/` | 72 | 42 differ, worst 5.6e-14 |
| `path2/features/` | 4,301 | all differ, worst 3.0e-13 |
| `path3/features/` | 4,320 | 432 differ, worst 1.5e-16 — one ULP |
| `path3/frame/` | 1,440 | 74 baskets differ **in item order only**; identical as sets |

Three separate things are visible there, and it is worth not collapsing them.

`bars/` coming out identical is the **`DecimalType` decision** paying off across two Spark
versions and two core counts, not merely across shuffle widths on one machine — which is the
harder version of the claim the entryway was built to make.

The `topology/` exports coming out identical while `coefficients/` and `features/` do not is
exactly the split the **12-decimal-place rounding** was introduced to produce. A Pearson r
rounded to 12 dp survives a change of Spark version and core count; an unrounded fitted parameter
does not, and is deliberately left that way, because masking a float difference in a model
coefficient is not the same act as trimming meaningless precision off a diagnostic. The observed
drift, ~1e-13 relative, is the size that argument predicts.

`path3/frame/` is the one genuinely new finding. Its rows are identical as **sets** — every
basket holds the same tokens — but 74 of 1,440 have them in a different order, because the frame
is built with `collect_list` over a shuffle whose order Spark does not promise. Harmless to the
consumer, since a market basket is a set and the bans that protect it are about membership; but
it does mean `frame/` is not checksummable the way `topology/` was deliberately made to be.

Worth being precise about what the image is: **Glue's runtime, not Glue.** It pins the same
Spark, Python and Java and carries AWS's own jar set, which is what the strip test only
simulates. It runs `local[*]`, so the distributed half — a real cluster, S3, IAM, job bookmarks
and `GlueContext` — is the service around the runtime rather than the runtime itself.

## Deploying to AWS Glue 4.0

Glue 4.0 is **Spark 3.3.0 / Python 3.10 / Java 8**. The job is written to that API surface, which
is narrower than the local Spark 3.5.5 in three places that matter:

| Wanted | Why it fails on Glue | Used instead |
| --- | --- | --- |
| `func.pmod(...)` | Python wrapper is 3.4.0 | `func.expr("pmod(...)")` |
| `func.timestamp_micros(...)` | Python wrapper is 3.5.0 | `func.expr("timestamp_micros(...)")` |
| `func.bool_and(...)` | Python wrapper is 3.5.0 | `func.expr("bool_and(...)")` |

In each case the **SQL name exists in 3.3.0** and only the Python wrapper is missing, so `expr()`
reaches it. `min_by`/`max_by` are genuinely 3.3.0 and are called directly, and `array_compact`
(3.4.0) has no `expr()` fallback at all — Path 3 uses explode-then-filter instead.

This is verified by execution, not by reading release notes: every name whose `versionadded`
exceeds 3.3.0 is deleted from `pyspark.sql.functions` **and from `pyspark.ml`** (174 names), then
the job is re-run and diffed. All three path jobs come out byte-identical — logs and every
Parquet payload.

**The baseline runs through the same harness.** An earlier version of this test compared
`python job.py` against a stripped run launched via `runpy.run_path`, and that comparison
reported a difference on Path 2: 16 of 144 correlation cells moved by up to `1.11e-16`, which
then propagated into an iterative optimiser's coefficients. It was measured down to its cause —
running the job *unstripped* through the same `runpy` harness reproduced the difference exactly,
and two runs of each launch method agreed with themselves — so the culprit was the **launch
method, not the stripping**. The harness therefore has a `--no-strip` control mode, and baseline
and stripped now differ in exactly one thing. A test whose control differs from its treatment in
two ways cannot attribute what it finds.

Two limits worth stating. The scan is **class-level**, so a post-3.3.0 *parameter* on a
pre-3.3.0 class would slip through; the ones the path jobs lean on were checked by hand and both
landed in 3.1.0 (`CrossValidator.foldCol`, `VarianceThresholdSelector`). And it runs against
local Spark, so what it pins is the API surface itself: every name the path jobs reach for is one
that Spark 3.3.0 already has, the `expr()` fallbacks included. The runtime around that surface —
AWS's own Spark 3.3.0, Python 3.10 and Java 8 build, carrying its own jar set — is covered by
*Running the whole chain in the Glue 4.0 image* above, where all five scripts run inside it.

Upload and create the jobs:

```bash
aws s3 cp glue-jobs/glue-ingest-bars.py    s3://<bucket>/crypto_ticks/scripts/
aws s3 cp glue-jobs/glue-refinery-path1.py s3://<bucket>/crypto_ticks/scripts/
aws s3 cp glue-jobs/glue-refinery-path2.py s3://<bucket>/crypto_ticks/scripts/
aws s3 cp glue-jobs/glue-refinery-path3.py s3://<bucket>/crypto_ticks/scripts/
aws s3 cp glue-jobs/refinery_common.py     s3://<bucket>/crypto_ticks/scripts/
```

Create a Glue job of type Spark, Glue version 4.0, pointing at each script, with an IAM role
holding read/write on the bucket. The jobs take plain `argparse` arguments and parse them with
`parse_known_args`, because Glue appends `--JOB_NAME`, `--TempDir` and friends to `sys.argv` on
every run and a strict parser would exit 2 before Spark ever starts. No `--additional-python-modules`
is needed: everything is pure PySpark plus numpy, no pandas, no boto3, no `awsglue`.

**The path jobs need one extra job parameter.** `refinery_common.py` holds the verdict
vocabulary, the session builder and the bars reader shared by all three paths, and Glue uploads
one script per job, so it has to be put on `sys.path` explicitly:

```
--extra-py-files  s3://<bucket>/crypto_ticks/scripts/refinery_common.py
```

`glue-ingest-bars.py` does **not** need it — the entryway is self-contained, and keeping it that
way means the shared module can change without redeploying the job that 341M ticks flow through.
Locally nothing is needed at all: Python puts the running script's directory on `sys.path`, so
`import refinery_common` resolves to the file next to the job.

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

### The loader is a different kind of Glue job

`glue-dynamo.py` is a **Python shell** job, not a Spark one — a different job type in the console
and a different set of things that can go wrong. It needs no `--extra-py-files`, no
`--bar-interval`, no Glue version pin against Spark 3.3.0, and it does not take `--local`: there
is no master to set, so the only thing that changes between a laptop and Glue is whether the
`--path*` arguments start with `s3://`.

The table is **not** created by the job. A loader that creates tables is a loader holding IAM
permissions it has no other use for, and the key schema is a design decision that should live
somewhere a reviewer can see it, not inside a `try/except ResourceNotFoundException`:

```bash
aws dynamodb create-table \
  --table-name crypto_ticks_refinery \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

On-demand billing because the write pattern is 96 items once a month: provisioning capacity for
that means paying by the hour for a table that is idle by the hour.

```bash
aws s3 cp glue-jobs/glue-dynamo.py s3://<bucket>/crypto_ticks/scripts/

aws glue start-job-run --job-name crypto-ticks-load-dynamo \
  --arguments '{
    "--run-id":"2025-01",
    "--table":"crypto_ticks_refinery",
    "--path1":"s3://<bucket>/crypto_ticks/curated/path1/",
    "--path2":"s3://<bucket>/crypto_ticks/curated/path2/",
    "--path3":"s3://<bucket>/crypto_ticks/curated/path3/"
  }'
```

The job's IAM role needs `s3:GetObject` and `s3:ListBucket` on the curated prefixes, and
`dynamodb:BatchWriteItem` plus `dynamodb:Query` on the one table — `Query` for the read-back that
finds orphans, and `BatchWriteItem` for both halves of the sweep, since it covers deletes as well
as puts. No `CreateTable`, and nothing on any other table.

## Repository layout

```
glue-jobs/                     everything Glue runs, and nothing else
  glue-ingest-bars.py            the Glue 4.0 entrypoint, Steps 1-3 (shared, path-blind)
  refinery_common.py             verdict(), build_session(), load_bars() -- shared by the paths
  glue-refinery-path1.py         Steps 4-10 of the continuous path, plus --self-check
  glue-refinery-path2.py         Steps 4-10 of the categorical path, plus --self-check
  glue-refinery-path3.py         Steps 4-10 of the bandit path, plus --self-check
  glue-dynamo.py                 Python shell job: the three ledgers -> DynamoDB, plus --self-check
airflow-dag/
  dag-glue-workflow.py           Airflow: the monthly workflow, ingest -> fork -> load
tests/
  test_ingest_bars.py            every bar vs a pure-Decimal reference
  test_dag_workflow.py           DagBag check for the DAG -- graph, wait_for_completion, templating
local-docker-development.sh    the whole chain inside the Glue 4.0 image, one stage per argument
refinery-walkthrough.ipynb     119 cells, executed: entryway, fork, all three paths
data/sample/                   2.9 MB, 356,201 real ticks, committed
data/raw/                      gitignored -- the three source zips
data/unzipped/                 gitignored -- 25.6 GB extracted
```

`glue-jobs/` is one folder rather than a `spark-jobs/` and a `python-shell-jobs/`, because the
split that matters is enforced where it has consequences — `local-docker-development.sh` and the
DAG each run `glue-dynamo.py` differently — and a folder boundary asserting the same thing a
second time is one more place for the two to disagree. The five path and entryway scripts are
siblings for a load-bearing reason: Python puts the running script's directory on `sys.path`, so
`import refinery_common` resolves next to the job, which is what makes `--extra-py-files` a
deployment concern rather than a local one.

Every command in this README is written to be run **from the project root** — the notebook, the
tests and the shell script all use root-relative paths.

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

Limits and deliberate choices, stated plainly:

- **All five jobs run end to end inside AWS's own Glue 4.0 image** — real Spark 3.3.0, Python
  3.10 and Java 8, rather than the locally simulated API surface the strip test checks. That
  image is the runtime, not the service: it runs `local[*]`, so a cluster, S3, IAM and job
  bookmarks sit outside it. The `--month` argument makes the job single-month; a backfill loops
  it.
- **Targets are not computed by the entryway**, deliberately. A next-bar return computed per month
  freezes a `NULL` into the last bar of every month, so the target is not append-only and
  January's edge needs recomputing when February lands. Targets belong to the feature layer, over
  the concatenated series.
- **`glue-dynamo.py` is designed rather than lifted.** It is the one file here with no notebook
  prototype to diff against — the walkthrough stops at the three paths — so the table schema is
  a judgement call rather than a reproduction, and the key design is argued at length instead.
  Its items are built and encoded by boto3's own serializer against the real artifacts, which is
  what replaces the missing oracle. See *What "verified" means here*.
- **The DAG is checked as a graph, not as a schedule.** `dag-glue-workflow.py` parses into a
  DagBag with no import errors, and its graph and operator settings are asserted by
  `test_dag_workflow.py` against Airflow 2.9.3. Retry and backfill behaviour belongs to the
  scheduler, and is declared for it in `default_args`.
- **The correlation exports are rounded to 12 decimal places.** `Correlation.corr` is a float
  aggregate over partitions and float addition is not associative, so the last ULP of a *pooled*
  cell depends on how the work was scheduled — measured at up to `1.11e-16` across two launch
  methods of the same code on the same input, while the per-symbol cells (computed on smaller
  filtered frames) stayed put. A Pearson r carries about eight significant digits of real
  information on this data, so 12 dp loses nothing and makes `topology/` byte-reproducible,
  which is the property the entryway chose decimal money to protect. **Model coefficients are
  deliberately not rounded** — masking a float difference in a fitted parameter is a different
  thing from trimming meaningless precision off a diagnostic.
- **Path 2's `features/` ships the StandardScaler matrix, not the search winner.** The two
  winning-side legs are hand-rolled substitutions for transforms Spark does not ship, and
  `percent_rank` in particular cannot carry its own training quantiles to new data — so a
  matrix produced by it could not be reproduced on next month's bars. The search result is
  persisted in `scaling_search/` so the divergence is visible.
- **Path 2's quantile leg has a hard row ceiling.** `percent_rank` over an unpartitioned
  window collapses to a single executor per column, so above 250,000 rows the leg is
  skipped and reported. A `Bucketizer` fitted on dev quantiles would be both distributed
  *and* a more faithful QuantileTransformer — it would carry the training boundaries across
  — but it is a different transform and would not reproduce the notebook's measured search.
- **Path 1's target is manufactured and its cross-validation is time-blocked, not expanding.**
  Both are stated by the job at runtime. The framework gives no rule for manufacturing a
  target, so every Path 1 verdict is conditional on nominating the next-bar log return.
- **Path 3's engine is a replay, not a stream, and its reward is a modelling choice.** Both
  are stated by the job at runtime, not just in prose: the arm ranking inverts between
  `close > prev` and `close >= prev`, so the ordering it produces is an ordering of
  tick-flatness rather than a trading edge. Nothing the job emits is a trading result.
- **The Step 1 dedup is one full shuffle over 341M rows to print a line expected to read `N/A`.**
  Kept deliberately — an `N/A` the job did not measure is a lie, and Step 1's whole claim is that
  the merge is deterministic. It is the job's dominant cost. Contiguity alone cannot replace it:
  `{1,2,2,4}` has `max-min+1 == count` and still contains a duplicate.
