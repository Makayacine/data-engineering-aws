**Question 1**: How do you convert an epoch microsecond column into a timestamp on Glue 4.0 without landing 55,000 years out?

**Before Execution** (`[SCHEMA: event_time_us bigint]` | `[Raw ingested value]`):

```text
  col 4  event_time_us   bigint

raw event_time_us            1735689600010866   (16 digits)

```

**After Execution** (`[spark.sql CAST probe]` | `[Correct vs millisecond reading]`):

```text
timestamp_micros(v)       -> 2025-01-01 00:00:00.010866      <- correct
timestamp_millis(v)       -> +56971-10-25 00:00:10.866

```

**Code**:

```python
# The SQL name is registered in Spark 3.3.0, the Python wrapper is not
ticks = ticks.withColumn("event_time", func.expr("timestamp_micros(event_time_us)"))

```

**Answer:** Reach `timestamp_micros` through `func.expr("timestamp_micros(event_time_us)")`, because the SQL function name is registered in Spark 3.3.0, which Glue 4.0 runs, while the Python wrapper is `versionadded 3.5.0` and raises `AttributeError`.

**Notes**:

* **Core Execution Mechanic:** `expr()` hands the string to Spark's SQL parser, which resolves the name directly to the Catalyst `MicrosToTimestamp` expression, bypassing the `pyspark.sql.functions` module.


* **Core Execution Mechanic (Dummy Translation):** Spark's SQL engine has known this function longer than the Python shortcut has existed, so you spell it out as a SQL string and it works on the older engine.


* **Boundary / Memory Constraint:** Glue 4.0 is Spark 3.3.0 / Python 3.10 / Java 8, a narrower API surface than the local Spark 3.5.5 the job is developed against.


* **Boundary / Memory Constraint (Dummy Translation):** The cloud runs an older Spark than your laptop, so some functions that work locally are not there.


* **Failure Mode / Downstream Impact:** A millisecond reading fails silently, producing a well-typed timestamp in the year 56971 with no error and no null, which propagates into every date partition.


* **Failure Mode / Downstream Impact (Dummy Translation):** The wrong conversion does not crash and leaves no blanks, it quietly writes dates 55,000 years in the future, so nothing looks broken until a date filter returns nothing.

---

**Question 2**: How do you keep a money aggregation byte-identical across re-runs when the shuffle width changes?

**Before Execution** (`[cast("double")]` | `[sum(quote_qty), 356,201 ticks]`):

```text
   3 partitions   double 210231898.95614502
  11 partitions   double 210231898.95614326
  29 partitions   double 210231898.95614254

```

**After Execution** (`[DecimalType(18,8)]` | `[same three shuffle widths]`):

```text
   3 partitions   decimal 210231898.95614160
  11 partitions   decimal 210231898.95614160
  29 partitions   decimal 210231898.95614160

```

**Code**:

```python
MONEY = DecimalType(18, 8)

# The same sum taken at three shuffle widths: double moves, decimal does not.
for n in (3, 11, 29):
    ent_d = ent_ticks.repartition(n).agg(
        func.sum(func.col("quote_qty").cast("double"))).first()[0]
    ent_m = ent_ticks.repartition(n).agg(func.sum("quote_qty")).first()[0]
    print(f"  {n:>2} partitions   double {ent_d!r:<22}   decimal {ent_m}")

```

**Answer:** Declare `price`, `qty` and `quote_qty` as `DecimalType(18,8)` in the read schema, making every sum exact fixed-point addition whose result does not depend on the order partial sums happen to merge in.

**Notes**:

* **Core Execution Mechanic:** Catalyst sums into a fixed-point accumulator that widens to `decimal(28,8)` and stays exact at every partial-aggregate merge, whereas double addition rounds at each step in an order Spark never promises.


* **Core Execution Mechanic (Dummy Translation):** Adding decimals gives the same total no matter which order you add them in; adding floats does not, and the cluster decides that order on the fly.


* **Boundary / Memory Constraint:** 18 digits of precision with 8 fractional leaves margin against a maximum observed 6 integer digits (`quote_qty` 1,852,816.09952780 on BTC).


* **Boundary / Memory Constraint (Dummy Translation):** The type is sized with room to spare above the biggest real trade in the sample.


* **Failure Mode / Downstream Impact:** A double pipeline is not idempotent — the same job over byte-identical input emits different bars — which breaks any checksum gate.


* **Failure Mode / Downstream Impact (Dummy Translation):** Re-run yesterday's job and the numbers no longer match yesterday's file, so any check comparing the two flags a difference that is not really there.

---

**Question 3**: How do you derive a bar's open and close so repartitioning cannot change them?

**Before Execution** (`[func.first / func.last]` | `[3 vs 29 shuffle partitions]`):

```text
bars out of 4,312 that change between 3 and 29 shuffle partitions:
  first()/last()          : 3,343

```

**After Execution** (`[min_by / max_by on trade_id]` | `[same two runs]`):

```text
  min_by/max_by(trade_id) : 0

```

**Code**:

```python
# open/close keyed on trade_id, strictly increasing and contiguous (asserted in Step 1)
# a strictly increasing id over a non-decreasing clock IS time order
bars = (ticks.groupBy("symbol", "bar_us").agg(
    func.min_by("price", "trade_id").alias("open"),
    func.max_by("price", "trade_id").alias("close"),
))

```

**Answer:** Take `open` and `close` as `min_by("price", "trade_id")` and `max_by("price", "trade_id")`, never `first()`/`last()`, because `trade_id` is a total order where the microsecond clock is only a partial one. Both are available in Spark 3.3.0.

**Notes**:

* **Core Execution Mechanic:** `min_by`/`max_by` are argmin/argmax aggregates that carry the tie-break key through the hash aggregate, so the winning row is a property of the data, not of arrival order.


* **Core Execution Mechanic (Dummy Translation):** Instead of asking which row showed up first, it asks which row has the smallest trade number, and that answer is the same no matter how the work was divided up.


* **Boundary / Memory Constraint:** `event_time` is only non-decreasing: on the full month 24,851 tie groups carry a non-constant price, so a timestamp ordering leaves `open` and `close` undefined.


* **Boundary / Memory Constraint (Dummy Translation):** Thousands of trades land in the exact same microsecond at different prices, so the clock cannot say which came first, but the trade numbers can.


* **Failure Mode / Downstream Impact:** 3,343 of 4,312 bars changed their `first()`/`last()` values between two shuffle widths of the same input, so every downstream checksum shifts with the cluster configuration.


* **Failure Mode / Downstream Impact (Dummy Translation):** The prices still look completely normal, so you cannot spot the error by looking, only by running the job twice with different settings.

---

**Question 4**: How do you bucket microsecond ticks onto a fixed 5-second grid without division or a timezone shift?

**Before Execution** (`[ticks after Step 2]` | `[epoch microseconds, ungrouped]`):

```text
+----------+----------------+--------------+
|trade_id  |event_time_us   |price         |
+----------+----------------+--------------+
|4359935386|1735689600010866|93576.00000000|
|4359935387|1735689600074095|93576.00000000|
+----------+----------------+--------------+

```

**After Execution** (`[bar_us via pmod]` | `[grid joined, tz pinned]`):

```text
+----------------+--------------+-------+
|bar_us          |open          |n_ticks|
+----------------+--------------+-------+
|1735689600000000|93576.00000000|76     |
|1735689605000000|93576.00000000|385    |
+----------------+--------------+-------+

```

**Code**:

```python
# No division -- cast() truncates toward zero rather than flooring
ticks = ticks.withColumn(
    "bar_us",
    func.col("event_time_us") - func.expr(f"pmod(event_time_us, {interval_us})"))

# Unconditional -- the build box defaults to Africa/Johannesburg
spark.conf.set("spark.sql.session.timeZone", "UTC")

```

**Answer:** Compute the bucket key as `event_time_us - pmod(event_time_us, interval_us)`, which floors in the source unit and stays a bigint, then pin `spark.sql.session.timeZone` to UTC so the calendar columns do not depend on the host.

**Notes**:

* **Core Execution Mechanic:** Subtracting `pmod` floors to a half-open `[bar_us, bar_us + interval)` window in 64-bit integers, while `/` promotes to Double and `cast` truncates toward zero.


* **Core Execution Mechanic (Dummy Translation):** Chopping off the remainder is plain integer maths, so every tick in the same five seconds gets the same bucket number.


* **Boundary / Memory Constraint:** `func.pmod` is versionadded 3.4.0 and Glue 4.0 is Spark 3.3.0, so the key is built through `expr()`, where the SQL name exists.


* **Boundary / Memory Constraint (Dummy Translation):** The Glue runtime is one version behind the Python helper, so the operation is called by its SQL name instead.


* **Failure Mode / Downstream Impact:** Without the UTC pin the session takes the machine's zone, and on the UTC+2 build box `to_date()` drops 120 January bars per symbol into February.


* **Failure Mode / Downstream Impact (Dummy Translation):** Run the same code on a laptop set to another time zone and part of January quietly files itself under February, with no warning.

---

**Question 5**: How do you measure gaps against a declared calendar rather than one inferred from the data?

**Before Execution** (`[Inferred Grid]` | `[Bounds From min/max Of The Frame Being Checked]`):

```text
lo = min(bar_us)   hi = max(bar_us)
# a download that lost its last three days still reports a complete calendar
# -- 0 gaps, by construction

```

**After Execution** (`[--month / --calendar-start / --calendar-end]` | `[Declared Grid]`):

```text
declared calendar: 1,440 slots of 5s x 3 symbols = 4,320 rows

APPLIES  -- Step 3 bars: 8/4,320 empty at 5s (0.185%)

```

**Code**:

```python
# The window comes from the ARGUMENT, never from the data
start_us, end_us = month_bounds_us(month)

```

**Answer:** The calendar comes from `--month`, cross-joined against the symbol list before the data is joined onto it, so a gap is measured against a reference declared independently of the frame under test.

**Notes**:

* **Core Execution Mechanic:** `month_bounds_us()` returns the half-open `[start, end)` of the month in epoch microseconds, and the aggregate is left-joined onto that grid so an empty bar becomes a NULL.


* **Core Execution Mechanic (Dummy Translation):** A timetable built out of the trains that showed up is never late, so you write the timetable first.


* **Boundary / Memory Constraint:** The validation reads the distinct slot keys, not the 340,971,834 ticks behind them, so it costs the same on the committed sample and on the full month.


* **Boundary / Memory Constraint (Dummy Translation):** Checking the calendar reads the tiny list of time slots, not the hundreds of millions of trades underneath it.


* **Failure Mode / Downstream Impact:** `func.pmod` raises `AttributeError` on Glue 4.0's Spark 3.3.0, and the `%` that compiles in its place yields a negative remainder for a pre-1970 backfill.


* **Failure Mode / Downstream Impact (Dummy Translation):** The convenient function does not exist on the cluster, and the obvious replacement quietly misfiles anything dated before 1970.

---

**Question 6**: How do you report a check that ran and found nothing without letting a forbidden step reach its handler?

**Before Execution** (`[step1_dedup_check]` | `[Per-Symbol Contiguity Profile]`):

```text
  BTCUSDT  173,468 rows   trade_id 4,359,935,386 .. 4,360,108,853   span 173,468
  ETHUSDT  101,012 rows   trade_id 2,010,047,664 .. 2,010,148,675   span 101,012
  SOLUSDT   81,721 rows   trade_id 916,174,351 .. 916,256,071   span 81,721

```

**After Execution** (`[Verdict Ledger]` | `[Return Value Of verdict()]`):

```text
N/A      -- Step 1 dedup: 0 duplicate (symbol, trade_id) keys in 356,201 rows
BANNED   -- Step 1 temporal join: aligning symbols on event_time fabricates phantom rows -- BTC/ETH/SOL routinely share a microsecond

```

**Code**:

```python
def verdict(applies, message, banned=False):
    label = ("BANNED   -- " if banned
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    LOG.info("%s%s", label, message)
    return bool(applies) and not banned

```

**Answer:** `verdict()` prints `N/A` when the check ran and measured nothing, and returns `False` for `BANNED` as well as for `N/A`, so `if verdict(...)` never opens a forbidden handler.

**Notes**:

* **Core Execution Mechanic:** An if-else chain over the flags picks the badge, and the return value collapses three states into one boolean gate.


* **Core Execution Mechanic (Dummy Translation):** It always runs the test and writes down the number, so "nothing found" is a result it earned, not a step it skipped.


* **Boundary / Memory Constraint:** The dedup verdict shuffles 356,201 rows on the sample and 340,971,834 on the full month to print a line expected to read `N/A`.


* **Boundary / Memory Constraint (Dummy Translation):** Proving there are no duplicates across 341 million rows is expensive, but that is what the merge step claims.


* **Failure Mode / Downstream Impact:** `trade_id` alone is not the key: its three ranges happen to be disjoint in 2025-01, so a bare `trade_id` key collides on the first id reset.


* **Failure Mode / Downstream Impact (Dummy Translation):** Two exchanges can hand out the same ticket number, so the symbol has to be part of the key.

---

**Question 7**: How do you keep Step 4 on the branch the measured target missingness actually selects?

**Before Execution** (`[Measured Target Missingness]` | `[Post-Fork Frame, 4,320 Rows]`):

```text
rows                                  4,320
target nulls                          19  (0.440%)

```

**After Execution** (`[Default Branch Stands]` | `[Median Impute On X, Complete-Case On y]`):

```text
APPLIES  -- Step 4 Path 1: complete-case on y (4,320 -> 4,301 rows), median impute on X (11 cells) -- the sub-5% branch the framework states but never demonstrates

```

**Code**:

```python
MISSINGNESS_BAN_THRESHOLD = 0.05

def firewall_fires(rate):
    return rate > MISSINGNESS_BAN_THRESHOLD

if firewall_fires(rate):                      # OVERRIDE: medians banned, X nulls evacuated too
    step4 = complete.na.drop(subset=BASE_X)
else:
    imputer = Imputer(strategy="median", inputCols=BASE_X, outputCols=BASE_X)
    step4 = imputer.fit(complete).transform(complete)

```

**Answer:** The threshold is a named constant read through `firewall_fires()`, whose strict inequality `--self-check` asserts at the boundary; measured target missingness of 0.440% is far under 5%, so median imputation runs.

**Notes**:

* **Core Execution Mechanic:** The rate is `avg(y IS NULL)` over the post-fork frame, measured on the target rather than per-column across X.


* **Core Execution Mechanic (Dummy Translation):** It counts the rows with no answer to predict, and because that count is low the holes get filled with the middle value instead of dropped.


* **Boundary / Memory Constraint:** 19 target nulls in 4,320 rows is 0.440%, an order of magnitude below the firewall.


* **Boundary / Memory Constraint (Dummy Translation):** The measurement is nowhere near the line, so this run is not a close call, and only eleven numbers out of nearly seventy thousand get filled in at all.


* **Failure Mode / Downstream Impact:** At exactly 5.0% a `>=` picks the opposite branch with nothing observable at runtime except a different badge in the log.


* **Failure Mode / Downstream Impact (Dummy Translation):** One character flips the whole handling strategy, nothing crashes, and the only clue is one word in a log line.

---

**Question 8**: How do you keep cross-validation from training on the future when rows are a time-ordered bar series?

**Before Execution** (`[CrossValidator defaults]` | `[Unmaterialised fold plan]`):

```text
CrossValidator(numFolds=3, foldCol="")        # Spark 3.3.0 defaults
input: 4,301 rows, ordered on bar_us over a uniform 5s grid, 3 symbols per instant

```

**After Execution** (`[cv_in.groupBy("fold").show()]` | `[Materialised time blocks]`):

```text
+-------+----+----------------+
|p1_fold|rows|         from_us|
+-------+----+----------------+
|      0| 864|1735689600000000|
|      1| 860|1735691040000000|
|      2| 861|1735692480000000|
|      3| 860|1735693915000000|
|      4| 856|1735695355000000|
+-------+----+----------------+

```

**Code**:

```python
span = frame.select(func.min("bar_us").alias("lo"), func.max("bar_us").alias("hi")).first()
width = (span["hi"] - span["lo"] + 1) / FOLDS
cv_in = frame.withColumn("fold", func.least(
    func.lit(FOLDS - 1),
    func.floor((func.col("bar_us") - func.lit(span["lo"])) / func.lit(width))).cast("int")).cache()

cv = CrossValidator(estimator=estimator, estimatorParamMaps=grid,
                    evaluator=RegressionEvaluator(labelCol="y", metricName="rmse"),
                    numFolds=FOLDS, foldCol="fold", parallelism=1, seed=seed)

```

**Answer:** You build an integer `fold` column of contiguous `bar_us` blocks and pass it as `foldCol`, setting `numFolds` to match by hand because `foldCol` does not imply a fold count.

**Notes**:

* **Core Execution Mechanic:** `foldCol` swaps Spark's random partitioning for a caller-supplied integer, and keying it on `bar_us` keeps one instant's three symbol rows in the same fold.


* **Core Execution Mechanic (Dummy Translation):** Rather than dealing rows out like shuffled cards, you slice the timeline into five consecutive chunks and hand Spark the slice numbers.


* **Boundary / Memory Constraint:** Width is `(hi - lo + 1) / FOLDS` because the span is inclusive, and without the `+1` the last bar clamps into a one-row fifth block.


* **Boundary / Memory Constraint (Dummy Translation):** The chunks are equal lengths of clock time, not equal numbers of rows, so quiet market stretches give slightly smaller chunks.


* **Failure Mode / Downstream Impact:** `numFolds` still defaults to 3 when `foldCol` is set, and the mismatch raises "Fold number must be in range [0, 3), but got 3" after the folds are materialised.


* **Failure Mode / Downstream Impact (Dummy Translation):** Setting the fold column without the fold count blows up late, and even with time blocks fold 0 is still graded by a model trained on later data.

---

**Question 9**: How do you stop a variance filter from judging a categorical column by how its levels were numbered?

**Before Execution** (`[PRUNE-FIRST counterfactual]` | `[variance of the index codes]`):

```text
PRUNE-FIRST -- variance of the index codes for p2_symbol_state (5 levels):
   stringOrderType='frequencyDesc' (default) : 0.676535
   stringOrderType='alphabetAsc'             : 2.666355

```

**After Execution** (`[ENCODE-FIRST, then VarianceThresholdSelector]` | `[variance per level]`):

```text
ENCODE-FIRST -- variance of each level of the same column:
   oh__p2_symbol_state__BTCUSDT__true                      0.222274
   oh__p2_symbol_state__ETHUSDT__SYSTEM_STATE_UNKNOWN      0.001156
   oh__p2_symbol_state__SOLUSDT__SYSTEM_STATE_UNKNOWN      0.000694

```

**Code**:

```python
# One-hot FIRST, then prune: the selector sees one column per level, not one ordinal code.
arr = vector_to_array(func.col(f"_ohe_{cat}"))
for i, level in enumerate(indexer.labels):
    encoded = encoded.withColumn(f"oh__{cat}__{level}", arr.getItem(i))

selector = VarianceThresholdSelector(featuresCol="features_raw", outputCol="features",
                                     varianceThreshold=VARIANCE_THRESHOLD).fit(encoded)

```

**Answer:** You one-hot encode before the variance filter runs, so the selector reads each level's own indicator column instead of an integer code that scores 0.676535 under `frequencyDesc` and 2.666355 under `alphabetAsc` on identical data.

**Notes**:

* **Core Execution Mechanic:** `VarianceThresholdSelector` computes sample variance over a numeric vector, so one-hot indicators give it `p(1 - p)` per level instead of `StringIndexer`'s arbitrary integers.


* **Core Execution Mechanic (Dummy Translation):** Turning BTC, ETH and SOL into 0, 1 and 2 measures the numbers you invented, not how often each coin shows up.


* **Boundary / Memory Constraint:** Encode-first raises the filter's input from 3 categorical columns to 24 assembled columns and moves the decision from column granularity to level granularity.


* **Boundary / Memory Constraint (Dummy Translation):** You check more columns, but the ruling lands on each level instead of the whole field.


* **Failure Mode / Downstream Impact:** Prune-first can delete the column whole on an ordinal fiction, taking Step 4's named `SYSTEM_STATE_UNKNOWN` level with it.


* **Failure Mode / Downstream Impact (Dummy Translation):** Nothing errors — the model quietly trains on a different set of inputs, chosen by how the encoder alphabetised the coins.

---

**Question 10**: How do you median-impute price and volume columns on assets whose prices differ by three orders of magnitude?

**Before Execution** (`[p2_forked null counts]` | `[8 empty bars, row-shaped missingness]`):

```text
   open                      8 null  (0.185%)
   close                     8 null  (0.185%)
   volume                    8 null  (0.185%)
```

**After Execution** (`[per-symbol medians]` | `[0 nulls remain]`):

```text
   BTCUSDT   close    93,898.13   volume       0.1821
   ETHUSDT   close     3,354.62   volume       3.6017
   SOLUSDT   close       191.19   volume      44.7970

APPLIES  -- Step 4 numerics: 64 nulls across 8 columns median-imputed per symbol, 0 remain
```

**Code**:

```python
# A pooled median would hand an empty ETH bar a five-figure close
# percentile_approx through expr() because Glue 4.0 is Spark 3.3.0
medians = (imputed.groupBy("symbol")
           .agg(*[func.expr(f"percentile_approx({c}, 0.5)").alias(f"_med_{c}")
                  for c in MONEY_COLS]))
imputed = imputed.join(func.broadcast(medians), on="symbol", how="left")
for col in MONEY_COLS:
    imputed = imputed.withColumn(col, func.coalesce(func.col(col), func.col(f"_med_{col}")))
```

**Answer:** You compute the median inside a `groupBy("symbol")`, broadcast the three-row table back, and `coalesce` each column against its own symbol's median.

**Notes**:

* **Core Execution Mechanic:** `percentile_approx` runs inside a `groupBy("symbol")`, so each fill value is drawn only from its own symbol's distribution.


* **Core Execution Mechanic (Dummy Translation):** Work out the typical value for each coin, then plug that coin's gaps with its own number.


* **Boundary / Memory Constraint:** The broadcast table stays 3 rows by 8 medians, so join cost does not grow with the 1,607,040-bar full run.


* **Boundary / Memory Constraint (Dummy Translation):** The lookup table stays three rows whether you run two hours or a whole month.


* **Failure Mode / Downstream Impact:** A pooled median passes every completeness check — 0 nulls remain — while writing a value from the wrong price scale into `close`.


* **Failure Mode / Downstream Impact (Dummy Translation):** Fill an Ethereum gap with a number averaged across Bitcoin and Solana and nothing complains — a wrong number is not a missing one.

---

**Question 11**: How do you write a Spark ML feature matrix for non-Spark readers, and what gets rounded there?

**Before Execution** (`[step10 in memory]` | `[VectorUDT column]`):

```text
root
 |-- scaled: vector (nullable = true)

>>> VectorUDT.sqlType().simpleString()
struct<type:tinyint,size:int,indices:array<int>,values:array<double>>

```

**After Execution** (`[path1/features/ + path1/topology/]` | `[Materialized Parquet]`):

```text
_localrun/path1/features        4,301 rows x 11 columns

native (Spark 3.5.5 / 24 core) vs Glue 4.0 container (Spark 3.3.0 / 8 core), same input:
  topology/     open|high    0.999999994234       vs 0.999999994234        identical
  coefficients/ x_imb_ret    0.013196011020021485 vs 0.013196011020020819  rel 5.05e-14

```

**Code**:

```python
scaled = vector_to_array("scaled")
features = step10.select(
    "symbol", "bar_us", "bar_open_time_utc", "fold", "y",
    *[scaled[i].alias(f"{name}_scaled") for i, name in enumerate(survivors)])
features.write.mode("overwrite").parquet(f"{base}/features")

```

**Answer:** Call `vector_to_array` and alias each index to a named double before writing Parquet, rounding the diagnostic correlation export to 12 decimal places at the same boundary while leaving fitted coefficients unrounded.

**Notes**:

* **Core Execution Mechanic:** `VectorUDT` serialises to Parquet as a struct whose interpretation lives in Spark's UDT registry, so `vector_to_array` projects each index to its own physical `double` column.


* **Core Execution Mechanic (Dummy Translation):** Spark's vector type is a little bundle with a code, a length and two lists inside, and only Spark knows how to unpack it.


* **Boundary / Memory Constraint:** A Pearson `r` carries roughly 8 significant digits of real information, so truncating the 289-cell topology export at 12 dp discards only noise.


* **Boundary / Memory Constraint (Dummy Translation):** Cutting a correlation off at twelve decimals throws away nothing usable, because there was never that much real accuracy in it.


* **Failure Mode / Downstream Impact:** A vector written unexpanded is unreadable in Athena and pandas without a Spark-side decode step.


* **Failure Mode / Downstream Impact (Dummy Translation):** Leave the vector packed and everything except Spark chokes on the file.

---

**Question 12**: How do you handle a step a path forbids, so the ban is auditable rather than indistinguishable from an oversight?

**Before Execution** (`[Step 7 output]` | `[Path 3 interaction frame]`):

```text
APPLIES  -- Step 7 interaction frame: 1,440 token sets, 28 distinct raw string tokens

```

**After Execution** (`[Steps 8, 9, 10]` | `[three BANNED verdicts]`):

```text
BANNED   -- Step 8 prune: one-hot encoding + VarianceThreshold destroys the token identity
BANNED   -- Step 9 structural: Elastic Net is supervised and Path 3 has no y
BANNED   -- Step 10 domain: 3 of 6 standardised shape parameters are <= 0

```

**Code**:

```python
# Each ban is REPORTED, not skipped.
def step9_regularisation_banned():
    verdict(False, "Step 9 structural: Elastic Net is supervised and Path 3 has no y. The "
                   "reward is per-arm and per-timestep and the arms that were not pulled "
                   "have no observation at all", banned=True)

```

**Answer:** Each forbidden step keeps its own function and reports itself through `verdict()`, which returns False when `banned=True`, so the ban is logged with its reason rather than silently skipped.

**Notes**:

* **Core Execution Mechanic:** `verdict()` collapses six framework states onto one boolean return, `bool(applies) and not banned`, so the log line and the control-flow gate cannot drift apart.


* **Core Execution Mechanic (Dummy Translation):** One function writes the log line and answers "should the next bit run?", and for a banned step the answer is always no.


* **Boundary / Memory Constraint:** Step 8 would flatten 1,440 variable-length baskets into 1,440 fixed-width rows of 28 binary columns.


* **Boundary / Memory Constraint (Dummy Translation):** Flattening baskets into fixed rows of 0s and 1s makes every basket the same shape, the one thing association mining cannot use.


* **Failure Mode / Downstream Impact:** `np.random.beta(-1.1311, 1.1452)` raises `ValueError: a <= 0`, halting Thompson sampling rather than degrading it.


* **Failure Mode / Downstream Impact (Dummy Translation):** A crash tells you at once, but a step skipped with no log line leaves a deliberate rule looking like an oversight.

---

**Question 13**: How do you verify a PySpark job runs on Glue 4.0's narrower API surface before deployment?

**Before Execution** (`[local pyspark 3.5.5]` | `[unstripped function namespace]`):

```text
local environment:  pyspark 3.5.5 -- func.pmod, func.timestamp_micros and func.bool_and all resolve
Glue 4.0 image:     pyspark 3.3.0+amzn.1.dev0

```

**After Execution** (`[strip test]` | `[simulated Spark 3.3.0 surface]`):

```text
deleted from pyspark.sql.functions and pyspark.ml: 174 names (versionadded > 3.3.0)
path1  logs and every Parquet payload: byte-identical to the control run

```

**Code**:

```python
# The SQL name exists in 3.3.0; only the Python wrapper is 3.4.0 / 3.5.0, so expr() reaches it
ticks = ticks.withColumn("event_time", func.expr("timestamp_micros(event_time_us)"))
bar_us = func.col("event_time_us") - func.expr(f"pmod(event_time_us, {interval_us})")

```

**Answer:** Delete every name whose `versionadded` exceeds 3.3.0 from `pyspark.sql.functions` and `pyspark.ml`, then diff each path job's logs and Parquet against a control run through the identical harness.

**Notes**:

* **Core Execution Mechanic:** The strip deletes from live module objects, not from release notes, so an `AttributeError` surfaces at import or first call.


* **Core Execution Mechanic (Dummy Translation):** Rip the newer functions out of your own Spark, rerun the job, and if it still works it will work on Glue.


* **Boundary / Memory Constraint:** The scan is class-level, so a post-3.3.0 *parameter* on a pre-3.3.0 class slips through — `CrossValidator.foldCol` and `VarianceThresholdSelector` were hand-checked and both landed in 3.1.0.


* **Boundary / Memory Constraint (Dummy Translation):** The check catches removed functions, not newer options added to old ones, so those two were checked by eye.


* **Failure Mode / Downstream Impact:** Comparing `python job.py` against a `runpy.run_path` stripped run once moved 16 of 144 Path 2 correlation cells by up to `1.11e-16`, which an unstripped `runpy` run reproduced exactly.


* **Failure Mode / Downstream Impact (Dummy Translation):** Change two things at once and you cannot tell which moved the numbers — here it was the launcher, not the strip.

---

**Question 14**: How do you submit a Glue job from Airflow and actually fail the task when the job fails?

**Before Execution** (`[Lab2 dag-glue-workflow.py]` | `[hand-rolled boto3 poller]`):

```text
if job_runs and job_runs[0]['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
    time.sleep(poll_interval)
else:
    logging.info(f"Glue job {job_name} has finished.")
    break

# JobRunState == 'FAILED' takes the else branch, logs "has finished", task goes green
```

**After Execution** (`[GlueJobOperator]` | `[test_dag_workflow.py]`):

```text
assert len(glue_tasks) == 5, f"expected 5 Glue jobs, found {len(glue_tasks)}"
assert task.wait_for_completion is True
```

**Code**:

```python
ingest_bars = GlueJobOperator(
    task_id='ingest_bars',
    job_name=INGEST_JOB,
    region_name=AWS_REGION,
    wait_for_completion=True,
    job_poll_interval=POLL_INTERVAL,      # 30 s, not the operator's 6 s default
)
```

**Answer:** Use `GlueJobOperator` with `wait_for_completion=True`, which keeps the `JobRunId` it started, polls that run, and raises on any terminal state other than `SUCCEEDED`.

**Notes**:

* **Core Execution Mechanic:** The operator holds the `JobRunId` returned by `start_job_run` and calls `GetJobRun` against it, while `get_job_runs(JobName=..., MaxResults=1)` reports only the newest run of a job *name*.


* **Core Execution Mechanic (Dummy Translation):** Starting a job hands you a ticket number, and the operator keeps that ticket instead of asking about whichever run happened to be newest.


* **Boundary / Memory Constraint:** Polling is set to 30 s rather than the operator's 6 s default because these are multi-minute jobs — the entryway measured 6 min 56 s on a full month.


* **Boundary / Memory Constraint (Dummy Translation):** Checking every six seconds on a job that takes seven minutes is just paying AWS to say "still going" seventy times.


* **Failure Mode / Downstream Impact:** The lab's loop exits on ANY non-running state, so `FAILED`, `TIMEOUT` and `STOPPED` all reach the `has finished` log and the task still succeeds.


* **Failure Mode / Downstream Impact (Dummy Translation):** The old code treats "the job stopped" as "the job worked", so a crashed job shows a green tick and everything after it processes stale data.

---

**Question 15**: How do you make one month value reach five Glue jobs from a single place in the DAG?

**Before Execution** (`[module constant]` | `[DAG parse time, unrendered]`):

```text
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

ingest_bars.script_args['--month']   -> "{{ data_interval_start.strftime('%Y-%m') }}"
load_dynamo.script_args['--run-id']  -> "{{ data_interval_start.strftime('%Y-%m') }}"

```

**After Execution** (`[one @monthly run]` | `[script_args rendered by the provider]`):

```text
--month        2025-01
--output       s3://nl-aws-de-labs/crypto_ticks/curated/2025-01/path1/
--run-id       2025-01

pk = "2025-01#path1"

```

**Code**:

```python
# One Jinja string, three destinations. A FIXED start_date, not days_ago(1).
'start_date': datetime(2025, 1, 1),
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
MONTH_PREFIX = f'{CURATED_PREFIX}{MONTH}/'

loader = dag.get_task('load_dynamo').script_args
assert loader['--run-id'] == dag.get_task('ingest_bars').script_args['--month']

```

**Answer:** Format `data_interval_start` once into a module-level Jinja string and pass it into every `script_args` dict, which the provider renders per run because `script_args` is a `GlueJobOperator` template field. `start_date` is fixed because the rendered month becomes the S3 prefix and half the DynamoDB partition key.

**Notes**:

* **Core Execution Mechanic:** Airflow renders template fields per task instance, so `data_interval_start` resolves at execution time rather than at DAG parse time.


* **Core Execution Mechanic (Dummy Translation):** You write the month once as a placeholder, and Airflow fills in the real value when each run happens.


* **Boundary / Memory Constraint:** Every job writes `mode("overwrite")` and none partitions, so `max_active_runs=1` and the `@monthly` cadence keep two runs off one prefix.


* **Boundary / Memory Constraint (Dummy Translation):** Each job wipes its output folder before writing, so two runs sharing a folder means the second erases the first.


* **Failure Mode / Downstream Impact:** Without `script_args` in `GlueJobOperator.template_fields`, which needs `apache-airflow-providers-amazon >= 8.0`, the jobs receive the literal string `{{ data_interval_start.strftime('%Y-%m') }}` as `--month`.


* **Failure Mode / Downstream Impact (Dummy Translation):** If the placeholder never gets filled in, the job is handed the placeholder text itself as the month, and builds a bar calendar for a month by that name.

---

**Question 16**: How do you key three artifacts at three grains into one DynamoDB table without silently overwriting rows?

**Before Execution** (`[three Parquet ledgers]` | `[before keying]`):

```text
path1: 18 rows from _localrun/docker/path1/coefficients -> 19 items (1 header + 18 grain)
path2: 72 rows from _localrun/docker/path2/coefficients -> 73 items (1 header + 72 grain)
path3:  3 rows from _localrun/docker/path3/arms         ->  4 items (1 header +  3 grain)

```

**After Execution** (`[--dry-run]` | `[boto3 TypeSerializer]`):

```text
dry run complete: 96 items encoded by boto3's own serializer and discarded.

```

**Code**:

```python
pk = f"{run_id}#{path}"
keys = {(i["pk"], i["sk"]) for i in items}
if len(keys) != len(items):
    raise ValueError(f"{path} produced {len(items)} items with only {len(keys)} keys")

# put_item cannot delete, so the partition is READ BACK and orphans removed.
orphans = sorted(held - written)

```

**Answer:** Key the table `pk = "<run-id>#<path>"` with `sk` the artifact's own grain — `feature#<name>`, `feature#<name>#class_name#<class>`, `symbol#<symbol>` — so one partition is one path's result from one run.

**Notes**:

* **Core Execution Mechanic:** The sort key alternates the column names with their values, so `begins_with(sk, "feature#imbalance#")` returns one Path 2 feature's three class rows.


* **Core Execution Mechanic (Dummy Translation):** The folder name is which run and which path, and the file name inside it is which row.


* **Boundary / Memory Constraint:** None of the 47 distinct grain tokens contains a `#`, and a grain value carrying one raises rather than forging a key boundary.


* **Boundary / Memory Constraint (Dummy Translation):** The `#` is the separator, so a value carrying one would fake an extra boundary — the job refuses instead.


* **Failure Mode / Downstream Impact:** `imbalance` appears in both Path 1's and Path 2's ledgers, so keying on feature name alone would have those rows fighting over one item.


* **Failure Mode / Downstream Impact (Dummy Translation):** Two paths use the same feature name, so naming items by feature alone silently lands one on top of the other.

---

**Question 17**: How do you run all five Glue scripts locally inside AWS's own Glue 4.0 image?

**Before Execution** (`[docker run IMAGE python3 job.py]` | `[ENTRYPOINT ["bash","-l"]]`):

```text
$ docker run --rm amazon/aws-glue-libs:glue_libs_4.0.0_image_01 python3 --version
/usr/local/bin/python3: /usr/local/bin/python3: cannot execute binary file
exit=126

```

**After Execution** (`[-c "$*"]` | `[measured, 8 CPUs / 7.8 GiB]`):

```text
no-argument chain, end to end: 12 min 15 s   (native chain ~7.5 min)

```

**Code**:

```bash
# Every stage goes through `-c` -- the only form that works for a shell script
# (spark-submit) and an ELF binary (python3) alike.
glue() {
  MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$(pwd):${WORKSPACE}" -w "${WORKSPACE}" \
    -e DISABLE_SSL=true \
    "${IMAGE}" -c "$*"
}

```

**Answer:** Route every container command through `bash -c`, because the image's `ENTRYPOINT` is `["bash","-l"]` and hands its argument to a login shell as a script filename. The loader `glue-dynamo.py` is a Python shell job and runs with `python3`, not `spark-submit`.

**Notes**:

* **Core Execution Mechanic:** A login shell treats its argument as a script path, so bash reads `python3` as text and exits 126.


* **Core Execution Mechanic (Dummy Translation):** Bash is the front door and wants a script name, not a program, so `-c` tells it to run a command instead.


* **Boundary / Memory Constraint:** The container gets 8 CPUs and 7.8 GiB against the host's 24 cores, so the chain runs 12 min 15 s against ~7.5 min native.


* **Boundary / Memory Constraint (Dummy Translation):** The container gets a third of the machine, so everything runs slower than on the laptop.


* **Failure Mode / Downstream Impact:** Against real Spark 3.3.0, Python 3.10 and Java 8, `bars/` came out identical to native while `coefficients/` differed by up to 9.2e-14.


* **Failure Mode / Downstream Impact (Dummy Translation):** The fitted model numbers wobbled in the fourteenth decimal, which is float arithmetic, not a bug.
