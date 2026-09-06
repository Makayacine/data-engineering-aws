**Question 1**: How do you convert a 16-digit epoch microsecond column into a real timestamp on Glue 4.0 without silently landing the whole month 55,000 years out?

**Before Execution** (`[SCHEMA: event_time_us bigint]` | `[Raw ingested value]`):

```text
  col 4  event_time_us   bigint

raw event_time_us            1735689600010866   (16 digits)

```

**After Execution** (`[spark.sql CAST probe]` | `[Three readings of one value]`):

```text
raw event_time_us            1735689600010866   (16 digits)
timestamp_micros(v)       -> 2025-01-01 00:00:00.010866      <- correct
timestamp_millis(v)       -> +56971-10-25 00:00:10.866
cast(v / 1000 as ts)      -> +56971-10-25 00:00:10.86592

session: Spark 3.5.5, tz UTC

```

**Code**:

```python
# timestamp_micros through expr(): the SQL name is registered in Spark 3.3.0, the Python wrapper is not
ticks = ticks.withColumn("event_time", func.expr("timestamp_micros(event_time_us)"))

# event_time_us is KEPT alongside event_time -- it is the raw ingested value and cannot be
# re-derived once dropped.
verdict(True, "Step 2 timestamp: event_time_us (epoch microseconds) -> event_time via "
              "timestamp_micros, session timezone pinned to UTC")

```

**Answer:** Read the column with `timestamp_micros`, and reach it through `func.expr("timestamp_micros(event_time_us)")` rather than the `func.timestamp_micros` wrapper. The SQL function name is registered in Spark 3.3.0, which is what Glue 4.0 runs, while the Python wrapper is `versionadded 3.5.0` and raises `AttributeError` there.

**Notes**:

* **Core Execution Mechanic:** `expr()` hands the string to Spark's SQL parser, which resolves the registered function name directly to the Catalyst `MicrosToTimestamp` expression, bypassing the `pyspark.sql.functions` module surface entirely; `timestamp_millis` and `v / 1000` resolve to legal expressions over the same bigint and return a valid `TimestampType`, not a null.


* **Core Execution Mechanic (Dummy Translation):** Spark's SQL engine has known this function for years, but the Python shortcut for it was only added in a newer release, so you spell it out as a SQL string instead of calling it as a Python method and it works on the older engine anyway.


* **Boundary / Memory Constraint:** Glue 4.0 is Spark 3.3.0 / Python 3.10 / Java 8, so the deployed API surface is narrower than the local Spark 3.5.5 the job is developed against. `timestamp_micros` is one of four functions this project reaches through `expr()` for that reason; Question 13 covers how the whole surface is verified rather than assumed.


* **Boundary / Memory Constraint (Dummy Translation):** The cloud runs an older Spark than your laptop, so some functions that work locally simply are not there — this is one of them, and there is a separate check that catches all of them at once.


* **Failure Mode / Downstream Impact:** A wrapper call fails loudly on the driver, but only after the Step 1 shuffle has scanned 340,971,834 ticks; a millisecond reading fails silently, producing a well-typed timestamp in the year 56971 with no error, no null and no row-count change, which then propagates into every derived calendar column and date partition.


* **Failure Mode / Downstream Impact (Dummy Translation):** The wrong conversion does not crash and does not leave blanks — it just quietly writes dates 55,000 years in the future, so nothing looks broken until a downstream date filter returns nothing at all.



---

**Question 2**: How do you keep a money aggregation byte-identical across re-runs when the shuffle width changes?

**Before Execution** (`[cast("double")]` | `[sum(quote_qty), 356,201 ticks]`):

```text
sum(quote_qty) over the whole sample, by shuffle width:
   3 partitions   double 210231898.95614502
  11 partitions   double 210231898.95614326
  29 partitions   double 210231898.95614254

5,000 real values, float   : forward 2992974.0347169107  reversed 2992974.034716914  equal=False

```

**After Execution** (`[DecimalType(18,8)]` | `[same three shuffle widths]`):

```text
   3 partitions   decimal 210231898.95614160
  11 partitions   decimal 210231898.95614160
  29 partitions   decimal 210231898.95614160

5,000 real values, Decimal : forward 2992974.03471690  reversed 2992974.03471690  equal=True

APPLIES  -- Step 2 numeric type: decimal(18,8) on price/qty/quote_qty -- a double pipeline is not idempotent across shuffle widths

```

**Code**:

```python
# Every value on the wire is an exact 8-decimal-place string, so decimal(18,8) is the wire
# format, not an approximation of it.
MONEY = DecimalType(18, 8)

# The same sum taken at three shuffle widths: double moves, decimal does not.
for n in (3, 11, 29):
    ent_d = ent_ticks.repartition(n).agg(
        func.sum(func.col("quote_qty").cast("double"))).first()[0]
    ent_m = ent_ticks.repartition(n).agg(func.sum("quote_qty")).first()[0]
    print(f"  {n:>2} partitions   double {ent_d!r:<22}   decimal {ent_m}")

```

**Answer:** Declare `price`, `qty` and `quote_qty` as `DecimalType(18,8)` in the read schema so every sum is fixed-point rather than IEEE-754. Decimal addition is associative, so the result does not depend on the order partial sums happen to merge in.

**Notes**:

* **Core Execution Mechanic:** Catalyst evaluates `sum` over a fixed-point `Decimal` accumulator that widens to `decimal(28,8)` and is exact at every partial-aggregate merge, whereas double addition rounds at each step and Spark makes no promise about the order in which partition-local results are combined.


* **Core Execution Mechanic (Dummy Translation):** Adding decimals gives the same total no matter which order you add them in; adding floats does not, and the cluster decides the order on the fly, so the float total changes when the work is split differently.


* **Boundary / Memory Constraint:** 18 digits of precision with 8 fractional against a maximum observed 6 integer digits (`quote_qty` 1,852,816.09952780 on BTC); the exchange's own notional is summed rather than a recomputed `price * qty`, keeping the accumulator at `decimal(28,8)` instead of burning the overflow margin on `decimal(38,16)`. Casting to double happens only at the feature boundary, for `log()` and `pyspark.ml`, never for an aggregation.


* **Boundary / Memory Constraint (Dummy Translation):** The type is sized with room to spare for the biggest real trade, and floats are only allowed in at the very end where the machine-learning library insists on them.


* **Failure Mode / Downstream Impact:** A double pipeline is not idempotent — the same job over byte-identical input emits different bars — which breaks any checksum gate and any backfill-versus-incremental reconciliation. Decimal also has no NaN, and NaN sorts as the largest value in Spark, so a single NaN price would win `max("price")` and every argmax downstream.


* **Failure Mode / Downstream Impact (Dummy Translation):** Re-running yesterday's job would produce numbers that do not match yesterday's file, so any check comparing the two reports a difference that is not really there — and one bad float value would win every "highest price" comparison in the pipeline.



---

**Question 3**: How do you derive the open and close price of a bar so that the values do not change when the data is repartitioned?

**Before Execution** (`[func.first / func.last]` | `[3 vs 29 shuffle partitions]`):

```text
microsecond tie groups: 26,918   of which price is not constant: 9,203   largest tie group: 1331 ticks in one microsecond

bars out of 4,312 that change between 3 and 29 shuffle partitions:
  first()/last()          : 3,343

(4,312 not 4,320 -- the comparison runs over the NON-EMPTY bars, since the 8 empty ones
 have no ticks to tie-break and carry NULL open/close under either method.)

```

**After Execution** (`[min_by / max_by on trade_id]` | `[same two runs]`):

```text
bars out of 4,312 that change between 3 and 29 shuffle partitions:
  min_by/max_by(trade_id) : 0

+----------------+--------------+--------------+--------------+--------------+-------+
|bar_us          |open          |high          |low           |close         |n_ticks|
+----------------+--------------+--------------+--------------+--------------+-------+
|1735689600000000|93576.00000000|93576.01000000|93576.00000000|93576.00000000|76     |
|1735689605000000|93576.00000000|93576.01000000|93548.16000000|93548.16000000|385    |
+----------------+--------------+--------------+--------------+--------------+-------+

```

**Code**:

```python
# open/close keyed on trade_id, which is strictly increasing and contiguous (asserted in Step 1);
# a strictly increasing id over a non-decreasing clock IS time order, so it is sufficient alone.
bars = (ticks.groupBy("symbol", "bar_us").agg(
    func.min_by("price", "trade_id").alias("open"),
    func.max("price").alias("high"),
    func.min("price").alias("low"),
    func.max_by("price", "trade_id").alias("close"),
    func.count("*").alias("n_ticks"),
))

```

**Answer:** Take `open` and `close` as `min_by("price", "trade_id")` and `max_by("price", "trade_id")`, never `first()`/`last()` and never an ordering on the event timestamp. `trade_id` is strictly increasing and contiguous per symbol, so it is a total order where the microsecond clock is only a partial one.

**Notes**:

* **Core Execution Mechanic:** `min_by`/`max_by` are argmin/argmax aggregates that carry the tie-break key through the hash aggregate, so the winning row is a property of the data rather than of arrival order; `first()`/`last()` return whichever row reached the partition first, which is a scheduling artifact. `min_by` also keeps the plan on the hash path with no `Sort` node, unlike the `max(struct(...))` idiom, which forces a `SortAggregate`.


* **Core Execution Mechanic (Dummy Translation):** Instead of asking "which row showed up first", it asks "which row has the smallest trade number", and that answer is the same every time no matter how the work was divided up.


* **Boundary / Memory Constraint:** `event_time` is only non-decreasing: on the full month 75% of ticks share a microsecond with their predecessor and 24,851 tie groups carry a non-constant price, so a timestamp ordering leaves `open` and `close` genuinely undefined on a large fraction of bars. `min_by`/`max_by` are genuinely available in Spark 3.3.0 and are called directly rather than through `expr()`.


* **Boundary / Memory Constraint (Dummy Translation):** Thousands of trades land in the exact same microsecond, sometimes at different prices, so the clock alone cannot say which one came first — but the trade numbers can.


* **Failure Mode / Downstream Impact:** A wrong tie-break still emits perfectly plausible OHLC that no eyeball check catches: 3,343 of 4,312 bars changed their `first()`/`last()` values between two shuffle widths of the same input, so every return, every target and every checksum built on those bars shifts with the cluster configuration.


* **Failure Mode / Downstream Impact (Dummy Translation):** The prices still look completely normal, which is the problem — you cannot spot the error by looking, only by running the job twice with different settings and comparing.



---

**Question 4**: How do you bucket microsecond-resolution ticks onto a fixed 5-second grid without a division, a truncation, or a timezone shift?

**Before Execution** (`[ticks after Step 2]` | `[epoch microseconds, ungrouped]`):

```text
+----------+----------------+--------------+
|trade_id  |event_time_us   |price         |
+----------+----------------+--------------+
|4359935386|1735689600010866|93576.00000000|
|4359935387|1735689600074095|93576.00000000|
|4359935388|1735689600074095|93576.00000000|
+----------+----------------+--------------+

```

**After Execution** (`[bar_us via pmod]` | `[grid joined, tz pinned]`):

```text
Spark 3.5.5 | tz UTC
declared calendar: 1,440 slots of 5s x 3 symbols = 4,320 rows

+----------------+--------------+-------+
|bar_us          |open          |n_ticks|
+----------------+--------------+-------+
|1735689600000000|93576.00000000|76     |
|1735689605000000|93576.00000000|385    |
+----------------+--------------+-------+

APPLIES  -- Step 3 bars: 8/4,320 empty at 5s (0.185%)

```

**Code**:

```python
# Pure integer arithmetic, no division: event_time_us / I returns a Double and cast() truncates
# toward zero rather than flooring. pmod through expr() -- func.pmod is versionadded 3.4.0 and
# Glue 4.0 is Spark 3.3.0, while the SQL name exists there.
ticks = ticks.withColumn(
    "bar_us",
    func.col("event_time_us") - func.expr(f"pmod(event_time_us, {interval_us})"))

# Unconditional, and NOT behind --local: the build box defaults to Africa/Johannesburg.
spark.conf.set("spark.sql.session.timeZone", "UTC")

```

**Answer:** Compute the bucket key as `event_time_us - pmod(event_time_us, interval_us)`, which floors in the source unit and stays a bigint throughout. Pin `spark.sql.session.timeZone` to UTC unconditionally, so the calendar columns derived from that key do not depend on the machine the job happens to run on.

**Notes**:

* **Core Execution Mechanic:** Subtracting `pmod` performs an exact floor to a half-open `[bar_us, bar_us + interval)` window entirely in 64-bit integers, and hands back a value that `timestamp_micros` converts directly. `date_trunc` has no 5-second unit, `/` promotes to Double, `cast` truncates toward zero instead of flooring, and the `%` operator maps to `Remainder`, which is negative for negative operands and therefore wrong on a pre-1970 backfill.


* **Core Execution Mechanic (Dummy Translation):** Chopping off the remainder is just integer maths — no decimals get involved and nothing gets rounded — so every tick in the same five seconds gets the exact same bucket number.


* **Boundary / Memory Constraint:** Integer modulo is evaluated per row with no window, no shuffle and no timezone database lookup, so bucketing 340,971,834 ticks costs one arithmetic op each; `date_trunc` would additionally resolve the session zone per row. The two-hour sample buckets into 1,440 slots x 3 symbols = 4,320 rows; the full month is 1,607,040 bars.


* **Boundary / Memory Constraint (Dummy Translation):** The empty-slot calendar is built by counting up in five-second steps, so if the starting time is not itself on a five-second mark, none of the slots can ever line up with real data — and that check runs instantly, before any heavy work starts.


* **Failure Mode / Downstream Impact:** Without the UTC pin the session takes the machine's zone; on the UTC+2 build box the last bar of January renders as 1 February and `to_date()` drops 120 January bars per symbol into a February partition, while Glue itself defaults to UTC — so the same job emits different partitions in the two places, with no error and nothing visible in a diff. Integer bucket arithmetic is immune to all of it, which is the reason to prefer it over `date_trunc` for the grid itself.


* **Failure Mode / Downstream Impact (Dummy Translation):** Run the same code on a laptop set to a different time zone and some of January's data quietly files itself under February; nothing warns you, and the file listing looks perfectly normal until someone queries January and comes up short.



---

**Question 5**: How do you keep Step 3's gap count a measurement of the data rather than of the extract, and prove that the grid which reaches a path is actually dense?

**Before Execution** (`[Inferred Grid]` | `[Bounds From min/max Of The Frame Being Checked]`):

```text
lo = min(bar_us)   hi = max(bar_us)
# every observed slot lies inside a range defined by the observed slots, so a download that
# lost its last three days still reports a complete calendar -- 0 gaps, by construction
#
# the opposite error is equally silent in the other direction: run the two-hour committed
# sample against the default month-wide grid and Step 3 truthfully reports
#   1,606,390 of 1,607,040 bars missing

```

**After Execution** (`[--month / --calendar-start / --calendar-end]` | `[Declared Grid, Validated By load_bars()]`):

```text
declared calendar: 1,440 slots of 5s x 3 symbols = 4,320 rows
+-------+----------+------+
| symbol|empty_bars| ticks|
+-------+----------+------+
|BTCUSDT|         0|173468|
|ETHUSDT|         5|101012|
|SOLUSDT|         3| 81721|
+-------+----------+------+

APPLIES  -- Step 3 bars: 8/4,320 empty at 5s (0.185%)

```

**Code**:

```python
# The window comes from the ARGUMENT, never from the data; the overrides only narrow it
start_us, end_us = month_bounds_us(month)          # [start, end) of YYYY-MM in epoch us, UTC
if start_iso:
    start_us = int(datetime.fromisoformat(start_iso)
                   .replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
if start_us % interval_us:
    raise ValueError(f"calendar start {start_us} is not on a {interval_us}us boundary -- "
                     f"the grid would be offset from the bar keys and every slot read empty")

# load_bars(): even tiling is necessary and not sufficient -- {0, 5, 10, 25} tiles at 8.
# pmod through expr(), NOT func.pmod: the Python wrapper is 3.4.0 and Glue 4.0 is Spark 3.3.0.
# `%` compiles there but maps to Remainder, which is negative for negative operands.
off_grid = slots.filter(
    func.expr(f"pmod(bar_us - {span['lo']}, {interval_us})") != 0).count()
if off_grid:
    raise ValueError(f"{off_grid} slot keys are off the {interval_us}us grid")
if rows != n_slots * len(symbols):
    raise ValueError(f"{rows} bars is not {n_slots} slots x {len(symbols)} symbols -- the grid "
                     f"is not dense across every symbol")

```

**Answer:** The calendar is derived from `--month` (or from the explicit `--calendar-start` / `--calendar-end` overrides) and cross-joined against the symbol list before the data is joined onto it, so a gap is measured against a reference declared independently of the frame under test. `load_bars()` then re-derives the interval from the slot span, rejects uneven tiling, tests every distinct key with `pmod` through `expr()`, and asserts `rows == n_slots x symbols`.

**Notes**:

* **Core Execution Mechanic:** `month_bounds_us()` returns the half-open `[start, end)` of the named month in epoch microseconds; the grid is generated as `start + i * interval` and cross-joined with the symbol frame, then left-joined onto the aggregate, so an empty bar arrives as a NULL in a row that exists rather than as an absent row. `load_bars()` recovers `interval_us = (hi - lo) // (n_slots - 1)`, rejects a non-integral tiling, and uses `pmod` — which returns a non-negative residue in `[0, interval)` — where `%` maps to Remainder and carries the sign of the dividend.


* **Core Execution Mechanic (Dummy Translation):** You write down the timetable first, then tick off which trains actually showed up. If you build the timetable out of the trains that showed up, nothing is ever late.


* **Boundary / Memory Constraint:** The declared window is 1,440 slots x 3 symbols = 4,320 rows on the committed sample and 535,680 x 3 = 1,607,040 on the full month. The grid validation runs on the distinct slot keys — one `distinct`, one aggregate, one filtered count — not on the 340,971,834 ticks, so it costs the same at both scales.


* **Boundary / Memory Constraint (Dummy Translation):** Checking the calendar only looks at the list of time slots, which is tiny, not at the hundreds of millions of trades behind them, so it is cheap no matter how big the month is.


* **Failure Mode / Downstream Impact:** `func.pmod` raises `AttributeError` on Glue 4.0's Spark 3.3.0, and the `%` that compiles in its place is identical for 2025 epochs and wrong for a pre-1970 backfill, where a negative operand yields a negative remainder and the bucket key truncates toward zero instead of flooring. A grid that passes even-tiling but has holes lets `lead()` return the bar after a gap: a target measured over a doubled horizon, with no null and no row-count change.


* **Failure Mode / Downstream Impact (Dummy Translation):** The convenient function does not exist on the cluster, and the obvious replacement quietly misfiles anything dated before 1970. And if a slot is missing, "the next row" is not the next five seconds — it is ten, and nothing anywhere tells you.



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
# both calls return False; the handler guarded by `if verdict(...)` is not entered on either

```

**Code**:

```python
# Three states, one return value: BANNED returns False for the same reason N/A does
def verdict(applies, message, banned=False):
    label = ("BANNED   -- " if banned
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    LOG.info("%s%s", label, message)
    return bool(applies) and not banned

# The handler runs if and only if verdict() said APPLIES
if verdict(dupes > 0,
           f"Step 1 dedup: {dupes:,} duplicate (symbol, trade_id) keys in {raw_rows:,} rows"):
    raise ValueError(
        f"{dupes} duplicate (symbol, trade_id) keys -- Binance trade ids are unique per "
        f"symbol, so this is a double-ingested file split, not a data property")

```

**Answer:** `verdict()` prints `N/A` when the check ran and the measurement came back empty, so the line still carries the number it was measured from rather than recording a skip. It returns `bool(applies) and not banned`, which is `False` for `BANNED` as well as for `N/A`, so `if verdict(...)` can never open a handler the framework forbids.

**Notes**:

* **Core Execution Mechanic:** The badge is selected by an if-else chain over the keyword flags and written to the job's own logger, while the return value collapses the states to a single boolean gate. `dupes` comes from a real `groupBy("symbol", "trade_id").count().filter(count > 1).count()` — the `N/A` is emitted by the same expression that would have emitted `APPLIES`, so the two states are indistinguishable in cost and differ only in the measured value.


* **Core Execution Mechanic (Dummy Translation):** It always does the test and always writes down the number. "Nothing found" is a result it earned, not a step it skipped.


* **Boundary / Memory Constraint:** The dedup verdict costs one full shuffle over a two-column projection — 356,201 rows on the sample, 340,971,834 on the full month, essentially one group per row — to print a line that is expected to read `N/A`. It is kept because an `N/A` the job did not measure is a lie.


* **Boundary / Memory Constraint (Dummy Translation):** Proving there are no duplicates across 341 million rows is genuinely expensive, and the whole claim of the merge step is that there are none, so the bill gets paid.


* **Failure Mode / Downstream Impact:** `trade_id` alone is not the key — it is a per-symbol Binance sequence whose three ranges happen to be disjoint in 2025-01, so a bare `trade_id` key passes on this month and collides the first time a fourth symbol or an id reset overlaps it. Dedup on its own also cannot distinguish "no duplicates" from "equal numbers of duplicates and gaps", which is why the contiguity assertion (`span == rows`) runs alongside it and catches a truncated download.


* **Failure Mode / Downstream Impact (Dummy Translation):** Two exchanges can hand out the same ticket number, so the symbol has to be part of the key or the pipeline is only correct by luck. And counting duplicates will not notice that the last three days of the file never arrived, so the row-span check is there too.



---

**Question 7**: How do you keep Path 1's Step 4 on the branch the missingness measurement actually selects, when the threshold is a single character nobody demonstrates?

**Before Execution** (`[Measured Target Missingness]` | `[Post-Fork Frame, 4,320 Rows]`):

```text
rows                                  4,320
target nulls                          19  (0.440%)
  of which last bar of a symbol       3  (structural: no successor)
  of which the bar itself is empty    8  (NULL close, Step 3 flagged)
  of which the NEXT bar is empty      8  (lead() pulls the null back)
Step 3 is_missing_bar rate            0.185%  (under-counts the target: one-bar flag, two-bar target)

rate excluding the structural tail    0.370%

```

**After Execution** (`[Default Branch Stands]` | `[Median Impute On X, Complete-Case On y]`):

```text
N/A      -- Step 4 firewall: target missingness 0.440% vs the 5% threshold -- the OVERRIDE does NOT fire, so the framework's default (median impute) is the branch that stands
complete-case on the target: 4,320 -> 4,301 rows (19 evacuated)
feature-column nulls surviving the complete-case cut: {'p1_ret1': 11}
   p1_ret1         11 cells <- median +0.000000e+00
cells filled: 11 of 68,816
APPLIES  -- Step 4 Path 1: complete-case on y (4,320 -> 4,301 rows), median impute on X (11 cells) -- the sub-5% branch the framework states but never demonstrates

```

**Code**:

```python
# Strictly greater, in a named function with a self-check, because `>` reading as `>=` is silent
MISSINGNESS_BAN_THRESHOLD = 0.05

def firewall_fires(rate):
    return rate > MISSINGNESS_BAN_THRESHOLD

assert not firewall_fires(MISSINGNESS_BAN_THRESHOLD), \
    "5.0% exactly must NOT fire -- both sources state the threshold strictly"
assert firewall_fires(0.050001), "just above the threshold must fire"

if firewall_fires(rate):                      # OVERRIDE: medians banned, X nulls evacuated too
    step4 = complete.na.drop(subset=BASE_X)
else:                                         # DEFAULT: the transformer the OVERRIDE forbids
    imputer = Imputer(strategy="median", inputCols=BASE_X, outputCols=BASE_X)
    step4 = imputer.fit(complete).transform(complete)

```

**Answer:** The threshold is a named constant read through `firewall_fires()`, a one-line function that exists only so the strict inequality can be asserted at the boundary by `--self-check`. Measured missingness on the target is 0.440%, well under 5%, so the override does not fire and median imputation — the branch the framework states but never demonstrates — is the one that runs.

**Notes**:

* **Core Execution Mechanic:** The rate is `avg(y IS NULL)` over the post-fork frame, evaluated on the target rather than per-column across X, because the source wording is "target missingness". Both branches are implemented and both apply `na.drop(subset=["y"])`; only the override additionally drops the residual X nulls and never constructs the `Imputer` at all.


* **Core Execution Mechanic (Dummy Translation):** It counts how many rows have no answer to predict, compares that to five percent, and picks one of two ways to handle the holes. Here the count is low, so the holes get filled with the middle value instead of the rows being thrown away.


* **Boundary / Memory Constraint:** 19 of 4,320 target nulls is 0.440%, an order of magnitude below the firewall, and the gap between the raw rate and the rate excluding the structural tail is three rows — so the branch does not turn on that reporting choice either. The fill touches 11 cells of 68,816, all in `p1_ret1`, at a median of exactly `+0.000000e+00`.


* **Boundary / Memory Constraint (Dummy Translation):** The measurement is nowhere near the line, so this run is not a close call, and only eleven numbers out of nearly seventy thousand get filled in at all.


* **Failure Mode / Downstream Impact:** At exactly 5.0% a `>=` selects the opposite branch, and nothing about that is observable at runtime: no exception is raised, the frame that leaves Step 4 carries the same schema, and the row count differs only by the residual X nulls that nothing asserts on. The only trace is a different badge in the log. Separately, the median written into `p1_ret1` is an exact zero and a zero in a return column is indistinguishable from an observed flat bar, so the fill is unrecoverable after the fact.


* **Failure Mode / Downstream Impact (Dummy Translation):** One character flips the whole handling strategy and nothing crashes, nothing looks different, and the only clue is one word in a log line. And once a zero is written into a returns column, nobody can later tell it apart from a bar where the price genuinely did not move.



---

**Question 8**: How do you keep cross-validation from training on the future when the rows are a time-ordered bar series rather than exchangeable records?

**Before Execution** (`[CrossValidator defaults]` | `[Unmaterialised fold plan]`):

```text
CrossValidator(numFolds=3, foldCol="")        # Spark 3.3.0 defaults
input: 4,301 rows, ordered on bar_us over a uniform 5s grid, 3 symbols per instant
random assignment -> every fold draws rows from the whole span, and a bar's own
immediate neighbours -- which are near-duplicates of it -- land on the other side

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

selected  regParam 1e-06 | elasticNetParam 1.0 (pure L1)
CV RMSE   0.00016487  (target sd 0.00016641) over 5 time-blocked folds

```

**Code**:

```python
# Folds cut on bar_us, not ntile over rows: keying on the timestamp keeps the three
# symbols of one instant in the same block. hi - lo + 1 because the span is inclusive.
span = frame.select(func.min("bar_us").alias("lo"), func.max("bar_us").alias("hi")).first()
width = (span["hi"] - span["lo"] + 1) / FOLDS
cv_in = frame.withColumn("fold", func.least(
    func.lit(FOLDS - 1),
    func.floor((func.col("bar_us") - func.lit(span["lo"])) / func.lit(width))).cast("int")).cache()

blocks = (cv_in.groupBy("fold").agg(func.count("*").alias("rows"),
                                    func.min("bar_us").alias("from_us")).orderBy("fold").collect())
# numFolds is NOT implied by foldCol -- it defaults to 3, and the fit dies with "Fold
# number must be in range [0, 3), but got 3" only AFTER the folds are materialised.
if len(blocks) != FOLDS or {r["fold"] for r in blocks} != set(range(FOLDS)):
    raise ValueError(f"fold assignment produced {sorted(r['fold'] for r in blocks)}, "
                     f"expected 0..{FOLDS - 1}")

cv = CrossValidator(estimator=estimator, estimatorParamMaps=grid,
                    evaluator=RegressionEvaluator(labelCol="y", metricName="rmse"),
                    numFolds=FOLDS, foldCol="fold", parallelism=1, seed=seed)

```

**Answer:** You materialise an explicit integer `fold` column of contiguous `bar_us` blocks and pass it as `foldCol`, and you set `numFolds` to the same count by hand. `foldCol` overrides the random split but does not tell `CrossValidator` how many folds to expect.

**Notes**:

* **Core Execution Mechanic:** `foldCol` replaces Spark's hash-based random partitioning of the input with a caller-supplied integer assignment; keying the block index on `bar_us` rather than on row ordinal guarantees that the three symbol rows sharing one 5s instant receive the same index, so no correlated triple straddles a boundary.


* **Core Execution Mechanic (Dummy Translation):** Instead of letting Spark shuffle the rows and deal them out like cards, you slice the timeline into five consecutive chunks and hand Spark the slice numbers, so a bar and the bars either side of it always stay in the same pile.


* **Boundary / Memory Constraint:** The fold width is `(hi - lo + 1) / FOLDS`; without the `+1` the last bar computes to index `FOLDS` and the `least()` clamp fires on every run, leaving four full blocks and a one-row fifth. The measured blocks are 864/860/861/860/856 rows — uneven by row count because they are even by time, which is the intended trade.


* **Boundary / Memory Constraint (Dummy Translation):** The chunks are equal lengths of clock time, not equal numbers of rows, so quiet stretches of the market produce slightly smaller chunks. The off-by-one in the width matters because it would squash the whole last chunk down to a single row.


* **Failure Mode / Downstream Impact:** `numFolds` still defaults to 3 when `foldCol` is set; the mismatch raises "Fold number must be in range [0, 3), but got 3" only after the folds have been computed and the frame materialised, so the cost is paid before the error surfaces. Time blocks fix the near-duplicate leak but not the direction — fold 0 is still validated against a model trained on folds 1-4, which are later. Path 1's own metric here is CV RMSE 0.00016487 against a target sd of 0.00016641; the size of the leak is easier to read on Path 2, whose classification grid shows random folds beating time-ordered folds by +0.0346 to +0.0460 accuracy on every grid point over the same bars.


* **Failure Mode / Downstream Impact (Dummy Translation):** Setting the fold column without also setting the fold count blows up late, after the slow part already ran. And even with proper time blocks this is not a real forward-looking test — the first chunk is still being graded by a model that has seen later data, and the leak is worth roughly three to five accuracy points, which is why random folds always look better than they are.



---

**Question 9**: How do you stop a variance filter's keep/drop decision on a categorical column from being an artefact of how the encoder happened to number the levels?

**Before Execution** (`[PRUNE-FIRST counterfactual]` | `[variance of the index codes]`):

```text
PRUNE-FIRST -- variance of the index codes for p2_symbol_state (5 levels):
   stringOrderType='frequencyDesc' (default) : 0.676535
   stringOrderType='alphabetAsc'             : 2.666355
   the data is identical; the difference is the numbering

  threshold   encode-first kept   UNKNOWN levels kept   prune-first: symbol_state
     0.0010                 17                     2  kept (as 1 ordinal column)
     1.0000                  1                     0      DELETED (all 5 levels)

```

**After Execution** (`[ENCODE-FIRST, then VarianceThresholdSelector]` | `[variance per level]`):

```text
ENCODE-FIRST -- variance of each level of the same column:
   oh__p2_symbol_state__BTCUSDT__true                      0.222274
   oh__p2_symbol_state__SOLUSDT__true                      0.222042
   oh__p2_symbol_state__ETHUSDT__true                      0.221886
   oh__p2_symbol_state__ETHUSDT__SYSTEM_STATE_UNKNOWN      0.001156
   oh__p2_symbol_state__SOLUSDT__SYSTEM_STATE_UNKNOWN      0.000694

N/A      -- Step 8 prune at threshold 0.0: 0 of 24 columns removed -- nothing in this frame is constant

```

**Code**:

```python
# One-hot FIRST, then prune: the selector sees one column per level, not one ordinal code.
arr = vector_to_array(func.col(f"_ohe_{cat}"))
for i, level in enumerate(indexer.labels):
    encoded = encoded.withColumn(f"oh__{cat}__{level}", arr.getItem(i))
    dummies.append(f"oh__{cat}__{level}")

assembled_cols = NUMERIC_X + dummies
encoded = VectorAssembler(inputCols=assembled_cols, outputCol="features_raw").transform(encoded)
selector = VarianceThresholdSelector(featuresCol="features_raw", outputCol="features",
                                     varianceThreshold=VARIANCE_THRESHOLD).fit(encoded)

# The ENFORCE's justification, measured on this run. Read-only, on a throwaway frame:
# the number a prune-first filter would consult, under two indexer orderings.
def encoded_index_variance(frame, column, order_type):
    indexed = StringIndexer(inputCol=column, outputCol="_idx_alt",
                            stringOrderType=order_type).fit(frame).transform(frame)
    return indexed.select(func.var_samp("_idx_alt").alias("v")).first()["v"]

```

**Answer:** You one-hot encode before the variance filter runs, so the selector measures the variance of each level's own indicator column instead of the variance of an integer code. The counterfactual proves the alternative is not measuring the data: the same 4,320 rows give 0.676535 under `frequencyDesc` and 2.666355 under `alphabetAsc`.

**Notes**:

* **Core Execution Mechanic:** `VarianceThresholdSelector` computes sample variance over a numeric vector column, so on an ordinal index it is measuring the spread of arbitrary integers assigned by `StringIndexer`'s `stringOrderType`, whereas on one-hot indicators it measures `p(1 - p)` per level — a genuine property of the level's observed frequency.


* **Core Execution Mechanic (Dummy Translation):** If you turn "BTC / ETH / SOL" into 0, 1, 2 and then ask how spread out those numbers are, you are asking about the numbers you invented, not the coins. Give each coin its own yes/no column first and the spread you measure is the real one — how often that coin actually shows up.


* **Boundary / Memory Constraint:** Encode-first raises the filter's input from 3 categorical columns to 24 assembled columns and moves the decision from column granularity to level granularity: at threshold 0.0010 encode-first keeps 17 columns including 2 `SYSTEM_STATE_UNKNOWN` levels, while prune-first can only keep or delete `p2_symbol_state` whole. The threshold is 0.0 precisely because Step 8 runs before Step 10's scaling, so raw variances span twelve orders of magnitude and only genuine constants can be removed safely.


* **Boundary / Memory Constraint (Dummy Translation):** You end up with more columns to check, but you get a per-level verdict instead of an all-or-nothing one on the whole field. The cutoff is set to zero because the columns have not been put on a common scale yet, so anything stricter would be comparing apples to kilometres.


* **Failure Mode / Downstream Impact:** Prune-first also annuls Step 4's named `SYSTEM_STATE_UNKNOWN` level — a level has no independent existence until one-hot gives it a column — and propagates an ordinal fiction that the single-forward-pass rule forbids Step 8 from re-running to undo. Nothing errors: the pipeline simply carries a differently-shaped feature matrix into Step 9, and swapping the indexer's default ordering would silently change which columns Step 9 ever sees.


* **Failure Mode / Downstream Impact (Dummy Translation):** In the wrong order, a column can be thrown away because of how the encoder alphabetised things, and the deliberately named "we don't know" bucket disappears with it. No error message, no warning — the model just quietly trains on a different set of inputs, and you only find out by running it the other way and comparing.



---

**Question 10**: How do you median-impute price and volume columns on a frame that holds several assets whose prices differ by three orders of magnitude?

**Before Execution** (`[p2_forked null counts]` | `[8 empty bars, row-shaped missingness]`):

```text
   open                      8 null  (0.185%)
   high                      8 null  (0.185%)
   low                       8 null  (0.185%)
   close                     8 null  (0.185%)
   volume                    8 null  (0.185%)
   quote_volume              8 null  (0.185%)
   taker_buy_qty             8 null  (0.185%)
   taker_buy_quote_qty       8 null  (0.185%)

8 columns x 8 empty bars = 64 nulls. Every one sits on the same 8 rows: missingness here is
row-shaped, not column-shaped, because an empty bar has no ticks to aggregate at all.

```

**After Execution** (`[per-symbol medians]` | `[0 nulls remain]`):

```text
per-symbol medians used as the fill value:
   BTCUSDT   close    93,898.13   volume       0.1821
   ETHUSDT   close     3,354.62   volume       3.6017
   SOLUSDT   close       191.19   volume      44.7970

APPLIES  -- Step 4 numerics: 64 nulls across 8 columns median-imputed per symbol, 0 remain

```

**Code**:

```python
# THE GROUPING IS THE TRAP. A single median over the frame blends three price scales
# and would hand an empty ETH bar a five-figure close. percentile_approx through expr()
# because Glue 4.0 is Spark 3.3.0 and the Python wrapper is not there.
medians = (imputed.groupBy("symbol")
           .agg(*[func.expr(f"percentile_approx({c}, 0.5)").alias(f"_med_{c}")
                  for c in MONEY_COLS]))
imputed = imputed.join(func.broadcast(medians), on="symbol", how="left")
for col in MONEY_COLS:
    imputed = imputed.withColumn(col, func.coalesce(func.col(col), func.col(f"_med_{col}")))

left = imputed.select([func.count(func.when(func.col(c).isNull(), c)).alias(c)
                       for c in MONEY_COLS]).first().asDict()
if sum(left.values()):
    raise ValueError(f"{sum(left.values())} nulls survived the median fill -- a symbol had "
                     f"no observed value at all for some column, so its median is null too")

```

**Answer:** You compute the median with a `groupBy("symbol")`, broadcast the resulting three-row table back, and `coalesce` each column against its own symbol's median. A pooled median over the whole frame would fill an empty ETH bar from a distribution that also contains BTC at roughly $94k and SOL at roughly $190.

**Notes**:

* **Core Execution Mechanic:** `percentile_approx` is evaluated inside a `groupBy("symbol")` so each symbol's fill value is drawn only from its own marginal distribution; the three-row result is broadcast-joined back and applied through `coalesce`, which touches only the null cells and leaves observed values bit-identical.


* **Core Execution Mechanic (Dummy Translation):** Work out the typical value separately for each coin, then use each coin's own number to plug its own gaps. Because the lookup table is three rows, Spark ships it to every worker instead of reshuffling the whole dataset.


* **Boundary / Memory Constraint:** The missingness is row-shaped — a bar either had ticks or it did not — so all eight money columns are null on exactly the same 8 rows, giving 64 cells over 3 symbols. The broadcast table is 3 rows by 8 medians regardless of how many bars are in the month, so the join cost does not grow with the 1,607,040-bar full run.


* **Boundary / Memory Constraint (Dummy Translation):** All the holes line up on the same rows, because an empty five-second window is missing everything at once, not one field at a time. The little lookup table stays three rows whether you run two hours or a whole month, so this step never becomes the slow one.


* **Failure Mode / Downstream Impact:** A pooled median passes every completeness check — the null count still reads 0 remaining — while writing a value from the wrong price scale into `close`, which then flows into Step 5's derived ratios, Step 6's correlations and Step 9's coefficients with no error anywhere. The explicit `ValueError` guards the opposite case: a symbol with no observed value at all for some column has a null median, so `coalesce` silently leaves the null in place, and the check turns that into a failure rather than a quiet gap.


* **Failure Mode / Downstream Impact (Dummy Translation):** Fill an Ethereum gap with a number averaged across Bitcoin and Solana and nothing complains — the "no missing values" check still passes, because a wrong number is not a missing one. It just carries a wrong price through every calculation downstream. The extra check exists for the other direction: if a coin never traded at all, there is nothing to copy from, and the job stops instead of pretending it filled the hole.



---

**Question 11**: How do you write a Spark ML feature matrix so that a non-Spark reader can open it, and which float-valued exports get rounded at that same write boundary?

**Before Execution** (`[step10 in memory]` | `[VectorUDT column]`):

```text
root
 |-- symbol: string (nullable = true)
 |-- bar_us: long (nullable = true)
 |-- final_features: vector (nullable = true)
 |-- scaled: vector (nullable = true)

>>> VectorUDT.sqlType().simpleString()
struct<type:tinyint,size:int,indices:array<int>,values:array<double>>

```

**After Execution** (`[path1/features/ + path1/topology/]` | `[Materialized Parquet]`):

```text
_localrun/path1/features        4,301 rows x 11 columns
symbol: string
bar_us: int64
bar_open_time_utc: timestamp[us, tz=UTC]
fold: int32 not null
y: double
x_imb_ret_scaled: double
imbalance_scaled: double
x_ret_ticks_scaled: double
body_bp_scaled: double
x_range_qv_scaled: double
volume_scaled: double

native (Spark 3.5.5 / 24 core) vs Glue 4.0 container (Spark 3.3.0 / 8 core), same input:
  topology/     open|high    0.999999994234       vs 0.999999994234        identical
  coefficients/ x_imb_ret    0.013196011020021485 vs 0.013196011020020819  rel 5.05e-14

```

**Code**:

```python
# A VectorUDT round-trips through Parquet as an opaque struct only Spark understands,
# so it is expanded into named doubles at the write boundary.
scaled = vector_to_array("scaled")
features = step10.select(
    "symbol", "bar_us", "bar_open_time_utc", "fold", "y",
    *[scaled[i].alias(f"{name}_scaled") for i, name in enumerate(survivors)])
features.write.mode("overwrite").parquet(f"{base}/features")

# Correlation.corr is a float aggregate over partitions and float addition is not
# associative, so a pooled cell's last ULP is a function of scheduling. Rounded here;
# fitted model coefficients are deliberately NOT rounded.
return [(cols[i], cols[j],
         None if math.isnan(matrix[i][j]) else round(float(matrix[i][j]), 12))
        for i in range(len(cols)) for j in range(len(cols))]

```

**Answer:** Call `vector_to_array` on the vector column and alias each index to a named double before writing Parquet. Round the diagnostic correlation export to 12 decimal places at that same boundary, and leave the fitted coefficients unrounded.

**Notes**:

* **Core Execution Mechanic:** `VectorUDT` serialises to Parquet as `struct<type:tinyint,size:int,indices:array<int>,values:array<double>>`, a sparse-or-dense discriminated union whose interpretation lives in Spark's UDT registry rather than in the file; `vector_to_array` materialises the dense payload so each index can be projected to its own physical `double` column.


* **Core Execution Mechanic (Dummy Translation):** Spark's vector type is a little bundle with a code, a length and two lists inside it, and only Spark knows how to unpack that. Pulling the numbers out into plain named columns means anything that reads Parquet can just read them.


* **Boundary / Memory Constraint:** The projection is width-bounded by the Step 9 survivors — 6 scaled doubles plus 5 key columns, so `features/` lands at 4,301 x 11 — and a Pearson `r` carries roughly 8 significant digits of real information here, so truncating the 289-cell topology export at 12 dp discards only sub-information noise.


* **Boundary / Memory Constraint (Dummy Translation):** You only write out the six columns that survived, so the file stays small. And cutting a correlation off at twelve decimals throws away nothing anyone could ever use, because there was never that much real accuracy in it.


* **Failure Mode / Downstream Impact:** A vector written unexpanded is unreadable in Athena and pandas without a Spark-side decode step; and an unrounded pooled aggregate makes the artifact non-checksummable — measured across two Spark versions and two core counts, `path1/topology/` (289 rows) and `path2/topology/` (144 rows) came back identical while all 18 `path1/coefficients/` rows differed, worst relative 9.2e-14.


* **Failure Mode / Downstream Impact (Dummy Translation):** Leave the vector packed and everything except Spark chokes on the file. And if you do not trim the correlation numbers, the same job on a different machine writes slightly different digits, so a file-comparison check screams about a difference that means nothing. The model coefficients are left raw on purpose — hiding a wobble in a fitted number is a different thing from trimming a diagnostic.



---

**Question 12**: How do you handle a refinery step that a given path forbids, so the ban is auditable rather than indistinguishable from a step nobody thought of?

**Before Execution** (`[Step 7 output]` | `[Path 3 interaction frame]`):

```text
APPLIES  -- Step 7 interaction frame: 1,440 token sets, 28 distinct raw string tokens

items: array<string>, VARIABLE length per basket
  28 distinct raw tokens, e.g. HOUR_01, ETHUSDT_TAKER_HEAVY, SOLUSDT_NO_TRADES

arm         alpha      beta   alpha+beta     mean
BTCUSDT     524.0     917.0       1441.0   0.3636
ETHUSDT     576.0     855.0       1431.0   0.4025
SOLUSDT     590.0     845.0       1435.0   0.4111

```

**After Execution** (`[Steps 8, 9, 10]` | `[three BANNED verdicts, frame untouched]`):

```text
BANNED   -- Step 8 prune: one-hot encoding + VarianceThreshold destroys the token identity
            and the variable-length set geometry that Apriori, ECLAT and the RBM consume
BANNED   -- Step 9 regularise: Elastic Net zeroes the low-frequency tail; lift is
            P(B|A)/P(B), maximised by rare antecedents -- the two criteria disagree
            precisely on the tail Path 3 exists to mine
BANNED   -- Step 9 structural: Elastic Net is supervised and Path 3 has no y
BANNED   -- Step 10 scale: StandardScaler on alpha/beta yields negative shape parameters
BANNED   -- Step 10 domain: 3 of 6 standardised shape parameters are <= 0

frame unchanged: 1,440 baskets, 28 raw string tokens, alpha/beta still counts

```

**Code**:

```python
# Each ban is REPORTED, not skipped. verdict() returns False for BANNED as well as for
# N/A, so `if verdict(...)` can never gate a handler the framework forbids.
def step8_pruning_banned(n_tokens, n_baskets):
    verdict(False, f"Step 8 prune: one-hot + VarianceThreshold destroys the token identity "
                   f"and the variable-length set geometry -- {n_tokens} raw string tokens "
                   f"are preserved instead of becoming {n_tokens} binary columns over "
                   f"{n_baskets:,} fixed-width rows", banned=True)

def step9_regularisation_banned():
    verdict(False, "Step 9 structural: Elastic Net is supervised and Path 3 has no y. The "
                   "reward is per-arm and per-timestep and the arms that were not pulled "
                   "have no observation at all", banned=True)

def step10_scaling_banned(alpha_beta, arms):
    params = np.array([[alpha_beta[a][0], alpha_beta[a][1]] for a in arms], dtype=float)
    scaled = (params - params.mean(axis=0)) / params.std(axis=0, ddof=1)
    negatives = int((scaled <= 0).sum())
    verdict(False, f"Step 10 domain: {negatives} of {2 * len(arms)} standardised shape "
                   f"parameters are <= 0, so numpy's Beta sampler raises rather than "
                   f"degrades", banned=True)

```

**Answer:** Pruning would delete the raw token strings Apriori and ECLAT count co-occurrences over, regularisation would shrink exactly the rare high-lift itemsets the mining exists to surface, and scaling α and β into z-scores would break the Beta conjugate update `α_new = γα + x`. Each ban is a consequence of a named downstream engine, not a stylistic preference, and the step still reports itself through `verdict()` rather than vanishing (see Question 6).

**Notes**:

* **Core Execution Mechanic:** `verdict()` collapses six framework states onto one boolean return — `bool(applies) and not banned` — so the log line and the control-flow gate are produced by a single call and cannot drift apart; the three ban functions take the run's live measurements as arguments so the reason is priced in this run's numbers rather than asserted from the spec.


* **Core Execution Mechanic (Dummy Translation):** One function both writes the log line and answers "should the next bit run?". Because it always answers no for a banned step, you cannot forget the check and quietly do the forbidden thing. And it prints the actual numbers from this run, not a stock explanation.


* **Boundary / Memory Constraint:** The bans are what keeps the geometry: Step 8 would flatten 1,440 variable-length baskets into 1,440 fixed-width rows of 28 binary columns, and `VarianceThresholdSelector(threshold=0.0049)` on that matrix keeps 26 and deletes 2 — `ETHUSDT_NO_TRADES` (5 / 1,440, variance 0.003460) and `SOLUSDT_NO_TRADES` (3 / 1,440, variance 0.002079). Step 10's standardisation moves `alpha + beta` from a 1,431–1,441 observation span to a 0.0141 / -0.0802 / 0.0660 span.


* **Boundary / Memory Constraint (Dummy Translation):** Turning each basket into a fixed row of 0s and 1s makes every basket the same shape, which is the one thing association mining cannot work with. And the variance filter throws out the rarest tokens first — the exact ones you were looking for. Scaling wipes out the count of how much evidence each arm has, which is the whole reason the bandit knows when to keep exploring.


* **Failure Mode / Downstream Impact:** Step 9's failure is the silent one: at `regParam=0.001` L1 zeroes `SOLUSDT_TAKER_HEAVY` — the consequent of the highest-lift rule in the feature set, support 0.00347, confidence 1.000, lift 4.645 — while 16 of 25 columns survive and nothing raises. Step 10's is loud by contrast: `np.random.beta(-1.1311, 1.1452)` raises `ValueError: a <= 0`, halting Thompson sampling rather than degrading it.


* **Failure Mode / Downstream Impact (Dummy Translation):** The regularisation one is nasty because nothing goes wrong on screen — it just deletes the rare-but-strong pattern you were mining for, and the job carries on looking healthy. The scaling one at least crashes straight away, so you find out immediately. A skipped step with no log line would leave you unable to tell a deliberate rule from something that was simply forgotten.



---

**Question 13**: How do you verify a PySpark job will run on Glue 4.0's narrower API surface before any of it is deployed?

**Before Execution** (`[local pyspark 3.5.5]` | `[unstripped function namespace]`):

```text
local development environment:
  pyspark 3.5.5 -- func.pmod, func.timestamp_micros, func.bool_and and func.array_compact
                   all resolve, because every one of them was added after 3.3.0

the runtime actually deployed to, probed inside the Glue 4.0 image:
  boto3 1.24.70 | pyarrow 10.0.0 | numpy 1.23.5 | pyspark 3.3.0+amzn.1.dev0

```

**After Execution** (`[strip test]` | `[simulated Spark 3.3.0 surface]`):

```text
deleted from pyspark.sql.functions and pyspark.ml: 174 names (versionadded > 3.3.0)
path1  logs and every Parquet payload: byte-identical to the control run
path2  logs and every Parquet payload: byte-identical to the control run
path3  logs and every Parquet payload: byte-identical to the control run

control mode: --no-strip, launched through the SAME runpy harness

```

**Code**:

```python
# The SQL name exists in 3.3.0; only the Python wrapper is 3.4.0 / 3.5.0, so expr() reaches it
ticks = ticks.withColumn("event_time", func.expr("timestamp_micros(event_time_us)"))
bar_us = func.col("event_time_us") - func.expr(f"pmod(event_time_us, {interval_us})")
agg = func.expr("bool_and(is_best_match)").alias("all_best_match")

# array_compact (3.4.0) has NO expr fallback, so Path 3 explodes and filters instead
tokens = frame.select("bar_us", "hour_utc", func.explode(func.array(
    func.when(func.col("is_missing_bar") == 1, namespaced(func.lit("NO_TRADES"))),
    func.when(func.col("regime_token").isNotNull(), namespaced(func.col("regime_token"))),
)).alias("token")).where(func.col("token").isNotNull())

# Glue uploads ONE script per job, so the shared module goes on sys.path explicitly:
#   --extra-py-files  s3://<bucket>/crypto_ticks/scripts/refinery_common.py
from refinery_common import build_session, load_bars, verdict as _verdict

```

**Answer:** Delete every name whose `versionadded` exceeds 3.3.0 from `pyspark.sql.functions` and `pyspark.ml` — 174 names — then re-run all three path jobs and diff logs and Parquet against a control that went through the identical harness. Anything the jobs still need is reached through `func.expr()`, and `refinery_common.py` is put on `sys.path` with `--extra-py-files` because Glue uploads one file per job.

**Notes**:

* **Core Execution Mechanic:** The strip runs against the live module objects rather than release notes, so an `AttributeError` surfaces at import or first call on a laptop; `func.expr()` routes to Catalyst's SQL function registry, where `pmod`, `timestamp_micros` and `bool_and` all exist in 3.3.0 even though their Python wrappers do not.


* **Core Execution Mechanic (Dummy Translation):** Instead of reading the docs to guess what Glue has, you delete everything newer than Glue's Spark and run the job again — if it still works, it will work there. The missing helpers are still reachable by writing them as SQL text.


* **Boundary / Memory Constraint:** The scan is class-level, so a post-3.3.0 *parameter* on a pre-3.3.0 class slips through — `CrossValidator.foldCol` and `VarianceThresholdSelector` were checked by hand and both landed in 3.1.0. `glue-ingest-bars.py` is deliberately excluded from `--extra-py-files` so the shared module can change without redeploying the job 341M ticks flow through.


* **Boundary / Memory Constraint (Dummy Translation):** The check looks at whole function names, not at newer options bolted onto old functions, so those two were checked by eye. The entryway job carries no shared file on purpose, so editing the shared file never forces a redeploy of the biggest job.


* **Failure Mode / Downstream Impact:** The baseline must run through the same launch method. Comparing `python job.py` against a `runpy.run_path` stripped run once reported 16 of 144 Path 2 correlation cells moving by up to `1.11e-16`, which propagated into the optimiser's coefficients; running *unstripped* through `runpy` reproduced it exactly, so the cause was the launch method, not the stripping.


* **Failure Mode / Downstream Impact (Dummy Translation):** If the "before" and "after" runs differ in two ways at once, you cannot tell which one caused the difference you found — and here the difference was the way the job was started, not the thing being tested.



---

**Question 14**: How do you submit a Glue job from Airflow and actually fail the task when the job fails?

**Before Execution** (`[Lab2 dag-glue-workflow.py]` | `[hand-rolled boto3 poller]`):

```text
response = client.get_job_runs(JobName=job_name, MaxResults=1)
job_runs = response.get('JobRuns', [])

if job_runs and job_runs[0]['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
    time.sleep(poll_interval)
else:
    logging.info(f"Glue job {job_name} has finished.")
    break

# JobRunState == 'FAILED' takes the else branch, logs "has finished", task goes green
# and start_job_run's JobRunId is never stored at all

```

**After Execution** (`[GlueJobOperator]` | `[test_dag_workflow.py]`):

```text
assert len(glue_tasks) == 5, f"expected 5 Glue jobs, found {len(glue_tasks)}"
assert task.wait_for_completion is True

Verified against Airflow 2.9.3 / apache-airflow-providers-amazon 8.25.0

```

**Code**:

```python
# The operator keeps the run id it started, polls THAT run, and raises on a terminal
# state that is not SUCCEEDED. No script_location and no create_job_kwargs: a missing
# job fails the task instead of being quietly created here with the operator's defaults.
ingest_bars = GlueJobOperator(
    task_id='ingest_bars',
    job_name=INGEST_JOB,
    region_name=AWS_REGION,
    wait_for_completion=True,
    job_poll_interval=POLL_INTERVAL,      # 30 s, not the operator's 6 s default
    script_args={
        '--input': f's3://{BUCKET_NAME}/{RAW_PREFIX}',
        '--bars-output': f's3://{BUCKET_NAME}/{BARS_PREFIX}{MONTH}/',
        '--month': MONTH,
        '--bar-interval': BAR_INTERVAL,
    }
)

```

**Answer:** Use `GlueJobOperator` from `apache-airflow-providers-amazon` with `wait_for_completion=True`, which keeps the `JobRunId` it started, polls that run, and raises on any terminal state other than `SUCCEEDED`. The provider is already a dependency of this repository for the sibling project's `EmrServerlessStartJobOperator`, so preferring it over hand-rolled boto3 is existing precedent.

**Notes**:

* **Core Execution Mechanic:** `start_job_run` returns a `JobRunId` that identifies one execution; `get_job_runs(JobName=..., MaxResults=1)` identifies only the newest run of a job *name*. The operator holds the returned id and calls `GetJobRun` against it, then maps the terminal state onto the task's exit status.


* **Core Execution Mechanic (Dummy Translation):** Starting a job gives you a ticket number. The lab throws the ticket away and then asks "how's the most recent job with this name doing" — which may be somebody else's run entirely. The operator keeps the ticket and asks about that one.


* **Boundary / Memory Constraint:** Polling is set to 30 s rather than the operator's 6 s default: these are multi-minute jobs — the entryway measured 6 min 56 s on a full month — so a 6 s poll is a few hundred `GetJobRun` calls per run to learn nothing.


* **Boundary / Memory Constraint (Dummy Translation):** Checking every six seconds on a job that takes seven minutes is just paying AWS to say "still going" seventy times.


* **Failure Mode / Downstream Impact:** The lab's loop exits on ANY non-running state, so `FAILED`, `TIMEOUT` and `STOPPED` all reach the `has finished` log and the task succeeds. A dead Glue job hands a green task to the next one, which then runs against last month's artifacts under this month's prefixes.


* **Failure Mode / Downstream Impact (Dummy Translation):** The old code treats "the job stopped" as "the job worked". A crashed job shows up as a green tick, and everything after it happily processes stale data.



---

**Question 15**: How do you make one month value reach five Glue jobs from a single place in the DAG?

**Before Execution** (`[module constant]` | `[DAG parse time, unrendered]`):

```text
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

ingest_bars.script_args['--month']   -> "{{ data_interval_start.strftime('%Y-%m') }}"
refine_path1.script_args['--output'] -> "s3://nl-aws-de-labs/crypto_ticks/curated/{{ ... }}/path1/"
load_dynamo.script_args['--run-id']  -> "{{ data_interval_start.strftime('%Y-%m') }}"

```

**After Execution** (`[one @monthly run]` | `[script_args rendered by the provider]`):

```text
--month        2025-01
--bars-output  s3://nl-aws-de-labs/crypto_ticks/curated/bars/2025-01/
--output       s3://nl-aws-de-labs/crypto_ticks/curated/2025-01/path1/
--run-id       2025-01

pk = "2025-01#path1"

```

**Code**:

```python
# One Jinja string, three destinations: the entryway's --month, every path job's output
# prefix, and the loader's --run-id. A FIXED start_date, not days_ago(1).
'start_date': datetime(2025, 1, 1),

MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
MONTH_PREFIX = f'{CURATED_PREFIX}{MONTH}/'

# The one assumption about the provider rather than about this repository:
assert 'script_args' in GlueJobOperator.template_fields, (
    "GlueJobOperator.script_args is not templated in this provider version, so MONTH "
    "would be passed through literally -- pin apache-airflow-providers-amazon >= 8.0")

loader = dag.get_task('load_dynamo').script_args
assert loader['--run-id'] == dag.get_task('ingest_bars').script_args['--month']

```

**Answer:** Format `data_interval_start` once into a module-level Jinja string and pass it into every `script_args` dict, relying on `script_args` being a `GlueJobOperator` template field so the provider renders it per run. `start_date` is a fixed `datetime(2025, 1, 1)` because the rendered month becomes both the S3 prefix and half the DynamoDB partition key.

**Notes**:

* **Core Execution Mechanic:** Airflow renders template fields per task instance against the run's context, so `data_interval_start` — the first instant of the month on an `@monthly` schedule — is resolved at execution time, not at parse time. `test_dag_workflow.py` asserts membership in `GlueJobOperator.template_fields` and that the loader's `--run-id` is the same expression object as the entryway's `--month`.


* **Core Execution Mechanic (Dummy Translation):** You write the month once as a placeholder, and Airflow fills in the real value when each run happens. A test checks that the operator actually knows how to fill it in, and that all five jobs are filling in the same blank.


* **Boundary / Memory Constraint:** Every job writes `mode("overwrite")` and none partitions, so two runs pointed at one prefix are one run's artifacts. `max_active_runs=1` and `@monthly` — the cadence of the source archives — keep them apart; the reference lab's `*/5 * * * *` with multi-minute jobs does not.


* **Boundary / Memory Constraint (Dummy Translation):** Each job wipes its output folder before writing, so two runs sharing a folder means the second erases the first. Only one run at a time, once a month.


* **Failure Mode / Downstream Impact:** Without the template field the jobs receive the literal string `{{ data_interval_start.strftime('%Y-%m') }}` as `--month` and the entryway builds a bar calendar for a month of that name. `days_ago(1)` fails differently and worse: a dynamic `start_date` moves on every scheduler re-parse, so the S3 prefix and the DynamoDB partition key would depend on when a file was last read.


* **Failure Mode / Downstream Impact (Dummy Translation):** If the placeholder never gets filled in, the job is handed the placeholder text itself as the month. And a start date that means "yesterday" changes every time the file is re-read, so the folder name and the database key keep moving — a key that drifts is not a key.



---

**Question 16**: How do you key three artifacts at three different grains into one DynamoDB table without silently overwriting rows?

**Before Execution** (`[three Parquet ledgers]` | `[before keying]`):

```text
path1: 18 rows from _localrun/docker/path1/coefficients -> 19 items (1 header + 18 grain)
path2: 72 rows from _localrun/docker/path2/coefficients -> 73 items (1 header + 72 grain)
path3:  3 rows from _localrun/docker/path3/arms         ->  4 items (1 header +  3 grain)

# `imbalance` is a feature name in path1's ledger AND in path2's -- the only shared name

```

**After Execution** (`[--dry-run]` | `[boto3 TypeSerializer]`):

```text
path1 {'M': {'pk': {'S': '2025-01-sample#path1'}, 'sk': {'S': 'feature#imbalance'}, 'run_id': {'S': '2025-01-sample'}, 'path': {'S': 'path1'}, 'feature': {'S': 'imbalance'}, 'coefficient': {'N': '0.00003151237034105486'}, 'survived_step9': {'BOOL': True}, 'mu': {'N': '0.5634270990339224'}, 'sigma': {'N': '0.3793985875258609'}, 'bp_per_sd': {'N': '0.11955748796988044'}}}
path2 {'M': {'pk': {'S': '2025-01-sample#path2'}, 'sk': {'S': 'feature#imbalance#class_name#down'}, 'run_id': {'S': '2025-01-sample'}, 'path': {'S': 'path2'}, 'feature': {'S': 'imbalance'}, 'class_name': {'S': 'down'}, 'coefficient': {'N': '-0.12208986818821997'}, 'max_abs_across_classes': {'N': '0.12208986818821997'}, 'survived_step9': {'BOOL': True}}}
dry run complete: 96 items encoded by boto3's own serializer and discarded.

```

**Code**:

```python
# bool BEFORE int: isinstance(True, int) is True, so the wrong order writes
# survived_step9 as the number 1 -- a valid item, a successful write, a wrong table.
if isinstance(value, bool):
    return value
if isinstance(value, int):
    return value
if isinstance(value, float):
    encoded = Decimal(str(value))     # not Decimal(value): prec=38 raises Inexact
    # The magnitude band boto3 does NOT police: 1E-131 and 1E126 serialize cleanly and
    # DynamoDB rejects both. Its range is 1E-130 to 9.9999...E125.
    if encoded and not (DDB_MIN_EXP <= encoded.adjusted() <= DDB_MAX_EXP):
        raise ValueError(f"{name} is {value!r}, outside DynamoDB's number range ...")
    return encoded

pk = f"{run_id}#{path}"
keys = {(i["pk"], i["sk"]) for i in items}
if len(keys) != len(items):
    raise ValueError(f"{path} produced {len(items)} items with only {len(keys)} keys")

# put_item cannot delete, so the partition is READ BACK and orphans removed.
orphans = sorted(held - written)

```

**Answer:** Key the table `pk = "<run-id>#<path>"` with `sk` the artifact's own grain — `feature#<name>`, `feature#<name>#class_name#<class>`, `symbol#<symbol>` — so one partition is exactly one path's result from one run, and prove `(pk, sk)` unique before the first write. After writing each partition, Query it back and delete whatever this run did not produce, because `put_item` overwrites but cannot delete.

**Notes**:

* **Core Execution Mechanic:** The sort key alternates the artifact's own column names with their values, so an item reads back against its Parquet without a translation table and `begins_with(sk, "feature#imbalance#")` returns one Path 2 feature's three class rows. `"#model"` sorts first because `#` is `0x23` and every grain prefix starts with a letter, so a Query returns the model header without being asked to.


* **Core Execution Mechanic (Dummy Translation):** The folder name is "which run, which path" and the file name inside it is "which row". Because the row name spells out its own columns, you can read an item back and know what it is without looking anything up.


* **Boundary / Memory Constraint:** 96 items once a month, so the table is on-demand billing — provisioned capacity would be paying by the hour for a table idle by the hour. All 47 distinct grain tokens across the three artifacts are `[A-Za-z0-9_]`, none contains a `#`, and the longest is 47 characters; a grain value carrying a `#` raises rather than forging a key boundary.


* **Boundary / Memory Constraint (Dummy Translation):** Ninety-six rows once a month is nothing, so you pay per write instead of renting capacity. The `#` is the separator, so a value containing one would fake an extra boundary — the job refuses instead.


* **Failure Mode / Downstream Impact:** `imbalance` appears in both Path 1's and Path 2's ledgers — the only shared name — so a key built from the feature name alone would have those rows fighting over one item today. `batch_writer` de-duplicates nothing: a repeat inside one batch is a `ValidationException`, a repeat across batches is a silent overwrite that turns 72 rows into 24 items with no error. Skipping the read-back leaves last month's dropped features in the partition still claiming to be current.


* **Failure Mode / Downstream Impact (Dummy Translation):** Two different paths happen to use the same feature name, so naming items by feature alone would have them land on top of each other and nobody would be told. And since writing cannot remove anything, a month that keeps fewer features leaves the old ones sitting there looking current unless you go back and sweep them out.



---

**Question 17**: How do you run all five Glue scripts locally inside AWS's own Glue 4.0 image?

**Before Execution** (`[docker run IMAGE python3 job.py]` | `[ENTRYPOINT ["bash","-l"]]`):

```text
$ docker image inspect amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    --format 'Entrypoint={{json .Config.Entrypoint}} User={{.Config.User}}'
Entrypoint=["bash","-l"] User=glue_user

$ docker run --rm amazon/aws-glue-libs:glue_libs_4.0.0_image_01 python3 --version
/usr/local/bin/python3: /usr/local/bin/python3: cannot execute binary file
exit=126

```

**After Execution** (`[-c "$*"]` | `[measured, 8 CPUs / 7.8 GiB]`):

```text
selfcheck  24 s
ingest     46 s
path1      3 min 43 s
path2      6 min 52 s
path3      1 min 33 s
dynamo      5 s

no-argument chain, end to end: 12 min 15 s   (native chain ~7.5 min)

```

**Code**:

```bash
# One wrapper, and every stage goes through `-c` -- the only form that works for a shell
# script (spark-submit) and an ELF binary (python3) alike. No argument contains a space.
glue() {
  MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$(pwd):${WORKSPACE}" -w "${WORKSPACE}" \
    -e DISABLE_SSL=true \
    "${IMAGE}" -c "$*"
}

# Spark jobs:
glue spark-submit "glue-jobs/glue-refinery-${stage}.py" --local \
  --input "${OUT}/bars" --output "${OUT}/${stage}"

# The loader is a Glue PYTHON SHELL job and is never spark-submitted:
glue python3 glue-jobs/glue-dynamo.py --dry-run --run-id "${RUN_ID}" \
  --path1 "${OUT}/path1" --path2 "${OUT}/path2" --path3 "${OUT}/path3"

```

**Answer:** Route every container command through `bash -c`, because the image's `ENTRYPOINT` is `["bash","-l"]` and hands the command to a login shell as the name of a script file rather than exec'ing it. `spark-submit` survives direct invocation only because it is itself a bash script; `glue-dynamo.py` is a Python shell job and runs with `python3`, never `spark-submit`, matching the split the deployed job definitions make.

**Notes**:

* **Core Execution Mechanic:** A login-shell ENTRYPOINT treats its argument as a script path, so `python3` is located on PATH and read as text; its ELF magic is not a shebang and bash exits 126, "cannot execute binary file". `-c` makes the argument a command string instead, which works uniformly for both an interpreter binary and a shell script.


* **Core Execution Mechanic (Dummy Translation):** The container's front door is bash, and bash expects the name of a shell script. Hand it a program instead and it tries to read the program as if it were text, chokes, and quits. Adding `-c` tells bash "this is a command, run it" and everything works.


* **Boundary / Memory Constraint:** Docker Desktop gives the container 8 CPUs and 7.8 GiB against the host's 24 cores, so the chain is slower than native and the gap is widest on Path 2 — the stage whose cost is its fit count (24 logistic regressions at Step 9, three more at Step 10) rather than its row count. Nothing is installed: the image already carries pyspark 3.3.0+amzn.1, boto3 1.24.70, pyarrow 10.0.0 and numpy 1.23.5.


* **Boundary / Memory Constraint (Dummy Translation):** The container gets a third of the machine, so everything takes longer, and the worst hit is the stage that trains lots of small models rather than the one reading lots of rows. Nothing has to be installed — the image already ships every library these jobs import.


* **Failure Mode / Downstream Impact:** This is the stronger version of the strip test: real Spark 3.3.0, Python 3.10 and Java 8 rather than a locally simulated API surface. `bars/` and both `topology/` exports came out identical to native, while `coefficients/` differed by up to 9.2e-14 and `path3/frame/` had 74 of 1,440 baskets in a different item order — identical as sets. The image is Glue's runtime rather than the Glue service: it pins the same Spark, Python, Java and jar set, and runs them `local[*]`, so a cluster, S3, IAM and job bookmarks sit outside it.


* **Failure Mode / Downstream Impact (Dummy Translation):** Running in AWS's own image proves the code works on the real Spark version, and the decimal money columns held up exactly — the bars came back byte for byte the same. The fitted model numbers wobbled in the fourteenth decimal, which is float arithmetic, not a bug. It is the same engine AWS runs, on one machine rather than a cluster.



---
