"""Crypto ticks -> OHLCV bars: the refinery's shared entryway, Steps 1-3 only.

AWS Glue 4.0 entrypoint. Applies the path-blind founding steps of the 10-step statistical
refinery to Binance spot trade archives, and stops dead at the architectural fork:

  raw headerless trade CSVs, one per symbol   (3 x ~1e8 rows x 7 cols)
      -> Step 1   ingest + dedup       union with a symbol discriminator; key (symbol, trade_id)
      -> Step 2   syntax normalisation explicit StructType; epoch MICROseconds -> timestamp, UTC
      -> Step 2.5 granularity          ticks -> OHLCV bars on the --bar-interval grid
      -> Step 3   missingness flags    dense calendar left-join -> is_missing_bar (flag, never fill)
      ================= THE FORK =================
      Steps 4-10 are path-isolated and live in glue-refinery-path{1,2,3}.py.
      Nothing below this line imputes, scales, encodes or selects features.
      -> bars Parquet  (one row per (symbol, bar), dense over the whole --month)

Every step reports through verdict(), which prints APPLIES / N/A / BANNED. An N/A here is
always backed by a number measured on this run, never by an assumption.

Pure PySpark: no pandas, no boto3, no awsglue. This runs on the cluster, not in the scheduler.

Local acceptance run, against the committed two-hour sample::

    python glue-ingest-bars.py --local \\
        --input data/sample \\
        --bars-output _localrun/bars \\
        --bar-interval 5s \\
        --calendar-start 2025-01-01T00:00:00 \\
        --calendar-end   2025-01-01T02:00:00

The calendar overrides are what make the sample run meaningful: the sample is two hours, so
the default month-wide grid would report 1,606,390 of 1,607,040 bars missing -- true, and a
measurement of the extract rather than of the data. The full month needs neither override::

    python glue-ingest-bars.py --local \\
        --input data/unzipped \\
        --bars-output _localrun/bars \\
        --month 2025-01 --bar-interval 5s
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (BooleanType, DecimalType, LongType, StructField,
                               StructType)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_ingest_bars")

APP_NAME = "CryptoTicksIngestBars"

# Every value on the wire is an exact 8-decimal-place decimal string, so decimal(18,8) is the
# wire format, not an approximation of it. Double was considered and rejected: summing 2M real
# quote_qty values under 3 / 11 / 29 shuffle partitions gave THREE different doubles
# (826888559.988 / ...9879998 / ...9879997) and one identical decimal. Float addition is not
# associative and Spark does not promise a stable merge order, so a Double pipeline is not
# idempotent -- the same job over byte-identical input emits different bars, which breaks any
# checksum gate or backfill-vs-incremental reconciliation. Decimal also has no NaN, and NaN
# sorts as the LARGEST value in Spark, so one NaN price would win max("price") and every argmax.
# 10 integer digits against a max observed 6 (quote_qty 1,852,816.09952780 on BTC).
MONEY = DecimalType(18, 8)

# The CSVs are HEADERLESS, so Step 2's "sanitise the column headers" is not a rename -- it is
# the SUPPLY of names by positional contract, and this StructType is the entire step. Column
# ORDER is load-bearing and this is the only place it is written down. inferSchema is banned:
# it costs a second read pass and it would silently re-type a column when the source changes.
SCHEMA = StructType([
    StructField("trade_id", LongType(), True),        # not Integer: BTC max is 4,495,881,900
    StructField("price", MONEY, True),
    StructField("qty", MONEY, True),
    StructField("quote_qty", MONEY, True),
    # Named _us, never "time". A column called "time" invites the /1000 "convert from millis"
    # reflex; this one is epoch MICROseconds (16 digits) and that reflex lands the whole month
    # in the year 56971 without raising. See step2_normalise().
    StructField("event_time_us", LongType(), True),
    # The literal strings are Python-cased "True"/"False". Verified: Spark's CSV reader parses
    # those into BooleanType with no help, so do NOT add a when(col == "true", ...) mapping.
    # It also parses "1"/"yes" to NULL under the default PERMISSIVE mode with no error, which
    # is why _nulls is counted in the bar aggregate and asserted.
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match", BooleanType(), True),
])

DATA_COLS = [f.name for f in SCHEMA.fields]

# Binance monthly archive naming. The zip holds one CSV of exactly this basename; unzipping is
# the DAG's / local-docker-development.sh's job, not this job's -- Spark reads gzip/bzip2/lz4,
# and pointing it at a .zip does not error, it decodes the archive bytes as UTF-8 and yields
# rows starting "PK\x03\x04".
# The trailing * matches both the plain .csv of a full extract and the .csv.gz of the committed
# sample, so --local and the full run take the same code path. Only the sample is gzipped: gzip
# is NOT splittable, so a 10 GB .csv.gz would be handed to a single task, while the 1.3 MB
# sample fits in one task anyway and is worth the 10x saving in the repo.
FILE_TEMPLATE = "{symbol}-trades-{month}.csv*"

# Bar widths in MICROseconds, matching the source unit so the bucket key needs no division.
# 5s is the default on measured grounds (full-month pass over all 340,971,834 ticks): it is the
# only width where the flat class close == previous close stays material (13.25-20.04%, so a
# 3-class target on Path 2 is real rather than a fiction), Step 3 has something honest to flag
# (613 empty bars, 0.003-0.084%, real but under the framework's 5% override threshold), and the
# bars are still bars (median 98-129 ticks, zero-range bars <= 2.17%). At 1s a quarter of the
# bars degenerate to O=H=L=C; at 15s and coarser Step 3 finds literally nothing.
BAR_INTERVALS = {"1s": 1_000_000, "5s": 5_000_000, "15s": 15_000_000,
                 "1m": 60_000_000, "5m": 300_000_000}
DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT"

# Columns carried through the aggregate purely to be asserted on or reported, then dropped.
GUARD_COLS = ["_nulls", "_zero_qty", "_zero_price", "_not_best_match"]


def verdict(applies, message, banned=False):
    """Three-state section verdict, extending the notebook's APPLIES / N/A.

    APPLIES -- the check ran and there is work to do.
    N/A     -- the check ran and found nothing. Never printed on an assumption:
               every N/A in this job is backed by a number in `message`.
    BANNED  -- forbidden here, and `message` is the reason, not an apology.

    Returns False for BANNED as well as for N/A, so `if verdict(...):` can
    never gate a handler the framework forbids.
    """
    label = ("BANNED   -- " if banned
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    LOG.info("%s%s", label, message)
    return bool(applies) and not banned


def month_bounds_us(month):
    """[start, end) of a YYYY-MM month as epoch microseconds, UTC.

    Derived from the ARGUMENT, never from min/max of the observed data: an observed-range
    calendar reports zero gaps even when the last three days failed to download. That is the
    whole reason Step 3 can say anything at all -- a gap is only visible against a calendar
    that was declared independently of the data being checked.
    """
    year, mon = (int(part) for part in month.split("-"))
    start = datetime(year, mon, 1, tzinfo=timezone.utc)
    end = datetime(year + (mon == 12), mon % 12 + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()) * 1_000_000, int(end.timestamp()) * 1_000_000


def calendar_bounds_us(month, start_iso, end_iso, interval_us):
    """The declared calendar, defaulting to --month and overridable for a partial extract.

    The override exists because the committed sample is two hours, not a month: run against it
    with the month calendar and Step 3 truthfully reports 1,606,390/1,607,040 bars missing,
    which is arithmetically correct and completely useless -- it measures "the sample is a
    sample", not "the data has gaps". Declaring the narrower window keeps the check meaningful
    at both scales while keeping the calendar declared rather than inferred.
    """
    start_us, end_us = month_bounds_us(month)
    if start_iso:
        start_us = int(datetime.fromisoformat(start_iso)
                       .replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
    if end_iso:
        end_us = int(datetime.fromisoformat(end_iso)
                     .replace(tzinfo=timezone.utc).timestamp()) * 1_000_000

    if end_us <= start_us:
        raise ValueError(f"empty calendar window: {start_us} .. {end_us} (epoch microseconds)")
    # The grid is generated as start + i*interval, so a start off the interval boundary would
    # produce slots no tick can ever land in -- every bar_us from pmod IS a multiple of the
    # interval. The join would then miss on every row and Step 3 would report 100% missing
    # with no null and no row-count change to give it away.
    if start_us % interval_us:
        raise ValueError(
            f"calendar start {start_us} is not on a {interval_us}us boundary -- the grid would "
            f"be offset from the bar keys and every slot would read empty")
    if (end_us - start_us) % interval_us:
        raise ValueError(f"interval does not tile the calendar evenly: "
                         f"{end_us - start_us} microseconds / {interval_us}")
    return start_us, end_us


def build_session(local, shuffle_partitions):
    """Local runs need a master and a shuffle width; Glue supplies both itself."""
    builder = SparkSession.builder.appName(APP_NAME)
    if local:
        # local[*] uses every core. The shuffle width is a flag rather than a constant because
        # this job has two shuffles that differ by five orders of magnitude: the Step 3 grid
        # join touches 1.6M rows, while the Step 1 dedup groups 341M ticks by (symbol,
        # trade_id) -- essentially one group per row. The sibling job's 8 is right for the
        # sample and pathological for the month: 8 reducers for 341M keys spill relentlessly,
        # and the run goes I/O-bound at a fraction of one core. Left unset, Spark's own default
        # of 200 applies, which is the sane full-month value.
        builder = builder.master("local[*]")
        if shuffle_partitions:
            builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    # Spark's Parquet default is INT96, which is deprecated and reads back badly in Athena.
    # TIMESTAMP_MICROS matches the source resolution exactly.
    builder = builder.config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Unconditional, and NOT behind --local. Measured on a dev box: the session timezone
    # defaulted to Africa/Johannesburg, under which the last bar of January renders as
    # 2025-02-01 01:59 and to_date() drops 120 January bars per symbol into a February
    # partition. Glue defaults to UTC, so without this line the same job emits different
    # partitions in the two places -- silent, data-corrupting, and invisible in a diff.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    LOG.info("Spark %s | parallelism %s | tz %s | local=%s",
             spark.version, spark.sparkContext.defaultParallelism,
             spark.conf.get("spark.sql.session.timeZone"), local)
    return spark


# --------------------------------------------------------------------------------------
# STEP 1 -- STATEFUL RELATIONAL INGESTION / DEDUPLICATION
# --------------------------------------------------------------------------------------

def step1_ingest(spark, input_path, symbols, month):
    """Union the per-symbol files with a discriminator column. Key is (symbol, trade_id)."""
    base = input_path.rstrip("/")
    frames = []
    for symbol in symbols:
        path = f"{base}/{FILE_TEMPLATE.format(symbol=symbol, month=month)}"
        # No .option("header", ...): the files have none, which is the whole point of SCHEMA.
        frames.append(spark.read.schema(SCHEMA).csv(path)
                      .withColumn("symbol", func.lit(symbol)))

    ticks = frames[0]
    for frame in frames[1:]:
        ticks = ticks.unionByName(frame)

    # The symbol column is what makes this a merge rather than blind row-stacking. Without it
    # the union is 341M rows in which a $109,588 BTC print and a $168 SOL print are
    # indistinguishable and every downstream mean averages three price scales.
    verdict(True, f"Step 1 ingest: {len(symbols)} files -> one frame keyed (symbol, trade_id), "
                  f"symbols {', '.join(symbols)}")

    # The framework's phantom rows come "from misaligned timestamps". The tick-domain version
    # of that mistake is joining the three symbols on event_time so each row carries BTC, ETH
    # and SOL side by side. BTC alone prints ~52 trades/second and 75% of rows share a
    # microsecond with their predecessor (max 1,110 ticks in ONE microsecond), so such a join
    # fans out many-to-many into rows describing trades that never happened. Cross-symbol
    # alignment is deferred to the shared bar grid below, where it is a grouping, not a join.
    verdict(False, "Step 1 temporal join: aligning symbols on event_time fabricates phantom "
                   "rows -- BTC/ETH/SOL routinely share a microsecond", banned=True)
    return ticks


def step1_dedup_check(ticks):
    """Measure the duplicate count on (symbol, trade_id). Returns rows read.

    One shuffle over a 341M-row two-column projection to print a verdict that is expected to
    read N/A. It is kept deliberately: an N/A the job did not measure is a lie, and Step 1's
    entire claim is that the merge is deterministic.
    """
    # trade_id ALONE is not the key. It is a per-symbol Binance sequence. The three id ranges
    # happen to be disjoint in 2025-01, so a bare trade_id key passes every test on this month
    # and collides the first time a fourth symbol or an id reset overlaps it. Designing against
    # a coincidence is how a pipeline is right by luck.
    dupes = (ticks.select("symbol", "trade_id")
                  .groupBy("symbol", "trade_id").count()
                  .filter(func.col("count") > 1)
                  .count())

    # Contiguity is nearly free (one 3-row aggregate) and catches what dedup structurally
    # cannot see: a truncated download. On its own it also cannot distinguish "no duplicates"
    # from "equal numbers of duplicates and gaps", which is why both checks exist.
    profile = (ticks.groupBy("symbol")
                    .agg(func.count("*").alias("rows"),
                         func.min("trade_id").alias("tid_min"),
                         func.max("trade_id").alias("tid_max"))
                    .orderBy("symbol")
                    .collect())
    raw_rows = sum(row["rows"] for row in profile)
    for row in profile:
        span = row["tid_max"] - row["tid_min"] + 1
        LOG.info("%s: %s rows, trade_id %s..%s (span %s)",
                 row["symbol"], row["rows"], row["tid_min"], row["tid_max"], span)
        if span != row["rows"]:
            raise ValueError(
                f"{row['symbol']} trade_id is not contiguous: {row['rows']} rows span {span} "
                f"ids -- the download is truncated or a file split was lost")

    # The one place the three-state return value is load-bearing rather than decorative: the
    # handler below runs if and only if verdict() said APPLIES. That is the contract the
    # notebook's verdict() established (it "returns the flag so callers can gate the handling
    # step") and the reason BANNED returns False -- a banned step can never reach its handler
    # by accident, even if someone later writes `if verdict(...)` around one.
    if verdict(dupes > 0,
               f"Step 1 dedup: {dupes:,} duplicate (symbol, trade_id) keys in {raw_rows:,} rows"):
        raise ValueError(
            f"{dupes} duplicate (symbol, trade_id) keys -- Binance trade ids are unique per "
            f"symbol, so this is a double-ingested file split, not a data property")
    return raw_rows


# --------------------------------------------------------------------------------------
# STEP 2 -- STRUCTURAL SYNTAX NORMALISATION
# --------------------------------------------------------------------------------------

def step2_normalise(ticks, raw_rows):
    """Report the typing decisions SCHEMA already made, and derive the real timestamp."""
    verdict(True, "Step 2 schema: 7 headerless columns named and typed by position -- "
                  "3 decimal(18,8), 2 bigint, 2 boolean; inferSchema not used")

    # `event_time_us` is epoch MICROseconds. Verified on the real value 1735689600010866:
    #   timestamp_micros -> 2025-01-01 00:00:00.010866   (correct)
    #   timestamp_millis -> +56971-10-25 00:00:10.866    (silent, no error)
    #   to_timestamp(v/1000) -> +56971-10-25 02:00:10.86592
    # Both wrong forms produce a valid-looking timestamp 55,000 years out. timestamp_micros is
    # SQL-only in Spark 3.3.0 (Glue 4.0) -- the Python wrapper lands in 3.5.0 -- hence expr().
    # event_time_us is KEPT alongside event_time: it is the raw ingested value and it cannot be
    # re-derived once dropped.
    ticks = ticks.withColumn("event_time", func.expr("timestamp_micros(event_time_us)"))
    verdict(True, "Step 2 timestamp: event_time_us (epoch microseconds) -> event_time via "
                  "timestamp_micros, session timezone pinned to UTC")

    # Step 2's "trim whitespace, lowercase the headers" half is a measured no-op here: after
    # SCHEMA is applied there is not one StringType column in the frame. `symbol` is ours,
    # uppercase by construction from the filename.
    string_cols = [f.name for f in ticks.schema.fields
                   if f.dataType.simpleString() == "string" and f.name != "symbol"]
    verdict(bool(string_cols),
            f"Step 2 text: {len(string_cols)} string columns survive the schema -- "
            f"trim/lower has nothing to normalise")

    # is_best_match is zero-variance on the archives profiled so far: 0 False rows in all
    # 340,971,834. The temptation is to drop it here. The framework settles it against us:
    # its own constant column (constant_metric) is present in the Step 1, 2 and 3 matrices and
    # is deleted only at Step 8, which is path-isolated and BANNED on Path 3. The entryway
    # runs before the router reads y's geometry, so it is structurally unable to know whether
    # Step 8 will even execute; dropping the column here would enforce a Path 1 decision on a
    # path where the framework forbids it.
    # CONFLICTING BRIEF, stated rather than buried: the interval brief argues for dropping it
    # at ingest, because Path 3 never reaches Step 8 and a token present in 100% of
    # transactions pollutes every support/lift figure in association mining. That is a real
    # cost, but it is Path 3's cost to pay -- glue-refinery-path3.py must drop the column
    # explicitly and say so. Framework fidelity wins here: the entryway does not pre-empt a
    # path-isolated step.
    # The message states the RULE, not a count, because the constancy has not been measured yet
    # on THIS run -- it is counted in the same shuffle that builds the bars and reported as its
    # own ledger line in assert_bar_invariants(). A verdict that quotes a number it did not
    # measure is exactly the lie the three-state contract exists to prevent.
    verdict(False, "Step 2 prune: is_best_match is a variance question and VarianceThreshold is "
                   "Step 8 -- path-isolated, BANNED on Path 3, so the entryway carries the "
                   "column through as all_best_match instead of pre-empting the decision",
            banned=True)
    return ticks


# --------------------------------------------------------------------------------------
# STEP 2.5 -- ROW GRANULARITY  (ticks -> bars)
# --------------------------------------------------------------------------------------
# The framework does not number this step and pretending otherwise would be dishonest. It sits
# between 2 and 3 for two hard reasons: you cannot bucket by time until the microsecond epoch
# is a real timestamp and you cannot sum money until price/qty are Decimal (both Step 2), and
# Step 3's flag must describe the row that EXITS the entryway -- which is a bar, not a tick --
# while the fork reads y's geometry off a bar-level quantity that does not exist at tick grain.
# The prototype notebook keeps "7. Row Granularity" as its own numbered stage for the same
# reason: a granularity change is neither a rename, a cast, nor a deduplication.

def step2_5_aggregate(ticks, interval_us, interval_label, raw_rows):
    """Collapse ticks into OHLCV bars on a half-open [bar_us, bar_us + interval) grid."""
    # Pure integer arithmetic, no division. `time / I` returns a Double in Spark and cast()
    # truncates toward zero rather than flooring; pmod is exact for any interval and correct
    # for pre-1970 epochs, and it hands back bar_us directly for timestamp_micros.
    #
    # Through expr(), NOT func.pmod(): the Python wrapper for pmod is versionadded 3.4.0 and
    # Glue 4.0 is Spark 3.3.0, so func.pmod raises AttributeError there -- on the one line
    # every bar depends on, and only AFTER the Step 1 shuffle has scanned 341M rows. The SQL
    # name IS registered in 3.3.0, so expr() reaches it. Same reason as timestamp_micros and
    # bool_and below; this is the third instance of the rule, not an exception to it.
    # The `%` operator would also compile on 3.3.0 but maps to Remainder, which is negative
    # for negative operands -- identical here (2025 epochs are positive), wrong for a pre-1970
    # backfill. pmod costs nothing extra and is correct for both.
    ticks = ticks.withColumn(
        "bar_us",
        func.col("event_time_us")
        - func.expr(f"pmod(event_time_us, {interval_us})"))

    zero = func.lit(0).cast(MONEY)
    bars = (ticks.groupBy("symbol", "bar_us").agg(
        # min_by/max_by on trade_id, NOT first()/last() and NOT an ordering on event_time.
        # first()/last() over a groupBy return whatever reached the partition first, which
        # changes with partitioning and with a re-run. event_time is only NON-DECREASING: 75%
        # of rows share a microsecond with their predecessor and 24,851 of those tie groups
        # have a non-constant price, so an event_time ordering leaves open/close genuinely
        # undefined on a large fraction of bars. trade_id is strictly increasing and
        # contiguous (asserted in Step 1), and a strictly increasing id over a non-decreasing
        # clock IS time order, so it is sufficient alone.
        # The sibling job's max(struct(n, sector)) idiom would also work, but it forces
        # SortAggregate (two Sort nodes in the plan) where min_by stays on the hash path with
        # no Sort at all, and it makes the float price a silent third tiebreaker the moment
        # the ordering key can tie.
        func.min_by("price", "trade_id").alias("open"),
        func.max("price").alias("high"),
        func.min("price").alias("low"),
        func.max_by("price", "trade_id").alias("close"),
        func.sum("qty").alias("volume"),
        # The exchange's own notional, not a recomputed price*qty. Verified exactly equal on
        # 2.8M sampled rows across all three symbols (max |diff| = 0), and equal by
        # construction: price carries <= 2 significant decimals and qty <= 5, so the product
        # needs <= 7 and the field stores 8. Using it saves a multiply over 341M rows, keeps
        # the sum at decimal(28,8) instead of burning the overflow margin on decimal(38,16),
        # and lets the result reconcile digit-for-digit against Binance klines.
        func.sum("quote_qty").alias("quote_volume"),
        func.count("*").alias("n_ticks"),
        func.min("trade_id").alias("first_trade_id"),
        func.max("trade_id").alias("last_trade_id"),
        # is_buyer_maker = True means the BUYER was resting on the book, so the SELLER
        # crossed the spread: True is an aggressive SELL. Verified on 2M real ticks --
        # P(next tick up | is_buyer_maker False) = 0.9960 vs 0.0033 for True, and
        # corr(imbalance, bar return) = +0.5096 with this convention and exactly -0.5096 with
        # the other. Reading the field name as "the buyer made the trade happen" gets the sign
        # backwards, and the ~50/50 global base rate means no ratio sanity-check would catch it.
        # .otherwise(zero) is load-bearing: a bar whose every taker sold is a real observed
        # zero, not a gap, and without it sum(NULL) makes it null and poisons the feature.
        func.sum(func.when(~func.col("is_buyer_maker"), func.col("qty"))
                     .otherwise(zero)).alias("taker_buy_qty"),
        func.sum(func.when(~func.col("is_buyer_maker"), func.col("quote_qty"))
                     .otherwise(zero)).alias("taker_buy_quote_qty"),
        # bool_and has no Python wrapper before Spark 3.5, but the SQL name exists in 3.3.0.
        # This is the zero-variance column surviving the granularity change intact -- see the
        # BANNED verdict in step2_normalise().
        func.expr("bool_and(is_best_match)").alias("all_best_match"),
        # Tick-grain guards, folded into this shuffle so they cost no extra pass. Under the
        # default PERMISSIVE mode an unparseable field becomes NULL with no error, so a
        # Binance format change would turn is_buyer_maker into a silently 100%-null column and
        # every taker aggregate above would quietly go wrong.
        func.sum(func.greatest(*[func.col(c).isNull().cast("int")
                                 for c in DATA_COLS]).cast("long")).alias("_nulls"),
        func.sum((func.col("qty") == 0).cast("long")).alias("_zero_qty"),
        func.sum((func.col("price") == 0).cast("long")).alias("_zero_price"),
        # Evidence for the Step 2 prune verdict, re-measured every run rather than trusted:
        # the moment a future month ships a non-best-match tick, the ledger says so.
        func.sum((~func.col("is_best_match")).cast("long")).alias("_not_best_match"),
    ))

    verdict(True, f"Step 2.5 granularity: {raw_rows:,} ticks -> OHLCV bars at {interval_label} "
                  f"(open/close by min_by/max_by on trade_id, never first/last)")
    return bars


# --------------------------------------------------------------------------------------
# STEP 3 -- MISSINGNESS INDICATOR GENERATION
# --------------------------------------------------------------------------------------

def step3_missingness(spark, agg, symbols, start_us, end_us, interval_us, interval_label):
    """Left-join a generated calendar onto the bars and flag the slots the data never filled.

    Missingness here is ROW-shaped, not cell-shaped. If a bar exists at all then at least one
    tick landed in it, so OHLCV are non-null by construction; the only thing that CAN be
    missing is a whole bar, and a missing row is invisible until a reference calendar is joined
    onto it. That is why the grid is materialised first and the flag is read off the join.
    """
    # Divisibility and boundary alignment were validated in calendar_bounds_us() before Spark
    # started, so a bad --bar-interval fails in milliseconds rather than after the 341M-row
    # Step 1 shuffle.
    n_slots = (end_us - start_us) // interval_us

    symbol_frame = spark.createDataFrame([(s,) for s in symbols], "symbol string")
    grid = (spark.range(0, n_slots)
                 .select((func.lit(start_us)
                          + func.col("id") * func.lit(interval_us)).alias("bar_us"))
                 .crossJoin(func.broadcast(symbol_frame)))

    # A dense grid rather than a sparse frame, even when it adds only a few hundred rows: the
    # downstream target is a NEXT-bar return, which needs a well-defined successor, and Step 3
    # needs an actual column to flag rather than an absence to infer.
    bars = grid.join(agg, on=["symbol", "bar_us"], how="left")

    bars = (bars
            .withColumn("is_missing_bar",
                        func.when(func.col("n_ticks").isNull(), 1).otherwise(0))
            # n_ticks is coalesced but NOTHING ELSE IS. A count over an empty bucket is an
            # exact observation -- zero trades were seen -- and it is what makes the flag
            # auditable, because a reader can recompute is_missing_bar from it instead of
            # trusting it. sum(qty) over an empty bucket is a different claim: coalesce(volume,
            # 0) would silently assert "there was trading interest and it netted zero" over
            # "we observed nothing", and it would do it in a line that does not look like
            # imputation. See the BANNED verdict below.
            .withColumn("n_ticks", func.coalesce(func.col("n_ticks"), func.lit(0)))
            # Bar timestamp is the bucket START, matching Binance's kline open_time, so bars
            # reconcile row-for-row against the public klines API and the key stays stable
            # under a change of interval. The _utc suffix and the explicit "open_time" are the
            # guard against the seam bug: an end-labelled feature table joined to a
            # start-labelled target table on a bare `ts` column hands every row the target one
            # bar early -- every key matches, no nulls, no row-count change, and the backtest
            # reads the future.
            .withColumn("bar_open_time_utc", func.expr("timestamp_micros(bar_us)")))

    missing = bars.filter(func.col("is_missing_bar") == 1).count()
    total = n_slots * len(symbols)
    verdict(missing > 0,
            f"Step 3 bars: {missing:,}/{total:,} empty at {interval_label} "
            f"({100.0 * missing / total:.3f}%)")

    # The last bar of the window has no successor and the first has no predecessor, so any
    # close-to-close return is NULL at both edges. Flagging them costs two literal comparisons
    # and no shuffle; coalescing that NULL to 0 would hand every month seam a free "the market
    # does not move at month end" prior, and concatenated months would show a discontinuity at
    # each join.
    last_bar_us = end_us - interval_us
    bars = (bars
            .withColumn("is_first_bar",
                        func.when(func.col("bar_us") == func.lit(start_us), 1).otherwise(0))
            .withColumn("is_last_bar",
                        func.when(func.col("bar_us") == func.lit(last_bar_us), 1).otherwise(0)))
    verdict(True, f"Step 3 window edges: is_first_bar / is_last_bar flagged at "
                  f"{start_us} and {last_bar_us} (epoch microseconds)")

    # Four standard OHLC gap fixes, all forbidden here, all Step 4 decisions: forward-filling
    # close into an empty bar (the standard and standardly wrong fix -- it invents a print),
    # dropping the empty rows (that is Path 1's complete-case exclusion and only Path 1's),
    # coalesce(volume, 0), and back-filling open from the next bar. Path 3's Step 4 override is
    # explicit that gaps are unobserved latent states and imputing one fabricates a signal that
    # never existed.
    verdict(False, "Step 3 fill: forward-fill / coalesce(volume, 0) / row-drop are Step 4 -- "
                   "path-isolated, decided after the fork, not here", banned=True)

    # lead() over the bar series is likewise not the entryway's to compute: run per-month it
    # bakes a permanent NULL into the last bar of every month, so the target is not
    # append-only and January's edge would need recomputing when February lands. Targets belong
    # to the feature layer, over the concatenated series.
    verdict(False, "Step 3 target: next-bar return via lead() is a post-fork feature-layer "
                   "construct -- computing it per month freezes a NULL at every seam",
            banned=True)
    return bars, total


def assert_bar_invariants(bars, raw_rows, interval_label):
    """One aggregate over the bar frame. Every failure names its cause, not its check."""
    checks = bars.agg(
        func.sum("n_ticks").alias("ticks"),
        func.coalesce(func.sum("_nulls"), func.lit(0)).alias("nulls"),
        func.coalesce(func.sum("_zero_qty"), func.lit(0)).alias("zero_qty"),
        func.coalesce(func.sum("_zero_price"), func.lit(0)).alias("zero_price"),
        func.sum((func.col("low") > func.least("open", "close")).cast("int")).alias("bad_low"),
        func.sum((func.col("high") < func.greatest("open", "close")).cast("int")).alias("bad_high"),
        func.sum((func.col("taker_buy_qty") > func.col("volume")).cast("int")).alias("bad_taker"),
        func.coalesce(func.sum("_not_best_match"), func.lit(0)).alias("not_best_match"),
    ).collect()[0]

    # The row-accounting identity, and the single best assertion in the file: it proves the
    # granularity change neither lost nor invented a tick. It is also what catches a dropped
    # file split and ticks that fell outside --month, both of which the calendar left-join
    # would otherwise swallow in silence.
    assert checks["ticks"] == raw_rows, (
        f"bars account for {checks['ticks']} ticks but {raw_rows} were read -- ticks fell "
        f"outside --month and were dropped by the calendar join, or a file split was lost")
    assert checks["nulls"] == 0, (
        f"{checks['nulls']} ticks parsed to NULL under PERMISSIVE mode -- the source column "
        f"order or the True/False casing changed")
    assert checks["bad_low"] == 0 and checks["bad_high"] == 0, (
        f"{checks['bad_low']} bars have low > min(open, close) and {checks['bad_high']} have "
        f"high < max(open, close) -- an aggregate is wired to the wrong column")
    assert checks["bad_taker"] == 0, (
        f"{checks['bad_taker']} bars have taker_buy_qty > volume -- the is_buyer_maker "
        f"predicate is inverted")

    # Disguised missingness, the notebook's lesson that a gap can hide as a legal value. A
    # zero qty or a zero price would be a gap wearing a number; a zero taker_buy_qty would not,
    # and is deliberately not counted here.
    verdict(checks["zero_qty"] + checks["zero_price"] > 0,
            f"Step 3 ticks: {checks['nulls']:,} nulls, {checks['zero_qty']:,} zero-qty, "
            f"{checks['zero_price']:,} zero-price in {raw_rows:,} ticks")

    # The measured half of the Step 2 prune verdict. It reads N/A precisely because the column
    # has no variance to prune -- which is the finding, not an absence of one.
    verdict(checks["not_best_match"] > 0,
            f"Step 2 prune evidence: is_best_match is False in "
            f"{checks['not_best_match']:,}/{raw_rows:,} ticks -- kept as all_best_match for "
            f"Step 8 to prune on the paths where Step 8 runs")


def write_bars(bars, path, interval_label):
    """Parquet, unpartitioned: the three sibling refinery jobs each read the whole frame."""
    out = bars.drop(*GUARD_COLS).select(
        "symbol", "bar_us", "bar_open_time_utc",
        "open", "high", "low", "close", "volume", "quote_volume",
        "n_ticks", "first_trade_id", "last_trade_id",
        "taker_buy_qty", "taker_buy_quote_qty", "all_best_match",
        "is_missing_bar", "is_first_bar", "is_last_bar")
    # Count BEFORE the write. The frame is cached by main(), so this is the cheap direction:
    # counting after the write would still be a second scan of the cache, and without the cache
    # it would re-run the whole 341M-row aggregation.
    rows = out.count()
    out.write.mode("overwrite").parquet(path)
    LOG.info("wrote %s rows x %s columns of %s bars -> %s",
             rows, len(out.columns), interval_label, path)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True,
                        help="prefix holding <SYMBOL>-trades-<month>.csv, unzipped "
                             "(s3:// or a local path)")
    parser.add_argument("--bars-output", required=True,
                        help="destination for the OHLCV bars Parquet")
    parser.add_argument("--month", default="2025-01",
                        help="YYYY-MM; names the input files and, by default, the calendar grid")
    parser.add_argument("--calendar-start", default=None,
                        help="ISO8601 UTC override for the grid start, e.g. 2025-01-01T00:00:00 "
                             "-- use it when the input is a partial extract such as the "
                             "committed sample, so Step 3 measures gaps rather than the extract")
    parser.add_argument("--calendar-end", default=None,
                        help="ISO8601 UTC override for the grid end (exclusive)")
    parser.add_argument("--bar-interval", default="5s", choices=sorted(BAR_INTERVALS),
                        help="bar width; 5s keeps a material flat class and a non-zero "
                             "Step 3 count, 1s is the documented missingness stress run")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS,
                        help="comma-separated symbols, one input file each")
    parser.add_argument("--local", action="store_true",
                        help="run against the local filesystem with master local[*]")
    parser.add_argument("--shuffle-partitions", type=int, default=None,
                        help="spark.sql.shuffle.partitions under --local; 8 suits the committed "
                             "sample, leave unset for the full month so Spark's default of 200 "
                             "applies (the Step 1 dedup groups 341M ticks by (symbol, trade_id))")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run, and a strict parser exits 2 on them before Spark ever starts. This
    # is the same parse_known_args awsglue's getResolvedOptions uses internally, minus the
    # awsglue import that would break the local no-AWS-account run.
    args, _ = parser.parse_known_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    interval_us = BAR_INTERVALS[args.bar_interval]
    start_us, end_us = calendar_bounds_us(args.month, args.calendar_start,
                                          args.calendar_end, interval_us)

    spark = build_session(args.local, args.shuffle_partitions)
    try:
        ticks = step1_ingest(spark, args.input, symbols, args.month)
        # Deliberately NOT cached: at 341M CSV rows the cache would spill and cost more than
        # the re-reads. The measured verdicts are the price of the framework's claim that the
        # merge is deterministic, and they are paid in scans of the source.
        raw_rows = step1_dedup_check(ticks)

        ticks = step2_normalise(ticks, raw_rows)
        agg = step2_5_aggregate(ticks, interval_us, args.bar_interval, raw_rows)
        bars, expected = step3_missingness(spark, agg, symbols, start_us, end_us,
                                           interval_us, args.bar_interval)

        # The bar frame is small (1.6M rows at 5s) and is scanned three more times -- the
        # invariant aggregate, the count, the write. Cache here and the 341M-row aggregation
        # upstream runs exactly once.
        bars = bars.cache()
        rows = bars.count()
        if rows != expected:
            raise ValueError(f"grid join changed the row count: expected {expected} "
                             f"({len(symbols)} symbols x calendar slots), got {rows}")

        assert_bar_invariants(bars, raw_rows, args.bar_interval)
        write_bars(bars, args.bars_output, args.bar_interval)

        LOG.info("entryway complete -- Steps 1-3 applied; Steps 4-10 are path-isolated and "
                 "belong to glue-refinery-path{1,2,3}.py")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
