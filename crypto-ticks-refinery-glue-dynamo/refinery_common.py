"""Shared surface for the three path jobs: the verdict vocabulary, the session, the bars reader.

Extracted at the THIRD copy, not the second. Glue uploads one file per job, so a shared module
means ``--extra-py-files`` on every job definition -- a real cost paid once per deployment. At
two copies that cost outweighed ~90 duplicated lines; at three it does not, and the grid
validation below is the kind of logic where three drifting copies eventually disagree about
what a valid input is.

Deployment: upload this alongside the job scripts and add

    --extra-py-files s3://<bucket>/jobs/refinery_common.py

to each Glue job. Locally nothing is needed -- Python puts the running script's directory on
sys.path, so ``import refinery_common`` resolves to the file next to the job.

Deliberately NOT in here: anything a path decides for itself. No feature lists, no thresholds,
no step functions. A helper that knows what Step 4 does on Path 2 is a helper that has to be
edited when Path 1 changes, which is the coupling the path-isolation rule exists to prevent.
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

LOG = logging.getLogger("refinery_common")


def verdict(applies, message, banned=False, override=False, limit=False, enforce=False,
            log=LOG):
    """Section verdict. The framework's five states, in one place.

    APPLIES  -- the check ran and there is work to do.
    N/A      -- the check ran and found nothing. Never printed on an assumption: every N/A in
                these jobs is backed by a number measured on the run that printed it.
    OVERRIDE -- the framework's default operation is REPLACED by a different one on this path.
    LIMIT    -- the operation runs, but a narrower version of it.
    ENFORCE  -- the operation runs, but its ORDERING relative to another step is mandated.
    BANNED   -- forbidden on this path, and `message` is the reason, not an apology.

    Returns False for BANNED as well as for N/A, so ``if verdict(...):`` can never gate a
    handler the framework forbids -- the contract the entryway established and the one reason
    this function has a return value at all.

    A NOTE ON THE BADGE SET, because this reverses a decision the earlier copies recorded.
    While each job carried its own copy, each carried only the badges its own path used, on the
    grounds that an unused label in a helper is one somebody eventually uses to mean something
    it does not. That reasoning was about DRIFT between copies, and a single shared definition
    is the stronger answer to drift: the framework defines exactly five states, so the helper
    defines exactly five. Which badges a given path may legitimately use is stated in that
    job's module docstring, where a reader is actually looking for it.

    `log` is a parameter so each job's lines carry that job's logger name in CloudWatch. The
    default exists for a caller that has not got one, not as the expected usage.
    """
    label = ("BANNED   -- " if banned
             else "OVERRIDE -- " if override
             else "ENFORCE  -- " if enforce
             else "LIMIT    -- " if limit
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    log.info("%s%s", label, message)
    return bool(applies) and not banned


def build_session(app_name, local, shuffle_partitions=None):
    """Local runs need a master and a shuffle width; Glue supplies both itself.

    The timezone pin is unconditional and NOT behind `local`. The session timezone defaults to
    the machine's -- Africa/Johannesburg (UTC+2) on the box this was written on -- while Glue is
    UTC. Every hour()/minute()/to_date() call therefore shifts by two between the two places
    while the underlying instant stays correct, so the job emits different HOUR buckets, different
    cyclical coordinates and different partitions depending on where it ran. Silent, and invisible
    in a diff.
    """
    builder = SparkSession.builder.appName(app_name)
    if local:
        builder = builder.master("local[*]")
        if shuffle_partitions:
            builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    # Spark's Parquet default is INT96, which is deprecated and reads back badly in Athena.
    # TIMESTAMP_MICROS matches the source resolution exactly.
    builder = builder.config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def load_bars(spark, input_path, required_cols, label):
    """Read the entryway's output and prove the grid is dense before anything trusts it.

    Returns ``(bars, symbols, interval_us, n_slots, rows)`` and RAISES on anything that would
    make a window function lie. It deliberately logs nothing: every caller narrates its own
    input line in its own vocabulary (Path 3 calls the symbols "arms"), and a helper that also
    prints is a helper whose output has to be re-checked when a job's log format changes.

    Every path leans on "the next row IS the next bar" -- Path 1 and Path 2 for a lead() target,
    Path 3 for lag() and for replay order. lead() over a SPARSE grid silently returns the next
    bar that HAPPENS to be present, which across a hole is a close from two intervals later: a
    target measured over the wrong horizon, with no null and no row-count change to give it
    away. The entryway writes a dense calendar; this checks that what arrived actually is one.
    """
    bars = spark.read.parquet(input_path)

    missing = [c for c in required_cols if c not in bars.columns]
    if missing:
        raise ValueError(f"{input_path} is not a glue-ingest-bars.py bars frame: missing "
                         f"{missing} -- {label} consumes bars, not ticks")

    symbols = sorted(r[0] for r in bars.select("symbol").distinct().collect())
    if not symbols:
        raise ValueError(f"{input_path} holds no rows -- there is nothing for {label} to read")

    slots = bars.select("bar_us").distinct()
    span = slots.agg(func.min("bar_us").alias("lo"), func.max("bar_us").alias("hi"),
                     func.count("*").alias("n")).first()
    n_slots = span["n"]
    if n_slots < 2:
        raise ValueError(f"{n_slots} distinct slot(s) -- every path needs a bar and its "
                         f"successor, so a single slot is not a series")

    total = span["hi"] - span["lo"]
    if total % (n_slots - 1):
        raise ValueError(f"slot keys do not tile evenly: {total} microseconds over "
                         f"{n_slots - 1} steps -- the bars frame is not a dense grid")
    interval_us = total // (n_slots - 1)

    # Even tiling is necessary and not sufficient: {0, 5, 10, 25} tiles at 8 and is still not a
    # grid. pmod through expr(), NOT func.pmod -- the Python wrapper is versionadded 3.4.0 and
    # Glue 4.0 is Spark 3.3.0, where only the SQL name is registered. `%` would compile there
    # but maps to Remainder, which is negative for negative operands and would wave a pre-1970
    # backfill straight through.
    off_grid = slots.filter(
        func.expr(f"pmod(bar_us - {span['lo']}, {interval_us})") != 0).count()
    if off_grid:
        raise ValueError(f"{off_grid} slot keys are off the {interval_us}us grid -- the bars "
                         f"frame has holes in its calendar, which Step 3 does not produce")

    rows = bars.count()
    if rows != n_slots * len(symbols):
        raise ValueError(f"{rows} bars is not {n_slots} slots x {len(symbols)} symbols -- the "
                         f"grid is not dense across every symbol, so a window function would "
                         f"reach past a gap and compare non-adjacent bars")

    return bars, symbols, interval_us, n_slots, rows
