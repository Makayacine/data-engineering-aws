"""Path 1 -- the continuous sub-refinery: Steps 4-10, post-fork, path-isolated.

AWS Glue 4.0 entrypoint. Reads the bars written by ``glue-ingest-bars.py`` (Steps 1-3, the
shared path-blind entryway) and applies the Path 1 column of the framework's Steps 4-10 matrix:

      bars Parquet   (one row per (symbol, bar), dense over the declared calendar)
  ->  Step 4   Imputation      OVERRIDE at >5% -- else median impute X, complete-case y
  ->  Step 5   Diagnostics     APPLIES  -- univariate density profile, READ-ONLY
  ->  Step 6   Topology        APPLIES  -- global cross-correlation matrix, READ-ONLY
  ->  Step 7   Feature Eng     APPLIES  -- deterministic cross-products, nothing learned
  ->  Step 8   Pruning         APPLIES  -- VarianceThreshold; categorical strings deleted
  ->  Step 9   Regularisation  APPLIES  -- cross-validated Elastic Net on time-blocked folds
  ->  Step 10  Scaling         LIMIT    -- linear scalers only, so coefficients stay in bp
  ->  features/ + topology/ + coefficients/ Parquet

THREE THINGS STATED UP FRONT, BECAUSE EACH IS EASY TO MISREAD AS A RESULT
------------------------------------------------------------------------

1.  **The target is manufactured, and the framework offers no guidance on manufacturing one.**
    NexusMart arrived with three genuine experiments and three genuine targets; a bar file
    arrives with none. The nominated target is the next-bar log return,
    ``log(lead(close) / close)`` over a window partitioned by symbol and ordered by ``bar_us``.
    It is a real-valued continuous double, so the fork routes to Path 1 -- but every verdict
    below is conditional on that nomination, which is this project's decision and not the
    framework's.

2.  **The cross-validation is time-blocked, not a true expanding window.** Spark's
    ``CrossValidator`` uses RANDOM folds by default; on ordered bars that trains on the future
    and tests on the past, and adjacent bars are near-duplicates, so a random split puts a row
    and its own neighbours on both sides of the boundary. ``foldCol`` with contiguous
    ``bar_us`` blocks is used instead. That is an improvement, not a fix: block 0 is still
    validated against a model trained on blocks 1-4, which are later. A real expanding window
    needs a manual loop and is outside both the framework and this job.

3.  **Step 3's missingness flag cannot survive Path 1's Step 4, and the framework says it
    should.** The flag is supposed to become a predictive feature here. But ``is_missing_bar``
    is 1 exactly when the bar has a NULL close, and a NULL close makes the target NULL, so
    every row the flag marks is evacuated by the complete-case cut -- leaving the column a flat
    zero that Step 8 then deletes for having no variance. This is structural, not a property of
    one month. Path 1's Step 4 and Path 1's Step 3 flag cannot both stand, and the job reports
    the collision rather than quietly dropping the column.

Money stays ``decimal(18,8)`` until the one cast boundary at the entrance to ``pyspark.ml``,
which is Double/Float only. ``log()`` has no decimal form in Spark, so the three log columns
cross earlier -- each having computed its argument in decimal first.

Local acceptance run. Path 1 consumes bars, not ticks, so the entryway runs first -- against
the committed two-hour sample, with the calendar overrides that keep Step 3 meaningful::

    python glue-ingest-bars.py --local \\
        --input data/sample \\
        --bars-output _localrun/bars \\
        --bar-interval 5s \\
        --calendar-start 2025-01-01T00:00:00 \\
        --calendar-end   2025-01-01T02:00:00

    python glue-refinery-path1.py --local \\
        --input _localrun/bars \\
        --output _localrun/path1
"""

import argparse
import logging
import math

from pyspark.sql import Window
from pyspark.sql import functions as func
from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                               StructType)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (Imputer, StandardScaler, VarianceThresholdSelector,
                                VectorAssembler)
# A Spark ML vector is a VectorUDT struct, NOT an array -- getItem/[i] raise
# INVALID_EXTRACT_BASE_FIELD_TYPE on it.
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import LinearRegression
from pyspark.ml.stat import Correlation
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Shared with the sibling path jobs. On Glue this needs
#   --extra-py-files s3://<bucket>/jobs/refinery_common.py
# Locally nothing is needed: Python puts the running script's directory on sys.path.
from refinery_common import build_session, load_bars, verdict as _verdict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_refinery_path1")

APP_NAME = "CryptoTicksRefineryPath1"

# Hard-coded, named, and tested STRICTLY. Source wording: ">5% missingness" (the step grid) and
# "exceeds the 5% firewall threshold" (the LaTeX). Both are strict, so 5.0% exactly lands on the
# default branch and the override does not fire. That boundary is never demonstrated by the
# framework, which is exactly why it is a named constant with a self-check rather than an
# inline `> 0.05` somebody later reads as `>=`.
MISSINGNESS_BAN_THRESHOLD = 0.05

# Exactly 0.0: removes features whose sample variance is <= 0, i.e. the genuine constants and
# nothing else. On NexusMart's toy matrix every column is O(1)-O(400) and any threshold behaves.
# Here quote_volume is O(1e5) and the target is O(1e-4), so ANY positive raw-variance threshold
# deletes every return column before it touches anything uninformative. The framework forbids
# fixing that by scaling first -- Step 10 runs AFTER Step 8 and the pass does not loop -- so
# 0.0 keeps the step to what the source actually demonstrates.
VARIANCE_THRESHOLD = 0.0

FOLDS = 5
REG_PARAMS = [1e-8, 1e-6, 1e-4]
ELASTIC_NET_PARAMS = [0.25, 0.5, 1.0]        # 1.0 is pure L1, 0.0 would be pure ridge

# The feature matrix, before Step 7's cross-products. first_trade_id / last_trade_id / bar_us
# are EXCLUDED rather than pruned: an identifier is not a low-variance column, it is a column
# whose variance is meaningless. `symbol` is a categorical string and Path 1 deletes those
# outright -- see step8_pruning().
BASE_X = ["open", "high", "low", "close",
          "volume", "quote_volume", "n_ticks",
          "taker_buy_qty", "taker_buy_quote_qty",
          "all_best_match", "is_missing_bar",
          "ret1", "range_bp", "body_bp", "imbalance", "log_qv"]

CROSS_X = ["x_imb_ret", "x_range_qv", "x_imb_range", "x_ret_ticks"]

REQUIRED_COLS = ["symbol", "bar_us", "bar_open_time_utc",
                 "open", "high", "low", "close", "volume", "quote_volume",
                 "n_ticks", "taker_buy_qty", "taker_buy_quote_qty", "all_best_match",
                 "is_missing_bar", "is_last_bar"]


def verdict(applies, message, banned=False, override=False, limit=False):
    """Path 1's badge set, bound to this job's logger.

    Path 1 uses APPLIES / N/A / OVERRIDE (Step 4's firewall) / LIMIT (Step 10) / BANNED.
    ENFORCE is a Path 2 badge and is not legitimate here, so it is not exposed even though
    refinery_common.verdict() implements all five.
    """
    return _verdict(applies, message, banned=banned, override=override, limit=limit, log=LOG)


def firewall_fires(rate):
    """Does Step 4's OVERRIDE fire? STRICTLY greater, so 5.0% exactly keeps the default.

    Split out as a named function for one reason: it is a one-character decision the framework
    states twice and demonstrates never, and `>` silently becoming `>=` would swap the entire
    Step 4 branch without changing a row count or raising anything. --self-check pins it.
    """
    return rate > MISSINGNESS_BAN_THRESHOLD


def bp_per_sd(coefficient, sigma):
    """A +1 sd move in a feature, in basis points of next-bar log return.

    THIS is what Step 10's LIMIT exists to protect. StandardScaler is affine, so sigma survives
    as a real number and a coefficient multiplies straight back into the unit a reviewer reads.
    A quantile or power transform turns the feature into a percentile rank and this arithmetic
    stops meaning anything measurable.
    """
    return 1e4 * coefficient * sigma


def fold_width(lo, hi, folds):
    """Width of one contiguous time block, in microseconds.

    `hi - lo + 1` and not `hi - lo`: the span is inclusive of both endpoints, and without the
    +1 the LAST bar lands in fold `folds` rather than `folds - 1`. The least() clamp in
    fold_column() covers that, but a clamp that fires on every run is a bug wearing a guard's
    uniform -- the last block would be one row and the first four would carry the rest.
    """
    return (hi - lo + 1) / folds


def fold_column(lo, width, folds):
    """Contiguous time-block fold index, keyed on bar_us.

    Keyed on the TIMESTAMP, not ntile over rows: it keeps all symbols of a single instant in
    the same fold. Three correlated rows split across a fold boundary is the same leak as a
    random fold, just smaller.
    """
    return func.least(
        func.lit(folds - 1),
        func.floor((func.col("bar_us") - func.lit(lo)) / func.lit(width))).cast("int")


# --------------------------------------------------------------------------------------
# THE FORK, AND THE POST-FORK WINDOWED PASS
# --------------------------------------------------------------------------------------

def fork_and_derive(bars, interval_us):
    """Read y's geometry, then build the target and the base features in ONE windowed pass.

    lead() for the target, lag() for the one backward-looking feature, and the per-bar
    scale-free ratios all in one pass, so that Step 4 -- which runs next and never runs again --
    can see every null the construction creates. Doing the lag in Step 7 instead would
    manufacture nulls AFTER the only step allowed to handle them.
    """
    # The fork reads y's TYPE, not y's content. The framework is explicit that the path is
    # chosen before any imputation, never after -- missingness is Step 4's problem, and Step 4
    # is downstream of this line.
    verdict(True, f"Fork: target geometry is a real-valued continuous double (next-bar log "
                  f"return over a {interval_us:,}us grid) -> Path 1. Steps 4-10 below are "
                  f"Path 1's and no other path's")
    verdict(False, "Fork provenance: the framework gives no rule for MANUFACTURING a target -- "
                   "nominating the next-bar log return is this project's decision, and every "
                   "Path 1 verdict below is conditional on it")

    window = Window.partitionBy("symbol").orderBy("bar_us")
    return (bars
            .withColumn("_close_next", func.lead("close").over(window))
            .withColumn("_close_prev", func.lag("close").over(window))
            # THE TARGET. Decimal arithmetic all the way to the log: close is decimal(18,8) and
            # decimal/decimal division is exact and partition-order independent, so the gross
            # return is byte-identical under any shuffle width. Casting close to double FIRST
            # and dividing there is the version that gives three different answers at 3 / 11 /
            # 29 partitions. log() has no decimal implementation, so the cast lands on the
            # finished ratio and nowhere earlier.
            #
            # Empty bars carry a NULL close (Step 3 flagged and deliberately did not fill), so
            # both the empty bar AND the bar before it get a null target -- the null propagates
            # BACKWARDS through lead() as well as forwards. That is why Step 4's rate below is
            # not simply the empty-bar rate.
            .withColumn("y", func.log(
                (func.col("_close_next") / func.col("close")).cast("double")))
            .withColumn("ret1", func.log(
                (func.col("close") / func.col("_close_prev")).cast("double")))
            # Scale-free per-bar features. BTC prints near $94k and SOL near $190 in this
            # window, so a raw price or a raw dollar range is not comparable across three
            # symbols in one matrix; a basis-point ratio is. Kept in decimal -- the *10000 is
            # exact.
            .withColumn("range_bp",
                        (func.col("high") - func.col("low")) / func.col("close")
                        * func.lit(10000))
            .withColumn("body_bp",
                        (func.col("close") - func.col("open")) / func.col("close")
                        * func.lit(10000))
            # taker_buy = (is_buyer_maker == False): is_buyer_maker True means the BUYER was
            # resting on the book, so the trade is an aggressive SELL. The entryway measured
            # corr(imbalance, contemporaneous return) = +0.5096 with this convention and
            # exactly -0.5096 inverted, and the ~50/50 base rate means no ratio sanity-check
            # would catch the flip. Re-measured in step6_topology() on this frame.
            .withColumn("imbalance",
                        func.col("taker_buy_quote_qty") / func.col("quote_volume"))
            .withColumn("log_qv", func.log(func.col("quote_volume").cast("double")))
            .drop("_close_next", "_close_prev"))


# --------------------------------------------------------------------------------------
# STEP 4 -- IMPUTATION  (OVERRIDE above the 5% firewall, default below it)
# --------------------------------------------------------------------------------------

def step4_imputation(frame, rows):
    """Measure target missingness, then take the branch the measurement selects.

    Both branches are implemented. The framework STATES the sub-5% branch and demonstrates only
    the override, and on a quiet extract the measured rate lands under the firewall -- so a job
    that implemented only the demonstrated branch would do nothing at all on its own data.
    """
    # The framework evaluates the rate as a mean over the flag Step 3 manufactured
    # (mean(is_missing_order_total) > 0.05). That does not transfer unchanged: Step 3's flag
    # describes ONE BAR and the target is a TWO-BAR construct, so the flag under-counts the
    # target's nulls. Both are printed and the TARGET's own rate is the one the firewall reads,
    # because the LaTeX says "target missingness" -- the test is on y, not per-column across X.
    stats = frame.select(
        func.avg(func.col("y").isNull().cast("double")).alias("y_null_rate"),
        func.avg(func.col("is_missing_bar").cast("double")).alias("flag_rate"),
        func.sum(func.col("y").isNull().cast("int")).alias("y_nulls"),
        func.sum((func.col("y").isNull() & (func.col("is_last_bar") == 1))
                 .cast("int")).alias("tail"),
        func.sum((func.col("y").isNull() & (func.col("is_missing_bar") == 1))
                 .cast("int")).alias("own"),
    ).first()

    y_nulls, tail, own = stats["y_nulls"], stats["tail"], stats["own"]
    rate = stats["y_null_rate"]
    LOG.info("   rows                               %s", f"{rows:,}")
    LOG.info("   target nulls                       %s  (%.3f%%)", f"{y_nulls:,}", 100 * rate)
    LOG.info("     of which last bar of a symbol    %s  (structural: no successor)", f"{tail:,}")
    LOG.info("     of which the bar itself is empty %s  (NULL close, Step 3 flagged)",
             f"{own:,}")
    LOG.info("     of which the NEXT bar is empty   %s  (lead() pulls the null back)",
             f"{y_nulls - tail - own:,}")
    LOG.info("   Step 3 is_missing_bar rate         %.3f%%  (under-counts the target: "
             "one-bar flag, two-bar target)", 100 * stats["flag_rate"])
    # The structural tail is a mechanical artefact of the window, not a data defect, and the
    # framework never says whether it belongs inside or outside the rate. Both are reported;
    # the firewall reads the raw rate because that is the quantity the framework names.
    LOG.info("   rate excluding the structural tail %.3f%%", 100 * (y_nulls - tail) / rows)

    fires = firewall_fires(rate)
    branch = ("FIRES, so medians are banned and the affected rows are passed raw" if fires
              else "does NOT fire, so the framework default (median impute) is the branch "
                   "that stands")
    verdict(fires,
            f"Step 4 firewall: target missingness {100 * rate:.3f}% vs the strict "
            f"{100 * MISSINGNESS_BAN_THRESHOLD:.0f}% threshold -- the OVERRIDE {branch}",
            override=fires)

    # THE CAST BOUNDARY. Everything above is decimal(18,8) and exact; pyspark.ml is Double/Float
    # only -- Imputer, VectorAssembler, VarianceThresholdSelector and LinearRegression all
    # reject DecimalType or silently widen it. The cast happens HERE, once, at the entrance to
    # the ml package, and never inside an aggregation. ret1, y and log_qv crossed earlier
    # because log() has no decimal form, and each computed its argument in decimal first.
    typed = frame.select("symbol", "bar_us", "bar_open_time_utc", "is_last_bar",
                         *[func.col(c).cast("double").alias(c) for c in BASE_X],
                         func.col("y").cast("double").alias("y"))

    # Complete-case on the TARGET, on both branches. na.drop(subset=["y"]) is the framework's
    # operation: the row leaves the matrix entirely rather than being blanked or flagged.
    #
    # It is NOT median-filled even on the permissive branch, and that is a deviation the
    # framework does not authorise. Its own reasoning for complete-case is distributional
    # ("filling these 66 rows with a median would artificially compress the distribution's
    # tails"); the reason here is stronger and different in kind. A null target on the last bar
    # of a symbol is not an unobserved value -- there is no successor bar in the universe to
    # observe. Filling it with a median return fabricates a future.
    dropped = frame.filter(func.col("y").isNull() & (func.col("is_last_bar") == 1))
    for row in dropped.groupBy("symbol").agg(func.count("*").alias("n"),
                                             func.max("bar_us").alias("at")) \
                      .orderBy("symbol").collect():
        LOG.info("   last bar of %s: %s row, NULL target at bar_us %s -- dropped, not filled",
                 row["symbol"], row["n"], row["at"])

    complete = typed.na.drop(subset=["y"])
    n_complete = complete.count()
    LOG.info("   complete-case on the target: %s -> %s rows (%s evacuated)",
             f"{rows:,}", f"{n_complete:,}", rows - n_complete)

    x_nulls = complete.select([func.sum(func.col(c).isNull().cast("int")).alias(c)
                               for c in BASE_X]).first().asDict()
    dirty = {c: n for c, n in x_nulls.items() if n}
    LOG.info("   feature-column nulls surviving the complete-case cut: %s", dirty or "none")

    if fires:
        # THE OVERRIDE BRANCH. Medians are banned outright; the residual X nulls are evacuated
        # as complete cases too. The framework's wording is "ban medians, pass raw for
        # complete-case analysis", so the transformer is never constructed -- not constructed
        # and skipped, but absent from the branch entirely.
        verdict(False, f"Step 4 median impute: BANNED above the firewall -- filling "
                       f"{sum(dirty.values())} cells at {100 * rate:.3f}% missingness would "
                       f"compress the distribution's tails, so the affected rows are passed "
                       f"raw for complete-case analysis instead", banned=True)
        step4 = complete.na.drop(subset=BASE_X)
        n_final = step4.count()
        verdict(True, f"Step 4 Path 1 OVERRIDE: complete-case on y and on X "
                      f"({rows:,} -> {n_final:,} rows); no median was computed",
                override=True)
        medians = {}
    else:
        # THE DEFAULT BRANCH -- the one the framework states but never demonstrates. The
        # transformer the OVERRIDE would have banned runs here, and it is FITTED rather than
        # merely named, so the medians it writes are real numbers this run measured.
        #
        # The residual X nulls are ret1 on the FIRST bar of each symbol (no predecessor) and on
        # every bar whose predecessor was empty.
        imputer = Imputer(strategy="median", inputCols=BASE_X, outputCols=BASE_X)
        model = imputer.fit(complete)
        step4 = model.transform(complete)
        n_final = n_complete
        medians = dict(zip(BASE_X, model.surrogateDF.first()))
        for col, n in dirty.items():
            # Scientific notation, because a log return's median is small enough that fixed
            # notation would hide whether it is a small number or an exact zero. On a quiet
            # window it prints as exactly +0.000000e+00 and that is not a rounding artefact:
            # the modal 5-second bar closes where it opened, so the median one-bar return IS
            # zero. Worth saying out loud -- a zero written into a return column is
            # indistinguishable from an observed flat bar, so this fill is unrecoverable after
            # the fact.
            LOG.info("      %-14s %3d cells <- median %+.6e", col, n, medians[col])
        verdict(True, f"Step 4 Path 1: complete-case on y ({rows:,} -> {n_final:,} rows), "
                      f"median impute on X ({sum(dirty.values())} cells) -- the sub-firewall "
                      f"branch the framework states but never demonstrates")

    # The framework's flag-then-impute pairing does not survive a lagged feature, and this is
    # the first place it shows. Step 3's flag marks the EMPTY bar; the cells the median fill
    # touches are that bar's SUCCESSORS, which carry no flag of their own.
    verdict(False, "Step 4 flag pairing: Step 3's is_missing_bar marks the EMPTY bar, but the "
                   "rows a fill touches here are that bar's SUCCESSORS -- the framework's "
                   "flag-then-impute pairing does not cover a lagged feature", banned=True)
    return step4.cache(), n_final, medians


# --------------------------------------------------------------------------------------
# STEP 5 -- DIAGNOSTICS  (read-only)
# --------------------------------------------------------------------------------------

def step5_diagnostics(frame):
    """Univariate density profile of the target. Emits a log line and changes nothing.

    The LaTeX is explicit: "What changed: Nothing inside the table." The row and column counts
    are asserted identical on the way out, because "read-only" is a claim and an assert is the
    only version of it that survives an edit.
    """
    before = (frame.count(), len(frame.columns))

    shape = frame.select(
        func.count("y").alias("n"), func.avg("y").alias("mean"), func.stddev("y").alias("sd"),
        func.skewness("y").alias("skew"), func.kurtosis("y").alias("kurt"),
        func.min("y").alias("min"), func.max("y").alias("max"),
    ).first()

    LOG.info("   target: next-bar log return, all symbols pooled")
    LOG.info("      n %s | mean %+.4f bp | sd %.4f bp", f"{shape['n']:,}",
             1e4 * shape["mean"], 1e4 * shape["sd"])
    LOG.info("      skewness %+.4f | excess kurtosis %+.4f", shape["skew"], shape["kurt"])
    LOG.info("      range [%+.2f, %+.2f] bp", 1e4 * shape["min"], 1e4 * shape["max"])

    # relativeError 0.0 is an exact quantile, i.e. a full sort. It is affordable because this
    # frame is bars, not ticks -- 1.6M rows at 5s over a month, not 341M.
    probs = [0.01, 0.25, 0.5, 0.75, 0.99]
    LOG.info("      percentiles (bp)  %s", "  ".join(
        f"p{int(p * 100):02d} {1e4 * v:+.3f}"
        for p, v in zip(probs, frame.approxQuantile("y", probs, 0.0))))

    # Per-symbol, because a pooled skewness over three price scales is an average of three
    # different distributions. The flat share is the mass at exactly zero.
    for row in (frame.groupBy("symbol").agg(
            func.count("*").alias("n"),
            (func.avg((func.col("y") == 0).cast("double")) * 100).alias("flat_pct"),
            (func.stddev("y") * 1e4).alias("sd_bp"),
            func.skewness("y").alias("skew"),
            func.kurtosis("y").alias("kurt")).orderBy("symbol").collect()):
        LOG.info("      %-9s n %s | flat %.2f%% | sd %.4f bp | skew %+.4f | kurt %+.4f",
                 row["symbol"], f"{row['n']:,}", row["flat_pct"], row["sd_bp"],
                 row["skew"], row["kurt"])

    # The operator-audit line the framework asks for: NAME the extreme observation rather than
    # smoothing it away. Its NexusMart equivalent is "High-value outlier detected at
    # session_884."
    extreme = frame.orderBy(func.abs(func.col("y")).desc()).first()
    LOG.info("   Target Skewness Flagged: extreme move %+.2f bp on %s at bar_us %s (%.1f sd)",
             1e4 * extreme["y"], extreme["symbol"], extreme["bar_us"],
             abs(extreme["y"] / shape["sd"]) if shape["sd"] else float("nan"))

    verdict(True, "Step 5 diagnostics: read-only univariate density profile emitted for the "
                  "target; the flat share above is a property of THIS window, not of the month")
    verdict(False, "Step 5 t-SNE sandbox: no t-SNE in pyspark.ml at all, and the framework "
                   "hard-quarantines it to an async read-only operator plot -- it never feeds "
                   "K-Means, Hierarchical or DBSCAN, so nothing downstream may consume it",
            banned=True)

    after = (frame.count(), len(frame.columns))
    if after != before:
        raise ValueError(f"Step 5 is read-only but the frame changed: {before} -> {after}")
    LOG.info("   read-only confirmed: %s rows x %s columns, unchanged", f"{before[0]:,}",
             before[1])
    return shape


# --------------------------------------------------------------------------------------
# STEP 6 -- TOPOLOGY  (read-only)
# --------------------------------------------------------------------------------------

def step6_topology(frame):
    """Global cross-correlation matrix, exported as a dependency schema. Changes nothing.

    "No value changes. The system exports an internal mathematical dependency schema to guide
    downstream feature regularizers." The schema is EXPORTED, not returned into the pipeline --
    the forward-pass rule forbids Step 8 or Step 9 from reading it back, so this function hands
    back rows to persist and nothing that main() feeds into a later step.
    """
    # y rides along as the last element, so one pass gives both the X-X dependency structure
    # and every feature's marginal relationship with the target.
    cols = BASE_X + ["y"]
    assembled = VectorAssembler(inputCols=cols, outputCol="_corr_vec",
                                handleInvalid="error").transform(frame)
    matrix = Correlation.corr(assembled, "_corr_vec", "pearson").collect()[0][0].toArray()

    # Half one: the collinear cluster. A raw OHLC block on a seconds grid is four measurements
    # of the same price seconds apart, so near-perfect correlation is expected -- and it is
    # precisely what an L1 penalty resolves ARBITRARILY, keeping whichever of four near-
    # identical columns the solver reaches first. Step 9 is told about it here and cannot read
    # it back.
    LOG.info("   |r| >= 0.90 among X (the collinear cluster Step 9 will have to break):")
    clusters = 0
    for i in range(len(BASE_X)):
        for j in range(i + 1, len(BASE_X)):
            r = matrix[i][j]
            if not math.isnan(r) and abs(r) >= 0.90:
                clusters += 1
                LOG.info("      %-20s %-20s %+.4f", BASE_X[i], BASE_X[j], r)

    # Half two: marginal correlation with the target. NaN appears for a constant column -- a
    # zero-variance column has an undefined correlation, which is Step 8's finding arriving one
    # step early and by accident rather than by design.
    LOG.info("   marginal corr with the next-bar target:")
    ranked = sorted(((BASE_X[i], matrix[i][-1]) for i in range(len(BASE_X))),
                    key=lambda kv: -abs(kv[1]) if not math.isnan(kv[1]) else 1)
    for name, r in ranked:
        LOG.info("      %-20s %s", name,
                 "nan (zero variance)" if math.isnan(r) else f"{r:+.4f}")

    # The imbalance sign check, re-measured on this frame. The entryway verified
    # corr(imbalance, CONTEMPORANEOUS bar return) = +0.5096 on 2M ticks. Against the NEXT bar
    # it is near zero, and that is the correct, boring answer -- order-flow imbalance is a
    # same-bar identity, not a forecast. A large positive number on the next-bar line would
    # mean the target is leaking.
    same_bar = frame.select(func.corr("imbalance", "body_bp")).first()[0]
    next_bar = matrix[BASE_X.index("imbalance")][-1]
    LOG.info("   corr(imbalance, SAME-bar body_bp)  %+.4f   (entryway: +0.5096 on 2M ticks)",
             same_bar)
    LOG.info("   corr(imbalance, NEXT-bar target)   %+.4f   (near zero is the honest answer)",
             next_bar)
    if same_bar is not None and same_bar < 0:
        raise ValueError(
            f"corr(imbalance, same-bar body) is {same_bar:+.4f}, i.e. NEGATIVE -- the "
            f"is_buyer_maker polarity is inverted somewhere upstream. is_buyer_maker True "
            f"means the buyer was the MAKER, so the trade is an aggressive SELL")

    verdict(True, f"Step 6 topology: {len(cols)}x{len(cols)} Pearson matrix exported as a "
                  f"dependency schema, {clusters} pairs at |r| >= 0.90; no value changed")
    verdict(False, "Step 6 feedback: the forward pass is non-looping, so Step 8 may not consult "
                   "this matrix and Step 9 may not read it back -- it is exported for an "
                   "operator, not returned into the pipeline", banned=True)

    return [(cols[i], cols[j], None if math.isnan(matrix[i][j]) else float(matrix[i][j]))
            for i in range(len(cols)) for j in range(len(cols))]


# --------------------------------------------------------------------------------------
# STEP 7 -- FEATURE ENGINEERING  (deterministic cross-products)
# --------------------------------------------------------------------------------------

def step7_cross_products(frame):
    """Four named products of columns that already exist. Nothing learned, nothing sampled.

    The word DETERMINISTIC is load-bearing: no learned, sampled or stochastic construction is
    permitted on Path 1, because Path 1's output has to stay auditable in real units.
    pyspark.ml.feature.Interaction and PolynomialExpansion would produce the same numbers;
    explicit withColumn is used so each column has a name a reviewer can read, which is the
    whole point of the constraint.
    """
    out = (frame
           # order-flow pressure conditional on direction
           .withColumn("x_imb_ret", func.col("imbalance") * func.col("ret1"))
           # volatility conditional on participation
           .withColumn("x_range_qv", func.col("range_bp") * func.col("log_qv"))
           # pressure conditional on volatility
           .withColumn("x_imb_range", func.col("imbalance") * func.col("range_bp"))
           # momentum conditional on trade count
           .withColumn("x_ret_ticks", func.col("ret1") * func.col("n_ticks")))

    # summary() returns every statistic as a STRING, not a double -- it is built for .show().
    # float() here rather than a %s format, so the log stays in scientific notation and a NULL
    # (which summary renders as None) is visible as such instead of crashing the formatter.
    for row in out.select(*CROSS_X).summary("mean", "stddev", "min", "max").collect():
        LOG.info("   %-8s %s", row["summary"], "  ".join(
            f"{c}={'None' if row[c] is None else format(float(row[c]), '+.6g')}"
            for c in CROSS_X))

    verdict(True, f"Step 7 Path 1: column dimensions expand {len(BASE_X)} -> "
                  f"{len(BASE_X) + len(CROSS_X)}; {len(CROSS_X)} deterministic products "
                  f"appended, nothing learned or sampled")
    verdict(False, "Step 7 ceiling: interactions deeper than a manual pairwise product are a "
                   "MODELLING escalation (the framework's ANN clause), not more Step 7 -- a "
                   "learned interaction here would break the deterministic constraint",
            banned=True)
    return out


# --------------------------------------------------------------------------------------
# STEP 8 -- PRUNING  (VarianceThreshold; categorical strings deleted)
# --------------------------------------------------------------------------------------

def step8_pruning(frame, all_x):
    """Two operations: the variance filter over the numerics, and exclusion for the strings.

    The sources disagree on the second half -- the HTML says "bypass categorical dummy
    strings", the LaTeX says "marketing text string variables are entirely deleted" and its
    matrix trace shows the column gone after Step 8. DELETE is taken as the reading, so
    `symbol` never enters the matrix and is never one-hot encoded.
    """
    assembled = VectorAssembler(inputCols=all_x, outputCol="features",
                                handleInvalid="error").transform(frame)
    model = VarianceThresholdSelector(featuresCol="features", outputCol="selected",
                                      varianceThreshold=VARIANCE_THRESHOLD).fit(assembled)
    step8 = model.transform(assembled)

    kept = [all_x[i] for i in model.selectedFeatures]
    dropped = [c for c in all_x if c not in kept]
    LOG.info("   kept    %s/%s", len(kept), len(all_x))
    LOG.info("   dropped %s", dropped or "nothing")

    # Why each one went, measured rather than asserted.
    for col in dropped:
        stats = frame.select(func.var_samp(col).alias("v"),
                             func.countDistinct(col).alias("d"),
                             func.first(col).alias("f")).first()
        LOG.info("      %-16s var %.6g  distinct %s  value %s", col, stats["v"], stats["d"],
                 stats["f"])

    verdict(True, f"Step 8 Path 1: VarianceThreshold({VARIANCE_THRESHOLD}) removed "
                  f"{len(dropped)} zero-variance columns {dropped}; `symbol` deleted as a "
                  f"categorical string, never encoded")

    # all_best_match is the framework's constant_metric, verbatim: is_best_match was True in all
    # 340,971,834 ticks of the month, so bool_and is True in every bar. The entryway carried it
    # through ON PURPOSE, because pruning is Step 8 and Step 8 is path-isolated -- it is BANNED
    # on Path 3, so the shared entryway had no right to make this decision on Path 3's behalf.
    # This is the path where the decision is Step 8's to make, and it makes it.
    if "all_best_match" in dropped:
        verdict(True, "Step 8 constant_metric: all_best_match dropped here, which is the "
                      "decision the entryway deliberately deferred -- Step 8 is path-isolated "
                      "and BANNED on Path 3, so only this path may take it")

    # The internal inconsistency, stated rather than buried. Step 3's flag is supposed to
    # survive Step 4 and become a predictive feature on Path 1. But is_missing_bar is 1 exactly
    # when close is NULL, a NULL close makes y NULL, and every NULL-y row was evacuated by the
    # complete-case cut -- so the column is a flat 0 by the time Step 8 measures it, and Step 8
    # deletes it. That is structural, not a property of one month. The LaTeX trace quietly
    # loses the same column between Step 7 and Step 8 without comment.
    verdict("is_missing_bar" in dropped,
            "Step 8 flag collision: is_missing_bar is 1 exactly when close is NULL, which makes "
            "y NULL, which Step 4 evacuates -- so the flag the framework says should become a "
            "Path 1 predictor is structurally constant by the time Step 8 measures it. "
            "Path 1's Step 4 and Path 1's Step 3 flag cannot both stand")

    verdict(False, f"Step 8 scale ordering: a positive raw-variance threshold would delete "
                   f"every return column (O(1e-4)) before touching quote_volume (O(1e5)); "
                   f"Step 10 fixes the scales and runs AFTER, and the pass does not loop -- "
                   f"hence {VARIANCE_THRESHOLD}, which removes only genuine constants",
            banned=True)
    return step8, kept


# --------------------------------------------------------------------------------------
# STEP 9 -- REGULARISATION  (cross-validated Elastic Net, time-blocked folds)
# --------------------------------------------------------------------------------------

def step9_elastic_net(frame, kept, target_sd, seed):
    """Supervised selection. Step 8 was target-blind and only ever looked at variance.

    Columns whose coefficient the L1 penalty drives to exactly zero are the ones that VARY but
    do not PREDICT -- which is a different question from the one Step 8 asked, and the reason
    both steps exist.
    """
    span = frame.select(func.min("bar_us").alias("lo"),
                        func.max("bar_us").alias("hi")).first()
    width = fold_width(span["lo"], span["hi"], FOLDS)
    cv_in = frame.withColumn("fold", fold_column(span["lo"], width, FOLDS)).cache()

    blocks = (cv_in.groupBy("fold").agg(func.count("*").alias("rows"),
                                        func.min("bar_us").alias("from_us"))
              .orderBy("fold").collect())
    for row in blocks:
        LOG.info("      fold %s: %s rows from bar_us %s", row["fold"], f"{row['rows']:,}",
                 row["from_us"])
    # numFolds is NOT implied by foldCol -- it defaults to 3, and the fit dies with "Fold
    # number must be in range [0, 3), but got 3" only AFTER the folds have been materialised.
    # An empty or missing block would fail the same way, so it is caught here instead.
    if len(blocks) != FOLDS or {r["fold"] for r in blocks} != set(range(FOLDS)):
        raise ValueError(f"fold assignment produced {sorted(r['fold'] for r in blocks)}, "
                         f"expected 0..{FOLDS - 1} -- CrossValidator would fail after the "
                         f"folds were already materialised")

    # standardization=True is Spark's DEFAULT and it is set EXPLICITLY because it silently
    # neutralises the Step 8 / Step 10 ordering complaint above: Spark standardises internally
    # for the optimiser so the L1 penalty is applied on a common scale, then reports the
    # coefficients back in raw units. The framework's forward pass would have left the penalty
    # scale-dependent; the implementation quietly does not. Stating it beats inheriting it.
    estimator = LinearRegression(featuresCol="selected", labelCol="y",
                                 standardization=True, maxIter=100)
    grid = (ParamGridBuilder()
            .addGrid(estimator.regParam, REG_PARAMS)
            .addGrid(estimator.elasticNetParam, ELASTIC_NET_PARAMS)
            .build())
    cv = CrossValidator(estimator=estimator, estimatorParamMaps=grid,
                        evaluator=RegressionEvaluator(labelCol="y", metricName="rmse"),
                        numFolds=FOLDS, foldCol="fold", parallelism=1, seed=seed)
    cv_model = cv.fit(cv_in)
    best = cv_model.bestModel
    cv_rmse = float(min(cv_model.avgMetrics))

    coefficients = dict(zip(kept, (float(c) for c in best.coefficients)))
    live = sorted(((n, c) for n, c in coefficients.items() if c != 0.0),
                  key=lambda kv: -abs(kv[1]))
    zeroed = [n for n in kept if coefficients[n] == 0.0]

    LOG.info("   selected  regParam %g | elasticNetParam %s (%s)", best.getRegParam(),
             best.getElasticNetParam(),
             "pure L1" if best.getElasticNetParam() == 1.0 else "L1/L2 mix")
    # min() over avgMetrics, not best.summary.rootMeanSquaredError: the latter is the TRAINING
    # error of the refit best model on the whole frame, which is optimistic by construction and
    # is not the number the tuning actually selected on.
    LOG.info("   CV RMSE   %.8f  (target sd %.8f) over %s time-blocked folds",
             cv_rmse, target_sd, FOLDS)
    LOG.info("   non-zero coefficients: %s/%s", len(live), len(kept))
    LOG.info("   shrunk to exactly 0.0: %s", zeroed or "none")
    LOG.info("   top features by |coefficient| (raw units, y in log-return):")
    for name, c in live[:6]:
        LOG.info("      %-20s %+.6e", name, c)

    verdict(len(zeroed) > 0,
            f"Step 9 Path 1: CV Elastic Net kept {len(live)}/{len(kept)} columns; "
            f"{len(zeroed)} shrunk to exactly zero")
    verdict(False, "Step 9 fold geometry: CrossValidator's default RANDOM folds train on the "
                   "future and split near-duplicate adjacent bars across the boundary; "
                   "contiguous bar_us blocks fix the duplicate leak but NOT the direction -- "
                   "block 0 is still validated against later blocks, and a true expanding "
                   "window needs a manual loop outside the framework", banned=True)
    return cv_in, best, coefficients, [n for n, _ in live], cv_rmse


# --------------------------------------------------------------------------------------
# STEP 10 -- SCALING  (LIMIT: linear scalers only)
# --------------------------------------------------------------------------------------

def step10_scaling(frame, survivors, coefficients):
    """StandardScaler, and the back-translation that is the entire point of the LIMIT.

    StandardScaler centres and divides by sigma, which is AFFINE, so the spacing of the data is
    preserved and a coefficient on a scaled feature multiplies straight back into real units.
    QuantileTransformer would turn a feature value into a percentile rank and the coefficient
    would stop meaning anything measurable. On Path 1 the unit is basis points of next-bar
    return, and a reviewer has to be able to read the sentence.
    """
    # Step 9 SELECTED; it did not shrink the vector. A LinearRegressionModel reports a zero
    # coefficient, it does not delete a column, so the survivors are re-assembled here -- the
    # framework is explicit that a zeroed column "is dropped", and carrying dead columns into
    # the scaler would leave a matrix whose width disagrees with the Step 9 ledger line.
    assembled = VectorAssembler(inputCols=survivors, outputCol="final_features",
                                handleInvalid="error").transform(frame)
    # withMean=True is NOT the default and is the half that makes the centring real. It
    # densifies sparse vectors, which would matter on a wide one-hot matrix -- Path 2's
    # problem, not Path 1's, because Path 1 deleted its categoricals at Step 8.
    model = StandardScaler(inputCol="final_features", outputCol="scaled",
                           withMean=True, withStd=True).fit(assembled)
    step10 = model.transform(assembled)

    mu = [float(v) for v in model.mean]
    sigma = [float(v) for v in model.std]
    LOG.info("   %-20s %16s %16s", "feature", "mu", "sigma")
    for name, m, s in zip(survivors, mu, sigma):
        LOG.info("   %-20s %16.6g %16.6g", name, m, s)

    # THE POINT OF THE LIMIT: sigma is retained, so a Step 9 coefficient back-translates into
    # the unit a non-technical reviewer reads. y is a log return, so 1e4 * y is basis points.
    LOG.info("   back-translation (this is what a non-linear scaler would destroy):")
    translated = {}
    for name, s in zip(survivors, sigma):
        translated[name] = bp_per_sd(coefficients[name], s)
        LOG.info("      +1 sd of %-20s (sd = %-14.6g) -> %+.4f bp of next-bar return",
                 name, s, translated[name])

    # Cheap proof the scaler did what it says: the first scaled column is mean 0, sd 1.
    first_scaled = vector_to_array("scaled")[0]
    check = step10.select(func.avg(first_scaled).alias("m"),
                          func.stddev(first_scaled).alias("s")).first()
    LOG.info("   scaled %s: mean %+.9f, sd %.6f  (0 and 1 by construction)",
             survivors[0], check["m"], check["s"])
    if abs(check["m"]) > 1e-6 or abs(check["s"] - 1.0) > 1e-6:
        raise ValueError(f"StandardScaler did not centre/scale: mean {check['m']}, "
                         f"sd {check['s']}")

    verdict(True, f"Step 10 Path 1 LIMIT: StandardScaler(withMean=True, withStd=True) over "
                  f"{len(survivors)} columns; mu and sigma retained so coefficients stay in bp",
            limit=True)
    verdict(False, "Step 10 non-linear transforms: QuantileTransformer / PowerTransformer are "
                   "blacklisted on Path 1 -- a percentile rank has no basis-point meaning and "
                   "the model must stay explainable to a non-technical reviewer. Spark ships "
                   "neither, so the LIMIT costs nothing to obey here; that is luck, not "
                   "compliance", banned=True)
    return step10, mu, sigma, translated


COEFFICIENT_SCHEMA = StructType([
    StructField("feature", StringType(), False),
    StructField("coefficient", DoubleType(), False),
    StructField("survived_step9", BooleanType(), False),
    StructField("mu", DoubleType(), True),
    StructField("sigma", DoubleType(), True),
    StructField("bp_per_sd", DoubleType(), True),
    StructField("reg_param", DoubleType(), False),
    StructField("elastic_net_param", DoubleType(), False),
    StructField("intercept", DoubleType(), False),
    StructField("cv_rmse", DoubleType(), False),
    StructField("target_sd", DoubleType(), False),
    StructField("n_rows", DoubleType(), False),
])

TOPOLOGY_SCHEMA = StructType([
    StructField("col_a", StringType(), False),
    StructField("col_b", StringType(), False),
    StructField("pearson_r", DoubleType(), True),
])


def write_outputs(spark, out, step10, survivors, kept, coefficients, mu, sigma, translated,
                  topology, best, cv_rmse, target_sd, n_rows):
    """Three artifacts under one prefix, one per thing the path produces.

    features/      the Step 10 scaled matrix -- what Part II consumes
    topology/      Step 6's dependency schema, exported for an operator and never read back
    coefficients/  the Step 9 + Step 10 ledger, and glue-dynamo.py's input
    """
    base = out.rstrip("/")

    # The scaled vector is expanded into named double columns. A VectorUDT round-trips through
    # Parquet as an opaque struct that only Spark understands; named doubles are readable by
    # Athena, pandas and anything else that opens the file.
    scaled = vector_to_array("scaled")
    features = step10.select(
        "symbol", "bar_us", "bar_open_time_utc", "fold", "y",
        *[scaled[i].alias(f"{name}_scaled") for i, name in enumerate(survivors)])
    features.write.mode("overwrite").parquet(f"{base}/features")

    spark.createDataFrame(topology, TOPOLOGY_SCHEMA) \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/topology")

    # Every column Step 8 kept gets a row, including the ones Step 9 zeroed: the ledger is more
    # useful as "here is what happened to each candidate" than as a list of winners.
    rows = [(name, coefficients[name], name in survivors,
             mu[survivors.index(name)] if name in survivors else None,
             sigma[survivors.index(name)] if name in survivors else None,
             translated.get(name),
             float(best.getRegParam()), float(best.getElasticNetParam()),
             float(best.intercept), cv_rmse, float(target_sd), float(n_rows))
            for name in kept]
    spark.createDataFrame(rows, COEFFICIENT_SCHEMA) \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/coefficients")

    LOG.info("wrote features (%s x %s) -> %s/features | topology (%s) -> %s/topology | "
             "coefficients (%s) -> %s/coefficients", f"{n_rows:,}", len(features.columns),
             base, len(topology), base, len(rows), base)


def self_check():
    """Assert the three pure decisions that would go wrong silently. No Spark, no data.

    Everything else on this path is pyspark.ml, which is tested upstream. What is NOT tested
    upstream is the framework's own arithmetic: a strict inequality nobody demonstrates, a
    fold width that is off by one row, and a unit conversion that is the entire justification
    for Step 10's LIMIT.
    """
    # 1. The firewall is STRICT. Both sources say ">5%" / "exceeds", so 5.0% exactly keeps the
    #    default branch. `>` quietly becoming `>=` swaps the whole Step 4 branch without
    #    changing a row count or raising anything.
    assert not firewall_fires(0.0), "the override fired on a clean frame"
    assert not firewall_fires(0.0499)
    assert not firewall_fires(MISSINGNESS_BAN_THRESHOLD), \
        "5.0% exactly must NOT fire -- both sources state the threshold strictly"
    assert firewall_fires(0.050001), "just above the threshold must fire"
    assert firewall_fires(0.5)

    # 2. The fold width is inclusive of both endpoints. Without the +1 the LAST bar lands in
    #    fold `folds` and the least() clamp fires on every single run, which silently makes the
    #    final block one row wide. Checked across spans that do and do not divide evenly.
    for lo, n_slots, step in [(0, 100, 1), (1735689600000000, 1440, 5000000),
                              (0, 7, 1), (0, 1000003, 3)]:
        hi = lo + (n_slots - 1) * step
        width = fold_width(lo, hi, FOLDS)
        first = int((lo - lo) // width)
        last = int((hi - lo) // width)
        assert first == 0, f"first bar landed in fold {first}"
        assert last == FOLDS - 1, (
            f"last bar of span {lo}..{hi} landed in fold {last}, not {FOLDS - 1} -- the "
            f"least() clamp would be firing on every run")

    # 3. The back-translation. The notebook's measured pair, to the digit it printed.
    assert abs(bp_per_sd(1.319601e-02, 0.000109658) - 0.014468) < 1e-5, "bp_per_sd drifted"
    assert abs(bp_per_sd(3.151237e-05, 0.379399) - 0.119558) < 1e-5
    assert bp_per_sd(0.0, 1.0) == 0.0
    # Sign must survive: a negative coefficient is a negative number of basis points.
    assert bp_per_sd(-1.119101e-05, 0.066632) < 0

    LOG.info("self-check passed: the strict %.0f%% firewall boundary, inclusive fold widths "
             "over 4 spans, and the bp back-translation",
             100 * MISSINGNESS_BAN_THRESHOLD)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input",
                        help="prefix holding the bars Parquet written by glue-ingest-bars.py "
                             "(s3:// or a local path)")
    parser.add_argument("--output",
                        help="destination prefix; features/, topology/ and coefficients/ are "
                             "written beneath it")
    parser.add_argument("--seed", type=int, default=42,
                        help="CrossValidator seed; the folds themselves are deterministic "
                             "time blocks, so this only affects tie-breaking")
    parser.add_argument("--local", action="store_true",
                        help="run against the local filesystem with master local[*]")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the firewall boundary, the fold width and the bp "
                             "back-translation, then exit; no Spark, no input, no output")
    parser.add_argument("--shuffle-partitions", type=int, default=None,
                        help="spark.sql.shuffle.partitions under --local; 8 suits the "
                             "committed sample, leave unset for a full month")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run and a strict parser exits 2 on them before Spark ever starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return
    # Checked here rather than by required=True so that --self-check needs neither of them;
    # argparse would reject the flag combination before self_check() could run.
    if not args.input or not args.output:
        parser.error("--input and --output are required unless --self-check is given")

    spark = build_session(APP_NAME, args.local, args.shuffle_partitions)
    try:
        bars, symbols, interval_us, n_slots, rows = load_bars(
            spark, args.input, REQUIRED_COLS, "Path 1")
        LOG.info("Spark %s | tz %s | local=%s", spark.version,
                 spark.conf.get("spark.sql.session.timeZone"), args.local)
        LOG.info("Path 1 input: %s bars | %s slots x %s symbols %s | step %sus (%.4gs)",
                 f"{rows:,}", f"{n_slots:,}", len(symbols), symbols, f"{interval_us:,}",
                 interval_us / 1e6)

        frame = fork_and_derive(bars, interval_us)
        step4, n_rows, _ = step4_imputation(frame, rows)
        shape = step5_diagnostics(step4)
        topology = step6_topology(step4)
        step7 = step7_cross_products(step4)
        step8, kept = step8_pruning(step7, BASE_X + CROSS_X)
        cv_in, best, coefficients, survivors, cv_rmse = step9_elastic_net(
            step8, kept, shape["sd"], args.seed)
        step10, mu, sigma, translated = step10_scaling(cv_in, survivors, coefficients)

        write_outputs(spark, args.output, step10, survivors, kept, coefficients, mu, sigma,
                      translated, topology, best, cv_rmse, shape["sd"], n_rows)

        LOG.info("   %-34s %7s %7s", "stage", "rows", "X cols")
        LOG.info("   %-34s %7s %7s", "entryway output (Steps 1-3)", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "post-fork, y + base features", f"{rows:,}", len(BASE_X))
        LOG.info("   %-34s %7s %7s", "4  imputation", f"{n_rows:,}", len(BASE_X))
        LOG.info("   %-34s %7s %7s", "5  diagnostics (read-only)", f"{n_rows:,}", len(BASE_X))
        LOG.info("   %-34s %7s %7s", "6  topology (read-only)", f"{n_rows:,}", len(BASE_X))
        LOG.info("   %-34s %7s %7s", "7  cross-products", f"{n_rows:,}",
                 len(BASE_X) + len(CROSS_X))
        LOG.info("   %-34s %7s %7s", f"8  VarianceThreshold({VARIANCE_THRESHOLD})",
                 f"{n_rows:,}", len(kept))
        LOG.info("   %-34s %7s %7s", "9  CV Elastic Net", f"{n_rows:,}", len(survivors))
        LOG.info("   %-34s %7s %7s", "10 StandardScaler (LIMIT)", f"{n_rows:,}", len(survivors))
        LOG.info("Path 1 complete -- the target is MANUFACTURED (next-bar log return), the "
                 "cross-validation is time-blocked but not a true expanding window, and "
                 "Gate 1 downstream will see n = %s, not %s", f"{n_rows:,}", f"{rows:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
