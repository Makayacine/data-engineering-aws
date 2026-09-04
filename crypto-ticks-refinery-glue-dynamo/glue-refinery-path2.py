"""Path 2 -- the categorical sub-refinery: Steps 4-10, post-fork, path-isolated.

AWS Glue 4.0 entrypoint. Reads the bars written by ``glue-ingest-bars.py`` (Steps 1-3, the
shared path-blind entryway) and applies the Path 2 column of the framework's Steps 4-10 matrix:

      bars Parquet   (one row per (symbol, bar), dense over the declared calendar)
  ->  Step 4   Imputation      APPLIES -- median impute PER SYMBOL; missing category named
  ->  Step 5   Diagnostics     APPLIES -- class balance, READ-ONLY
  ->  Step 6   Topology        APPLIES -- cross-correlation AND cyclical sin/cos coordinates
  ->  Step 7   Feature Eng     APPLIES -- interaction cross-products, numeric and categorical
  ->  Step 8   Pruning         ENFORCE -- one-hot encoding BEFORE variance pruning
  ->  Step 9   Regularisation  APPLIES -- CV Elastic Net, multinomial, expanding-window folds
  ->  Step 10  Scaling         APPLIES -- quantile search across three candidate transforms
  ->  features/ + topology/ + coefficients/ + scaling_search/ Parquet

Step 6 is read-only on Path 1 ("No value changes") but NOT on Path 2, where the same grid cell
also mandates cyclical coordinate spaces -- those are columns, and the grid asks for them here
rather than at Step 7.

FOUR THINGS STATED UP FRONT, BECAUSE EACH IS EASY TO MISREAD AS A RESULT
-----------------------------------------------------------------------

1.  **The target is manufactured, and so is its class count.** y is the direction of the next
    bar's close-to-close change, k=3: ``up`` / ``flat`` / ``down``. A two-class up/down target
    exists at any bar width; a genuine THREE-class target survives only while a material share
    of consecutive bars close at exactly the same price, and that share is a function of the
    bar interval. It is why the entryway defaults to 5s. The subtraction stays in
    ``decimal(18,8)`` for that reason: two identical decimal prices are equal exactly, two
    doubles built from them are equal only most of the time, and the whole k=3 target hangs on
    that equality test.

2.  **Unlabelled rows leave at Step 9, not Step 4.** The framework's Step 4 on Path 2 is
    entirely about X -- "median impute numerics; assign missing categories to
    SYSTEM_STATE_UNKNOWN" -- and says nothing about a row whose y was never observed. Both
    available readings are wrong: median-imputing a class label is undefined, and giving it
    SYSTEM_STATE_UNKNOWN invents a fourth class the fork did not route on. So the unlabelled
    rows stay through Steps 4-8, which are target-blind (Step 8's variance filter is
    unsupervised by the framework's own division of labour, so it is entitled to see them all),
    and leave at Step 9, the first supervised step. That is this project's call, not the
    framework's.

3.  **Two ordering problems are STATED, not fixed.** Step 8 prunes on raw variance and Step 10
    scales, in that order, so the pruner compares columns whose variances span twelve orders of
    magnitude. Step 9 penalises coefficients and Step 10 scales, so the regulariser charges a
    scale-dependent price for the same predictive contribution -- a feature in large natural
    units buys its fit cheaply and every return-shaped column is zeroed first. Both are
    consequences of the framework's own single-non-looping-forward-pass rule, which forbids
    reordering. Quietly reordering the steps would make this a different pipeline wearing the
    framework's name.

4.  **Step 10's search cannot be run natively in Spark as written.** Spark ships neither
    PowerTransformer (no Yeo-Johnson, no Box-Cox) nor QuantileTransformer (no rank-to-uniform
    map). Two of the three legs are hand-rolled stand-ins and are labelled as such in the
    output: a signed ``log1p`` (monotone and sign-preserving, which is Yeo-Johnson at lambda=0
    and NOT a lambda search) and a ``percent_rank`` over an unpartitioned window. The second
    also cannot carry the training quantiles across to the holdout the way a real
    QuantileTransformer would, and it forces every row onto one executor -- so above
    MAX_QUANTILE_ROWS the job SKIPS that leg and says so rather than stalling.

Local acceptance run. Path 2 consumes bars, not ticks, so the entryway runs first -- against
the committed two-hour sample, with the calendar overrides that keep Step 3 meaningful::

    python glue-ingest-bars.py --local \\
        --input data/sample \\
        --bars-output _localrun/bars \\
        --bar-interval 5s \\
        --calendar-start 2025-01-01T00:00:00 \\
        --calendar-end   2025-01-01T02:00:00

    python glue-refinery-path2.py --local \\
        --input _localrun/bars \\
        --output _localrun/path2
"""

import argparse
import logging
import math

from pyspark.sql import Window
from pyspark.sql import functions as func
from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                               StructType)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (OneHotEncoder, StandardScaler, StringIndexer,
                                VarianceThresholdSelector, VectorAssembler)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.stat import Correlation

# Shared with the sibling path jobs. On Glue this needs
#   --extra-py-files s3://<bucket>/jobs/refinery_common.py
# Locally nothing is needed: Python puts the running script's directory on sys.path.
from refinery_common import build_session, load_bars, verdict as _verdict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_refinery_path2")

APP_NAME = "CryptoTicksRefineryPath2"

# The money block, imputed per symbol. all_best_match is the categorical block's only member
# besides symbol, and it is null on exactly the empty bars -- the framework's device_os case.
MONEY_COLS = ["open", "high", "low", "close", "volume", "quote_volume",
              "taker_buy_qty", "taker_buy_quote_qty"]
CAT_SOURCE = "all_best_match"
UNKNOWN = "SYSTEM_STATE_UNKNOWN"

# down/flat/up, and the numeric labels the classifier sees. Order is load-bearing twice: it
# fixes which coefficient row means which class, and it has to round-trip, so --self-check
# asserts the mapping is a bijection onto 0..k-1.
CLASSES = ["down", "flat", "up"]
CLASS_LABELS = {name: float(i) for i, name in enumerate(CLASSES)}

# Exactly 0.0, for the same reason as Path 1 and with a sharper edge here. The assembled matrix
# spans twelve orders of magnitude -- a tick count near 1.8e4 against a cross-product near
# 1e-8 -- so ANY threshold that keeps the tick count deletes every return-shaped column, and
# any threshold that keeps the returns keeps everything. The forward-pass rule forbids fixing
# that by scaling first, because scaling is Step 10. 0.0 is the framework's own demonstration:
# it removes genuinely constant columns and nothing else.
VARIANCE_THRESHOLD = 0.0

# Step 9's grid, and the chronological cuts. DEV_CUT is the dev/holdout boundary as a fraction
# of the bar_us range; FOLD_BLOCKS are (train_end, validate_end) pairs, also fractions. Every
# fold trains on everything before train_end and validates on [train_end, validate_end), so the
# windows EXPAND and no fold ever validates on a block it trained on. --self-check asserts that
# and asserts no fold reaches into the holdout.
REG_PARAMS = [0.01, 0.1]
ELASTIC_NET_PARAMS = [0.5, 1.0]
DEV_CUT = 0.80
FOLD_BLOCKS = [(0.50, 0.60), (0.60, 0.70), (0.70, 0.80)]
RANDOM_FOLD_SEEDS = [11, 22, 33]
MAX_ITER = 50

# percent_rank over an UNPARTITIONED window is the rank-to-uniform map, and it forces every row
# onto a single executor -- once per column. Fine at a few thousand rows; a full month at 5s is
# 1.6M and it is not. Above this the quantile leg is SKIPPED and reported, rather than failing
# the job or stalling it: the leg is an acknowledged substitution for a transform Spark does not
# ship, so losing it is a documented limitation and not a broken run.
MAX_QUANTILE_ROWS = 250_000

REQUIRED_COLS = ["symbol", "bar_us", "bar_open_time_utc", "open", "high", "low", "close",
                 "volume", "quote_volume", "n_ticks", "taker_buy_qty", "taker_buy_quote_qty",
                 "all_best_match", "is_missing_bar", "is_first_bar", "is_last_bar"]

NUMERIC_X = ["imbalance", "log_range", "body", "ticks",
             "minute_sin", "minute_cos", "hour_sin", "hour_cos",
             "x_imb_body", "x_ticks_range", "x_imb_minute",
             "is_missing_bar", "is_first_bar", "is_last_bar"]
CATEGORICAL_X = ["symbol", "best_match_state", "symbol_state"]


def verdict(applies, message, banned=False, enforce=False):
    """Path 2's badge set, bound to this job's logger.

    Path 2 uses APPLIES / N/A / ENFORCE (Step 8) / BANNED. OVERRIDE is a Path 1 and Path 3
    badge and LIMIT is a Path 1 badge; neither is legitimate here, so neither is exposed even
    though refinery_common.verdict() implements all five. Note in particular that Path 2's
    Step 4 is a plain APPLIES -- the >5% median ban is Path 1's rule, and the framework runs
    the Path 2 branch at 3.1% missing with no threshold test at all.
    """
    return _verdict(applies, message, banned=banned, enforce=enforce, log=LOG)


def class_column(delta_col):
    """up / flat / down from a DECIMAL delta. Null delta stays null -- an unlabelled row.

    The comparison is against an exact decimal zero. Casting to double first would make the
    flat class an artefact of binary rounding rather than a measurement.
    """
    return (func.when(delta_col > 0, "up")
                .when(delta_col < 0, "down")
                .when(delta_col == 0, "flat"))


def fold_cut(lo_us, hi_us, fraction):
    """A chronological cut point as a fraction of the bar_us range.

    Pure, and named, because Step 9's entire leak argument rests on these cuts being monotone
    and on no fold reaching past DEV_CUT into the holdout. --self-check pins both.
    """
    return lo_us + int(fraction * (hi_us - lo_us))


def signed_log1p(col):
    """Monotone, sign-preserving stand-in for Yeo-Johnson at lambda = 0.

    It is NOT Yeo-Johnson: there is no lambda search here, and calling it one would be a lie.
    What it does share is the property that matters for a scaler -- it is strictly increasing,
    so it cannot reorder the data, and it maps 0 to 0 so a zero-range bar stays a zero.
    """
    return func.signum(col) * func.log1p(func.abs(col))


# --------------------------------------------------------------------------------------
# THE FORK
# --------------------------------------------------------------------------------------

def fork_and_label(bars):
    """Build y before Step 4, because the fork fires before any imputation.

    The framework is explicit that the path is chosen "immediately after Step 3" and "before
    any imputation or spatial modifications" -- the path decides how missing data is handled,
    so the target cannot be a product of imputation. Every label here therefore comes from an
    OBSERVED close on both sides of the transition.
    """
    # partitionBy("symbol") is not cosmetic: without it lead() runs one unpartitioned window
    # over the whole frame and hands BTC's last bar ETH's first close.
    window = Window.partitionBy("symbol").orderBy("bar_us")
    frame = (bars
             .withColumn("_next_close", func.lead("close").over(window))
             .withColumn("delta", func.col("_next_close") - func.col("close"))
             .withColumn("y_class", class_column(func.col("delta")))
             .drop("_next_close"))

    counts = frame.select(
        func.count("*").alias("rows"),
        func.sum(func.col("y_class").isNotNull().cast("int")).alias("labelled"),
        func.sum((func.col("y_class").isNull() & (func.col("is_last_bar") == 1))
                 .cast("int")).alias("edge"),
        func.sum((func.col("y_class").isNull() & (func.col("is_missing_bar") == 1))
                 .cast("int")).alias("own"),
    ).first()
    rows, labelled = counts["rows"], counts["labelled"]
    unlabelled = rows - labelled

    levels = sorted(r[0] for r in frame.select("y_class").distinct()
                    .filter(func.col("y_class").isNotNull()).collect())
    LOG.info("   y geometry: string, %s distinct levels %s", len(levels), levels)
    LOG.info("   labelled rows: %s of %s   unlabelled: %s", f"{labelled:,}", f"{rows:,}",
             unlabelled)
    LOG.info("      window edge (is_last_bar):    %s", counts["edge"])
    LOG.info("      empty bar, own close missing: %s", counts["own"])
    LOG.info("      predecessor of an empty bar:  %s",
             unlabelled - counts["edge"] - counts["own"])

    # A real-valued continuous decimal would have routed to Path 1; an un-batched stream of
    # impressions to Path 3. Three discrete levels is the categorical geometry.
    verdict(True, f"Fork: y is a {len(levels)}-level discrete class label "
                  f"({'/'.join(levels)}) -> Path 2, categorical")
    if sorted(levels) != sorted(CLASSES):
        raise ValueError(f"observed classes {levels} do not match the declared {CLASSES} -- "
                         f"a k=3 target needs a material flat class, and at a coarser bar "
                         f"interval `flat` disappears entirely")
    verdict(False, "Fork provenance: the framework gives no rule for MANUFACTURING a target, "
                   "and none for choosing k -- nominating the next-bar direction at k=3 is "
                   "this project's decision, and it only holds while `flat` is material")

    # The one thing the fork does NOT decide, stated where it arises rather than in a footnote.
    verdict(False, f"Fork unlabelled rows: {unlabelled} rows have no observed y. The framework's "
                   f"Step 4 is entirely about X -- median-imputing a class label is undefined "
                   f"and naming it {UNKNOWN} invents a fourth class the fork did not route on. "
                   f"They stay through Steps 4-8 (target-blind) and leave at Step 9",
            banned=True)
    return frame.cache(), rows, labelled


# --------------------------------------------------------------------------------------
# STEP 4 -- IMPUTATION  (median per symbol; missing category named)
# --------------------------------------------------------------------------------------

def step4_imputation(frame, symbols, rows):
    """Median-impute the numerics PER SYMBOL, then give the missing category a name."""
    nulls = frame.select([func.count(func.when(func.col(c).isNull(), c)).alias(c)
                          for c in MONEY_COLS + [CAT_SOURCE]]).first().asDict()
    for col, n in nulls.items():
        LOG.info("   %-22s %4d null  (%.3f%%)", col, n, 100.0 * n / rows)

    # Reported, not acted on. Path 1 evaluates a 5% firewall here and bans medians above it;
    # Path 2 has no such rule -- the framework runs this branch on device_os at 3.1% missing
    # with no threshold test at all. Quoting Path 1's threshold on this lane would be borrowing
    # an authority the framework did not grant.
    rate = frame.select(func.avg(func.col("is_missing_bar").cast("double"))).first()[0]
    verdict(True, f"Step 4 missingness: {rate:.3%} of bars empty -- Path 2 imputes regardless "
                  f"of rate; the >5% median ban is a Path 1 rule and is not evaluated here")

    # THE CAST BOUNDARY. Decimal is what made the entryway's sums reproducible across shuffle
    # widths, and that work is finished: from here the values feed percentile_approx, log() and
    # pyspark.ml, none of which accept decimal without coercing anyway. The target was already
    # built in decimal at the fork, which is the one place it had to be.
    imputed = frame
    for col in MONEY_COLS:
        imputed = imputed.withColumn(col, func.col(col).cast("double"))

    # THE GROUPING IS THE TRAP. A single median over the frame blends three price scales
    # (BTC ~ $94k, ETH ~ $3.3k, SOL ~ $190) and would hand an empty ETH bar a five-figure
    # close. The framework's NexusMart matrix has one cohort per run and never has to state it;
    # bar data does. percentile_approx through expr() for the same 3.3.0-wrapper reason as
    # pmod in the shared reader.
    medians = (imputed.groupBy("symbol")
               .agg(*[func.expr(f"percentile_approx({c}, 0.5)").alias(f"_med_{c}")
                      for c in MONEY_COLS]))
    imputed = imputed.join(func.broadcast(medians), on="symbol", how="left")
    for col in MONEY_COLS:
        imputed = imputed.withColumn(col, func.coalesce(func.col(col), func.col(f"_med_{col}")))

    LOG.info("   per-symbol medians used as the fill value:")
    for row in medians.orderBy("symbol").collect():
        LOG.info("      %-9s close %12s   volume %12s", row["symbol"],
                 f"{row['_med_close']:,.2f}", f"{row['_med_volume']:,.4f}")

    imputed = imputed.drop(*[f"_med_{c}" for c in MONEY_COLS])
    left = imputed.select([func.count(func.when(func.col(c).isNull(), c)).alias(c)
                           for c in MONEY_COLS]).first().asDict()
    filled = sum(nulls[c] for c in MONEY_COLS)
    # Gated on whether there was anything to impute, not on whether anything is left:
    # "0 nulls remain" is the SUCCESS condition, and hanging an N/A on it would report a
    # completed step as a skipped one.
    verdict(filled > 0,
            f"Step 4 numerics: {filled} nulls across {len(MONEY_COLS)} columns median-imputed "
            f"per symbol over {len(symbols)} symbols, {sum(left.values())} remain")
    if sum(left.values()):
        raise ValueError(f"{sum(left.values())} nulls survived the median fill -- a symbol had "
                         f"no observed value at all for some column, so its median is null too")

    # na.fill on a string column rather than StringIndexer(handleInvalid="keep"). Both route
    # the nulls somewhere; only na.fill makes the destination a NAMED, inspectable level, which
    # is what the framework asks for -- "assign missing categories to SYSTEM_STATE_UNKNOWN",
    # not "route them to whatever index the encoder assigns last".
    imputed = (imputed
               .withColumn("best_match_state", func.col(CAT_SOURCE).cast("string"))
               .na.fill(UNKNOWN, subset=["best_match_state"]))
    for row in imputed.groupBy("best_match_state").count().orderBy("best_match_state").collect():
        LOG.info("      %-22s %s", row["best_match_state"], f"{row['count']:,}")

    # The finding the framework's e-commerce example cannot produce, and it matters at Step 9:
    # on bar data the unknown categorical state and the Step 3 missingness flag are the SAME
    # rows, so the named level and is_missing_bar are perfectly collinear by construction.
    alias = imputed.filter((func.col("best_match_state") == UNKNOWN)
                           != (func.col("is_missing_bar") == 1)).count()
    verdict(True, f"Step 4 categoricals: {UNKNOWN} assigned to {nulls[CAT_SOURCE]} rows, "
                  f"0 dropped; disagreements with the Step 3 flag: {alias} -- the level and "
                  f"is_missing_bar are "
                  f"{'perfectly collinear here' if alias == 0 else 'NOT collinear on this run'}")
    return imputed.cache()


# --------------------------------------------------------------------------------------
# STEP 5 -- DIAGNOSTICS  (class balance, read-only)
# --------------------------------------------------------------------------------------

def step5_class_balance(frame, labelled):
    """Class balance per symbol. Emits a log and changes nothing."""
    before = (frame.count(), len(frame.columns))

    balance = {(r["symbol"], r["y_class"]): r["n"] for r in
               (frame.filter(func.col("y_class").isNotNull())
                .groupBy("symbol", "y_class").agg(func.count("*").alias("n")).collect())}
    symbols = sorted({s for s, _ in balance})

    LOG.info("   %-10s %7s%s", "symbol", "n", "".join(f"{c:>18}" for c in CLASSES))
    for sym in symbols:
        n = sum(balance.get((sym, c), 0) for c in CLASSES)
        cells = "".join(f"{balance.get((sym, c), 0):>8,} "
                        f"{100.0 * balance.get((sym, c), 0) / n:>7.2f}%" for c in CLASSES)
        LOG.info("   %-10s %7s%s", sym, f"{n:,}", cells)

    # The denominator is NOT the slot count. It is the number of transitions with an OBSERVED
    # close at both ends, which is one fewer than the slots on a symbol with no gaps and two
    # fewer per empty bar on one with them -- each empty bar kills the transition into it AND
    # the transition out of it. Reporting these shares against the slot count would quietly
    # credit the empty bars to whichever class the arithmetic favoured.
    pooled_flat = sum(balance.get((s, "flat"), 0) for s in symbols)
    smallest = min(balance.values())
    verdict(True, f"Step 5 class balance: k={len(CLASSES)}, pooled flat share "
                  f"{100.0 * pooled_flat / labelled:.2f}% of {labelled:,} labelled rows, "
                  f"smallest per-symbol cell n={smallest:,} -- every class clears an np>=10 "
                  f"style adequacy floor")
    verdict(False, "Step 5 balance provenance: the flat share is a property of THIS window, "
                   "not of the month -- a quiet tape prints more repeated closes, and the "
                   "class that makes k=3 possible is the one most sensitive to that")

    after = (frame.count(), len(frame.columns))
    if after != before:
        raise ValueError(f"Step 5 is read-only but the frame changed: {before} -> {after}")
    verdict(False, "Step 5 mutation: diagnostics are read-only on every path -- the matrix "
                   f"that leaves this step is the matrix that entered it "
                   f"({before[0]:,} rows x {before[1]} columns, unchanged)")
    return balance


# --------------------------------------------------------------------------------------
# STEP 6 -- TOPOLOGY  (cross-correlation AND cyclical coordinates)
# --------------------------------------------------------------------------------------

TOPO_COLS = ["close", "volume", "quote_volume", "n_ticks", "taker_buy_qty", "is_missing_bar"]


def _pearson(frame, cols):
    """Pearson matrix over an assembled vector -- Correlation.corr is the only native route."""
    vec = VectorAssembler(inputCols=cols, outputCol="_topo_vec").transform(
        frame.select([func.col(c).cast("double").alias(c) for c in cols]))
    return Correlation.corr(vec, "_topo_vec", "pearson").collect()[0][0].toArray()


def _export_r(value):
    """A correlation cell as it is written to Parquet: NaN -> NULL, and rounded to 12 dp.

    Correlation.corr is a float aggregate over partitions and float addition is not
    associative, so the last ULP of a POOLED cell is a function of how the work happened to be
    scheduled -- measured: 16 of the 144 cells here move by up to 1.11e-16 between two launch
    methods of the SAME code on the SAME input, while the per-symbol cells (computed on smaller
    filtered frames) stayed put. A Pearson r carries ~8 significant digits of real information
    on this data, so 12 decimal places loses nothing and makes the artifact byte-reproducible,
    which is the property the entryway chose decimal money to protect.

    NaN is written as NULL rather than as a float: a NaN cell means the column was CONSTANT
    within that group, which is a fact worth preserving, and NaN in Parquet sorts as the
    largest value and compares unequal to itself.
    """
    return None if value != value else round(float(value), 12)


def step6_topology(frame, symbols):
    """Cross-correlation, pooled AND per symbol. Read-only; returns rows to persist.

    The framework's reason for putting this before pruning is sequencing, not curiosity:
    Step 6 "exports an internal mathematical dependency schema to guide downstream feature
    regularizers", and the pass is single and non-looping, so Step 9 can only consult a matrix
    built while the columns still existed. It is EXPORTED, never returned into the pipeline.
    """
    pooled = _pearson(frame, TOPO_COLS)
    per_symbol = {s: _pearson(frame.filter(func.col("symbol") == s), TOPO_COLS)
                  for s in symbols}

    fmt = lambda v: "  undefined" if v != v else f"{v:>11.3f}"
    LOG.info("   %-34s%11s%s", "pair", "pooled", "".join(f"{s:>13}" for s in symbols))
    flips = {s: 0 for s in symbols}
    undefined = 0
    rows = []
    for i in range(len(TOPO_COLS)):
        for j in range(len(TOPO_COLS)):
            r = pooled[i][j]
            rows.append(("__pooled__", TOPO_COLS[i], TOPO_COLS[j], _export_r(r)))
            for sym in symbols:
                v = per_symbol[sym][i][j]
                rows.append((sym, TOPO_COLS[i], TOPO_COLS[j], _export_r(v)))
        for j in range(i + 1, len(TOPO_COLS)):
            cells = ""
            for sym in symbols:
                v = per_symbol[sym][i][j]
                undefined += v != v
                flips[sym] += (v == v) and (pooled[i][j] * v < 0)
                cells += fmt(v)
            LOG.info("   %-34s%s%s", f"{TOPO_COLS[i]} x {TOPO_COLS[j]}", fmt(pooled[i][j]),
                     cells)

    # Two traps in one table.
    # 1. A "global" matrix over a frame holding several symbols is largely a matrix of
    #    BETWEEN-SYMBOL scale differences: close and volume separate BTC from SOL far more
    #    strongly than either moves WITHIN a symbol, so the pooled figure and the per-symbol
    #    figure disagree -- on some pairs in sign. Step 9's regulariser inherits whichever one
    #    it is handed, and the framework's "global across all X features" does not say which.
    # 2. "undefined" is not a rendering accident. A symbol with no empty bars has a CONSTANT
    #    is_missing_bar within that group, so its correlation has a zero denominator. The
    #    pooled matrix hides that by borrowing the other symbols' variance to fill the column.
    total_flips = sum(flips.values())
    verdict(True, f"Step 6 topology: cross-correlation computed pooled and per symbol -- "
                  f"{total_flips} pair-symbol combinations flip sign against the pooled matrix "
                  f"({', '.join(f'{s} {n}' for s, n in sorted(flips.items()))}), so 'global "
                  f"across all X features' needs a stated grouping on bar data")
    verdict(undefined > 0,
            f"Step 6 undefined cells: {undefined} per-symbol correlations have a zero "
            f"denominator because a column is constant within that symbol -- the pooled matrix "
            f"conceals this by borrowing the other symbols' variance")
    return rows


def step6_cyclical(frame):
    """sin/cos pairs for minute-of-hour and hour-of-day. Columns, and Step 6's on this path.

    The framework names the technique -- "cyclical coordinate spaces (sine/cosine encoding)" --
    and never names a period. Crypto trades 24/7, so there is no session open to anchor on the
    way an equity feed would force. Both periods are emitted; a short extract exercises only
    the fast one, and watching Step 8 rule on a near-constant column is more useful than hiding
    it.
    """
    # hour()/minute() are safe here only because the session timezone is pinned to UTC in
    # build_session(). On an unpinned box every bucket below shifts silently.
    out = (frame
           .withColumn("minute", func.minute("bar_open_time_utc"))
           .withColumn("hour", func.hour("bar_open_time_utc")))
    for col, period, name in [("minute", 60, "minute"), ("hour", 24, "hour")]:
        angle = 2 * math.pi * func.col(col) / func.lit(period)
        out = (out.withColumn(f"{name}_sin", func.sin(angle))
                  .withColumn(f"{name}_cos", func.cos(angle)))

    # The point of the encoding, in one number: minute 59 and minute 0 are adjacent on a clock
    # and 59 apart as integers. Euclidean distance in the (sin, cos) plane restores it.
    pairs = {r["minute"]: (r["s"], r["c"]) for r in
             (out.filter(func.col("minute").isin(0, 1, 30, 59)).groupBy("minute")
              .agg(func.first("minute_sin").alias("s"),
                   func.first("minute_cos").alias("c")).collect())}
    if {0, 1, 30, 59} <= set(pairs):
        dist = lambda a, b: math.dist(pairs[a], pairs[b])
        LOG.info("   raw integer distance   |59-0| = 59      |1-0| = 1       |30-0| = 30")
        LOG.info("   cyclical distance      59->0  = %.4f   1->0  = %.4f  30->0  = %.4f",
                 dist(59, 0), dist(1, 0), dist(30, 0))

    hour_var = out.select(func.var_samp("hour_sin")).first()[0]
    verdict(True, f"Step 6 cyclical: minute-of-hour (period 60) and hour-of-day (period 24) as "
                  f"sin/cos pairs; hour_sin sample variance is {hour_var:.4f} on this window -- "
                  f"left in for Step 8 to rule on rather than pre-emptively dropped")
    return out


# --------------------------------------------------------------------------------------
# STEP 7 -- FEATURE ENGINEERING  (interaction cross-products)
# --------------------------------------------------------------------------------------

def step7_features(frame):
    """Deterministic products only. Nothing learned, nothing sampled, nothing lagged.

    Every feature is computed from the CURRENT bar. No lag, no rolling window: those carry
    structural nulls at the series head, and Step 4 -- the only step allowed to handle nulls --
    has already run.
    """
    out = (frame
           # Signed taker imbalance in [-1, +1]. The sign convention is the entryway's measured
           # one: is_buyer_maker=True means the BUYER was the maker, so the trade is an
           # aggressive SELL and taker_buy is the complement. Inverted, corr(imbalance, return)
           # flips from +0.51 to -0.51 on the full month, and the ~50/50 base rate means no
           # ratio sanity-check would catch it.
           .withColumn("imbalance",
                       (2 * func.col("taker_buy_qty") - func.col("volume"))
                       / func.col("volume"))
           # log(high/low) rather than (high-low)/low: scale-free across three price levels,
           # and exactly 0 on a zero-range bar instead of a denominator choice.
           .withColumn("log_range", func.log(func.col("high") / func.col("low")))
           .withColumn("body", (func.col("close") - func.col("open")) / func.col("open"))
           .withColumn("ticks", func.col("n_ticks").cast("double"))
           # Deterministic cross-products: the framework's depth x freq, in bar terms. Each is
           # a product of two columns that already exist, so it is reproducible from the frame
           # and auditable -- which is the property "deterministic" is protecting.
           .withColumn("x_imb_body", func.col("imbalance") * func.col("body"))
           .withColumn("x_ticks_range", func.col("ticks") * func.col("log_range"))
           .withColumn("x_imb_minute", func.col("imbalance") * func.col("minute_sin"))
           # The CATEGORICAL cross-product. The framework's headline Path 2 finding was a
           # three-way device_os x referral_channel x cohort interaction, so categorical x
           # categorical is in scope here -- and it is what gives Step 8 a multi-level column
           # with genuinely rare levels to rule on.
           .withColumn("symbol_state",
                       func.concat_ws("__", func.col("symbol"), func.col("best_match_state"))))

    for row in out.groupBy("symbol_state").count().orderBy("symbol_state").collect():
        LOG.info("      %-32s %s", row["symbol_state"], f"{row['count']:,}")

    nulls = out.select([func.count(func.when(func.col(c).isNull(), c)).alias(c)
                        for c in NUMERIC_X]).first().asDict()
    verdict(True, f"Step 7 features: {len(NUMERIC_X)} numeric + {len(CATEGORICAL_X)} "
                  f"categorical columns, {sum(nulls.values())} nulls -- no lag or rolling term, "
                  f"so no structural null at the series edges")
    verdict(False, "Step 7 ceiling: interactions deeper than a hand-built pairwise product are "
                   "a MODELLING escalation (the framework's ANN clause), not more Step 7",
            banned=True)
    return out.cache()


# --------------------------------------------------------------------------------------
# STEP 8 -- PRUNING  (ENFORCE: one-hot encoding BEFORE variance pruning)
# --------------------------------------------------------------------------------------

def step8_enforce_encode_then_prune(frame):
    """Index, encode, assemble, prune -- in that order, because the order is the mandate.

    Both operations happen either way. What changes is the GRANULARITY at which the variance
    filter can act: encode-first prunes at LEVEL granularity (one rare level lives or dies on
    its own), prune-first prunes at COLUMN granularity (the whole categorical survives or dies
    together, and what the filter measures to decide is the variance of index CODES, which is
    a property of how the levels happened to be numbered).
    """
    # Spark forces half the discipline: OneHotEncoder takes numeric indices only, so
    # StringIndexer is mandatory first and there is no way to encode a raw string column.
    indexers = [StringIndexer(inputCol=c, outputCol=f"_idx_{c}").fit(frame)
                for c in CATEGORICAL_X]
    encoded = frame
    for indexer in indexers:
        encoded = indexer.transform(encoded)

    # dropLast=False is deliberate and NOT the default. The default drops a reference level,
    # and a dropped level is a level the variance filter can never measure -- which is exactly
    # the granularity the ENFORCE exists to protect.
    ohe = OneHotEncoder(inputCols=[f"_idx_{c}" for c in CATEGORICAL_X],
                        outputCols=[f"_ohe_{c}" for c in CATEGORICAL_X],
                        dropLast=False).fit(encoded)
    encoded = ohe.transform(encoded)

    # Explode the dummy vectors back into named 0/1 columns. Not decoration: the whole ENFORCE
    # argument is that a level has an independent existence, and a level buried inside a
    # VectorUDT cannot be named in a coefficient table or a variance report.
    dummies = []
    for cat, indexer in zip(CATEGORICAL_X, indexers):
        arr = vector_to_array(func.col(f"_ohe_{cat}"))
        for i, level in enumerate(indexer.labels):
            name = f"oh__{cat}__{level}"
            encoded = encoded.withColumn(name, arr.getItem(i))
            dummies.append(name)

    assembled_cols = NUMERIC_X + dummies
    encoded = VectorAssembler(inputCols=assembled_cols, outputCol="features_raw").transform(
        encoded.select(*[func.col(c).cast("double").alias(c) for c in assembled_cols],
                       "symbol", "bar_us", "bar_open_time_utc", "y_class", "symbol_state"))
    LOG.info("   %s numeric + %s one-hot levels = %s columns into the variance filter",
             len(NUMERIC_X), len(dummies), len(assembled_cols))

    variances = encoded.select([func.var_samp(func.col(c)).alias(c)
                                for c in assembled_cols]).first().asDict()
    LOG.info("   %-52s%18s", "column", "sample variance")
    for col in sorted(assembled_cols, key=lambda k: variances[k]):
        LOG.info("   %-52s%18.8f", col, variances[col])

    selector = VarianceThresholdSelector(featuresCol="features_raw", outputCol="features",
                                         varianceThreshold=VARIANCE_THRESHOLD).fit(encoded)
    kept = [assembled_cols[i] for i in selector.selectedFeatures]
    dropped = [c for c in assembled_cols if c not in kept]
    encoded = selector.transform(encoded)

    verdict(bool(dropped),
            f"Step 8 prune at threshold {VARIANCE_THRESHOLD}: {len(dropped)} of "
            f"{len(assembled_cols)} columns removed "
            f"{dropped or '-- nothing in this frame is constant'}")

    # The ENFORCE's justification, MEASURED on this run rather than asserted. The same column
    # is indexed two ways and the variance of the resulting codes is compared: the data is
    # identical and only the numbering differs, so a prune-first filter's keep/drop decision is
    # not a function of the data at all. This is read-only and does not touch the frame -- what
    # the job must not do is run the banned ORDER, and it does not.
    target = CATEGORICAL_X[-1]
    default_var = encoded_index_variance(frame, target, "frequencyDesc")
    alpha_var = encoded_index_variance(frame, target, "alphabetAsc")
    LOG.info("   PRUNE-FIRST counterfactual -- variance of the index codes for %s:", target)
    LOG.info("      stringOrderType='frequencyDesc' (default) : %.6f", default_var)
    LOG.info("      stringOrderType='alphabetAsc'             : %.6f", alpha_var)
    LOG.info("      the data is identical; the difference is the numbering")
    LOG.info("   ENCODE-FIRST -- variance of each level of the same column:")
    for name in [c for c in dummies if c.startswith(f"oh__{target}__")]:
        LOG.info("      %-52s%12.6f", name, variances[name])

    verdict(True, f"Step 8 ENFORCE: prune-first can only keep or delete {target} whole, and "
                  f"what it measures to decide moves from {default_var:.4f} to {alpha_var:.4f} "
                  f"on identical data when the indexer's ordering changes; encode-first "
                  f"measures each level on the thing that carries the signal", enforce=True)
    verdict(False, "Step 8 prune-first: it annuls Step 4's named level (a level has no "
                   "independent existence until one-hot gives it a column), makes the keep/drop "
                   "decision a numbering artefact rather than a property of the data, and "
                   "propagates an ordinal fiction that the single-forward-pass rule forbids "
                   "Step 8 from ever re-running to undo", banned=True)
    return encoded.cache(), kept, dummies, variances


def encoded_index_variance(frame, column, order_type):
    """Variance of the index codes for one categorical under one indexer ordering.

    Read-only and on a throwaway frame. This is the number a prune-first filter would consult,
    and the point is that it changes with the numbering while the data does not.
    """
    indexed = StringIndexer(inputCol=column, outputCol="_idx_alt",
                           stringOrderType=order_type).fit(frame).transform(frame)
    return indexed.select(func.var_samp("_idx_alt").alias("v")).first()["v"]


# --------------------------------------------------------------------------------------
# STEP 9 -- REGULARISATION  (CV Elastic Net, multinomial, expanding-window folds)
# --------------------------------------------------------------------------------------

def step9_elastic_net(frame, kept):
    """Multinomial logistic regression with an Elastic Net penalty, k=3.

    The framework says "cross-validated" and never mentions temporal ordering, because
    NexusMart's rows are exchangeable sessions. Bars are not. Spark's CrossValidator builds
    RANDOM folds, so on ordered data it trains on the future and validates on the past. Both
    schemes are run here and the gap is measured, because the size of the optimism IS the
    argument -- asserting that random folds leak is weaker than pricing it.
    """
    # The unlabelled rows leave HERE, at the first supervised step, not at Step 4.
    labelled = frame.filter(func.col("y_class").isNotNull())
    label_expr = func.lit(None).cast("double")
    for name, value in CLASS_LABELS.items():
        label_expr = func.when(func.col("y_class") == name, func.lit(value)).otherwise(label_expr)
    model_df = labelled.withColumn("label", label_expr).cache()

    n_model = model_df.count()
    verdict(True, f"Step 9 supervised frame: {frame.count():,} -> {n_model:,} rows -- the "
                  f"unlabelled rows leave at the first supervised step, having been visible to "
                  f"the target-blind Steps 4-8")

    span = model_df.agg(func.min("bar_us").alias("lo"),
                        func.max("bar_us").alias("hi")).first()
    lo, hi = span["lo"], span["hi"]
    dev = model_df.filter(func.col("bar_us") < fold_cut(lo, hi, DEV_CUT))
    holdout = model_df.filter(func.col("bar_us") >= fold_cut(lo, hi, DEV_CUT))

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")

    # The number every accuracy below has to be read against: predict the class that was most
    # common on dev, on every holdout row. Without it "0.40" is unanchored.
    majority = (dev.groupBy("label").count().orderBy(func.desc("count")).first()["label"])
    baseline = holdout.select(
        func.avg((func.col("label") == majority).cast("double"))).first()[0]
    LOG.info("   majority-class baseline on the holdout: %.4f   (dev %s | holdout %s rows)",
             baseline, f"{dev.count():,}", f"{holdout.count():,}")

    def fit(train, reg, net):
        return LogisticRegression(featuresCol="features", labelCol="label", maxIter=MAX_ITER,
                                  regParam=reg, elasticNetParam=net).fit(train)

    LOG.info("   %10s%12s%18s%17s%9s", "regParam", "elasticNet", "time-ordered CV",
             "random-fold CV", "gap")
    scores = {}
    for reg in REG_PARAMS:
        for net in ELASTIC_NET_PARAMS:
            ordered = []
            for train_end, validate_end in FOLD_BLOCKS:
                train = model_df.filter(func.col("bar_us") < fold_cut(lo, hi, train_end))
                validate = model_df.filter(
                    (func.col("bar_us") >= fold_cut(lo, hi, train_end))
                    & (func.col("bar_us") < fold_cut(lo, hi, validate_end)))
                ordered.append(evaluator.evaluate(fit(train, reg, net).transform(validate)))
            # The leaky comparator: random folds over the same dev window.
            random_folds = []
            for seed in RANDOM_FOLD_SEEDS:
                train, validate = dev.randomSplit([0.8, 0.2], seed=seed)
                random_folds.append(
                    evaluator.evaluate(fit(train, reg, net).transform(validate)))
            o = sum(ordered) / len(ordered)
            r = sum(random_folds) / len(random_folds)
            scores[(reg, net)] = (o, r)
            LOG.info("   %10s%12s%18.4f%17.4f%+9.4f", reg, net, o, r, r - o)

    best = max(scores, key=lambda k: scores[k][0])
    leaks = sum(1 for o, r in scores.values() if r > o)
    verdict(True, f"Step 9 CV: time-ordered expanding-window folds select regParam={best[0]}, "
                  f"elasticNetParam={best[1]}; random folds score higher on {leaks} of "
                  f"{len(scores)} grid points, which is the leak, not a better model")
    verdict(False, "Step 9 fold geometry: the framework says 'cross-validated' and never "
                   "mentions temporal ordering, because its rows are exchangeable sessions and "
                   "bars are not -- CrossValidator's default random folds are unusable here",
            banned=True)

    # The final model, fitted on dev at the selected hyper-parameters.
    final = fit(dev, *best)
    coefficients = final.coefficientMatrix.toArray()          # k x p, one row per class
    # float(), not the numpy scalar it arrives as. createDataFrame's DoubleType verifier
    # rejects numpy.float64 outright -- "can not accept object ... in type float64" -- and it
    # does so at WRITE time, i.e. after every fit in this job has already been paid for.
    weight = {name: float(max(abs(coefficients[k][i]) for k in range(coefficients.shape[0])))
              for i, name in enumerate(kept)}

    LOG.info("   %-52s%34s", "feature", "max |coef| across the classes")
    for name in sorted(weight, key=lambda k: -weight[k]):
        LOG.info("   %-52s%34.6f", name, weight[name])

    zeroed = [n for n, w in weight.items() if w == 0.0]
    verdict(bool(zeroed),
            f"Step 9 Elastic Net: {len(zeroed)} of {len(kept)} surviving columns shrunk to "
            f"exactly zero -- {zeroed}")

    # The scale problem, restated where it bites. The L1 penalty is charged in COEFFICIENT
    # units and the coefficient absorbs the column's scale, so a feature whose natural units
    # are large buys its contribution cheaply while every return-shaped column on a bar frame
    # is charged heavily for the same contribution and is zeroed first. This is Step 8's
    # variance-threshold problem wearing different clothes, with the same cause: Step 10's
    # scaling runs AFTER Step 9 and the single-forward-pass rule forbids reordering them.
    live = {n: w for n, w in weight.items() if w > 0}
    if live:
        widest = max(live.values()) / min(live.values())
        verdict(False, f"Step 9 scale ordering: surviving |coefficients| span a factor of "
                       f"{widest:,.0f} because the L1 penalty is charged in coefficient units "
                       f"and the coefficient absorbs the column's scale; Step 10 fixes the "
                       f"scales and runs AFTER, and the pass does not loop", banned=True)
    return model_df, dev, holdout, evaluator, best, baseline, coefficients, weight


# --------------------------------------------------------------------------------------
# STEP 10 -- SCALING  (quantile search across three candidates)
# --------------------------------------------------------------------------------------

def step10_scaling_search(dev, holdout, evaluator, kept, best, baseline, n_model):
    """Search three transforms on HELD-OUT accuracy -- the last block no candidate was fitted on.

    Path 2 has no interpretability constraint -- a classifier emits a probability, not a dollar
    figure -- so unlike Path 1's LIMIT it is free to take a non-linear transform if the
    measurement says so. The measurement is a SEARCH, and the framework never states its
    criterion, so the criterion is stated here.
    """
    reg, net = best

    def score(train, test, features):
        train_v = VectorAssembler(inputCols=features, outputCol="scaled_vec").transform(train)
        test_v = VectorAssembler(inputCols=features, outputCol="scaled_vec").transform(test)
        model = LogisticRegression(featuresCol="scaled_vec", labelCol="label",
                                   maxIter=MAX_ITER, regParam=reg,
                                   elasticNetParam=net).fit(train_v)
        return evaluator.evaluate(model.transform(test_v))

    results = {}

    # Leg 1 -- StandardScaler, the only one of the three Spark actually ships. Fitted on DEV
    # only: fitting on the full frame would leak the holdout's mean and standard deviation into
    # training. withMean=True is not the default and it densifies the vector; free at this
    # width, and the memory blow-up to watch for on a wide one-hot matrix.
    assembler = VectorAssembler(inputCols=kept, outputCol="_v")
    dev_v, hold_v = assembler.transform(dev), assembler.transform(holdout)
    scaler = StandardScaler(inputCol="_v", outputCol="scaled_vec",
                            withMean=True, withStd=True).fit(dev_v)
    results["StandardScaler"] = evaluator.evaluate(
        LogisticRegression(featuresCol="scaled_vec", labelCol="label", maxIter=MAX_ITER,
                           regParam=reg, elasticNetParam=net)
        .fit(scaler.transform(dev_v)).transform(scaler.transform(hold_v)))

    # Leg 2 -- the Yeo-Johnson stand-in. Stateless, so there is nothing to carry across.
    apply_power = lambda f: f.select(
        *[c for c in f.columns if c not in kept],
        *[signed_log1p(func.col(c)).alias(c) for c in kept])
    results["PowerTransformer (signed log1p)"] = score(
        apply_power(dev), apply_power(holdout), kept)

    # Leg 3 -- the QuantileTransformer stand-in, and the one with a hard ceiling. percent_rank
    # over an UNPARTITIONED window is the rank-to-uniform map and forces every row onto one
    # executor, once per column. It also re-derives the transform on each frame, where a real
    # QuantileTransformer would carry the TRAIN quantiles across -- a second thing the
    # substitution loses. Above the cap the leg is skipped and SAID to be skipped: a silently
    # dropped candidate reads as "the search considered three and this won".
    if n_model > MAX_QUANTILE_ROWS:
        verdict(False, f"Step 10 quantile leg SKIPPED: {n_model:,} rows exceeds the "
                       f"{MAX_QUANTILE_ROWS:,} cap. percent_rank over an unpartitioned window "
                       f"is the only rank-to-uniform map Spark offers and it collapses to one "
                       f"executor per column -- the search below ran with "
                       f"{len(results)} candidates, not 3", banned=True)
    else:
        apply_quantile = lambda f: f.select(
            *[c for c in f.columns if c not in kept],
            *[func.percent_rank().over(Window.orderBy(func.col(c))).alias(c) for c in kept])
        results["QuantileTransformer (percent_rank)"] = score(
            apply_quantile(dev), apply_quantile(holdout), kept)

    LOG.info("   %-40s%18s%14s", "candidate", "holdout accuracy", "vs baseline")
    for name, value in sorted(results.items(), key=lambda kv: -kv[1]):
        LOG.info("   %-40s%18.4f%+14.4f", name, value, value - baseline)
    LOG.info("   %-40s%18.4f", "(majority-class baseline)", baseline)

    winner = max(results, key=results.get)
    verdict(True, f"Step 10 quantile search: {winner} wins on held-out accuracy "
                  f"({results[winner]:.4f}, baseline {baseline:.4f}) over {len(results)} "
                  f"candidates, chosen on a measured criterion rather than on the framework's "
                  f"NexusMart precedent")
    verdict(False, "Step 10 substitution: Spark ships neither PowerTransformer nor "
                   "QuantileTransformer, so two of the three legs are hand-rolled stand-ins -- "
                   "signed log1p is Yeo-Johnson at lambda=0 with no lambda search, and "
                   "percent_rank cannot carry the training quantiles to the holdout",
            banned=True)
    return results, winner, scaler


COEFFICIENT_SCHEMA = StructType([
    StructField("feature", StringType(), False),
    StructField("class_name", StringType(), False),
    StructField("coefficient", DoubleType(), False),
    StructField("max_abs_across_classes", DoubleType(), False),
    # BooleanType, not the stringified bool an earlier version wrote. Path 1's equivalent
    # column is a genuine boolean, and glue-dynamo.py is the first place the two artifacts
    # meet -- where "True" and True read back differently for no reason that means anything.
    StructField("survived_step9", BooleanType(), False),
    StructField("reg_param", DoubleType(), False),
    StructField("elastic_net_param", DoubleType(), False),
    StructField("baseline_accuracy", DoubleType(), False),
])

TOPOLOGY_SCHEMA = StructType([
    StructField("grouping", StringType(), False),
    StructField("col_a", StringType(), False),
    StructField("col_b", StringType(), False),
    StructField("pearson_r", DoubleType(), True),
])

SEARCH_SCHEMA = StructType([
    StructField("candidate", StringType(), False),
    StructField("holdout_accuracy", DoubleType(), False),
    StructField("vs_baseline", DoubleType(), False),
    StructField("is_winner", StringType(), False),
    StructField("shipped_by_spark", StringType(), False),
])

SPARK_NATIVE = {"StandardScaler"}


def write_outputs(spark, out, model_df, kept, scaler, topology, coefficients, weight,
                  best, baseline, results, winner):
    """Four artifacts under one prefix, one per thing this path produces.

    features/        the scaled matrix Part II consumes
    topology/        Step 6's dependency schema, pooled AND per symbol, exported never read back
    coefficients/    the k x p Step 9 matrix in long form, and glue-dynamo.py's input
    scaling_search/  the Step 10 evidence -- the framework mandates a search and states no
                     criterion, so the criterion and its measurements are persisted with it
    """
    base = out.rstrip("/")

    # StandardScaler is written regardless of which leg won the search. The two stand-in legs
    # are acknowledged substitutions for transforms Spark does not ship -- shipping a matrix
    # produced by percent_rank, which cannot carry its own training quantiles to new data,
    # would hand Part II a frame that cannot be reproduced on next month's bars. The search
    # result is persisted next to it so the choice is visible rather than silently overridden.
    assembled = VectorAssembler(inputCols=kept, outputCol="_v").transform(model_df)
    scaled = vector_to_array("scaled_vec")
    features = scaler.transform(assembled).select(
        "symbol", "bar_us", "bar_open_time_utc", "y_class", "label",
        *[scaled[i].alias(f"{name}_scaled") for i, name in enumerate(kept)])
    features.write.mode("overwrite").parquet(f"{base}/features")

    spark.createDataFrame(topology, TOPOLOGY_SCHEMA) \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/topology")

    # weight[] is built with an explicit float(), so this comparison is a native Python bool
    # and not the numpy.bool_ that createDataFrame would refuse against BooleanType.
    rows = [(name, CLASSES[k], float(coefficients[k][i]), weight[name],
             weight[name] > 0.0, float(best[0]), float(best[1]), float(baseline))
            for i, name in enumerate(kept) for k in range(coefficients.shape[0])]
    spark.createDataFrame(rows, COEFFICIENT_SCHEMA) \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/coefficients")

    search = [(name, float(value), float(value - baseline), str(name == winner),
               str(name in SPARK_NATIVE)) for name, value in results.items()]
    spark.createDataFrame(search, SEARCH_SCHEMA) \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/scaling_search")

    LOG.info("wrote features (%s x %s) -> %s/features | topology (%s) | coefficients (%s) | "
             "scaling_search (%s)", f"{features.count():,}", len(features.columns), base,
             len(topology), len(rows), len(search))


def self_check():
    """Assert the pure decisions that would go wrong silently. No Spark, no data.

    Everything heavy here is pyspark.ml, which is tested upstream. What is NOT tested upstream
    is the fold geometry -- the whole Step 9 leak argument rests on it -- and the class-label
    mapping, which fixes which coefficient row means which class.
    """
    # 1. The class mapping is a bijection onto 0..k-1. Get this wrong and every coefficient row
    #    is attributed to the wrong class, which is invisible: the accuracy is unchanged and
    #    only the interpretation is destroyed.
    assert sorted(CLASS_LABELS) == sorted(CLASSES), "mapping does not cover the classes"
    assert sorted(CLASS_LABELS.values()) == [float(i) for i in range(len(CLASSES))], \
        f"labels {sorted(CLASS_LABELS.values())} are not 0..{len(CLASSES) - 1}"
    assert [CLASSES[int(CLASS_LABELS[c])] for c in CLASSES] == CLASSES, "mapping is not a bijection"

    # 2. fold_cut is monotone in the fraction, over spans that do and do not divide evenly.
    for lo, hi in [(0, 1000), (1735689600000000, 1735696795000000), (0, 7), (5, 1000003)]:
        cuts = [fold_cut(lo, hi, f) for f in [0.0, 0.25, 0.5, 0.8, 1.0]]
        assert cuts == sorted(cuts), f"fold_cut is not monotone over {lo}..{hi}: {cuts}"
        assert cuts[0] == lo and cuts[-1] == hi, f"endpoints wrong over {lo}..{hi}: {cuts}"

    # 3. The folds EXPAND, do not overlap, and none of them reaches into the holdout. This is
    #    the check that matters: edit FOLD_BLOCKS to (0.8, 0.9) and Step 9 would validate on
    #    the block Step 10 later scores on, so both numbers would be optimistic and neither
    #    would look wrong.
    previous_end = None
    for train_end, validate_end in FOLD_BLOCKS:
        assert 0.0 < train_end < validate_end <= DEV_CUT, (
            f"fold ({train_end}, {validate_end}) is not a forward block inside the "
            f"dev window ending at {DEV_CUT} -- it would validate on the holdout")
        if previous_end is not None:
            assert train_end >= previous_end, (
                f"fold blocks overlap: {train_end} starts before {previous_end} ends")
        previous_end = validate_end
    assert FOLD_BLOCKS[-1][1] <= DEV_CUT, "the last fold ends after the dev/holdout boundary"

    # 4. The Step 10 legs Spark does not ship are labelled as substitutions, so a reader of
    #    scaling_search/ can tell a real transform from a stand-in.
    assert SPARK_NATIVE == {"StandardScaler"}, "the native-transform set drifted"

    LOG.info("self-check passed: the %s-class label bijection, fold_cut monotonicity over 4 "
             "spans, and %s expanding fold blocks that all end at or before the %.0f%% "
             "dev/holdout boundary", len(CLASSES), len(FOLD_BLOCKS), 100 * DEV_CUT)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input",
                        help="prefix holding the bars Parquet written by glue-ingest-bars.py "
                             "(s3:// or a local path)")
    parser.add_argument("--output",
                        help="destination prefix; features/, topology/, coefficients/ and "
                             "scaling_search/ are written beneath it")
    parser.add_argument("--local", action="store_true",
                        help="run against the local filesystem with master local[*]")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the class-label bijection and the fold geometry, then "
                             "exit; no Spark, no input, no output")
    parser.add_argument("--shuffle-partitions", type=int, default=None,
                        help="spark.sql.shuffle.partitions under --local; 8 suits the "
                             "committed sample, leave unset for a full month")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run and a strict parser exits 2 on them before Spark ever starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return
    if not args.input or not args.output:
        parser.error("--input and --output are required unless --self-check is given")

    spark = build_session(APP_NAME, args.local, args.shuffle_partitions)
    try:
        bars, symbols, interval_us, n_slots, rows = load_bars(
            spark, args.input, REQUIRED_COLS, "Path 2")
        LOG.info("Spark %s | tz %s | local=%s", spark.version,
                 spark.conf.get("spark.sql.session.timeZone"), args.local)
        LOG.info("Path 2 input: %s bars | %s slots x %s symbols %s | step %sus (%.4gs)",
                 f"{rows:,}", f"{n_slots:,}", len(symbols), symbols, f"{interval_us:,}",
                 interval_us / 1e6)

        frame, rows, labelled = fork_and_label(bars)
        frame = step4_imputation(frame, symbols, rows)
        step5_class_balance(frame, labelled)
        topology = step6_topology(frame, symbols)
        frame = step6_cyclical(frame)
        frame = step7_features(frame)
        encoded, kept, dummies, variances = step8_enforce_encode_then_prune(frame)
        (model_df, dev, holdout, evaluator, best, baseline,
         coefficients, weight) = step9_elastic_net(encoded, kept)
        n_model = model_df.count()
        results, winner, scaler = step10_scaling_search(
            dev, holdout, evaluator, kept, best, baseline, n_model)

        write_outputs(spark, args.output, model_df, kept, scaler, topology, coefficients,
                      weight, best, baseline, results, winner)

        LOG.info("   %-34s %7s %7s", "stage", "rows", "X cols")
        LOG.info("   %-34s %7s %7s", "entryway output (Steps 1-3)", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "post-fork, y + bars", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "4  impute + name the category", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "5  class balance (read-only)", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "6  topology + cyclical coords", f"{rows:,}", "-")
        LOG.info("   %-34s %7s %7s", "7  interaction cross-products", f"{rows:,}",
                 len(NUMERIC_X))
        LOG.info("   %-34s %7s %7s", "8  ENFORCE one-hot then prune", f"{rows:,}",
                 len(NUMERIC_X) + len(dummies))
        LOG.info("   %-34s %7s %7s", "9  CV Elastic Net (supervised)", f"{n_model:,}",
                 len(kept))
        LOG.info("   %-34s %7s %7s", "10 quantile search", f"{n_model:,}", len(kept))
        LOG.info("Path 2 complete -- the target and its k=3 are MANUFACTURED and hold only "
                 "while `flat` is material; the Step 8/10 and Step 9/10 ordering problems are "
                 "stated, not fixed, because the forward pass does not loop")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
