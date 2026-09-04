"""Path 3 -- the bandit sub-refinery: Steps 4-10, post-fork, path-isolated.

AWS Glue 4.0 entrypoint. Reads the bars written by ``glue-ingest-bars.py`` (Steps 1-3, the
shared path-blind entryway) and applies the Path 3 column of the framework's Steps 4-10 matrix:

      bars Parquet   (one row per (symbol, bar), dense over the declared calendar)
  ->  Step 4   Imputation      OVERRIDE -- native missingness kept as a latent state; no fill
  ->  Step 5   Diagnostics     APPLIES  -- stream diagnostics, then the reward DEFINITION
  ->  Step 6   Topology        APPLIES  -- cyclical time coordinates (sin/cos pairs)
  ->  Step 7   Feature Eng     APPLIES  -- interaction frame: variable-length raw-token baskets
  ->  Step 8   Pruning         BANNED   -- one-hot + VarianceThreshold destroys the token sets
  ->  Step 9   Regularisation  BANNED   -- L1 zeroes the low-support/high-lift tail
  ->  Step 10  Scaling         BANNED   -- standardised alpha/beta is not a Beta distribution
  ->  engine   Beta-Bernoulli Thompson Sampling, gamma = 0.97
  ->  features/ + frame/ + arms/ Parquet

The three bans are the substance of this path, not its footnotes. Each is reported through
verdict() with the reason it exists -- a banned step is never skipped in silence, because a
step that leaves no trace is indistinguishable from a step nobody thought of.

TWO THINGS STATED UP FRONT, BECAUSE BOTH ARE EASY TO MISREAD AS RESULTS
----------------------------------------------------------------------

1.  **This is a replay of a monthly archive, not a live stream.** Every bar was written to
    disk before this job started. The engine is driven by reading rows in ascending ``bar_us``,
    which imitates arrival order and imitates nothing else: no latency, no partial day, no arm
    added mid-flight, and -- the part that matters -- the file holds the reward of EVERY arm at
    EVERY slot, including the arms the bandit did not pull. A live bandit never sees a
    counterfactual. The replay below deliberately hides the unpulled arms' rewards from the
    update, but this job could cheat and a live system could not, so nothing here is evidence
    that the engine would behave this way in production.

2.  **The reward definition inverts the arm ranking.** ``close > prev`` and ``close >= prev``
    are both defensible English sentences and they rank the arms in opposite orders. The
    mechanism is the flat class: up and down are near-symmetric on every symbol, so
    ``up ~= (1 - flat) / 2`` and a difference of d points in FLAT share shows up as about d/2
    in the strict reward with the sign flipped. The apparent edge is the flat class changing
    hands, not direction. The strict definition is the one taken forward -- it at least
    requires a print above the previous one -- but under either definition the arm ordering is
    an ordering of tick-flatness, which is a property of tick size and quote density.
    Nothing this job emits is a trading result.

Pure PySpark plus numpy for the Beta draws: no pandas, no boto3, no awsglue, no pyspark.ml.
The alpha/beta counts are a Spark aggregate; everything after that is conjugate arithmetic on
three numbers per arm and belongs on the driver.

Local acceptance run. Path 3 consumes bars, not ticks, so the entryway runs first -- against
the committed two-hour sample, with the calendar overrides that keep Step 3 meaningful::

    python glue-ingest-bars.py --local \\
        --input data/sample \\
        --bars-output _localrun/bars \\
        --bar-interval 5s \\
        --calendar-start 2025-01-01T00:00:00 \\
        --calendar-end   2025-01-01T02:00:00

    python glue-refinery-path3.py --local \\
        --input _localrun/bars \\
        --output _localrun/path3
"""

import argparse
import logging
import math
from decimal import Decimal

import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as func
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

# Shared with the sibling path jobs. On Glue this needs
#   --extra-py-files s3://<bucket>/jobs/refinery_common.py
# Locally nothing is needed: Python puts the running script's directory on sys.path.
from refinery_common import build_session, load_bars, verdict as _verdict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_refinery_path3")

APP_NAME = "CryptoTicksRefineryPath3"

# gamma is the framework's, but its UNIT is not. "data from 50 impressions ago retains only
# 0.97^50 = 21%" counts IMPRESSIONS, and one impression here is one bar slot, so the ~33-pull
# effective memory (1/(1-gamma)) is about 2m45s of wall clock at 5s bars. Copy 0.97 onto 1h
# bars and the same constant means a day and a half. It is a named constant and not a flag for
# exactly that reason: it is only meaningful against a stated bar width, and the width is
# derived from the input below rather than passed in, so a --gamma flag would invite someone to
# tune the number without re-reading what it counts.
GAMMA = 0.97

# Beta(1,1): uniform on [0,1], no arm favoured before the first pull. The framework's own
# boot state -- "every ad boots up with zero tracking history, initialized to a flat uniform
# prior state of Beta(1,1)".
PRIOR_A, PRIOR_B = 1.0, 1.0

# Maker/taker regime cut points on taker_buy_qty / volume. Fixed and stated rather than fitted
# to quantiles of this extract: a quantile-derived band is a different token on every month,
# which makes support and lift figures incomparable across runs -- and comparability across
# runs is the whole reason the raw tokens are being preserved.
TAKER_EXTREME, TAKER_HEAVY = Decimal("0.90"), Decimal("0.60")
MAKER_HEAVY, MAKER_EXTREME = Decimal("0.40"), Decimal("0.10")

# The replay matrix is collected to the driver: one row per slot, one column per arm. At 5s
# over a month that is 535,680 rows of small Python objects, a few hundred MB -- fine. At 1s it
# is 2,678,400 and the driver is a real risk, so the cap fails fast with the fix in the message
# rather than dying in an OOM killer twenty minutes in.
MAX_REPLAY_SLOTS = 1_000_000


def verdict(applies, message, banned=False, override=False):
    """Path 3's badge set, bound to this job's logger.

    Path 3 uses APPLIES / N/A / OVERRIDE (Step 4) / BANNED (Steps 4, 6, 8, 9, 10). LIMIT is a
    Path 1 badge and ENFORCE a Path 2 badge; neither is legitimate on this path, so neither is
    exposed here even though refinery_common.verdict() implements all five.
    """
    return _verdict(applies, message, banned=banned, override=override, log=LOG)


REQUIRED_COLS = ["symbol", "bar_us", "bar_open_time_utc", "close", "volume", "taker_buy_qty",
                 "n_ticks", "is_missing_bar", "all_best_match"]


# ----------------------------------------------------------------------------------------
# STEP 4 -- IMPUTATION  (OVERRIDE: maintain native missingness)
# --------------------------------------------------------------------------------------

def step4_native_missingness(bars, rows):
    """Report the gaps and carry them through untouched. There is nothing to decide.

    Step 3 flagged the empty bars and deliberately did not fill them, because filling is a
    Step 4 decision and Step 4 is post-fork. The fork has now fired and Path 3's answer is
    that no fill is correct: no median, no forward fill, no coalesce(volume, 0), no row drop.

    The reason this is safe rather than lazy is structural. A transaction on Path 3 is a
    variable-length SET OF TOKENS THAT WERE OBSERVED, not a row with slots. An empty bar is
    not a defective row with holes in it -- it is a shorter basket. There is no cell to fill
    because the representation has no cells.
    """
    per_arm = (bars.groupBy("symbol")
               .agg(func.sum("is_missing_bar").alias("empty_bars"),
                    func.count("*").alias("slots"))
               .orderBy("symbol").collect())
    for row in per_arm:
        LOG.info("   %s  %s empty / %s slots (%.3f%%)", row["symbol"], row["empty_bars"],
                 f"{row['slots']:,}", 100.0 * row["empty_bars"] / row["slots"])
    empty = sum(r["empty_bars"] for r in per_arm)

    # Reported for the record and gating nothing. Path 1's Step 4 compares this against a 5%
    # firewall; Path 3's override is unconditional, so there is no threshold to cross and the
    # number is evidence rather than a branch condition.
    verdict(empty > 0,
            f"Step 4 native missingness: {empty:,} empty bars carried forward as unobserved "
            f"latent states -- no imputation, no row drop, row count unchanged at {rows:,}",
            override=True)

    # Named individually because each is a real temptation with real precedent in OHLCV code,
    # and because none of them would show up in a review: not one changes the row count or
    # leaves a null behind. That is exactly what makes them dangerous.
    verdict(False, "Step 4 impute: forward-fill close, coalesce(volume, 0), back-fill open "
                   "from the next bar, median fill, complete-case drop -- all fabricate a "
                   "print that never happened; Path 3 preserves the gap as the honest "
                   "representation", banned=True)
    return empty


# --------------------------------------------------------------------------------------
# STEP 5 -- DIAGNOSTICS  (stream diagnostics, then the reward definition)
# --------------------------------------------------------------------------------------

def step5_rewards(bars):
    """Attach prev_close, both candidate rewards, and the three-way direction label."""
    window = Window.partitionBy("symbol").orderBy("bar_us")

    # lag(), NOT last(..., ignorenulls=True). The ignorenulls form reaches back over the gap
    # and compares against a close from two bars ago -- forward-fill wearing a window
    # function's clothes, and precisely what the Step 4 override forbids. It would also be
    # invisible: the column would simply have fewer nulls in it.
    frame = bars.withColumn("prev_close", func.lag("close").over(window))

    # An empty bar costs TWO rewards, not one: its own (no close) and its successor's (no
    # previous close). That second cost is the one that gets forgotten, and it is why the
    # unobserved count runs to roughly double the empty-bar count rather than equalling it.
    unobserved = func.col("close").isNull() | func.col("prev_close").isNull()
    return (frame
            .withColumn("reward_strict",                       # close > prev close
                        func.when(unobserved, None)
                            .otherwise((func.col("close") > func.col("prev_close"))
                                       .cast("int")))
            .withColumn("reward_loose",                        # close >= prev close
                        func.when(unobserved, None)
                            .otherwise((func.col("close") >= func.col("prev_close"))
                                       .cast("int")))
            .withColumn("direction",
                        func.when(unobserved, None)
                            .when(func.col("close") > func.col("prev_close"), "UP")
                            .when(func.col("close") < func.col("prev_close"), "DOWN")
                            .otherwise("FLAT")))


def step5a_stream_diagnostics(frame, interval_us, rows):
    """Arrival-process view of the replay: evidence per pull, and how often none arrives."""
    stats = (frame.groupBy("symbol").agg(
        # percentile_approx has a Python wrapper from 3.1.0, but expr() keeps this line
        # identical to the notebook's and costs nothing.
        func.expr("percentile_approx(n_ticks, 0.5)").alias("median_ticks"),
        func.min("n_ticks").alias("min_ticks"),
        func.max("n_ticks").alias("max_ticks"),
        func.sum(func.col("reward_strict").isNull().cast("int")).alias("reward_unobserved"),
        func.sum("is_missing_bar").alias("empty_bars"),
    ).orderBy("symbol").collect())

    for row in stats:
        LOG.info("   %-9s median %s ticks (min %s, max %s) | %s unobserved rewards | "
                 "%s empty bars", row["symbol"], row["median_ticks"], row["min_ticks"],
                 row["max_ticks"], row["reward_unobserved"], row["empty_bars"])

    unobserved = sum(r["reward_unobserved"] for r in stats)
    verdict(True,
            f"Step 5 stream diagnostics: replay is ordered by bar_us ({interval_us:,}us step); "
            f"{unobserved:,} of {rows:,} arm-slots have an unobserved reward and will decay "
            f"without incrementing")
    return unobserved


def step5b_reward_definition(frame, arms):
    """Score the same bars two ways and show that the winner changes. Returns the rates.

    The reward is not read off the data; it is CHOSEN, and the choice changes the ranking.
    Both `close > prev` and `close >= prev` are defensible English sentences.
    """
    pivot = (frame.filter(func.col("direction").isNotNull())
             .groupBy("symbol").pivot("direction", ["UP", "FLAT", "DOWN"]).count()
             .orderBy("symbol").collect())

    rates = {}
    LOG.info("   %-9s %7s %7s %7s %7s   %11s %11s", "arm", "n", "up%", "flat%",
             "down%", "> (strict)", ">= (loose)")
    for row in pivot:
        # A direction class can be entirely absent on a short extract, and pivot() emits NULL
        # rather than 0 for it -- summing that straight into n makes the whole row NULL.
        up, flat, down = (row["UP"] or 0), (row["FLAT"] or 0), (row["DOWN"] or 0)
        n = up + flat + down
        rates[row["symbol"]] = (100.0 * up / n, 100.0 * flat / n, 100.0 * down / n)
        u, f, d = rates[row["symbol"]]
        LOG.info("   %-9s %7s %7.2f %7.2f %7.2f   %11.2f%% %11.2f%%",
                 row["symbol"], f"{n:,}", u, f, d, u, u + f)

    best_strict = max(rates, key=lambda s: rates[s][0])
    best_loose = max(rates, key=lambda s: rates[s][0] + rates[s][1])
    LOG.info("   up/down asymmetry (pp): %s",
             ", ".join(f"{s} {abs(v[0] - v[2]):.2f}" for s, v in sorted(rates.items())))

    # The arithmetic identity behind the inversion, computed rather than asserted: with up ~
    # down, the strict-reward gap between two arms is about half their flat-share gap, opposite
    # in sign. Two arms are enough to show it and this picks the two extremes of flat share.
    if len(arms) >= 2:
        flattest = max(rates, key=lambda s: rates[s][1])
        sharpest = min(rates, key=lambda s: rates[s][1])
        LOG.info("   %s - %s: strict-reward gap %.2fpp vs half the flat-share gap %.2fpp",
                 sharpest, flattest, rates[sharpest][0] - rates[flattest][0],
                 (rates[flattest][1] - rates[sharpest][1]) / 2)

    verdict(True, f"Step 5 reward: '>' picks {best_strict}, '>=' picks {best_loose} -- the "
                  f"ranking {'inverts' if best_strict != best_loose else 'is stable'} on the "
                  f"definition, so the reward is a modelling choice and is declared here, not "
                  f"discovered downstream")
    if best_strict != best_loose:
        LOG.warning("CAVEAT -- the arm ranking inverts with the reward definition. Up and down "
                    "are near-symmetric, so up ~= (1 - flat)/2 and this ordering is an "
                    "ordering of tick-flatness, not of direction. Taking '>' forward.")
    return rates, best_strict, best_loose


# --------------------------------------------------------------------------------------
# STEP 6 -- TOPOLOGY  (cyclical time coordinates)
# --------------------------------------------------------------------------------------

def step6_cyclical(frame):
    """sin/cos pairs for hour-of-day and minute-of-hour.

    Path 3 gets the cyclical encoding but not the global cross-correlation matrix Paths 1 and
    2 build -- consistent, since Step 6's dependency schema exists to guide Step 9's
    regulariser and Step 9 is banned here.

    The framework names the technique and never names a period. Crypto trades 24/7, so there
    is no session open to anchor on the way an equity feed would force. Two periods are
    emitted: hour-of-day (24) as the production coordinate and minute-of-hour (60) as the one
    a short extract actually exercises.
    """
    # Hand-rolled: pyspark.ml has no cyclical encoder. sin AND cos together, never one alone --
    # a lone sin() maps 01:00 and 11:00 to the same coordinate. hour()/minute() are safe here
    # only because the session timezone is pinned to UTC in build_session().
    frame = (frame
             .withColumn("hour_utc", func.hour("bar_open_time_utc"))
             .withColumn("minute_utc", func.minute("bar_open_time_utc"))
             .withColumn("hod_sin", func.sin(2 * math.pi * func.col("hour_utc") / 24))
             .withColumn("hod_cos", func.cos(2 * math.pi * func.col("hour_utc") / 24))
             .withColumn("moh_sin", func.sin(2 * math.pi * func.col("minute_utc") / 60))
             .withColumn("moh_cos", func.cos(2 * math.pi * func.col("minute_utc") / 60)))

    hours = sorted(r[0] for r in frame.select("hour_utc").distinct().collect())
    verdict(True, f"Step 6 cyclical: hour-of-day (period 24) and minute-of-hour (period 60) as "
                  f"sin/cos pairs; this input visits {len(hours)}/24 hour positions, so the "
                  f"hour coordinate is "
                  f"{'near-degenerate' if len(hours) < 24 else 'fully exercised'} here")

    # Stated rather than left for a reader to notice: neither named Path 3 engine consumes a
    # continuous coordinate. Thompson sampling takes counts; association mining takes tokens,
    # and the frame below uses the hour BUCKET (HOUR_xx), not the sin/cos pair. The pair is
    # persisted because Step 6 says to emit it and because the RBM the framework names for the
    # low-support tail would use it -- and the RBM is [GAP] in pyspark.ml.
    verdict(False, "Step 6 consumers: neither Thompson sampling (counts) nor association "
                   "mining (tokens) reads a continuous coordinate -- the sin/cos pairs are "
                   "written for the RBM the framework names, which pyspark.ml does not ship")
    return frame


# --------------------------------------------------------------------------------------
# STEP 7 -- FEATURE ENGINEERING  (interaction frame preparation)
# --------------------------------------------------------------------------------------

def step7_interaction_frame(frame, arms, n_slots):
    """One transaction per instant, holding the tokens that were observed at that instant.

    The lightest Step 7 of the three paths: no cross-products, because the Path 3 engines
    learn co-occurrence structure themselves. What Step 7 owes them is the FRAME.

    The transaction is one SLOT, not one bar: all arms share the grid Step 3 built, so grouping
    them by bar_us is a grouping and not a join. Aligning symbols on event_time instead is the
    phantom-row trap the entryway banned at Step 1 -- BTC, ETH and SOL routinely print inside
    the same microsecond.
    """
    # Decimal division, and the ratio stays decimal. Empty bars have NULL volume, so the ratio
    # is NULL and every when() below falls through to NULL -- no token, by construction rather
    # than by a special case. There is no float anywhere on this path: the engines take counts
    # and strings, so the entryway's decimal money never has to be cast at all.
    frame = frame.withColumn("taker_ratio", func.col("taker_buy_qty") / func.col("volume"))
    frame = frame.withColumn(
        "regime_token",
        func.when(func.col("taker_ratio").isNull(), None)
            .when(func.col("taker_ratio") >= func.lit(TAKER_EXTREME), "TAKER_EXTREME")
            .when(func.col("taker_ratio") >= func.lit(TAKER_HEAVY), "TAKER_HEAVY")
            .when(func.col("taker_ratio") >= func.lit(MAKER_HEAVY), "BALANCED")
            .when(func.col("taker_ratio") >= func.lit(MAKER_EXTREME), "MAKER_HEAVY")
            .otherwise("MAKER_EXTREME"))

    def namespaced(suffix_col):
        return func.concat_ws("_", func.col("symbol"), suffix_col)

    # explode-then-filter, NOT a fixed-width struct and NOT a sentinel string: the basket has
    # to be genuinely variable-length, so an unobservable token is dropped rather than encoded
    # as "" or "UNKNOWN", either of which association mining would dutifully count as an item.
    # array_compact() would say this in one call but landed in Spark 3.4; Glue 4.0 is 3.3.0.
    tokens = (frame.select(
        "bar_us", "hour_utc",
        func.explode(func.array(
            # An empty bar earns a token of its own. Step 3 measured n_ticks = 0 against a
            # calendar declared independently of the data, so this is an OBSERVED ABSENCE OF
            # TRADING, not a tracking failure -- while the quantities that cannot exist without
            # a trade contribute no token at all.
            func.when(func.col("is_missing_bar") == 1,
                      namespaced(func.lit("NO_TRADES"))),
            func.when(func.col("regime_token").isNotNull(),
                      namespaced(func.col("regime_token"))),
            func.when(func.col("direction").isNotNull(),
                      namespaced(func.concat(func.lit("DIR_"), func.col("direction")))),
            # all_best_match, resolved here rather than at Step 8. The entryway carried the
            # column through deliberately (dropping it would have pre-empted a path-isolated
            # step) and left Path 3 to settle it, so this is that settlement -- and it is a
            # Step 7 FRAME-CONSTRUCTION rule, not the banned Step 8 variance prune. The
            # difference is decidable: a variance filter deletes the column because it does not
            # vary; this rule emits a token only for the MINORITY state, and would emit one the
            # moment the column varied. A token present in 100% of baskets adds a constant to
            # every support count and drags every lift toward 1, so its absence is
            # informative-by-construction rather than empirically boring.
            func.when(~func.col("all_best_match"),
                      namespaced(func.lit("NOT_BEST_MATCH"))),
        )).alias("token"))
        .filter(func.col("token").isNotNull()))

    # No bare "<SYM> was present" token, for the same reason: the grid is dense, so it would
    # sit in 100% of transactions. Symbol identity is carried by the namespace prefix on the
    # tokens that DO vary, which is what "preserve the raw token" is protecting.
    baskets = (tokens.groupBy("bar_us")
               .agg(func.collect_set("token").alias("items"),
                    func.min("hour_utc").alias("hour_utc"))
               .withColumn("items", func.array_union(
                   func.col("items"),
                   func.array(func.format_string("HOUR_%02d", func.col("hour_utc")))))
               .select("bar_us", "items")).cache()

    n_baskets = baskets.count()
    vocab = (baskets.select(func.explode("items").alias("token"))
             .groupBy("token").count()
             .withColumn("support", func.col("count") / func.lit(n_baskets))
             .orderBy("count"))
    n_tokens = vocab.count()

    if n_baskets != n_slots:
        raise ValueError(f"{n_baskets} baskets from {n_slots} slots -- a slot produced no "
                         f"token at all, which the HOUR_xx token makes impossible unless the "
                         f"grid changed underneath this step")

    LOG.info("   rarest tokens in the frame:")
    for row in vocab.limit(5).collect():
        LOG.info("      %-24s seen %5s / %s (support %.5f)", row["token"],
                 f"{row['count']:,}", f"{n_baskets:,}", row["support"])

    verdict(True, f"Step 7 interaction frame: {n_baskets:,} variable-length token sets, "
                  f"{n_tokens} distinct raw string tokens over {len(arms)} arms, no "
                  f"cross-products (Apriori/ECLAT/RBM learn the co-occurrence themselves)")
    return frame.drop("all_best_match"), baskets, n_tokens


# --------------------------------------------------------------------------------------
# STEPS 8, 9, 10 -- BANNED
# --------------------------------------------------------------------------------------
# Each ban is reported, not skipped. A step that leaves no line in the log is indistinguishable
# from a step nobody thought of, and the whole claim of this path is that the three bans are
# load-bearing rather than omissions. The notebook (refinery-walkthrough.ipynb, Part 4) runs
# each banned operation ONCE on a copy to measure what it would cost; that demonstration is the
# notebook's job. A production job that ran them would be doing the thing it forbids.

def step8_pruning_banned(n_tokens, n_baskets):
    """One-hot + VarianceThreshold, and why the geometry does not survive it."""
    verdict(False,
            f"Step 8 prune: one-hot encoding + VarianceThreshold destroys the token identity "
            f"and the variable-length set geometry that Apriori, ECLAT and the RBM consume -- "
            f"{n_tokens} raw string tokens are preserved instead of becoming {n_tokens} binary "
            f"columns over {n_baskets:,} fixed-width rows", banned=True)

    # Three mechanisms, of which the third is the one that bites hardest. ECLAT's support is
    # |TID(A) n TID(B)| / N, an intersection of per-item transaction lists; after one-hot the
    # item is a COLUMN POSITION in a fixed schema and there is no list left to intersect.
    # Apriori's candidate generation joins frequent k-itemsets into (k+1)-itemsets, a lattice
    # walk defined over sets -- a fixed-width row makes every basket the same width and the
    # join has nothing to join.
    verdict(False, "Step 8 mechanism: a one-hot dummy for a token in a fraction p of "
                   "transactions has variance p(1-p) ~= p, so a variance threshold prunes in "
                   "ASCENDING ORDER OF TOKEN RARITY -- it deletes exactly the low-support tail "
                   "association mining is hunting. See refinery-walkthrough.ipynb Part 4 for "
                   "the fitted selector and the tokens it removed", banned=True)


def step9_regularisation_banned():
    """Elastic Net, and the two reasons it cannot run here."""
    verdict(False,
            "Step 9 regularise: Elastic Net minimises squared error plus an L1/L2 penalty, so "
            "a column's ability to earn its coefficient scales with its variance and the "
            "low-frequency tail is zeroed first; lift is P(B|A)/P(B), a conditional ratio with "
            "no P(A) in the numerator, which is MAXIMISED by rare antecedents -- the two "
            "criteria disagree precisely on the tail Path 3 exists to mine", banned=True)

    # The structural half, which the framework does not state and which makes the ban
    # unarguable rather than merely well-argued: to run Elastic Net here at all, a y has to be
    # invented -- and that invention is itself the argument.
    verdict(False, "Step 9 structural: Elastic Net is supervised and Path 3 has no y. The "
                   "reward is per-arm and per-timestep and the arms that were not pulled have "
                   "no observation at all, so there is no static target column to regress "
                   "against", banned=True)

    # A linear model selects COLUMNS. The highest-lift rules on this data have multi-token
    # antecedents, and no coefficient can represent a co-occurrence.
    verdict(False, "Step 9 representation: even with every token retained, a linear model has "
                   "one coefficient per column and none for an itemset -- a 4-token "
                   "antecedent is not expressible in the hypothesis class doing the pruning",
            banned=True)


def step10_scaling_banned(alpha_beta, arms):
    """StandardScaler on alpha/beta, priced in this run's own numbers.

    alpha and beta are COUNTS, not features. Three things break at once, and the arithmetic
    below is done by hand rather than by fitting a StandardScaler -- partly because fitting one
    would be running the banned step, and partly because the failure is more legible inline
    than hidden inside a model object.
    """
    verdict(False, "Step 10 scale: StandardScaler on alpha/beta yields negative shape "
                   "parameters (no Beta density), a uniform effective sample size (no "
                   "posterior width) and a state whose units no longer match the increment "
                   "(no conjugacy)", banned=True)

    params = np.array([[alpha_beta[a][0], alpha_beta[a][1]] for a in arms], dtype=float)
    if len(arms) < 2:
        return
    mu, sigma = params.mean(axis=0), params.std(axis=0, ddof=1)
    # A zero column standard deviation would divide to inf/nan and is not the failure being
    # demonstrated, so it is stepped around rather than allowed to muddy the report.
    if not sigma.all():
        LOG.info("   (alpha/beta have zero spread across arms on this run -- the scaled "
                 "arithmetic below is undefined and is skipped)")
        return
    scaled = (params - mu) / sigma

    LOG.info("   StandardScaler(withMean=True, withStd=True) on the (alpha, beta) columns:")
    LOG.info("      mu = %s   sigma = %s", mu.round(3), sigma.round(3))
    negatives = 0
    for arm, (a, b) in zip(arms, scaled):
        raw_n = sum(alpha_beta[arm])
        negatives += int(a <= 0) + int(b <= 0)
        LOG.info("      %-9s alpha %8.4f  beta %8.4f   (effective sample size %s -> %.4f)",
                 arm, a, b, f"{raw_n:,.1f}", a + b)

    # 1. Domain violation. The Beta density is a distribution only for alpha > 0 and beta > 0,
    #    and mean-centring makes roughly half the arms negative BY CONSTRUCTION.
    #    np.random.beta on a negative parameter does not return a wrong number -- it raises.
    #    Thompson sampling's single primitive has nothing to draw from and the engine halts.
    verdict(False, f"Step 10 domain: {negatives} of {2 * len(arms)} standardised shape "
                   f"parameters are <= 0, so numpy's Beta sampler raises rather than degrades "
                   f"-- Thompson sampling's one primitive has nothing to draw from",
            banned=True)

    # 2. The evidence count is annihilated. alpha + beta is the arm's effective sample size and
    #    the entire source of the explore/exploit signal, through
    #    Var[theta] = alpha*beta / ((alpha+beta)^2 (alpha+beta+1)). Standardisation is DEFINED
    #    to make the values unit-variance, so every arm ends up with the same implied sample
    #    size. This is a magnitude property, not a rank property -- scaling preserves the
    #    ordering of alpha across arms, which is exactly why the failure is easy to miss.
    raw_span = f"{min(sum(alpha_beta[a]) for a in arms):,.1f}-{max(sum(alpha_beta[a]) for a in arms):,.1f}"
    verdict(False, f"Step 10 evidence: alpha+beta carries the explore/exploit signal through "
                   f"Var[theta] = ab/((a+b)^2(a+b+1)); it goes from {raw_span} observations to "
                   f"{scaled.sum(axis=1).min():.4f}..{scaled.sum(axis=1).max():.4f} -- the "
                   f"posterior width, and with it the reason to explore, is gone", banned=True)

    # 3. The update loop stops closing. gamma*alpha + x works because alpha and x are in the
    #    same unit -- counts. After scaling the state is in units of sigma and the increment is
    #    still one impression; and in a replay mu and sigma move with every new row, so the
    #    basis under gamma shifts on every step.
    verdict(False, "Step 10 conjugacy: the update alpha <- gamma*alpha + x adds a count to a "
                   "count; after scaling the state is in units of sigma while x is still one "
                   "impression, and mu/sigma move with every row, so gamma decays a quantity "
                   "measured against a different basis at every step", banned=True)


# --------------------------------------------------------------------------------------
# THE ENGINE -- Beta-Bernoulli Thompson Sampling
# --------------------------------------------------------------------------------------
# Steps 4-10 are done and Path 3 hands straight to the engine. There is NO GATE SYSTEM on this
# path -- "gate system bypassed -> LIVE", "no gate system needed, the posterior is valid at
# every moment". Paths 1 and 2 have four gates to clear; running any of them here would be a
# misreading of the framework, not extra rigour.

def full_information_counts(frame, arms):
    """Spark's half: the alpha/beta counts. One shuffle, gamma = 1, every arm every slot.

    This is the FULL-INFORMATION posterior -- the quantity a replay can see and a live bandit
    cannot, because it reads the reward of arms that were never pulled. It is computed for two
    reasons and neither is the bandit: it gives Step 10 real numbers to break, and it gives the
    replay below a reference rate to be compared against.
    """
    counts = (frame.groupBy("symbol").agg(
        func.sum("reward_strict").alias("wins"),
        func.sum((func.col("reward_strict") == 0).cast("int")).alias("losses"),
        func.sum(func.col("reward_strict").isNull().cast("int")).alias("unobserved"),
    ).orderBy("symbol").collect())

    # `or 0`: sum() over an all-null column returns NULL, not 0, and an arm whose every
    # reward is unobserved is a legitimate (if useless) input rather than a crash.
    alpha_beta = {r["symbol"]: (PRIOR_A + (r["wins"] or 0), PRIOR_B + (r["losses"] or 0))
                  for r in counts}
    LOG.info("   full-information counts (gamma=1, every arm at every slot -- NOT the bandit):")
    for row in counts:
        a, b = alpha_beta[row["symbol"]]
        n = a + b
        LOG.info("      %-9s wins %s losses %s unobserved %s | alpha %.1f beta %.1f "
                 "mean %.4f sd %.5f", row["symbol"], f"{row['wins'] or 0:,}",
                 f"{row['losses'] or 0:,}", row["unobserved"], a, b, a / n,
                 math.sqrt(a * b / (n * n * (n + 1))))
    return alpha_beta


def collect_replay_matrix(frame, arms, n_slots):
    """Pivot the rewards to one row per slot and bring them to the driver.

    Small by construction: one row per slot, one column per arm. There is no bandit anywhere in
    pyspark.ml, and a Structured Streaming foreachBatch would only relocate the same fifteen
    lines of conjugate arithmetic onto a different thread.
    """
    if n_slots > MAX_REPLAY_SLOTS:
        raise ValueError(
            f"{n_slots:,} slots exceeds the {MAX_REPLAY_SLOTS:,} driver cap -- the replay "
            f"matrix is collected, so re-run the entryway at a coarser --bar-interval or "
            f"narrow --calendar-start/--calendar-end rather than raising this on a Glue driver")

    matrix = [tuple(r) for r in
              (frame.groupBy("bar_us").pivot("symbol", arms)
               .agg(func.first("reward_strict"))
               .orderBy("bar_us")
               .select("bar_us", *arms).collect())]
    LOG.info("   replay matrix: %s slots x %s arms, first slot %s, last slot %s",
             f"{len(matrix):,}", len(arms), matrix[0], matrix[-1])
    return matrix


def replay(matrix, arms, seed, gamma=GAMMA):
    """One discounted Thompson-sampling pass over the slots in bar_us order.

    Returns (alpha, beta, pulls, observed) per arm. Only the pulled arm is read -- the other
    arms' rewards are sitting right there in `matrix` and are deliberately not looked at,
    because a live bandit would never have them.

    Two departures from the framework document, both deliberate:

    * The decay is applied to the PULLED ARM ONLY, matching the equation
      ``alpha_new = gamma*alpha + x`` literally. An arm that is not pulled does not age.
    * When the pulled arm's reward is unobserved -- an empty bar, or the bar after one -- the
      update is ``alpha <- gamma*alpha``, ``beta <- gamma*beta`` with NO INCREMENT: the decay
      alone. Imputing x = 0 would record a failure that did not happen and gamma would carry
      that fiction forward for ~1/(1-gamma) pulls. This is the arithmetic form of the Step 4
      override, and the framework never writes it down; it is an inference from the override.
    """
    rng = np.random.default_rng(seed)
    alpha = {a: PRIOR_A for a in arms}
    beta = {a: PRIOR_B for a in arms}
    pulls = {a: 0 for a in arms}
    observed = {a: 0 for a in arms}
    for row in matrix:
        rewards = dict(zip(arms, row[1:]))
        arm = max(arms, key=lambda a: rng.beta(alpha[a], beta[a]))
        pulls[arm] += 1
        x = rewards[arm]
        if x is None:
            alpha[arm] *= gamma            # decay alone: time passed, no evidence arrived
            beta[arm] *= gamma
        else:
            observed[arm] += 1
            alpha[arm] = gamma * alpha[arm] + x
            beta[arm] = gamma * beta[arm] + (1 - x)
    return alpha, beta, pulls, observed


def run_bandit(matrix, arms, alpha_beta, seed, replicates):
    """The seeded run, then the same replay over many seeds. Returns per-arm result rows.

    One trajectory of a DISCOUNTED bandit is a sample, not a result: gamma caps the effective
    sample size at ~1/(1-gamma) pulls per arm, so the posterior never becomes confident and the
    final argmax is genuinely seed-dependent. Reporting a single trajectory as "the winner" is
    reporting noise.
    """
    LOG.warning("CAVEAT -- this is a REPLAY of an archive in timestamp order, not a live "
                "stream. The file holds every arm's reward at every slot; the loop below hides "
                "the unpulled arms, but a live system could not have cheated in the first "
                "place, so no number here is evidence of production behaviour.")

    alpha, beta, pulls, observed = replay(matrix, arms, seed)
    total = sum(pulls.values())
    LOG.info("   seeded replay (seed=%s), %s pulls", seed, f"{total:,}")
    for arm in arms:
        a, b = alpha[arm], beta[arm]
        n = a + b
        fa, fb = alpha_beta[arm]
        LOG.info("      %-9s pulls %s (%.1f%%) observed %s | alpha %.3f beta %.3f "
                 "mean %.4f sd %.4f | full-info %.4f", arm, f"{pulls[arm]:,}",
                 100.0 * pulls[arm] / total, f"{observed[arm]:,}", a, b, a / n,
                 math.sqrt(a * b / (n * n * (n + 1))), fa / (fa + fb))

    # Seeds are derived from the base seed rather than drawn, so the whole job is reproducible:
    # the same bars and the same --seed give byte-identical outputs, which is what makes a
    # backfill comparable to the incremental run that it replaces.
    reps = [replay(matrix, arms, seed + i) for i in range(replicates)]
    share = {a: np.array([r[2][a] / total for r in reps]) for a in arms}
    wins = {a: sum(1 for al, be, _, _ in reps
                   if max(arms, key=lambda k: al[k] / (al[k] + be[k])) == a)
            for a in arms}

    LOG.info("   %s independent replays of the same %s slots:", replicates, f"{total:,}")
    rows = []
    for arm in arms:
        s = share[arm]
        a, b = alpha[arm], beta[arm]
        n = a + b
        fa, fb = alpha_beta[arm]
        LOG.info("      %-9s budget share mean %6.1f%% sd %.3f min %5.1f%% max %5.1f%% | "
                 "won the run %5.1f%% | full-info rate %.4f", arm, 100 * s.mean(), s.std(),
                 100 * s.min(), 100 * s.max(), 100 * wins[arm] / replicates, fa / (fa + fb))
        rows.append({
            "symbol": arm, "pulls": pulls[arm], "observed": observed[arm],
            "budget_share": pulls[arm] / total,
            "alpha": a, "beta": b, "posterior_mean": a / n,
            "posterior_sd": math.sqrt(a * b / (n * n * (n + 1))),
            "full_info_alpha": fa, "full_info_beta": fb, "full_info_rate": fa / (fa + fb),
            "share_mean": float(s.mean()), "share_sd": float(s.std()),
            "share_min": float(s.min()), "share_max": float(s.max()),
            "won_final_argmax": wins[arm] / replicates,
        })

    top_share = max(arms, key=lambda a: share[a].mean())
    top_argmax = max(arms, key=lambda a: wins[a])
    verdict(True, f"Path 3 engine: {total:,} pulls replayed, budget concentrates on {top_share} "
                  f"({share[top_share].mean():.1%} mean share over {replicates} seeds), final "
                  f"argmax is {top_argmax} in {wins[top_argmax] / replicates:.1%} of runs -- "
                  f"gate system bypassed, no gate is run on this path")
    if top_share != top_argmax:
        LOG.warning("CAVEAT -- budget share and final argmax disagree (%s vs %s). Argmax of the "
                    "final posterior is a snapshot of the last ~%.0f pulls; budget share is the "
                    "whole trajectory. Reporting either alone as 'the winner' is reporting "
                    "noise.", top_share, top_argmax, 1 / (1 - GAMMA))
    return rows


ARMS_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("pulls", LongType(), False),
    StructField("observed", LongType(), False),
    StructField("budget_share", DoubleType(), False),
    StructField("alpha", DoubleType(), False),
    StructField("beta", DoubleType(), False),
    StructField("posterior_mean", DoubleType(), False),
    StructField("posterior_sd", DoubleType(), False),
    StructField("full_info_alpha", DoubleType(), False),
    StructField("full_info_beta", DoubleType(), False),
    StructField("full_info_rate", DoubleType(), False),
    StructField("share_mean", DoubleType(), False),
    StructField("share_sd", DoubleType(), False),
    StructField("share_min", DoubleType(), False),
    StructField("share_max", DoubleType(), False),
    StructField("won_final_argmax", DoubleType(), False),
])

# Columns carried on the per-bar frame for auditability but never read by an engine.
FEATURE_COLS = ["symbol", "bar_us", "bar_open_time_utc", "close", "prev_close", "volume",
                "taker_buy_qty", "taker_ratio", "regime_token", "n_ticks", "is_missing_bar",
                "reward_strict", "reward_loose", "direction",
                "hour_utc", "minute_utc", "hod_sin", "hod_cos", "moh_sin", "moh_cos"]


def write_outputs(spark, out, frame, baskets, arm_rows):
    """Three artifacts under one prefix, because they are one path's output written together.

    features/  the per-bar Path 3 frame -- Steps 5 and 6, auditable against the bars
    frame/     the interaction frame -- Step 7, the raw-token baskets the bans protect
    arms/      the bandit posterior -- the engine's result, and glue-dynamo.py's input
    """
    base = out.rstrip("/")
    features = frame.select(*FEATURE_COLS)
    features.write.mode("overwrite").parquet(f"{base}/features")
    baskets.write.mode("overwrite").parquet(f"{base}/frame")

    arms_df = spark.createDataFrame(
        [tuple(r[f.name] for f in ARMS_SCHEMA.fields) for r in arm_rows], ARMS_SCHEMA)
    # One file: it is one row per arm and glue-dynamo.py reads the whole thing.
    arms_df.coalesce(1).write.mode("overwrite").parquet(f"{base}/arms")

    LOG.info("wrote features -> %s/features | frame -> %s/frame | arms (%s rows) -> %s/arms",
             base, base, len(arm_rows), base)


def self_check():
    """Assert the conjugate loop's three silent-failure modes. No Spark, no data, no files.

    replay() is the only non-trivial arithmetic in this job and every way it can be wrong is
    quiet: a wrong decay still returns a valid Beta, imputing x = 0 for an unobserved reward
    still returns a valid Beta, and ageing the unpulled arms still returns a valid Beta. All
    three produce plausible posteriors and a plausible winner. These are the checks that fail.
    """
    arms = ["A", "B"]
    n = 200

    # 1. An unobserved reward DECAYS WITHOUT INCREMENTING, and an arm that is not pulled does
    #    not age. Every reward is None here, so alpha is PRIOR_A * gamma^(its own pulls) and
    #    nothing else -- which pins the decay, the missing increment and the per-arm ageing at
    #    once. Impute x = 0 instead and beta climbs toward 1/(1-gamma) ~ 33 rather than falling
    #    to zero, i.e. the engine records ~33 failures that never happened.
    alpha, beta, pulls, observed = replay([(i, None, None) for i in range(n)], arms, seed=1)
    assert sum(pulls.values()) == n, f"{sum(pulls.values())} pulls over {n} slots"
    assert sum(observed.values()) == 0, "an all-None matrix observed a reward"
    for arm in arms:
        assert abs(alpha[arm] - PRIOR_A * GAMMA ** pulls[arm]) < 1e-12, f"{arm} alpha decayed wrong"
        assert abs(beta[arm] - PRIOR_B * GAMMA ** pulls[arm]) < 1e-12, f"{arm} beta decayed wrong"

    # 2. The observed update is exactly alpha <- gamma*alpha + x, beta <- gamma*beta + (1-x).
    #    One arm, so the draw cannot change which arm is pulled and the recurrence is closed.
    alpha, beta, pulls, observed = replay([(i, 1) for i in range(n)], ["A"], seed=1)
    expect_a, expect_b = PRIOR_A, PRIOR_B
    for _ in range(n):
        expect_a, expect_b = GAMMA * expect_a + 1, GAMMA * expect_b
    assert observed["A"] == n and pulls["A"] == n
    assert abs(alpha["A"] - expect_a) < 1e-9, f"alpha {alpha['A']} != {expect_a}"
    assert abs(beta["A"] - expect_b) < 1e-9, f"beta {beta['A']} != {expect_b}"
    # The saturation the gamma comment claims: an always-winning arm approaches 1/(1-gamma)
    # FROM BELOW and never reaches it -- after n pulls it is short by (cap - PRIOR_A)*gamma^n,
    # which is 0.073 at n=200. Asserting equality to the cap is what a first draft of this
    # check did, and it failed: the cap is a limit, not a value the recurrence attains.
    cap = 1 / (1 - GAMMA)
    assert alpha["A"] < cap, "the evidence count exceeded 1/(1-gamma)"
    assert cap - alpha["A"] < 0.1, f"alpha saturated at {alpha['A']:.3f}, not near {cap:.3f}"

    # 3. Reproducibility, which is what makes a backfill comparable to the run it replaces.
    #    A None in the matrix must not make the seeding drift either, so the mixed case is the
    #    one that is repeated.
    mixed = [(i, None if i % 7 == 0 else i % 2, None if i % 5 == 0 else (i // 2) % 2)
             for i in range(n)]
    assert replay(mixed, arms, seed=7) == replay(mixed, arms, seed=7), "replay is not seeded"
    assert replay(mixed, arms, seed=7) != replay(mixed, arms, seed=8), "the seed does nothing"

    LOG.info("self-check passed: decay-without-increment, the conjugate recurrence, "
             "gamma saturation at %.1f, and seeded reproducibility", 1 / (1 - GAMMA))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input",
                        help="prefix holding the bars Parquet written by glue-ingest-bars.py "
                             "(s3:// or a local path)")
    parser.add_argument("--output",
                        help="destination prefix; features/, frame/ and arms/ are written "
                             "beneath it")
    parser.add_argument("--seed", type=int, default=20250101,
                        help="base seed for the Thompson draws; replicate i uses seed+i, so "
                             "the whole job is reproducible from this one number")
    parser.add_argument("--seed-replicates", type=int, default=200,
                        help="independent replays used to summarise budget share; a single "
                             "trajectory of a discounted bandit is a sample, not a result. "
                             "Cost is linear in slots x replicates -- see README for the "
                             "measured per-replay time before raising it on a full month")
    parser.add_argument("--local", action="store_true",
                        help="run against the local filesystem with master local[*]")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the conjugate loop against known answers and exit; no "
                             "Spark, no input, no output -- run it before trusting a run")
    parser.add_argument("--shuffle-partitions", type=int, default=None,
                        help="spark.sql.shuffle.partitions under --local; 8 suits the "
                             "committed sample, leave unset for a full month")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run and a strict parser exits 2 on them before Spark ever starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return
    if args.seed_replicates < 1:
        raise ValueError("--seed-replicates must be at least 1")

    # Checked here rather than by required=True so that --self-check needs neither of them;
    # argparse would reject the flag combination before self_check() could run.
    if not args.input or not args.output:
        parser.error("--input and --output are required unless --self-check is given")

    spark = build_session(APP_NAME, args.local, args.shuffle_partitions)
    try:
        bars, arms, interval_us, n_slots, rows = load_bars(
            spark, args.input, REQUIRED_COLS, "Path 3")
        LOG.info("Spark %s | tz %s | local=%s", spark.version,
                 spark.conf.get("spark.sql.session.timeZone"), args.local)
        LOG.info("Path 3 input: %s bars | %s slots x %s arms %s | step %sus (%.4gs)",
                 f"{rows:,}", f"{n_slots:,}", len(arms), arms, f"{interval_us:,}",
                 interval_us / 1e6)
        LOG.info("gamma=%s (per pull, ~%.0f pulls of memory = ~%.1f minutes at this bar width)",
                 GAMMA, 1 / (1 - GAMMA), interval_us / 1e6 / (1 - GAMMA) / 60)

        step4_native_missingness(bars, rows)

        frame = step5_rewards(bars)
        # Scanned repeatedly from here: the diagnostics, the direction pivot, the token
        # explode, the full-information counts and the replay pivot are five passes over the
        # same derived frame, and the lag() window behind it is a full sort per symbol.
        frame = frame.cache()
        step5a_stream_diagnostics(frame, interval_us, rows)
        step5b_reward_definition(frame, arms)

        frame = step6_cyclical(frame)
        frame, baskets, n_tokens = step7_interaction_frame(frame, arms, n_slots)
        frame = frame.cache()

        step8_pruning_banned(n_tokens, n_slots)
        step9_regularisation_banned()

        alpha_beta = full_information_counts(frame, arms)
        step10_scaling_banned(alpha_beta, arms)

        matrix = collect_replay_matrix(frame, arms, n_slots)
        arm_rows = run_bandit(matrix, arms, alpha_beta, args.seed, args.seed_replicates)

        write_outputs(spark, args.output, frame, baskets, arm_rows)

        LOG.info("Path 3 complete -- Steps 4-10 applied or banned with reasons; the reward is "
                 "a strict up-tick on a replayed archive, and the arm ordering it produces is "
                 "an ordering of tick-flatness, not a trading edge")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
