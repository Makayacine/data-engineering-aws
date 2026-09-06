"""Path 3 -- the bandit sub-refinery over the mailer star: Steps 4-10, then the engine.

AWS Glue **Python Shell** entrypoint, not Spark. The framework's Steps 1-3 -- acquisition,
cleaning, the type contract -- are done by the pipeline that fills the star: the extraction job
lands the mailer table in the raw zone, the raw-ingestion job merges it, and the processed-layer
job builds ``fact_mailer`` against ``dim_offer_arm``. By the time this job starts, every row it
reads has a risk band, an offer rate, an arm and an outcome. So it runs the Path 3 column of the
Steps 4-10 matrix and then the engine, and nothing else::

      processed_zone.fact_mailer   x   processed_zone.dim_offer_arm
  ->  Step 4   Imputation      OVERRIDE -- native missingness kept as a latent state, no fill
  ->  Step 5   Diagnostics     APPLIES  -- exposure and reward per (band, wave, arm); positivity
  ->  Step 6   Topology        N/A      -- an ordinal batch index has no cycle to encode
  ->  Step 7   Feature Eng     APPLIES  -- the interaction frame is the (band x arm) cell
  ->  Step 8   Pruning         BANNED   -- variance pruning deletes the rare arms first
  ->  Step 9   Regularisation  BANNED   -- L1 shrinks a thin arm back onto the prior
  ->  Step 10  Scaling         BANNED   -- a standardised count is not a Beta parameter
  ->  engine   Contextual Beta-Bernoulli Thompson sampling, one batched round per wave
  ->  processed_zone.bandit_posterior  +  processed_zone.bandit_policy_value

Every badge is printed through verdict(), including the four that forbid or excuse a step. A
step that leaves no line in the log is indistinguishable from a step nobody thought of, and the
three bans are the substance of this path rather than its footnotes.

TWO THINGS STATED UP FRONT, BECAUSE BOTH ARE EASY TO MISREAD AS RESULTS
----------------------------------------------------------------------

1.  **This is a replay of a completed cross-section, not a live bandit.** Every mailer in the
    star was posted in 2003, the interest rate on it was drawn by the lender's own
    randomisation, and the outcome was recorded years before this job existed. No arm below was
    ever *chosen* by this posterior. What the engine does is read the experiment in wave order
    and update as if each wave had arrived in turn, which imitates arrival order and imitates
    nothing else: no client is ever re-solicited, no arm is added mid-flight, and the file holds
    the outcome of every arm the experiment ran rather than only the one a policy picked.
    Reporting the argmax of the final posterior as "the rate the lender should have offered" is
    the misreading this paragraph exists to prevent.

2.  **The propensities are ESTIMATED, not known -- and that changes what the estimator is.**
    The randomisation design document is not published with the deposit, so the probability
    that a client of a given band was assigned a given price arm is not available. What is
    available is the realised assignment frequency inside each (band, evaluation wave) cell,
    and that is what ``e(a|x)`` below is. It is an unbiased estimate of the design probability
    because the rate genuinely was randomised. It is not the same guarantee, and the algebra
    says why. With ``e`` estimated on the same rows the SNIPS ratio

        V = sum_i (pi_i / e_i) y_i  /  sum_i (pi_i / e_i)

    collapses exactly -- not approximately -- to ``sum_a pi(a|x) * (k_a / n_a)``: the sample
    size cancels out of both sums and what is left is the policy-weighted average of the
    per-arm empirical take-up rates. A self-normalised importance-weighted estimator with
    estimated propensities IS a direct-method estimator wearing an importance-weighted name.
    The self-check pins that identity against a hand-computed case rather than leaving it as a
    claim, and the counts form is the one the code evaluates, because it is the cheaper
    arithmetic and it is the same number.

WHY THERE IS NO GAMMA, WHEN THE CRYPTO REFINERY'S PATH 3 HAS ONE
---------------------------------------------------------------

The sibling project (``crypto-ticks-refinery-glue-dynamo``) runs the same engine with a discount
of 0.97 per pull, and a reader who has seen it will look for the constant here. There is none,
and its absence is a decision rather than an omission.

A discount exists to forget: it caps an arm's effective sample size so a non-stationary arm can
be re-learned. Non-stationarity needs an arm whose reward distribution moves while the bandit
watches it. On this data each client is solicited exactly once, so no arm ever accumulates a
second observation from the same unit, and the reward is a property of a randomised assignment
that was fixed before the first mailer went out. There is nothing for a discount to track.
Worse, gamma counts PULLS -- copy 0.97 onto three batches of thousands of rows and it decays the
whole of wave 1 into irrelevance before wave 2 is finished, which would be a forgetting rate of
the framework's stream, applied to a batch design where it means nothing.

The batched update is therefore plain conjugacy: round r adds ``(k, n - k)`` to ``(alpha, beta)``
for each arm, and the final posterior after all rounds is identical to one batch over the pooled
counts. The self-check asserts exactly that, because it is the property that makes the wave
staging a matter of reporting rather than a matter of arithmetic.

WHAT THIS JOB READS: A HANDFUL OF ROWS, NOT THE WHOLE FACT
----------------------------------------------------------

A Beta-Bernoulli posterior's sufficient statistic is ``(successes, trials)``. The SNIPS form
above needs the same two numbers per arm and nothing else, and so -- because a bootstrap
resample of rows within a cell is exactly a multinomial draw over the (arm, outcome) categories
-- does the bootstrap. So the fact table is aggregated in the warehouse, by
``GROUP BY wave, risk_band, arm_id``, and this job pulls one row per arm per wave rather than
one row per mailer. The whole of the arithmetic below runs on arrays of six numbers.

That is not an optimisation for its own sake. This is a Python Shell container, and the
alternative -- ``fetchall()`` over the fact table, then a ``(replicates x rows)`` index matrix
for the bootstrap -- allocates hundreds of megabytes to compute a statistic that depends on
eighteen pairs of integers. The aggregate also runs where the data is, which is the reason to
have a warehouse.

The one thing the aggregate cannot see is a row-level null, so Step 4's evidence is a second,
separate aggregate over the treatment flags. It is the only query in the job that touches every
row, and it returns one row per wave.

WHAT THE CHECKS AND THE LOCAL RUN COVER
---------------------------------------

*   ``--self-check`` pins the pure arithmetic with no database, no network and no input: the
    conjugate update and its batch-invariance, the arm binner against all eighteen declared
    bands including the exact cut points, the SNIPS identity against a hand-computed two-arm
    case, and the independence of the two random streams.
*   The arm grid this job declares is checked against the ``dim_offer_arm`` rows the DDL seeded,
    boundary by boundary, before any count is read. Two independent statements of the same grid
    would normally be a duplication to delete; here the duplication IS the check, because a
    mis-seeded dimension would silently change what "arm 3" means between runs and the posterior
    would then accumulate counts across two different price bands under one arm_id.
*   The engine, its arithmetic and both output tables are exercised end to end on the local
    warehouse. What changes on a cluster is the write surface rather than the numbers, with one
    thing worth watching: ``ts_probability`` and the SNIPS columns are ``DECIMAL(10,8)``, so a
    difference in how the two engines round at the eighth decimal would show there first.

WHAT THE SEED PINS AND WHAT IT DOES NOT
---------------------------------------

The posterior columns are deterministic functions of the counts: no seed touches them, and two
runs over the same star produce byte-identical ``alpha``, ``beta``, ``posterior_mean`` and
``posterior_sd``. Everything else on this page is a Monte-Carlo estimate. ``ts_probability`` is
the share of Thompson draws an arm wins, the policy value inherits that error through ``pi``,
and the interval and the tail probability are bootstrap quantiles. The standard error of a
probability estimated from D draws is at most ``1 / (2 * sqrt(D))``, so those columns are stable
in their leading digits and move in their last ones. They are reproducible for a given seed,
draw count, replicate count AND consumption order -- change any one of the four, including by
reordering the loop that consumes the stream, and the last decimals move.

Two generators are built, one for the Thompson draws and one for the bootstrap, and they are
spawned from one seed sequence so that neither consumes the other's stream. Sharing a single
generator is the version of this that produces plausible numbers and a real bug: the reported
policy value would then depend on how many bootstrap replicates had already run, so raising
``--bootstrap-replicates`` would silently change the point estimate it was supposed to put an
interval around.

Local acceptance run, against the DuckDB warehouse the rest of the pipeline builds. Every
statement this job issues is plain ``SELECT`` / ``DELETE`` / ``INSERT``, so unlike the raw
ingestion job there is no dialect split at all: the SQL text is the same for both engines::

    python glue-jobs/glue-refinery-path3.py --self-check

    python glue-jobs/glue-refinery-path3.py --local \\
        --local-db _localrun/warehouse.duckdb --dry-run

    python glue-jobs/glue-refinery-path3.py --local \\
        --local-db _localrun/warehouse.duckdb
"""

import argparse
import bisect
import logging
import re

import numpy as np

# Shared with redshift-raw-ingestion.py and redshift-processed-layer.py. On Glue this needs
#   --extra-py-files s3://<bucket>/jobs/warehouse_common.py
# on the job definition. Locally nothing is needed: Python puts the running script's directory
# on sys.path, so the import resolves to the file next to this one.
from warehouse_common import add_local_arguments, connect, run

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_refinery_path3")

# The context. Order is the arm grid's own -- HIGH, MEDIUM, LOW -- and it is fixed rather than
# read off the data, because it is also the order in which the Thompson generator is consumed.
BANDS = ("HIGH", "MEDIUM", "LOW")

# arm_id = ARM_BASE[band] + arm_index + 1, which is the numbering the DDL seeds.
ARM_BASE = {"HIGH": 0, "MEDIUM": 6, "LOW": 12}
ARMS_PER_BAND = 6

# The declared price cuts, one list of five per band, giving six arms each. DECLARED, not
# quantiles of the arriving data: a grid recomputed per run makes round 1's "arm 3" a different
# price band from round 3's "arm 3", and the posterior would then be adding counts across two
# arms that are not the same arm. Same argument as the sibling project's declared bar calendar.
#
# Every cut is a multiple of 0.25 and therefore exact as a double, so the boundary comparison
# below is exact and a rate that equals a cut lands in the arm above with no rounding to argue
# about.
CUTS = {
    "HIGH": (5.50, 7.50, 9.00, 10.00, 11.00),
    "MEDIUM": (5.00, 6.75, 7.50, 8.25, 9.25),
    "LOW": (4.50, 5.50, 6.00, 6.75, 7.50),
}

# Beta(1, 1): uniform on [0, 1], no arm favoured before the first mailer.
PRIOR_A, PRIOR_B = 1.0, 1.0

# The 19 treatment flags fact_mailer carries with their native missingness. Listed here because
# Step 4's evidence is a count of how many of them are wholly NULL in a wave, and that count is
# measured on the run that prints it rather than asserted from the README.
TREATMENT_FLAGS = ("prize", "intshown", "dphoto_female", "dphoto_none", "dphoto_black",
                   "gender_match", "race_match", "nspeakeligible", "speak_trt", "oneln_trt",
                   "comploss_n", "use_any", "stripany", "comp_n", "deadlinemed", "deadlinelong",
                   "deadlong_elig", "deadshort_elig", "deadlineshortext")

# 2003 is the year of the experiment; the default is a fixed number so a rerun reproduces a run.
DEFAULT_SEED = 20031001
DEFAULT_DRAWS = 200_000
DEFAULT_REPLICATES = 2_000

# A percentile bootstrap interval, two-sided at 95%.
CI_PERCENTILES = (2.5, 97.5)

# run_id is half the primary key of both result tables and is interpolated into SQL text. The
# charset is narrow on purpose: this is the only value in the job that comes from outside the
# warehouse, and VARCHAR(32) is what the DDL declares.
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,32}$")


def verdict(applies, message, banned=False, override=False):
    """Section verdict. The framework's states, printed rather than implied.

    APPLIES  -- the check ran and there is work to do.
    N/A      -- the check ran and found nothing. Never printed on an assumption: the N/A below
                is backed by a count measured on the run that prints it.
    OVERRIDE -- the framework's default operation is REPLACED by a different one on this path.
    BANNED   -- forbidden on this path, and `message` is the reason, not an apology.

    Returns False for BANNED as well as for N/A, so ``if verdict(...):`` can never gate a
    handler the framework forbids.

    LIMIT and ENFORCE are the two badges the vocabulary also defines and this path never earns;
    they are not exposed here, so a later edit cannot reach for one by accident. The sibling
    project keeps all five in its shared helper because three jobs import it and a shared
    definition is the answer to drift between copies. This project has no such helper --
    warehouse_common.py holds the connection and nothing that knows what a step is -- so this
    copy carries exactly the badges this job uses.
    """
    label = ("BANNED   -- " if banned
             else "OVERRIDE -- " if override
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    LOG.info("%s%s", label, message)
    return bool(applies) and not banned


# ------------------------------------------------------------------------------------------
# THE ARM GRID
# ------------------------------------------------------------------------------------------

def arm_index_for_rate(band, rate):
    """Which of the band's six arms a monthly offer rate falls in.

    ``bisect_right`` is exactly the definition wanted: the number of declared cuts that are less
    than or equal to the rate. A rate equal to a cut therefore belongs to the arm ABOVE it,
    which matches ``rate_floor <= rate < rate_ceil`` with the top arm open above -- and the
    bottom arm open below, since nothing in the grid caps how low a rate may be.
    """
    return bisect.bisect_right(CUTS[band], rate)


def as_float(value):
    """DECIMAL columns arrive as Decimal from both drivers; nulls stay None."""
    return None if value is None else float(value)


def load_arm_grid(cursor):
    """Read dim_offer_arm and prove it is the grid this job declares. Returns {arm_id: (band, i)}.

    dim_offer_arm is seed data, written by the DDL and never by the pipeline, so it is the one
    input here that a human edits. Checking it costs one query over eighteen rows and catches
    the failure that is otherwise invisible: an edited cut point does not break any join, does
    not drop any row and does not raise anything -- it just makes this run's "MEDIUM arm 3" a
    different price band from last run's, while both are written under arm_id 10 into a table
    keyed by arm_id.
    """
    cursor.execute("SELECT arm_id, risk_band, arm_index, rate_floor, rate_ceil "
                   "FROM processed_zone.dim_offer_arm ORDER BY arm_id")
    rows = cursor.fetchall()
    expected = len(BANDS) * ARMS_PER_BAND
    if len(rows) != expected:
        raise ValueError("processed_zone.dim_offer_arm holds {} rows, not {} -- the arm grid is "
                         "seed data and an incomplete one would drop fact rows on the join that "
                         "builds fact_mailer".format(len(rows), expected))

    grid = {}
    by_band = {band: {} for band in BANDS}
    for arm_id, band, index, floor, ceil in rows:
        if band not in CUTS:
            raise ValueError("dim_offer_arm row {} has risk_band {!r}, which is not one of "
                             "{}".format(arm_id, band, list(BANDS)))
        if arm_id != ARM_BASE[band] + index + 1:
            raise ValueError("dim_offer_arm arm_id {} is {} arm {}, but the declared numbering "
                             "puts that arm at {}".format(arm_id, band, index,
                                                          ARM_BASE[band] + index + 1))
        grid[arm_id] = (band, index)
        by_band[band][index] = (as_float(floor), as_float(ceil))

    for band in BANDS:
        arms = by_band[band]
        if sorted(arms) != list(range(ARMS_PER_BAND)):
            raise ValueError("dim_offer_arm gives {} the arm indices {}, not 0..{}".format(
                band, sorted(arms), ARMS_PER_BAND - 1))
        # The five interior boundaries are read twice -- as the ceiling of one arm and as the
        # floor of the next -- so this compares the seeded grid against the declared cuts AND
        # proves the seeded arms are contiguous, which is what makes "the arm above" well
        # defined for a rate that falls exactly on a cut.
        ceilings = tuple(arms[i][1] for i in range(ARMS_PER_BAND - 1))
        floors = tuple(arms[i][0] for i in range(1, ARMS_PER_BAND))
        if ceilings != CUTS[band] or floors != CUTS[band]:
            raise ValueError("dim_offer_arm's {} boundaries are ceilings {} / floors {}, and "
                             "this job declares the cuts {} -- the seeded grid and the job "
                             "disagree about what an arm is".format(band, ceilings, floors,
                                                                    CUTS[band]))
        if arms[ARMS_PER_BAND - 1][1] is not None:
            raise ValueError("dim_offer_arm's top {} arm has a rate_ceil; the top arm is open "
                             "above, so a rate beyond the last cut has nowhere else to go"
                             .format(band))
    LOG.info("arm grid: %s arms over %s bands, boundaries match the declared cuts", len(grid),
             len(BANDS))
    return grid


# ------------------------------------------------------------------------------------------
# READING THE STAR
# ------------------------------------------------------------------------------------------

def load_cells(cursor, grid):
    """Aggregate the fact in the warehouse. Returns {(wave, band): (rewards[6], pulls[6])}.

    One row per (wave, arm), which is the sufficient statistic for every number this job
    computes. MIN and MAX of the rate come back with the counts so the binner can be checked
    against what the warehouse actually assigned, at no extra scan.
    """
    cursor.execute("SELECT wave, risk_band, arm_id, COUNT(*) AS pulls, "
                   "SUM(took_up) AS rewards, MIN(offer_rate) AS min_rate, "
                   "MAX(offer_rate) AS max_rate "
                   "FROM processed_zone.fact_mailer "
                   "GROUP BY wave, risk_band, arm_id "
                   "ORDER BY wave, arm_id")
    rows = cursor.fetchall()
    if not rows:
        raise ValueError("processed_zone.fact_mailer is empty -- the processed-layer job has "
                         "not run, and there is nothing for Path 3 to read")

    cells = {}
    for wave, band, arm_id, pulls, rewards, min_rate, max_rate in rows:
        wave = int(wave)
        if arm_id not in grid:
            raise ValueError("fact_mailer carries arm_id {}, which dim_offer_arm does not "
                             "define".format(arm_id))
        grid_band, index = grid[arm_id]
        if grid_band != band:
            raise ValueError("fact_mailer row for arm {} says band {!r}; dim_offer_arm says "
                             "{!r}".format(arm_id, band, grid_band))
        # The binner is the job's own statement of the grid, run against the rates the
        # warehouse's own join produced. If the two disagree, one of them has the wrong idea of
        # which arm a price belongs to, and every count below is attributed to the wrong arm.
        for rate in (as_float(min_rate), as_float(max_rate)):
            if arm_index_for_rate(band, rate) != index:
                raise ValueError("fact_mailer puts rate {} in {} arm {}, and the declared cuts "
                                 "{} put it in arm {}".format(rate, band, index, CUTS[band],
                                                              arm_index_for_rate(band, rate)))
        key = (wave, band)
        if key not in cells:
            cells[key] = (np.zeros(ARMS_PER_BAND), np.zeros(ARMS_PER_BAND))
        cells[key][0][index] = float(rewards or 0)
        cells[key][1][index] = float(pulls)

    waves = sorted({wave for wave, _ in cells})
    for wave in waves:
        for band in BANDS:
            # A band absent from a wave is not an error to raise here -- it is a positivity
            # failure, and Step 5 is where positivity is reported and where the consequence is
            # explained. Zeros keep the arrays the same shape so that reporting can happen.
            cells.setdefault((wave, band), (np.zeros(ARMS_PER_BAND), np.zeros(ARMS_PER_BAND)))
    total = sum(int(p.sum()) for _, p in cells.values())
    LOG.info("fact_mailer: %s mailers over waves %s, aggregated to %s (wave, arm) rows",
             "{:,}".format(total), waves, len(rows))
    return cells, waves


# ------------------------------------------------------------------------------------------
# STEP 4 -- IMPUTATION  (OVERRIDE: keep the native missingness)
# ------------------------------------------------------------------------------------------

def step4_native_missingness(cursor, waves):
    """Count the NULL treatment flags per wave, then carry them through untouched.

    The only query in this job that reads every fact row, and it exists because an aggregate
    over arms cannot see a null. It returns one row per wave.

    Wave 1 was a price-only experiment: the advertising layout arms and the deadline arms did
    not exist yet, so their columns are NULL for every wave-1 mailer. That NULL is a fact about
    the experiment, not a defect in the extract. A median fill, a zero fill, a mode fill or a
    complete-case drop would each state that the arm existed and that the client was not in it,
    which is a different experiment from the one that was run. The bandit does not read these
    columns at all -- its context is the risk band and its action is the price arm -- so there
    is not even a convenience argument for filling them.
    """
    nulls = ", ".join("SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}".format(c=col)
                      for col in TREATMENT_FLAGS)
    cursor.execute("SELECT wave, COUNT(*) AS n_rows, {} FROM processed_zone.fact_mailer "
                   "GROUP BY wave ORDER BY wave".format(nulls))

    wholly_null, partial, total_rows = {}, 0, 0
    for row in cursor.fetchall():
        wave, n_rows = int(row[0]), int(row[1])
        counts = [int(value) for value in row[2:]]
        wholly_null[wave] = sum(1 for value in counts if value == n_rows)
        partial += sum(1 for value in counts if 0 < value < n_rows)
        total_rows += n_rows
        LOG.info("   wave %-2s %9s rows | %2s of %s treatment flags NULL on every row | "
                 "%s NULL cells in total", wave, "{:,}".format(n_rows), wholly_null[wave],
                 len(TREATMENT_FLAGS), "{:,}".format(sum(counts)))

    latent = [wave for wave in waves if wholly_null.get(wave, 0) > 0]
    verdict(bool(latent),
            "Step 4 native missingness: wave(s) {} carry NULL in whole treatment flags -- the "
            "arms did not exist in those waves. Not filled, not dropped, not coalesced; the "
            "row count is unchanged at {} and the NULL is the latent state"
            .format(latent or "none", "{:,}".format(total_rows)), override=True)

    if partial:
        # Wholly-null is a design fact. Partially-null is a data fault, and the two would look
        # identical in a fill.
        LOG.warning("%s treatment flag(s) are null on SOME rows of a wave and not others, which "
                    "is not what a wave-level arm looks like -- inspect before trusting the "
                    "flags", partial)

    verdict(False, "Step 4 impute: median/mode fill, zero fill, forward fill from the previous "
                   "wave and complete-case drop are all rejected -- each asserts that an arm "
                   "existed in a wave that did not run it, and the complete-case drop would "
                   "delete the price-only wave entirely", banned=True)


# ------------------------------------------------------------------------------------------
# STEP 5 -- DIAGNOSTICS  (exposure, reward, positivity)
# ------------------------------------------------------------------------------------------

def step5_diagnostics(cells, waves):
    """Exposure and reward per (band, wave, arm), then the positivity check. Returns the minimum.

    Positivity is the assumption every off-policy estimate rests on: an arm the logging policy
    never played in a cell has ``e(a|x) = 0``, so ``pi/e`` is either undefined or unbounded, and
    an estimator that quietly drops such an arm is answering a question about a different policy
    -- the one that never plays it. Reported as a number rather than assumed, per wave and per
    band, because that is the grain at which the evaluation below actually divides.
    """
    smallest = None
    for wave in waves:
        for band in BANDS:
            rewards, pulls = cells[(wave, band)]
            cell_min = int(pulls.min())
            if smallest is None or cell_min < smallest[0]:
                smallest = (cell_min, wave, band, int(pulls.argmin()))
            LOG.info("   wave %-2s %-6s n %8s take-up %6.4f | arms %s", wave, band,
                     "{:,}".format(int(pulls.sum())),
                     rewards.sum() / pulls.sum() if pulls.sum() else float("nan"),
                     " ".join("{:.0f}/{:.0f}".format(k, n) for k, n in zip(rewards, pulls)))

    count, wave, band, arm = smallest
    verdict(True, "Step 5 diagnostics: exposure and reward counted for {} (band, wave, arm) "
                  "cells; the smallest is {} rows ({} wave {} arm {}){}"
            .format(len(waves) * len(BANDS) * ARMS_PER_BAND, "{:,}".format(count), band, wave,
                    arm, ", so the propensity denominator is non-zero everywhere" if count
                    else " -- POSITIVITY DOES NOT HOLD on this run"))
    if not count:
        # Not raised: the run is still worth having, and the posterior for every other arm is
        # unaffected. But the evaluation over that cell silently becomes the value of a
        # different policy -- the one that never plays the unexposed arm -- because snips()
        # drops the arm and renormalises the rest, which is arithmetic that cannot fail and
        # therefore cannot warn for itself.
        LOG.warning("CAVEAT -- %s wave %s arm %s was never played, so its propensity is zero. "
                    "The value reported for that cell is the value of the policy restricted to "
                    "the arms that were played, not of the policy in the posterior table.",
                    band, wave, arm)


# ------------------------------------------------------------------------------------------
# STEP 6 -- TOPOLOGY  (N/A, and measured)
# ------------------------------------------------------------------------------------------

def step6_topology(cursor, waves):
    """The framework's Path 3 topology entry is cyclical time coordinates. There is no cycle.

    A sin/cos pair encodes a coordinate that WRAPS -- hour of day, minute of hour, day of week
    -- so that the last value is adjacent to the first. ``wave`` is an ordinal batch index: the
    mailer waves ran once each, in order, and wave 3 is not adjacent to wave 1. Encoding it
    cyclically would place the last batch of the experiment next to the first and hand the model
    a neighbourhood that does not exist.

    The N/A is backed by measurement rather than by the argument, and both halves of it are
    measured on the run that prints them: the wave count comes from the fact itself, and the
    absence of any time-typed column is read out of the catalogue below rather than asserted
    from the DDL. A hand-written "there is no timestamp" would be a claim about the schema as
    it was on the day this was written, which is exactly the claim that stops being true first.
    """
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns "
                   "WHERE table_schema = 'processed_zone' AND table_name = 'fact_mailer'")
    columns = cursor.fetchall()
    if not columns:
        raise ValueError("information_schema reports no columns for processed_zone.fact_mailer "
                         "-- Step 6 cannot report N/A on a table it cannot see")
    temporal = sorted(name for name, data_type in columns
                      if any(token in data_type.upper()
                             for token in ("TIMESTAMP", "DATE", "TIME", "INTERVAL")))
    if temporal:
        raise ValueError(
            "fact_mailer now carries {}, so Step 6 is no longer N/A on this star: a cyclical "
            "coordinate may be derivable and this verdict has to be re-argued rather than "
            "re-printed".format(", ".join(temporal)))

    verdict(False, "Step 6 topology: cyclical encoding needs a coordinate with a period. The "
                   "only ordering column in the star is `wave`, measured here at {} distinct "
                   "values ({}), and none of fact_mailer's {} columns is time-typed, so there "
                   "is no period to wrap".format(len(waves), waves, len(columns)))


# ------------------------------------------------------------------------------------------
# STEP 7 -- FEATURE ENGINEERING  (the interaction frame)
# ------------------------------------------------------------------------------------------

def step7_interaction_frame(cells, waves):
    """The frame is the (risk_band x price arm) cell, and the interaction is the whole model.

    A contextual bandit does not consume a feature vector here. Its context is the band, its
    action is the arm, and the quantity it maintains is one posterior per (context, action)
    pair. That pairing IS the interaction term: a main effect for "band" plus a main effect for
    "arm" would assert that the price response has the same shape in every risk band, which is
    the hypothesis the experiment was run to test rather than an assumption to bake in.

    The arm shares are reported because they are what makes the logging policy non-uniform, and
    a non-uniform logging policy is the only reason an off-policy correction has anything to do.
    """
    cell_count = len(waves) * len(BANDS) * ARMS_PER_BAND
    shares = []
    for band in BANDS:
        pulls = sum(cells[(wave, band)][1] for wave in waves)
        shares.extend(pulls / pulls.sum())
    verdict(True, "Step 7 interaction frame: {} (band x arm) cells over {} waves = {} "
                  "(band, wave, arm) posteriors; arm shares within a band run {:.4f}..{:.4f}, "
                  "so the logging policy is not uniform and the importance weights below are "
                  "not all 1".format(len(BANDS) * ARMS_PER_BAND, len(waves), cell_count,
                                     min(shares), max(shares)))


# ------------------------------------------------------------------------------------------
# STEPS 8, 9, 10 -- BANNED
# ------------------------------------------------------------------------------------------

def step8_pruning_banned(cells, waves):
    """Variance pruning over one-hot arms, priced in this run's own share numbers."""
    thinnest, share = None, None
    for band in BANDS:
        pulls = sum(cells[(wave, band)][1] for wave in waves)
        fractions = pulls / pulls.sum()
        index = int(fractions.argmin())
        if share is None or fractions[index] < share:
            thinnest, share = (band, index), float(fractions[index])

    verdict(False, "Step 8 prune: a one-hot dummy for an arm played a fraction p of the time "
                   "has variance p(1-p), so a variance threshold removes arms in ascending "
                   "order of exposure -- it would delete {} arm {} first, at a share of {:.4f}, "
                   "which is the arm the posterior is least certain about and therefore the one "
                   "the bandit most needs to keep"
                   .format(thinnest[0], thinnest[1], share), banned=True)
    verdict(False, "Step 8 structural: the arms are the action space, not features. Pruning one "
                   "does not simplify a model, it deletes a price the lender can offer -- and "
                   "an action removed from the grid still appears in the logged data, so the "
                   "evaluation would be estimating a policy that cannot be run", banned=True)


def step9_regularisation_banned():
    """L1/L2 shrinkage, and why a bandit cannot accept it."""
    verdict(False, "Step 9 regularise: L1 shrinks a coefficient toward zero in proportion to "
                   "how little support it has, which on this grid means shrinking the thinnest "
                   "arms back onto the prior -- and the distance between an arm's posterior and "
                   "the prior is exactly the quantity Thompson sampling uses to decide whether "
                   "to explore it", banned=True)
    verdict(False, "Step 9 structural: shrinkage is a bias-variance trade for a POINT estimate. "
                   "The engine does not consume a point estimate; it draws from a posterior, "
                   "and a shrunk mean with an unshrunk variance is not the posterior of "
                   "anything -- the draw would no longer be a sample from the belief it claims "
                   "to represent", banned=True)


def step10_scaling_banned(alpha, beta):
    """StandardScaler on alpha/beta, evaluated on this run's own counts.

    The arithmetic is done inline rather than by fitting a scaler, partly because fitting one
    would be running the banned step, and partly because the failure is more legible here than
    inside a fitted object.
    """
    params = np.array([[alpha[band][i], beta[band][i]] for band in BANDS
                       for i in range(ARMS_PER_BAND)], dtype=float)
    mu, sigma = params.mean(axis=0), params.std(axis=0, ddof=1)
    if not sigma.all():
        LOG.info("   (alpha/beta have zero spread across arms on this run; the scaled "
                 "arithmetic is undefined and is skipped)")
        return
    scaled = (params - mu) / sigma
    negatives = int((scaled <= 0).sum())
    sizes = params.sum(axis=1)
    scaled_sizes = scaled.sum(axis=1)

    verdict(False, "Step 10 domain: the Beta density exists only for alpha > 0 and beta > 0, "
                   "and mean-centring puts {} of {} standardised shape parameters at or below "
                   "zero by construction -- numpy's Beta sampler raises on those rather than "
                   "degrading, so the engine's one primitive has nothing to draw from"
                   .format(negatives, scaled.size), banned=True)
    verdict(False, "Step 10 evidence: alpha + beta is the arm's effective sample size and the "
                   "whole source of the explore/exploit signal through "
                   "Var[theta] = ab/((a+b)^2(a+b+1)); standardising takes it from "
                   "{:,.0f}..{:,.0f} observations to {:.4f}..{:.4f} units of sigma, so every "
                   "arm ends up equally certain and the reason to explore is gone"
                   .format(sizes.min(), sizes.max(), scaled_sizes.min(), scaled_sizes.max()),
            banned=True)
    verdict(False, "Step 10 conjugacy: the update adds a count to a count. After scaling the "
                   "state is in units of sigma while the increment is still one mailer, and mu "
                   "and sigma move with every wave, so each round would add an integer to a "
                   "quantity measured against a different basis from the one before it",
            banned=True)


# ------------------------------------------------------------------------------------------
# THE ENGINE -- contextual Beta-Bernoulli Thompson sampling
# ------------------------------------------------------------------------------------------

def make_generators(seed):
    """The Thompson generator and the bootstrap generator, independent by construction.

    ``SeedSequence.spawn`` is the documented way to derive independent streams from one seed.
    ``default_rng(seed)`` and ``default_rng(seed + 1)`` would also be fine in practice -- the
    sequence hashes its entropy, so adjacent integers do not give correlated states -- but that
    is a property of the implementation rather than a guarantee of the interface, and spawning
    says what is meant.
    """
    thompson, bootstrap = np.random.SeedSequence(seed).spawn(2)
    return np.random.default_rng(thompson), np.random.default_rng(bootstrap)


def thompson_probabilities(rng, alpha, beta, draws):
    """pi(a|x): the share of posterior draws in which each arm is the largest.

    One matrix of shape (draws, arms) and one argmax. There is no closed form for the winning
    probability of one Beta among six, which is why this is Monte Carlo rather than arithmetic,
    and it is the only reason the job needs a seed at all.
    """
    sample = rng.beta(alpha, beta, size=(draws, len(alpha)))
    return np.bincount(sample.argmax(axis=1), minlength=len(alpha)) / float(draws)


def posterior_sd(alpha, beta):
    """sqrt(ab / ((a+b)^2 (a+b+1))) -- the Beta standard deviation, elementwise."""
    total = alpha + beta
    return np.sqrt(alpha * beta / (total * total * (total + 1.0)))


def run_rounds(cells, waves, rng, draws, run_id):
    """One batched round per wave. Returns (posterior rows, pi by (through_wave, band)).

    Round r observes every mailer of wave r at once and adds ``(k, n - k)`` to ``(alpha, beta)``
    for each arm of each band. Batching is not an approximation of a row-at-a-time update: the
    Beta-Bernoulli posterior depends on the data only through the counts, so the state after a
    round is identical whichever order the round's rows arrive in. What the batching does
    reflect is the pipeline it runs in -- a wave lands, the star is merged, this job runs -- and
    the wave is therefore the only decision point the lender ever actually had.

    The Thompson draw is taken at the END of each round, for every band, in the fixed
    (wave, band) order of the loop. That order is part of the reproducibility contract: the
    generator is consumed in it, so changing it changes the last decimals of every Monte-Carlo
    column. The probabilities computed here are written to bandit_posterior AND used as the
    evaluated policy below, so the policy being evaluated on wave w is the one a reader can read
    off the through_wave = w-1 rows of the table rather than a second quantity computed
    somewhere else.
    """
    alpha = {band: np.full(ARMS_PER_BAND, PRIOR_A) for band in BANDS}
    beta = {band: np.full(ARMS_PER_BAND, PRIOR_B) for band in BANDS}
    rows, policy = [], {}

    for wave in waves:
        for band in BANDS:
            rewards, pulls = cells[(wave, band)]
            alpha[band] = alpha[band] + rewards
            beta[band] = beta[band] + (pulls - rewards)
            probabilities = thompson_probabilities(rng, alpha[band], beta[band], draws)
            policy[(wave, band)] = probabilities
            means = alpha[band] / (alpha[band] + beta[band])
            sds = posterior_sd(alpha[band], beta[band])
            for index in range(ARMS_PER_BAND):
                rows.append({
                    "run_id": run_id,
                    "through_wave": wave,
                    "arm_id": ARM_BASE[band] + index + 1,
                    "risk_band": band,
                    "arm_index": index,
                    # Cumulative through this wave: alpha - PRIOR_A is the reward count and
                    # (alpha + beta) - (PRIOR_A + PRIOR_B) the exposure, so the two columns and
                    # the two shape parameters cannot drift apart.
                    "pulls": int(round(alpha[band][index] + beta[band][index]
                                       - PRIOR_A - PRIOR_B)),
                    "rewards": int(round(alpha[band][index] - PRIOR_A)),
                    "alpha": float(alpha[band][index]),
                    "beta": float(beta[band][index]),
                    "posterior_mean": float(means[index]),
                    "posterior_sd": float(sds[index]),
                    "ts_probability": float(probabilities[index]),
                })

    LOG.info("posterior after wave %s (Beta(%.0f, %.0f) prior, no discount):", waves[-1],
             PRIOR_A, PRIOR_B)
    for band in BANDS:
        means = alpha[band] / (alpha[band] + beta[band])
        LOG.info("   %-6s %s | argmax arm %s", band,
                 " ".join("{:.5f}".format(value) for value in means), int(means.argmax()))
    return rows, policy, alpha, beta


def snips(pi, rewards, pulls):
    """Self-normalised inverse-propensity value of the policy `pi` on one logged cell.

    ``V = sum_i (pi_i / e_i) y_i / sum_i (pi_i / e_i)`` with ``e`` the realised arm frequency in
    the cell. Written in counts because with an estimated ``e`` the two forms are the same
    number: ``e_a = n_a / n`` makes every row of arm a carry weight ``pi_a * n / n_a``, the n
    cancels between numerator and denominator, and what survives is the pi-weighted mean of the
    per-arm rates. The self-check proves that against a hand-computed case.

    An arm with no exposure in the cell is dropped and the remaining weights renormalise, which
    is what the ratio form does on its own. That is a real loss of coverage rather than a
    rounding detail -- the value returned is then the value of a DIFFERENT policy, the one that
    never plays the unexposed arm -- so Step 5 measures and reports the smallest cell instead of
    leaving this to be discovered here.
    """
    played = pulls > 0
    weight = pi[played].sum()
    if weight <= 0.0:
        raise ValueError("every arm this policy would play has zero exposure in the cell, so "
                         "no importance-weighted value is defined over it")
    return float((pi[played] * (rewards[played] / pulls[played])).sum() / weight)


def bootstrap_differences(rng, pi, rewards, pulls, replicates):
    """Percentile-bootstrap replicates of (SNIPS value - logging value) for one cell.

    Resampling the cell's rows with replacement is exactly a multinomial draw over the
    ``2 x arms`` (arm, outcome) categories at the cell's own empirical frequencies -- the rows
    are exchangeable within a category and carry no other information, so the two resamples have
    the same distribution. Drawing the counts directly is what keeps this inside a Python Shell
    container: the row-index form would allocate a (replicates x rows) matrix to compute a
    statistic that only depends on the counts it would then recount.

    Both ``e`` and ``V`` are recomputed on every replicate. Holding ``e`` fixed at its
    full-sample value would treat an estimated propensity as a known one and give an interval
    that is too narrow -- which is the same conflation the docstring's second point is about.
    """
    n = int(round(pulls.sum()))
    categories = np.concatenate([rewards, pulls - rewards]) / float(n)
    draw = rng.multinomial(n, categories, size=replicates).astype(float)
    k = draw[:, :len(pi)]
    total = k + draw[:, len(pi):]
    played = total > 0
    rate = np.divide(k, total, out=np.zeros_like(k), where=played)
    weight = (pi * played).sum(axis=1)
    if not weight.all():
        raise ValueError("a bootstrap replicate left every arm the policy plays unexposed")
    return (pi * rate).sum(axis=1) / weight - k.sum(axis=1) / float(n)


def evaluate_policy(cells, waves, policy, rng, replicates, run_id):
    """Off-policy evaluation: train on the waves before w, evaluate on wave w. Returns rows.

    The policy evaluated on wave w is the Thompson policy of the posterior through the previous
    wave -- the same probabilities written to bandit_posterior -- so the evaluation is genuinely
    out of sample: no row of wave w contributed to the belief that produced pi. The first wave
    has no predecessor and is therefore not evaluated, which is why a run over a star that holds
    only one wave writes no policy rows at all rather than evaluating a policy on its own
    training data.

    The comparison is against the logging policy's own value, which for a cell is just the plain
    take-up rate: the lender's randomisation IS a policy, and it is the only one with a value
    that needs no correction to estimate.
    """
    rows = []
    if len(waves) < 2:
        LOG.info("only wave %s is loaded, so there is no held-out wave to evaluate a policy on "
                 "-- bandit_policy_value stays empty for this run", waves[0])
        return rows

    LOG.info("off-policy evaluation, %s bootstrap replicates per cell:", "{:,}".format(
        replicates))
    for previous, wave in zip(waves[:-1], waves[1:]):
        for band in BANDS:
            rewards, pulls = cells[(wave, band)]
            pi = policy[(previous, band)]
            n = int(round(pulls.sum()))
            logging_value = float(rewards.sum() / n)
            snips_value = snips(pi, rewards, pulls)
            difference = snips_value - logging_value
            replicate = bootstrap_differences(rng, pi, rewards, pulls, replicates)
            low, high = np.percentile(replicate, CI_PERCENTILES)
            positive = float((replicate > 0.0).mean())
            rows.append({
                "run_id": run_id,
                "eval_wave": wave,
                "risk_band": band,
                "n_rows": n,
                "logging_value": logging_value,
                "snips_value": snips_value,
                "diff": difference,
                "ci_low": float(low),
                "ci_high": float(high),
                "p_diff_positive": positive,
            })
            LOG.info("   wave %-2s %-6s n %8s logging %.5f SNIPS %.5f  lift %+7.2f%% | "
                     "CI95 [%+.5f, %+.5f] P(diff>0) %.3f", wave, band, "{:,}".format(n),
                     logging_value, snips_value, 100.0 * difference / logging_value,
                     low, high, positive)

    clears = [row for row in rows if row["ci_low"] > 0.0]
    negative = [row for row in rows if row["diff"] < 0.0]
    verdict(True, "Path 3 engine: {} of {} evaluated cells have an interval clear of zero{}; {} "
                  "cell(s) came out NEGATIVE{} -- the policy trained on the earlier waves did "
                  "worse there than the lender's own randomisation, which is a result and not a "
                  "defect to suppress"
            .format(len(clears), len(rows),
                    " (" + ", ".join("wave {} {}".format(r["eval_wave"], r["risk_band"])
                                     for r in clears) + ")" if clears else "",
                    len(negative),
                    " (" + ", ".join("wave {} {}".format(r["eval_wave"], r["risk_band"])
                                     for r in negative) + ")" if negative else ""))
    LOG.warning("CAVEAT -- these are replayed values, not realised ones. The propensities are "
                "estimated from the logged assignment frequencies rather than read from a "
                "published randomisation design, and no client was ever solicited at a rate "
                "this posterior chose.")
    return rows


# ------------------------------------------------------------------------------------------
# WRITING THE RESULT TABLES
# ------------------------------------------------------------------------------------------

POSTERIOR_COLUMNS = ("run_id", "through_wave", "arm_id", "risk_band", "arm_index", "pulls",
                     "rewards", "alpha", "beta", "posterior_mean", "posterior_sd",
                     "ts_probability")
POLICY_COLUMNS = ("run_id", "eval_wave", "risk_band", "n_rows", "logging_value", "snips_value",
                  "diff", "ci_low", "ci_high", "p_diff_positive")

# How each column is rendered into the VALUES list. The DDL declares DECIMAL(12,2) on the shape
# parameters and DECIMAL(10,8) on the probabilities, so the literals are written at those
# scales: a longer literal would be rounded by the warehouse silently and by the local engine
# differently, and the two stores would then disagree in a digit nobody printed.
DECIMALS = {"alpha": 2, "beta": 2, "posterior_mean": 8, "posterior_sd": 8, "ts_probability": 8,
            "logging_value": 8, "snips_value": 8, "diff": 8, "ci_low": 8, "ci_high": 8,
            "p_diff_positive": 8}


def values_clause(rows, columns):
    """Render result rows as a VALUES list.

    Every value here was computed by this job or read back from the warehouse, and the two
    string columns are constrained before they arrive: run_id against RUN_ID_PATTERN and
    risk_band against the declared bands in load_arm_grid(). There is no free text to quote.
    """
    rendered = []
    for row in rows:
        cells = []
        for column in columns:
            value = row[column]
            if isinstance(value, str):
                cells.append("'{}'".format(value))
            elif column in DECIMALS:
                cells.append("{:.{}f}".format(value, DECIMALS[column]))
            else:
                cells.append(str(int(value)))
        rendered.append("({})".format(", ".join(cells)))
    return ",\n       ".join(rendered)


def write_results(cursor, run_id, posterior_rows, policy_rows):
    """Replace this run_id's rows in both result tables.

    DELETE then INSERT, not MERGE. Both tables are keyed by run_id and hold exactly what one run
    produced, so a rerun under the same run_id must end with this run's rows and no others -- a
    MERGE would leave any row the previous run wrote and this one did not, still carrying the
    old value, with nothing to mark it as stale. The result tables are output, never input, so
    there is nothing downstream that a delete could orphan.

    The two statements are issued back to back and committed once by the caller, so on Redshift
    the pair is atomic; locally DuckDB commits each of them as it runs and the pair is not. The
    ordering is what makes that difference tolerable: delete first, so an interruption between
    the two leaves the run's rows ABSENT rather than a mixture of two runs' numbers. A visible
    gap is the failure worth having, because a silently blended posterior still looks like a
    posterior.
    """
    for table, rows, columns in (
            ("processed_zone.bandit_posterior", posterior_rows, POSTERIOR_COLUMNS),
            ("processed_zone.bandit_policy_value", policy_rows, POLICY_COLUMNS)):
        # log=LOG so the statement lines carry this job's logger name in CloudWatch rather than
        # the helper's, which is what the helper's own `log` parameter is for.
        run(cursor, "DELETE FROM {} WHERE run_id = '{}'".format(table, run_id), log=LOG)
        if not rows:
            LOG.info("no rows to write to %s for run_id %s", table, run_id)
            continue
        run(cursor, "INSERT INTO {} ({})\nVALUES {}".format(
            table, ", ".join(columns), values_clause(rows, columns)), log=LOG)
        LOG.info("wrote %s rows to %s under run_id %s", len(rows), table, run_id)


# ------------------------------------------------------------------------------------------
# SELF-CHECK
# ------------------------------------------------------------------------------------------

def self_check():
    """Pin the pure arithmetic. No database, no network, no input, no output.

    Each of these fails silently if it is wrong: a mis-specified conjugate update still produces
    a valid Beta, a binner that puts a cut in the wrong arm still produces an arm, a SNIPS that
    forgets to normalise still produces a number between 0 and 1, and two generators sharing a
    stream still produce reproducible-looking output. All four give plausible results and a
    wrong table.
    """
    # 1. The conjugate update, and its batch-invariance. Beta(1,1) + (k, n-k) has mean
    #    (1+k)/(2+n); accumulating wave by wave must land exactly where one pooled batch lands,
    #    which is the property that lets the wave staging be a reporting decision rather than an
    #    arithmetic one. If it were false, the final posterior would depend on how the pipeline
    #    happened to slice the data.
    alpha, beta = np.full(2, PRIOR_A), np.full(2, PRIOR_B)
    batches = [(np.array([3.0, 1.0]), np.array([10.0, 10.0])),
               (np.array([5.0, 2.0]), np.array([20.0, 5.0]))]
    for rewards, pulls in batches:
        alpha = alpha + rewards
        beta = beta + (pulls - rewards)
    pooled_k = sum(rewards for rewards, _ in batches)
    pooled_n = sum(pulls for _, pulls in batches)
    assert np.allclose(alpha, PRIOR_A + pooled_k), "the alpha update is not a reward count"
    assert np.allclose(beta, PRIOR_B + pooled_n - pooled_k), "beta is not a failure count"
    assert np.allclose(alpha / (alpha + beta), (PRIOR_A + pooled_k) / (PRIOR_A + PRIOR_B
                                                                       + pooled_n)), \
        "the batched posterior mean is not (prior + k) / (prior total + n)"
    # The width shrinks as evidence arrives; an update that added to both parameters equally
    # would keep the mean right and the uncertainty wrong.
    assert (posterior_sd(alpha, beta) < posterior_sd(np.full(2, PRIOR_A),
                                                     np.full(2, PRIOR_B))).all(), \
        "the posterior did not narrow after two batches of evidence"

    # 2. The binner, against all eighteen declared bands and both sides of all fifteen cuts. A
    #    rate EQUAL to a cut belongs to the arm above -- the half-open convention the DDL's
    #    rate_floor <= rate < rate_ceil states -- and getting that backwards moves a whole slice
    #    of mailers between two adjacent arms without changing any row count.
    seen = set()
    for band in BANDS:
        assert arm_index_for_rate(band, CUTS[band][0] - 1.0) == 0, "below the first cut is arm 0"
        assert arm_index_for_rate(band, CUTS[band][-1] + 100.0) == ARMS_PER_BAND - 1, \
            "the top arm is not open above"
        for position, cut in enumerate(CUTS[band]):
            assert arm_index_for_rate(band, cut) == position + 1, \
                "{} rate {} did not land in the arm above the cut".format(band, cut)
            assert arm_index_for_rate(band, cut - 0.01) == position, \
                "{} rate just under {} did not land in the arm below".format(band, cut)
        for index in range(ARMS_PER_BAND):
            seen.add(ARM_BASE[band] + index + 1)
    assert seen == set(range(1, len(BANDS) * ARMS_PER_BAND + 1)), \
        "the declared arm_id numbering does not cover 1..{} exactly".format(len(seen))

    # 3. SNIPS on a two-arm case whose answer is computed by hand here.
    #    Arm A: 4 mailers, 1 take-up (rate 0.25). Arm B: 4 mailers, 3 take-ups (rate 0.75).
    #    The logging policy played them 50/50, so e = (0.5, 0.5); the evaluated policy is
    #    pi = (0.25, 0.75). Weights are pi/e = (0.5, 1.5), so
    #        numerator   = 0.5 * 1 + 1.5 * 3 = 5.0
    #        denominator = 0.5 * 4 + 1.5 * 4 = 8.0
    #        V           = 0.625
    #    and the counts form gives 0.25*0.25 + 0.75*0.75 = 0.625, the same number. Every value
    #    is a multiple of a quarter, so both are exact in binary and the equality is not an
    #    approximate one.
    pi = np.array([0.25, 0.75])
    rewards, pulls = np.array([1.0, 3.0]), np.array([4.0, 4.0])
    assert snips(pi, rewards, pulls) == 0.625, "SNIPS on the hand-computed case is wrong"
    arms = np.array([0, 0, 0, 0, 1, 1, 1, 1])
    outcomes = np.array([1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0])
    frequency = np.bincount(arms, minlength=2) / float(len(arms))
    weights = pi[arms] / frequency[arms]
    assert (weights * outcomes).sum() / weights.sum() == snips(pi, rewards, pulls), \
        "the row-wise ratio and the counts form disagree, so the collapse claimed in the " \
        "module docstring is false and the cheaper form is not the same estimator"
    # The logging policy's own value is the plain mean and needs no weights: 4 of 8.
    assert rewards.sum() / pulls.sum() == 0.5

    # An arm with no exposure is dropped and the rest renormalise -- pi = (0.5, 0.5) over an
    # unplayed arm B gives arm A's rate alone, NOT half of it. Forgetting the renormalisation is
    # the error that halves a value and still returns something plausible.
    assert snips(np.array([0.5, 0.5]), np.array([1.0, 0.0]), np.array([4.0, 0.0])) == 0.25

    # 4. The two generators must not share a stream. This is the reproducibility bug the module
    #    docstring names: with one generator, consuming bootstrap draws shifts every Thompson
    #    draw that follows, so the reported policy value would move when --bootstrap-replicates
    #    changed and the point estimate would depend on the width of its own interval.
    first, second = make_generators(1234)
    before = first.random()
    second.random(500)
    after = first.random()
    control_first, _ = make_generators(1234)
    assert control_first.random() == before and control_first.random() == after, \
        "consuming the bootstrap generator moved the Thompson generator: they share a stream"

    # 5. Reproducibility of the Monte-Carlo columns for a fixed seed and consumption order.
    alpha, beta = np.array([5.0, 2.0]), np.array([10.0, 20.0])
    left, _ = make_generators(7)
    right, _ = make_generators(7)
    assert np.array_equal(thompson_probabilities(left, alpha, beta, 1000),
                          thompson_probabilities(right, alpha, beta, 1000)), \
        "the Thompson draw is not reproducible from the seed"
    probabilities = thompson_probabilities(left, alpha, beta, 1000)
    assert abs(probabilities.sum() - 1.0) < 1e-12, "the arm win shares do not sum to 1"

    LOG.info("self-check passed: the conjugate update and its batch-invariance, the binner "
             "against %s declared bands and both sides of every cut, SNIPS against a "
             "hand-computed two-arm case in both forms, the unexposed-arm renormalisation, and "
             "two generators that do not share a stream",
             len(BANDS) * ARMS_PER_BAND)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # --local and the connection options come from the shared helper, so all three warehouse
    # jobs take the same flags.
    add_local_arguments(parser)
    parser.add_argument("--run-id", default=None,
                        help="identifies this run in both result tables (VARCHAR(32)). Derived "
                             "from the star when omitted -- the highest wave the fact holds is "
                             "the complete description of the warehouse state this run "
                             "summarises, so a rerun over the same state overwrites itself and "
                             "a run after the next wave lands writes a new set of rows")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help="seed for the two generators (default: {}). The posterior columns "
                             "do not depend on it; the Monte-Carlo ones do"
                             .format(DEFAULT_SEED))
    parser.add_argument("--draws", type=int, default=DEFAULT_DRAWS,
                        help="posterior draws per Thompson probability (default: {:,}). The "
                             "standard error of a probability from D draws is at most "
                             "1/(2*sqrt(D))".format(DEFAULT_DRAWS))
    parser.add_argument("--bootstrap-replicates", type=int, default=DEFAULT_REPLICATES,
                        help="resamples per evaluated cell for the interval on the difference "
                             "(default: {:,})".format(DEFAULT_REPLICATES))
    parser.add_argument("--dry-run", action="store_true",
                        help="read the star, run every step and the engine, print the results "
                             "and write nothing")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the pure arithmetic and exit; no database, no network, no "
                             "input -- run it before trusting a run")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run and a strict parser exits 2 on them before the job starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return
    if args.draws < 1 or args.bootstrap_replicates < 1:
        parser.error("--draws and --bootstrap-replicates must both be at least 1")
    if args.run_id is not None and not RUN_ID_PATTERN.match(args.run_id):
        parser.error("--run-id must be 1-32 characters of letters, digits, dot, dash or "
                     "underscore: it is half the primary key of both result tables and it is "
                     "written into SQL text")

    thompson, bootstrap = make_generators(args.seed)

    # connect() takes (local, local_db, secret_name, region). Under --local `local_db` is the
    # DuckDB file the
    # rest of the pipeline wrote; on Glue it is unused, and the host, database, user and
    # password
    # come from Secrets Manager inside the helper.
    conn = None
    try:
        conn = connect(args.local, args.local_db, args.secret_name, args.region)
        # No `conn.autocommit = False` here, although the reference lab writes one. It restates
        # the DB-API default that redshift_connector already applies, and DuckDB's connection
        # has no such attribute at all -- so the line buys nothing on the warehouse it is aimed
        # at and raises AttributeError on the one the local run uses.
        cursor = conn.cursor()

        grid = load_arm_grid(cursor)
        cells, waves = load_cells(cursor, grid)
        run_id = args.run_id or "through-wave-{}".format(waves[-1])
        LOG.info("run-id %s | seed %s | %s draws | %s bootstrap replicates%s", run_id,
                 args.seed, "{:,}".format(args.draws),
                 "{:,}".format(args.bootstrap_replicates),
                 " | DRY RUN, nothing will be written" if args.dry_run else "")

        step4_native_missingness(cursor, waves)
        step5_diagnostics(cells, waves)
        step6_topology(cursor, waves)
        step7_interaction_frame(cells, waves)
        step8_pruning_banned(cells, waves)
        step9_regularisation_banned()

        posterior_rows, policy, alpha, beta = run_rounds(cells, waves, thompson, args.draws,
                                                         run_id)
        # After the engine, because it prices the ban in the run's own alpha and beta rather
        # than in a hypothetical pair.
        step10_scaling_banned(alpha, beta)
        policy_rows = evaluate_policy(cells, waves, policy, bootstrap,
                                      args.bootstrap_replicates, run_id)

        if args.dry_run:
            LOG.info("dry run complete: %s posterior rows and %s policy rows computed and "
                     "discarded; nothing was written and no transaction was opened against the "
                     "result tables", len(posterior_rows), len(policy_rows))
            return

        write_results(cursor, run_id, posterior_rows, policy_rows)
        conn.commit()
        LOG.info("Path 3 complete -- Steps 4-10 applied, excused or banned with reasons, and "
                 "the posterior and its off-policy evaluation are in processed_zone under "
                 "run_id %s", run_id)
    except Exception:
        # Rolled back before re-raising so a half-written run_id cannot be left behind, and
        # re-raised so the Glue job fails and the Step Functions chain stops. Swallowing it
        # would exit 0 on a job that wrote nothing.
        if conn is not None:
            try:
                conn.rollback()
            except Exception as exc:
                # MEASURED, and the reason this is wrapped while the commit above is not: DuckDB
                # treats a commit with no open transaction as a no-op and a ROLLBACK with none
                # as an error. An exception is already on its way up the stack at this point,
                # and a cleanup that raises replaces it -- the same failure as the reference
                # lab's unbound `conn` in its finally block, where CloudWatch ends up holding
                # the second error instead of the first one.
                LOG.warning("rollback found no open transaction (%s); each statement above was "
                            "committed as it ran", exc)
        raise
    finally:
        # `conn = None` above is what makes this safe: if connect() itself raised, the name
        # would otherwise be unbound here and the connection error in CloudWatch would be
        # replaced by a NameError from this line.
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
