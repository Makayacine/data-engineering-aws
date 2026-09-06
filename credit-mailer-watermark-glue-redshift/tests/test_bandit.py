"""The bandit's arithmetic, pinned. No warehouse, no fact table, no randomness that matters.

Three things in ``glue-jobs/glue-refinery-path3.py`` can be wrong while every number it prints
stays plausible, and those three are what this file tests:

* the conjugate update, because a mis-specified one still produces a valid Beta distribution with
  a mean between 0 and 1;
* the arm binner, because a cut placed on the wrong side of its boundary still returns an arm
  index in range, and moves a slice of mailers between two adjacent arms without changing any
  row count anywhere;
* SNIPS, because an estimator that forgets to self-normalise still returns a number that looks
  like a take-up rate.

None of the three has an output shape that a schema check, a NOT NULL constraint or a row count
would flag. They are the reason this file exists and they are the only reason it exists: the
Monte-Carlo columns are not asserted here at all. Pinning a Thompson probability to five decimals
would be pinning a numpy generator's stream, which is a property of the library rather than of
this job, and the six acceptance figures in the README are checked by running the job.

The tests build their own arrays rather than reading ``processed_zone.fact_mailer``. A test that
needed a warehouse to answer "does a rate equal to a cut land in the arm above" would be a slower
way of asking a question that has nothing to do with a warehouse, and it could only be run after
five other jobs had succeeded.

Run from the project root::

    python -m pytest tests -q
"""

import importlib.util
import os
import sys

import numpy as np
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB_DIRECTORY = os.path.join(PROJECT_ROOT, "glue-jobs")


def load_job(relative_path, module_name):
    """Import a job file by path, with its own directory on ``sys.path`` first.

    The hyphen in ``glue-refinery-path3.py`` is not a Python identifier, so a plain import cannot
    reach the file. The names carry hyphens because the reference lab names its Glue scripts that
    way and this project is meant to be diffable against it, so the loader is here rather than a
    rename there.

    ``sys.path`` matters as well as the loader: the job does ``from warehouse_common import ...``,
    which resolves on Glue through ``--extra-py-files`` and locally because Python puts a running
    script's own directory on the path. Loading the file by path skips that step, so the
    directory is added explicitly and the import resolves to the helper sitting next to the job.
    """
    if JOB_DIRECTORY not in sys.path:
        sys.path.insert(0, JOB_DIRECTORY)
    path = os.path.join(PROJECT_ROOT, *relative_path.split("/"))
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


refinery = load_job("glue-jobs/glue-refinery-path3.py", "glue_refinery_path3")

# The declared cuts, written out a second time. This is the one duplication in the file that
# earns its place: the job's CUTS are a DECLARATION, seeded into dim_offer_arm by the DDL and
# never derived from the arriving data, so a test that imported them and compared them to
# themselves would pass through any edit at all. These literals are the build spec's, and if the
# two ever disagree then either the constant moved or the DDL did, and one of them is now
# attributing mailers to an arm the other does not define.
DECLARED_CUTS = {
    "HIGH": (5.50, 7.50, 9.00, 10.00, 11.00),
    "MEDIUM": (5.00, 6.75, 7.50, 8.25, 9.25),
    "LOW": (4.50, 5.50, 6.00, 6.75, 7.50),
}

# Two rounds of synthetic counts, one per wave, over all three bands. The numbers are invented --
# nothing here is a measured figure and nothing here should be read as one. What they have to be
# is arithmetically awkward: an arm with no exposure at all, an arm with exposure and no reward,
# and a wave that adds to an arm the previous wave never touched.
ROUND_ONE = {
    "HIGH": (np.array([3.0, 0.0, 1.0, 0.0, 2.0, 0.0]),
             np.array([40.0, 10.0, 20.0, 0.0, 30.0, 5.0])),
    "MEDIUM": (np.array([1.0, 2.0, 0.0, 0.0, 0.0, 0.0]),
               np.array([10.0, 12.0, 8.0, 4.0, 6.0, 0.0])),
    "LOW": (np.array([0.0, 1.0, 2.0, 3.0, 0.0, 0.0]),
            np.array([5.0, 7.0, 11.0, 13.0, 2.0, 3.0])),
}
ROUND_TWO = {
    "HIGH": (np.array([1.0, 4.0, 0.0, 2.0, 0.0, 1.0]),
             np.array([12.0, 25.0, 7.0, 9.0, 0.0, 6.0])),
    "MEDIUM": (np.array([0.0, 0.0, 3.0, 1.0, 2.0, 0.0]),
               np.array([3.0, 5.0, 14.0, 8.0, 11.0, 0.0])),
    "LOW": (np.array([2.0, 0.0, 1.0, 0.0, 1.0, 4.0]),
            np.array([9.0, 4.0, 6.0, 3.0, 8.0, 15.0])),
}

# Small enough to be instant, large enough that the win shares are a distribution rather than a
# single spike. Nothing in this file asserts anything about their VALUES.
DRAWS = 500


def _cells(rounds):
    """``{(wave, band): (rewards, pulls)}`` -- the shape ``load_cells`` returns from the fact."""
    return dict(((wave, band), counts)
                for wave, per_band in enumerate(rounds, start=1)
                for band, counts in per_band.items())


def _rows_by_key(rows):
    return dict(((row["through_wave"], row["risk_band"], row["arm_index"]), row) for row in rows)


def test_the_binner_agrees_with_the_declared_cuts():
    """The job's constant is the build spec's grid, band for band and cut for cut."""
    assert refinery.CUTS == DECLARED_CUTS
    assert set(refinery.BANDS) == set(DECLARED_CUTS)
    assert all(len(cuts) == refinery.ARMS_PER_BAND - 1 for cuts in DECLARED_CUTS.values())


def test_a_rate_equal_to_a_cut_belongs_to_the_arm_above():
    """All fifteen declared cut points, taken exactly, from the literals rather than the job's.

    ``rate_floor <= rate < rate_ceil`` is the DDL's convention and the half-open interval is the
    whole of the boundary decision. Getting it backwards is invisible: every mailer still lands
    in an arm, every arm still has a count, and the only symptom is that a price band's take-up
    rate is computed over a slightly different set of clients than the one it is labelled with.

    Every cut is a multiple of 0.25 and therefore exact as a double, so ``rate == cut`` here is a
    real equality and not a comparison that happens to work. That is a property of the declared
    grid, and it is asserted rather than assumed, because a future cut of 6.10 would make this
    test's premise quietly false.
    """
    for band, cuts in DECLARED_CUTS.items():
        for position, cut in enumerate(cuts):
            assert cut * 4.0 == float(int(cut * 4.0)), \
                "{} cut {} is not a multiple of 0.25, so the boundary is not exact".format(
                    band, cut)
            assert refinery.arm_index_for_rate(band, cut) == position + 1
            assert refinery.arm_index_for_rate(band, cut - 0.01) == position


def test_the_binner_covers_all_eighteen_arms_and_is_open_at_both_ends():
    """Six arms per band, eighteen arm_ids, 1..18 with no gap and no repeat.

    The grid is open below the first cut and above the last: the published rates run 3.25 to
    14.75 and no band's cuts reach either end, so an arm 0 that refused a rate under its floor
    would drop the cheapest mailers and an arm 5 that refused one over its ceiling would drop the
    dearest. Both would pass a NOT NULL check on arm_id by never producing the row at all.
    """
    seen = set()
    for band, cuts in DECLARED_CUTS.items():
        assert refinery.arm_index_for_rate(band, cuts[0] - 1.0) == 0
        assert refinery.arm_index_for_rate(band, 3.25) == 0
        assert refinery.arm_index_for_rate(band, 14.75) == refinery.ARMS_PER_BAND - 1
        assert refinery.arm_index_for_rate(band, cuts[-1] + 100.0) == refinery.ARMS_PER_BAND - 1
        for index in range(refinery.ARMS_PER_BAND):
            seen.add(refinery.ARM_BASE[band] + index + 1)
    assert seen == set(range(1, 19))


def test_the_conjugate_update_is_a_pair_of_counts():
    """Beta(1, 1) + (k, n - k). alpha counts take-ups, beta counts the mailers that did not.

    Asserted through ``run_rounds`` rather than against a re-implementation of the update, since
    a re-implementation would only prove that two copies of the same mistake agree. What is
    checked is the relation between the shape parameters and the raw counts that went in, which
    holds for the correct update and for essentially no other one.

    The ``pulls`` and ``rewards`` columns are checked against the same counts, because they are
    written to bandit_posterior beside alpha and beta and a reader will subtract the prior from
    one to sanity-check the other. If those two ever drift apart, the table's own internal
    arithmetic stops holding and there is no way to tell from the table which half moved.
    """
    rng, _ = refinery.make_generators(11)
    rows, _, alpha, beta = refinery.run_rounds(_cells([ROUND_ONE, ROUND_TWO]), [1, 2],
                                               rng, DRAWS, "test")

    assert len(rows) == 2 * 18, "one row per arm per through_wave"
    indexed = _rows_by_key(rows)

    for band in refinery.BANDS:
        rewards = ROUND_ONE[band][0] + ROUND_TWO[band][0]
        pulls = ROUND_ONE[band][1] + ROUND_TWO[band][1]
        assert np.array_equal(alpha[band], refinery.PRIOR_A + rewards)
        assert np.array_equal(beta[band], refinery.PRIOR_B + (pulls - rewards))

        for index in range(refinery.ARMS_PER_BAND):
            row = indexed[(2, band, index)]
            assert row["arm_id"] == refinery.ARM_BASE[band] + index + 1
            assert row["pulls"] == pulls[index]
            assert row["rewards"] == rewards[index]
            assert row["alpha"] == refinery.PRIOR_A + rewards[index]
            assert row["beta"] == refinery.PRIOR_B + pulls[index] - rewards[index]
            assert row["posterior_mean"] == pytest.approx(
                (refinery.PRIOR_A + rewards[index])
                / (refinery.PRIOR_A + refinery.PRIOR_B + pulls[index]))

    # An arm that the SECOND round never pulled still reports the first round's counts at
    # through_wave 2 -- it is carried, not dropped. HIGH arm 4 was pulled 30 times in round
    # one and 0 times in round two.
    untouched = indexed[(2, "HIGH", 4)]
    assert untouched["pulls"] == 30 and untouched["rewards"] == 2

    # And an arm nobody pulled in EITHER round keeps the prior exactly, with pulls = 0,
    # rather than being dropped: an unpulled arm is the one the posterior is most uncertain
    # about, and dropping it is how a bandit stops exploring. MEDIUM arm 5 is that arm.
    never = indexed[(2, "MEDIUM", 5)]
    assert never["pulls"] == 0 and never["rewards"] == 0
    assert never["alpha"] == refinery.PRIOR_A and never["beta"] == refinery.PRIOR_B


def test_batching_a_wave_at_a_time_lands_where_one_pooled_batch_lands():
    """Two rounds of counts reach the same posterior as their sum applied once.

    This is what makes the wave staging a reporting decision rather than an arithmetic one. The
    Beta-Bernoulli posterior depends on the data only through the counts, so the state after a
    round cannot depend on the order the round's rows arrived in -- and if that were false, the
    final posterior would depend on how the pipeline happened to slice the mailers, which is a
    property of the Step Functions chain and not of the lender's experiment.

    It is also the assertion that would fail first if a discount factor were ever added. The
    sibling crypto project uses one, because its arms are pulled repeatedly against a moving
    market; here each client is solicited exactly once, so there is no non-stationarity for a
    discount to track and a gamma would make this equality false for no gain.
    """
    pooled = dict((band, (ROUND_ONE[band][0] + ROUND_TWO[band][0],
                          ROUND_ONE[band][1] + ROUND_TWO[band][1]))
                  for band in refinery.BANDS)

    staged_rng, _ = refinery.make_generators(3)
    pooled_rng, _ = refinery.make_generators(3)
    _, _, staged_alpha, staged_beta = refinery.run_rounds(
        _cells([ROUND_ONE, ROUND_TWO]), [1, 2], staged_rng, DRAWS, "staged")
    _, _, pooled_alpha, pooled_beta = refinery.run_rounds(
        _cells([pooled]), [1], pooled_rng, DRAWS, "pooled")

    for band in refinery.BANDS:
        assert np.array_equal(staged_alpha[band], pooled_alpha[band])
        assert np.array_equal(staged_beta[band], pooled_beta[band])


def test_snips_matches_a_two_arm_case_computed_by_hand():
    """A worked example, both ways round, with every quantity a multiple of one thirty-second.

    Two arms, played 8 mailers each by the logging policy, so the realised propensity is
    e = (0.5, 0.5). Arm A took up 2 of 8, a rate of 0.25; arm B took up 6 of 8, a rate of 0.75.
    The policy being evaluated is pi = (0.125, 0.875).

    Row by row, which is the definition::

        weights     pi / e          = (0.25, 1.75)
        numerator   0.25*2 + 1.75*6 = 0.5 + 10.5 = 11
        denominator 0.25*8 + 1.75*8 = 2.0 + 14.0 = 16
        V           11 / 16         = 0.6875

    In counts, which is what the job computes::

        0.125 * 0.25 + 0.875 * 0.75 = 0.03125 + 0.65625 = 0.6875

    The two agree because self-normalising cancels the n out of every weight, and that collapse
    is the whole justification for the cheaper form. Asserting it with ``==`` rather than a
    tolerance is safe here and is the point of choosing eighths: every value is exact in binary,
    so a mismatch is a mistake and never a rounding.

    The logging policy's own value needs no weights at all -- 8 take-ups over 16 mailers -- and is
    asserted beside it, because the whole output table is a comparison of the two and a SNIPS
    that silently returned the logging value would otherwise report a lift of exactly zero and
    look merely disappointing.
    """
    pi = np.array([0.125, 0.875])
    rewards = np.array([2.0, 6.0])
    pulls = np.array([8.0, 8.0])

    assert refinery.snips(pi, rewards, pulls) == 0.6875
    assert rewards.sum() / pulls.sum() == 0.5

    # The same number from the row-wise ratio, built from the 16 logged mailers themselves.
    arms = np.array([0] * 8 + [1] * 8)
    outcomes = np.array([1.0, 1.0] + [0.0] * 6 + [1.0] * 6 + [0.0, 0.0])
    frequency = np.bincount(arms, minlength=2) / float(len(arms))
    weights = pi[arms] / frequency[arms]
    assert (weights * outcomes).sum() / weights.sum() == 0.6875


def test_snips_renormalises_over_an_arm_with_no_exposure():
    """An unexposed arm is dropped and the surviving weights renormalise.

    pi = (0.5, 0.5) over a cell where arm B was never played returns arm A's rate, not half of
    it. Forgetting the renormalisation halves the value and still returns something that reads
    like a take-up rate, which is exactly the failure this file is for.

    What the renormalisation costs is worth stating rather than only asserting: the value that
    comes back is the value of a DIFFERENT policy, the one that never plays the unexposed arm.
    That is why positivity is measured and reported by Step 5 instead of being left to be
    discovered here.
    """
    assert refinery.snips(np.array([0.5, 0.5]),
                          np.array([1.0, 0.0]),
                          np.array([4.0, 0.0])) == 0.25


def test_snips_refuses_a_cell_where_the_policy_has_no_exposure_at_all():
    """No arm the policy would play was ever played: there is no value to estimate, so it raises.

    Returning zero would be a policy value of zero -- a claim that nobody would take the offer --
    rather than the absence of evidence it actually is, and it would land in
    bandit_policy_value as a number a reader has no way to distinguish from a measured one.
    """
    with pytest.raises(ValueError):
        refinery.snips(np.array([0.5, 0.5]), np.array([0.0, 0.0]), np.array([0.0, 0.0]))


def test_the_jobs_own_self_check_passes():
    """``--self-check`` is what runs on Glue, where pytest is not installed.

    Called from here so it runs in CI as well, and so that a self-check which has quietly stopped
    agreeing with the code around it fails somewhere a person is looking.
    """
    refinery.self_check()
