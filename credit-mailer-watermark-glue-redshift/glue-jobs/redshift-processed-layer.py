"""Fill the processed zone: dim_client in full, fact_mailer incrementally, one commit at the end.

AWS Glue **Python shell** job, not Spark. Every row this job moves is moved by Redshift itself --
the job issues a short list of statements and reads a handful of counts back -- so there is no
SparkSession, no DynamicFrame, and not even pandas. The only import beyond the standard library
is ``warehouse_common``, which holds the connection and nothing that knows what a table is.

    raw_zone.client_attributes                      ->  processed_zone.dim_client   (full merge)

    raw_zone.mail_offers
      joined to raw_zone.client_attributes    for risk_band
      joined to processed_zone.dim_offer_arm  for arm_id    ->  processed_zone.fact_mailer
                                                                (incremental, by wave)

Deployment: this job imports ``warehouse_common``, so the Glue job definition needs

    --extra-py-files s3://<bucket>/jobs/warehouse_common.py

Locally nothing is needed -- Python puts the running script's directory on sys.path, so the
import resolves to the file next to the job.

This job creates no permanent table. The DDL in ``redshift/redshift-create-tables.sql`` is
applied once, by hand, and the two staging tables below are TEMP and die with the session. A job
that issues CREATE TABLE is a job holding a permission it needs on exactly one day of its life.

THE WATERMARK HERE IS AN ORDINAL, NOT A CLOCK
---------------------------------------------

The reference lab reads ``MAX(viewed_at)`` off its fact table and re-processes everything at or
after that instant. This fact has no instant to read. The deposit is a cross-section of a 2003
field experiment, and the only thing that orders it is ``wave``, a batch index taking the three
values 1, 2 and 3. So the Redshift-side watermark is ``MAX(wave)``:

    WHERE mo.wave >= (SELECT COALESCE(MAX(wave), 0) FROM processed_zone.fact_mailer)

Two things about that line are decisions rather than transcription.

``COALESCE(..., 0)`` is what makes the first run a full load without a separate branch for it.
An empty fact gives NULL, ``wave >= NULL`` is NULL, and a WHERE clause that is NULL on every row
returns nothing at all -- so the run that is supposed to load everything would load nothing, log
a successful merge of zero rows, and exit 0. The reference lab meets the same trap and answers it
in Python, by returning a sentinel timestamp when its fetch comes back empty; answering it in SQL
means there is no code path where a missing watermark becomes a value invented on this side.

``>=`` and not ``>`` because a wave may have been merged in part. This job's own commit is
atomic, but the pipeline around it is not: the raw zone holds a table's newest extract at a fixed
S3 key, and a failure between the raw ingestion and this job's commit leaves that wave present in
the raw zone and absent from the fact. With ``>`` the wave would then be skipped for good, and
nothing downstream would ever notice, because a watermark that has already advanced looks
exactly like a watermark that is up to date. ``>=`` costs a re-merge and cannot skip.

The cost is real and is stated rather than buried: every run re-stages the whole of the newest
wave. Run 4 of the four-run demonstration -- the run whose extract returns no rows at all, the
one that exists to prove the DynamoDB watermark is being read -- re-stages wave 3's 32,198 rows
and merges every one of them onto the value it already holds. The MERGE key ``(client_id, wave)``
is what makes that a no-op rather than a duplicate; without a key the same idea would double the
wave on every run.

Rejected: keeping this job's watermark in the DynamoDB config table alongside the extraction
watermarks. It would put the fact's state in a place the fact cannot enforce, so a DynamoDB
write that failed after a Redshift commit would leave the two disagreeing with no way to tell
which was right. ``MAX(wave)`` is read from the table it describes and cannot drift from it.

THE ARM GRID IS SEED DATA, SO IT IS CHECKED RATHER THAN MERGED
--------------------------------------------------------------

``dim_offer_arm`` holds 18 rows -- six declared price arms inside each of the three risk bands --
and they are seeded by the DDL, not produced by this pipeline. So this job does not merge it. It
asserts it, because two different failures of that table are silent:

*   **An empty or short arm table.** The band join would match nothing, every fact row would
    arrive with a NULL ``arm_id``, and ``arm_id`` is NOT NULL on the target -- so on Redshift the
    MERGE fails and the transaction rolls back, which is survivable, and on a warehouse with a
    laxer constraint it would not be. Counting the rows before the join turns a constraint
    violation of unclear origin into a sentence naming the arm table.
*   **A rate outside every declared band.** The join is a LEFT JOIN precisely so that this
    produces a NULL ``arm_id`` that can be counted and reported with the offending rates,
    instead of an inner join quietly returning a smaller fact table. A silently smaller fact is
    the worst of the available outcomes: the bandit downstream would compute a posterior over
    whichever rows happened to survive and report it with the same confidence as a complete one.

The staged row count is also compared with the count of ``mail_offers`` rows in the same wave
window, which catches both directions of a join that has gone wrong. Fewer staged rows than
source rows means a mailer whose client is missing from the CRM snapshot -- possible whenever the
two tables are loaded by separate Step Functions tasks, which they are. More staged rows than
source rows means two arms within one band overlap, so one mailer matched both, and the fact
would gain a duplicate ``(client_id, wave)`` that the MERGE cannot resolve.

``offer4`` is DECIMAL(5,2) in the raw zone and the cut points are DECIMAL(5,2) in the arm table,
so ``>= rate_floor`` and ``< rate_ceil`` are exact decimal comparisons: a rate landing exactly on
a cut belongs to the arm above, on every run, on every platform. Had either side been a float the
boundary rows would move between arms depending on how the value was parsed, and the posterior
counts would be reproducible only by accident.

BAD_ACCOUNT IS NULL, AND THE NULL IS THE VALUE
----------------------------------------------

``badacct_last`` is non-null on exactly the 4,381 rows where ``tookup = 1``, and null on the
other 53,787. It is carried through untouched. Coalescing it to 0 would state that 53,787 people
who were never lent a rand did not default, which is not a fact about them but a fact about the
loan that never happened. The same argument runs through the 19 treatment flags: 14 of them are
NULL for all 4,974 wave-1 rows because wave 1 was a price-only experiment, and that NULL means
"this arm did not exist yet". Filling it would assert that it did.

WHAT THE REFERENCE LAB DOES THAT THIS DOES NOT
-----------------------------------------------

Four things, each kept out on purpose rather than by oversight:

1.  Its ``execute_query()`` catches every exception, logs it, rolls back and **returns**. The
    caller cannot tell, so the job runs its remaining merges against a rolled-back transaction
    and exits 0. Glue then reports success, Step Functions goes green, and the processed zone is
    short of a merge nobody is looking for. Here every statement is run through
    ``warehouse_common.run()`` and every exception propagates: the job exits non-zero, the state
    machine fails, and the failure is visible on the day it happens.
2.  It commits inside ``execute_query()`` after every statement, despite ``autocommit = False``
    in ``main()``. So its dimension merge is already durable when its fact merge fails, and the
    warehouse is left in a state no single run ever intended. There is one commit here, at the
    end, after the last check has passed.
3.  Its watermark reader returns ``"1970-01-01 00:00:00"`` when the query raises. A permissions
    error or a typo therefore becomes a full reprocess wearing the word "incremental". The
    watermark here is a COALESCE inside the SQL, so there is no exception path in which this
    side invents one.
4.  Its ``finally`` closes ``cursor`` and ``conn`` unconditionally. If the connection itself
    failed, neither name is bound, and a ``NameError`` from the ``finally`` block replaces the
    real error in CloudWatch. Both are bound to None before the ``try`` here.

WHAT THE LOCAL RUN COVERS
--------------------------

``--self-check`` pins the pure text generation with no database, no network and no files: that
every target column is produced exactly once, that the INSERT column list and the VALUES list of
each MERGE are generated from that one list and therefore cannot drift apart, that the merge keys
are not also assigned in the UPDATE clause, and that nothing in the fact's staging projection
wraps a value in COALESCE. That last one is a rule about meaning, not syntax, so it is the kind
that gets edited away in a year by someone making a NOT NULL constraint go quiet.

``--dry-run`` builds both staging tables, runs every check against them, then rolls back. It
proves the SQL parses and the joins land. The merges are held for ``--local``, since a mode that
ends in a rollback can build and check the staging tables but has nothing durable for a MERGE
to write into.

``--local`` runs the same statements against DuckDB, whose MERGE INTO takes the same WHEN
MATCHED / WHEN NOT MATCHED text as Redshift's. The merge text is therefore the same text in both
places, which is the part worth having identical: it is the statement that can corrupt a table.
What belongs to the cluster rather than to the SQL stays with the cluster: the NOT NULL and
PRIMARY KEY declarations Redshift accepts but does not enforce, the distribution and sort keys,
the Secrets Manager entry, and the behaviour of MERGE under concurrent writers. This pipeline is
a sequential chain, so the last of those does not arise while it is the only writer.

This job runs no step of the ten-step framework, so it carries no verdict badges. APPLIES / N/A /
OVERRIDE / LIMIT / ENFORCE / BANNED belong to the refinery job, and a merge count wearing one of
them would read in CloudWatch like a verdict about the modelling.

Local acceptance run (the connection flags themselves come from ``warehouse_common``)::

    python redshift-processed-layer.py --self-check
    python redshift-processed-layer.py --local --dry-run
    python redshift-processed-layer.py --local
"""

import argparse
import logging

import warehouse_common as wh

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("redshift_processed_layer")

# Seeded by redshift-create-tables.sql: six price arms inside each of three risk bands.
EXPECTED_ARMS = 18

# The randomised treatment flags, carried into the fact with their native missingness. They are
# listed once and only once: the staging projection, the INSERT list and the VALUES list of the
# MERGE are all generated from this file's column tables, because three hand-maintained lists of
# this length is three chances to leave one column out and have the other two still line up.
TREATMENT_FLAGS = [
    "prize", "intshown", "dphoto_female", "dphoto_none", "dphoto_black", "gender_match",
    "race_match", "nspeakeligible", "speak_trt", "oneln_trt", "comploss_n", "use_any",
    "stripany", "comp_n", "deadlinemed", "deadlinelong", "deadlong_elig", "deadshort_elig",
    "deadlineshortext",
]

# (target column, the expression in the staging SELECT that produces it).
#
# `raw_zone` mirrors the source column names verbatim so a reader can diff it against the
# published extract; `processed_zone` uses business names. This table is the whole of the
# translation between the two, which is why it is a table and not prose.
DIM_CLIENT_SELECT = [
    ("client_id", "ca.client_id"),
    ("risk_band", "ca.risk"),
    ("race", "ca.race"),
    # SMALLINT 0/1 -> BOOLEAN, the reference lab's `(v.is_wishlisted = 'Y')` idiom. A NULL
    # source stays NULL through the comparison, which is the answer wanted: `edhi` is
    # self-reported, and "did not say" is not "not more educated".
    ("is_female", "(ca.female = 1)"),
    ("is_more_educated", "(ca.edhi = 1)"),
    ("months_dormant", "ca.dormancy"),
    ("prior_loans", "ca.trcount"),
]
DIM_CLIENT_KEYS = ["client_id"]

FACT_MAILER_SELECT = [
    ("client_id", "mo.client_id"),
    ("wave", "mo.wave"),
    ("arm_id", "arm.arm_id"),
    # The bandit's context, denormalised onto the fact. The alternative -- joining dim_client at
    # read time -- would let a later correction to a client's risk band silently restate which
    # arm the 2003 mailer was drawn from, and the posterior is a record of what was actually
    # offered rather than of what is currently believed about the client.
    ("risk_band", "ca.risk"),
    ("offer_rate", "mo.offer4"),
    ("applied", "mo.applied"),
    ("took_up", "mo.tookup"),
    ("amount_borrowed", "mo.amountbrw_unc"),
    # NOT coalesced. See the module docstring: null here means no loan was made, and 0 would
    # mean a loan was made and repaid. --self-check asserts this stays a bare column reference.
    ("bad_account", "mo.badacct_last"),
] + [(flag, "mo." + flag) for flag in TREATMENT_FLAGS]
FACT_MAILER_KEYS = ["client_id", "wave"]

# Read out of the table it describes, so it cannot drift from it. Embedded verbatim in both the
# staging predicate and the parity count below, so those two cannot disagree about the window.
WAVE_WATERMARK = "(SELECT COALESCE(MAX(wave), 0) FROM processed_zone.fact_mailer)"

# rate_floor <= offer_rate < rate_ceil, with the top arm of each band open above. The
# `rate_ceil IS NULL` branch is not defensive: it is how the top arm is declared. Dropping it
# would leave every rate above the last cut with no arm at all -- which the arm-miss check would
# catch, loudly, which is the point of having that check.
ARM_BAND_JOIN = ("arm.risk_band = ca.risk\n"
                 "       AND mo.offer4 >= arm.rate_floor\n"
                 "       AND (arm.rate_ceil IS NULL OR mo.offer4 < arm.rate_ceil)")


def columns(select):
    """The target column names of a (column, expression) table, in declaration order."""
    return [name for name, _ in select]


def projection(select):
    """The SELECT list of a staging table: one `expression AS column` per line."""
    return ",\n".join("    %s AS %s" % (expr, name) for name, expr in select)


def stage_dim_client_sql():
    """Every client in the CRM snapshot, every run.

    There is no watermark on this one and there cannot be: `client_attributes` is a snapshot with
    no event time, which is why its DynamoDB config carries `load_column = None` and its extract
    is a full load. All 58,168 rows arrive in the raw zone on every run, and all 58,168 are
    staged here. The MERGE is what makes that cheap to say and idempotent to run.
    """
    return ("CREATE TEMP TABLE stage_dim_client AS\n"
            "SELECT\n"
            "%s\n"
            "FROM raw_zone.client_attributes ca;" % projection(DIM_CLIENT_SELECT))


def stage_fact_mailer_sql():
    """The mailers at or after the fact's own high-water wave, banded into their price arms.

    The join to `client_attributes` is an inner join, because `risk_band` is NOT NULL on the fact
    and a mailer with no client row has no context to be evaluated in. It is also the join that
    the row-count parity check exists to police -- an inner join that drops rows is invisible in
    the output and visible in the count.

    The join to `dim_offer_arm` is a LEFT join, because a rate that matches no declared band must
    arrive as a countable NULL rather than as a missing row.
    """
    return ("CREATE TEMP TABLE stage_fact_mailer AS\n"
            "SELECT\n"
            "%s\n"
            "FROM raw_zone.mail_offers mo\n"
            "JOIN raw_zone.client_attributes ca\n"
            "     ON ca.client_id = mo.client_id\n"
            "LEFT JOIN processed_zone.dim_offer_arm arm\n"
            "     ON %s\n"
            "WHERE mo.wave >= %s;" % (projection(FACT_MAILER_SELECT), ARM_BAND_JOIN,
                                      WAVE_WATERMARK))


def merge_sql(target, stage, keys, select):
    """One MERGE, with its three column lists generated from one list.

    The reference lab writes the UPDATE assignments, the INSERT list and the VALUES list by hand,
    three times per table. That is survivable at 7 columns and not at 28: a column present in the
    INSERT list and missing from VALUES is a syntax error and therefore harmless, while two
    lists of the same length in a different order is a successful merge that puts `applied` in
    the `took_up` column. Generating all three from one ordered list makes the second failure
    unrepresentable rather than unlikely.

    The keys are excluded from the UPDATE clause -- assigning a matched row's key to itself is a
    no-op that some warehouses reject outright, and it reads as if the key were mutable.
    """
    names = columns(select)
    updates = [c for c in names if c not in keys]
    if not updates:
        raise ValueError("%s has no non-key columns to update, so a MERGE would be an "
                         "INSERT-if-absent and should be written as one" % target)
    return ("MERGE INTO %s\n"
            "USING %s AS source\n"
            "ON %s\n"
            "WHEN MATCHED THEN\n"
            "    UPDATE SET\n"
            "        %s\n"
            "WHEN NOT MATCHED THEN\n"
            "    INSERT (%s)\n"
            "    VALUES (%s);" % (
                target,
                stage,
                " AND ".join("%s.%s = source.%s" % (target, k, k) for k in keys),
                ",\n        ".join("%s = source.%s" % (c, c) for c in updates),
                ", ".join(names),
                ", ".join("source." + c for c in names)))


def scalar(cursor, sql):
    """Run a one-value query and return the value.

    Goes through `warehouse_common.run()` like every other statement, so the read is logged in
    the same place and the same format as the writes, under this job's logger name. The row is
    taken off the cursor rather than off run()'s return value, which is one less thing this file
    depends on: DB-API says execute() populates the cursor, and that holds on both drivers.
    """
    wh.run(cursor, sql, log=LOG)
    return cursor.fetchone()[0]


def build_stage(cursor, name, sql):
    """Create one staging table and return its row count.

    The DROP is not in the reference lab and is here for the local path: a Glue run gets a fresh
    session and therefore a fresh temp namespace, while a local run against a warehouse file may
    not, and `CREATE TEMP TABLE` on a name that already exists fails on both engines.
    """
    wh.run(cursor, "DROP TABLE IF EXISTS %s;" % name, log=LOG)
    wh.run(cursor, sql, log=LOG)
    return scalar(cursor, "SELECT COUNT(*) FROM %s;" % name)


def check_arm_grid(cursor):
    """The arm table is seed data. Prove it is there before anything joins to it."""
    held = scalar(cursor, "SELECT COUNT(*) FROM processed_zone.dim_offer_arm;")
    if held != EXPECTED_ARMS:
        raise ValueError("processed_zone.dim_offer_arm holds %s rows, expected %s -- it is "
                         "seeded by redshift-create-tables.sql and is not written by this "
                         "pipeline, so a wrong count means the DDL was not applied in full. "
                         "Every fact row would fail to find an arm on the band join"
                         % (held, EXPECTED_ARMS))
    LOG.info("dim_offer_arm holds %s arms, as declared", held)


def check_fact_stage(cursor, staged):
    """Two failures the fact's own constraints would report as something else.

    Both are counted before the MERGE runs, so the error names the cause rather than the
    constraint that happened to trip over it.
    """
    source = scalar(cursor, "SELECT COUNT(*) FROM raw_zone.mail_offers mo "
                            "WHERE mo.wave >= %s;" % WAVE_WATERMARK)
    if staged != source:
        raise ValueError("staged %s rows from %s mail_offers rows in the same wave window. "
                         "Fewer means the join to client_attributes dropped mailers whose "
                         "client is not in the CRM snapshot; more means two arms in one risk "
                         "band overlap and a mailer matched both, which would put a duplicate "
                         "(client_id, wave) into the MERGE" % (staged, source))

    missed = scalar(cursor, "SELECT COUNT(*) FROM stage_fact_mailer WHERE arm_id IS NULL;")
    if missed:
        wh.run(cursor, "SELECT risk_band, offer_rate, COUNT(*) AS n FROM stage_fact_mailer "
                       "WHERE arm_id IS NULL GROUP BY risk_band, offer_rate "
                       "ORDER BY n DESC LIMIT 10;", log=LOG)
        offending = ", ".join("%s %s (n=%s)" % row for row in cursor.fetchall())
        raise ValueError("%s of %s staged mailers matched no arm in dim_offer_arm. The arm grid "
                         "is declared, not derived, so a rate outside every band is a rate the "
                         "grid does not describe -- widen the outermost cut points rather than "
                         "dropping the rows. Worst offenders: %s" % (missed, staged, offending))
    LOG.info("every one of the %s staged mailers matched exactly one arm", staged)


def rollback(cursor):
    """Abandon the transaction on the way out of a failure.

    Swallows a failure of the ROLLBACK itself, deliberately: closing the connection abandons an
    open transaction on both engines anyway, so a rollback that cannot run costs nothing, while
    an exception raised from inside an `except` block replaces the error that caused it.
    """
    if cursor is None:
        return
    try:
        wh.run(cursor, "ROLLBACK;", log=LOG)
    except Exception:                                   # pragma: no cover
        LOG.warning("ROLLBACK failed; closing the connection ends the transaction in any case, "
                    "and the error above this line is the one that matters", exc_info=True)


def self_check():
    """Assert what would go wrong silently. No database, no network, no files.

    The SQL itself is checked by running it -- `--dry-run` against DuckDB parses every statement
    and executes every one that reads. What running it does NOT check is whether the generated
    column lists mean what they are supposed to mean, because a MERGE with two columns swapped
    parses, runs, commits and is wrong.
    """
    for label, select in [("dim_client", DIM_CLIENT_SELECT), ("fact_mailer", FACT_MAILER_SELECT)]:
        names = columns(select)
        assert len(set(names)) == len(names), "%s produces a column twice" % label

    # 1. The three generated lists are the same columns in the same order. Order is the half
    #    that matters: INSERT (a, b) VALUES (source.b, source.a) is valid SQL and silently
    #    transposes two columns for the life of the table.
    for target, stage, keys, select in [
            ("processed_zone.dim_client", "stage_dim_client", DIM_CLIENT_KEYS,
             DIM_CLIENT_SELECT),
            ("processed_zone.fact_mailer", "stage_fact_mailer", FACT_MAILER_KEYS,
             FACT_MAILER_SELECT)]:
        sql = merge_sql(target, stage, keys, select)
        names = columns(select)
        insert_list = sql.split("INSERT (")[1].split(")")[0].split(", ")
        values_list = sql.split("VALUES (")[1].split(")")[0].split(", ")
        assert insert_list == names, "%s INSERT list is not the column table" % target
        assert values_list == ["source." + c for c in names], \
            "%s VALUES list does not match its INSERT list, column for column" % target
        # 2. A merge key assigned in the UPDATE clause reads as if the key were mutable, and the
        #    row it would move is the row the ON clause just matched.
        for key in keys:
            assert "\n        %s = source.%s" % (key, key) not in sql, \
                "%s assigns its merge key %s in the UPDATE clause" % (target, key)
        assert all("%s.%s = source.%s" % (target, k, k) in sql for k in keys), \
            "%s does not match on every one of its keys" % target

    # 3. The fact carries all 19 treatment flags with their native missingness, and carries them
    #    from the raw column of the same name -- the 14 that are NULL for the whole of wave 1
    #    are the input to the refinery's Step 4, and a flag lost here is lost silently.
    fact = dict(FACT_MAILER_SELECT)
    for flag in TREATMENT_FLAGS:
        assert fact[flag] == "mo." + flag, "%s is not carried straight from the raw zone" % flag

    # 4. Nothing in the fact's projection fills a null. bad_account is the one this is really
    #    about: a NULL there means no loan was made, and 0 would mean a loan was made and repaid.
    #    Written against the projection rather than the whole statement because the statement
    #    contains one legitimate COALESCE -- the watermark's, on an aggregate that is NULL only
    #    when the table is empty.
    body = projection(FACT_MAILER_SELECT).lower()
    for filler in ["coalesce", "nvl", "isnull", "case when"]:
        assert filler not in body, \
            "the fact projection contains %r; a null in this fact is a fact about the world, " \
            "not a gap to be filled" % filler
    assert fact["bad_account"] == "mo.badacct_last"

    # 5. The incremental predicate. `>` here would skip a wave for good after a part-merged run,
    #    and a watermark without COALESCE would make the first run -- the one that must load
    #    everything -- load nothing and report success.
    staging = stage_fact_mailer_sql()
    assert "WHERE mo.wave >= " in staging, "the fact stage does not filter on wave with >="
    assert "COALESCE(MAX(wave), 0)" in staging, "the watermark can evaluate to NULL"
    assert staging.lower().count("coalesce") == 1, "the fact stage coalesces something else too"
    assert "LEFT JOIN processed_zone.dim_offer_arm" in staging, \
        "the arm join is not a LEFT join, so a rate outside every band would drop its mailer"
    assert "arm.rate_ceil IS NULL" in staging, "the top arm of each band is not open above"

    # 6. The two flags the DDL declares BOOLEAN are converted, not passed through as 0/1.
    assert dict(DIM_CLIENT_SELECT)["is_female"] == "(ca.female = 1)"
    assert dict(DIM_CLIENT_SELECT)["is_more_educated"] == "(ca.edhi = 1)"

    LOG.info("self-check passed: %s fact columns and %s dim_client columns generated once each, "
             "%s treatment flags carried unfilled, the merge keys matched and not assigned, and "
             "the wave watermark >= with its COALESCE intact",
             len(FACT_MAILER_SELECT), len(DIM_CLIENT_SELECT), len(TREATMENT_FLAGS))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # --local and the connection flags belong to warehouse_common, which is what opens the
    # connection; this job adds only the two flags that are about its own work.
    wh.add_local_arguments(parser)
    parser.add_argument("--dry-run", action="store_true",
                        help="build both staging tables and run every check against them, then "
                             "roll back. Merges nothing and commits nothing")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the generated SQL's column lists and the incremental "
                             "predicate, then exit; no database, no network, no input")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run, and a strict parser exits 2 on them before the job starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return

    conn = None
    cursor = None
    try:
        conn = wh.connect(args.local, args.local_db, args.secret_name, args.region)
        cursor = conn.cursor()
        # One transaction, and it is opened, committed and abandoned by SQL through the cursor
        # rather than through the connection object. Two reasons, both engine-specific and both
        # silent if got wrong:
        #
        #   * `conn.autocommit = False` is the reference lab's way in, and it is a
        #     redshift_connector attribute that DuckDB's connection object does not have --
        #     assigning it there raises AttributeError before the first statement runs.
        #   * DuckDB's `conn.cursor()` DUPLICATES the connection rather than opening a cursor on
        #     it, so the transaction these statements run in belongs to the cursor and not to
        #     `conn`. `conn.commit()` would then commit an empty transaction on the parent, the
        #     cursor's work would be abandoned when it closed, and the job would log a merge it
        #     did not keep.
        #
        # BEGIN, COMMIT and ROLLBACK as statements are understood by both engines and always act
        # on the session that ran the work. Inside Redshift's implicit transaction the BEGIN is
        # a no-op.
        wh.run(cursor, "BEGIN TRANSACTION;", log=LOG)

        check_arm_grid(cursor)

        clients = build_stage(cursor, "stage_dim_client", stage_dim_client_sql())
        LOG.info("staged %s clients for dim_client (full snapshot, no watermark: "
                 "client_attributes has no event time to filter on)", clients)

        through = scalar(cursor, "SELECT %s;" % WAVE_WATERMARK)
        staged = build_stage(cursor, "stage_fact_mailer", stage_fact_mailer_sql())
        LOG.info("fact_mailer holds waves up to %s, so %s mailers are staged from wave %s "
                 "onward -- '>=' re-merges the newest wave every run, which the "
                 "(client_id, wave) key makes a no-op", through, staged, through)
        check_fact_stage(cursor, staged)

        if args.dry_run:
            wh.run(cursor, "ROLLBACK;", log=LOG)
            LOG.info("dry run complete: %s client rows and %s mailer rows staged and checked, "
                     "nothing merged and nothing committed", clients, staged)
            return

        wh.run(cursor, merge_sql("processed_zone.dim_client", "stage_dim_client",
                                 DIM_CLIENT_KEYS, DIM_CLIENT_SELECT), log=LOG)
        wh.run(cursor, merge_sql("processed_zone.fact_mailer", "stage_fact_mailer",
                                 FACT_MAILER_KEYS, FACT_MAILER_SELECT), log=LOG)
        wh.run(cursor, "COMMIT;", log=LOG)

        LOG.info("committed: dim_client merged from %s staged clients, fact_mailer merged from "
                 "%s staged mailers, both in one transaction", clients, staged)
    except Exception:
        rollback(cursor)
        # Re-raised, not logged and swallowed. A non-zero exit is what fails the Glue job, and a
        # failed Glue job is what stops the Step Functions chain before the refinery reads a
        # half-merged fact and reports a posterior over it.
        raise
    finally:
        # Both bound to None above, so a connection that never opened closes nothing here rather
        # than replacing the real error with a NameError.
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
