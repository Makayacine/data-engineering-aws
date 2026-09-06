"""Load one landed CSV into raw_zone: TRUNCATE the staging table, COPY, MERGE, TRUNCATE, commit.

AWS Glue **Python shell** entrypoint, not Spark. This job issues five statements and moves no
rows through Python at all -- the warehouse does the loading -- so a Spark job would spend its
startup time doing nothing this file needs. No SparkSession, no JVM, no awsglue import. Glue
Python Shell hands ``--key value`` straight to ``sys.argv``, so the arguments are read with
argparse rather than ``getResolvedOptions``.

    s3://<bucket>/raw_landing_zone/credit_mailer_db/<table>/data.csv   written by mysql-extraction
      ->  COPY   into raw_zone.tmp_<table>
      ->  MERGE  into raw_zone.<table>   on the table's own key
      ->  TRUNCATE raw_zone.tmp_<table>
      ->  one commit                     (locally. On Redshift, see below -- TRUNCATE commits.)

``--table_name`` is ``mail_offers`` or ``client_attributes``, and the two differ in exactly one
respect that matters here: the merge key is ``(client_id, wave)`` for the first and
``(client_id)`` for the second. Everything else is the same statement with a different column
list, which is why the statements are generated from one table description instead of written
out twice.

WHY A STAGING TABLE AND A MERGE, RATHER THAN COPYING STRAIGHT INTO raw_zone
--------------------------------------------------------------------------

Because a COPY appends. The landing object is overwritten on every run and re-read from the
start, so a re-run of a load that already succeeded -- a Step Functions retry, a manual replay
after a downstream failure -- would put the same rows in twice.

What would normally stop that is the primary key, and on Redshift it does not: ``PRIMARY KEY``
is declarative there, used by the planner and never enforced, so the duplicate load raises
nothing and the table is simply wrong afterwards. DuckDB, which is the local stand-in, *does*
enforce it, so the same mistake fails loudly on a laptop and silently on the cluster. That
asymmetry is worth knowing about rather than relying on: the MERGE is what makes the load
idempotent, on both engines, and the key constraint is not doing the work on either.

The MERGE also matches how the source grows. ``mail_offers`` arrives a wave at a time and
``client_attributes`` arrives whole on every run (its watermark configuration has no load
column, so all 58,168 rows are re-extracted each time). The first case only ever inserts, the
second re-states rows that are already there; one statement covers both without the job needing
to know which case it is in.

TWO THINGS THE REFERENCE LAB GETS WRONG, FIXED HERE
---------------------------------------------------

1.  **Its ``finally: if conn: conn.close()`` raises ``NameError`` when the connect call itself
    failed.** ``conn`` is only bound by a successful ``connect()``, so a bad secret, a closed
    security group or an unreachable endpoint produces a ``NameError`` in CloudWatch *instead
    of* the connection error that caused it -- the one log line that would have said what was
    actually wrong is replaced by one about a variable name. ``conn = None`` before the ``try``
    costs a line and keeps the real error.

2.  **It truncates the staging table after the merge and never before the COPY.** That is only
    safe while every previous run reached its truncate. A run that fails between the COPY and
    the commit leaves rows in ``tmp_<table>``, and the next run COPYs on top of them and merges
    the union -- old rows re-stated as though they had just been extracted, with nothing in the
    logs to show it. Truncating before the COPY makes the staging table's contents a function of
    this run alone. The truncate after the merge is kept as well, because leaving a full staging
    table behind between runs is storage nobody is watching.

Both are the same shape of fault: an error path that produces a plausible outcome instead of a
loud one. Neither is visible in a green run, which is why they are named here.

AND THE THING THE DIAGRAM ABOVE OVERSTATES, ON REDSHIFT ONLY
-------------------------------------------------------------

"one commit" is true of the local DuckDB run and NOT of Redshift, because **Redshift's TRUNCATE
commits the transaction it is running inside**. So on the cluster this is not one atomic unit
ended by ``conn.commit()``; it is three, with the boundaries at the two truncates:

    TRUNCATE tmp  [commit]  ->  COPY, MERGE  ->  TRUNCATE tmp  [commit]  ->  conn.commit() (no-op)

The load is still correct, and it is correct because of fix (2) rather than in spite of it: a run
that dies after the MERGE has already committed the MERGE, and a run that dies before it leaves
staging rows that the NEXT run's pre-COPY truncate removes before they can be merged twice. What
is lost is the ability to say the merge and the staging cleanup succeed or fail together.

``DELETE FROM tmp_<table>`` is transactional on Redshift and would buy that property back, on
tables that never exceed 58,168 rows. It is not used, for one reason: the reference lab
truncates, this file is an imitation of the reference lab, and swapping the statement would hide
the difference rather than document it. If this pipeline were carrying a load where a
half-applied run mattered, DELETE is the change to make, and this paragraph is where to start.

ONE STATEMENT'S TEXT DIFFERS BETWEEN THE TWO MODES, AND IT IS NOT THE MERGE
---------------------------------------------------------------------------

The COPY is written twice -- ``IAM_ROLE ... CSV IGNOREHEADER 1`` for Redshift, and
``(FORMAT CSV, HEADER)`` for DuckDB reading a local file. There is no way around that: the two
engines spell bulk loading differently and one of them is reading S3.

The MERGE is not written twice. It is generated once, from the column list, and the same string
is sent to whichever engine is connected. That matters because the COPY and the MERGE fail
differently: a COPY that is wrong fails to load, and a MERGE that is wrong loads the wrong
thing. Rehearsing a rewritten MERGE locally would rehearse nothing about the one on the cluster;
rehearsing the same text does.

The local run issues one statement the AWS run does not: ``BEGIN TRANSACTION``. This is not
cosmetic. ``redshift_connector`` opens a transaction implicitly on the first statement because
autocommit is off, so ``conn.commit()`` at the end is the whole load. DuckDB's Python connection
commits each statement as it goes unless a transaction is open, and -- measured on DuckDB
1.5.5 -- ``rollback()`` with nothing open raises ``TransactionException: cannot rollback - no
transaction is active``, from inside the error handler, replacing the error being handled. That
is fault (1) again wearing different clothes, so the local path opens the transaction it needs
rather than pretending it has one.

The same measurement produced the second local-only line in ``main()``: DuckDB's ``cursor()``
returns an *independent connection over the same database*, not a cursor over the same session,
so a ``BEGIN`` sent through it belongs to a transaction that ``conn.commit()`` cannot see.
Locally the connection is therefore used as its own cursor. ``redshift_connector.cursor()`` is
a DB-API cursor on the existing session, so the AWS path is the reference lab's, unchanged.

EMPTY FIELDS, WHICH ARE NOT THE SAME AS EMPTY STRINGS
------------------------------------------------------

The extract is written by ``csv.DictWriter``, which renders a NULL as an empty field. Redshift's
CSV COPY turns an empty field into NULL for a numeric column but into an empty *string* for a
VARCHAR one, while DuckDB reads both as NULL. ``race`` is VARCHAR and has 298 nulls, so without
``EMPTYASNULL`` those 298 rows would hold ``''`` on Redshift and NULL locally -- the two runs
would disagree about the data, which would make every local rehearsal of a downstream statement
worth less than it looks. ``EMPTYASNULL`` is therefore on the COPY, and it is the reason the
COPY says more than the reference lab's does.

The numeric NULLs are load-bearing in the other direction and must survive: 14 columns are NULL
on all 4,974 wave-1 rows because wave 1 was a price-only experiment, and ``badacct_last`` is
NULL except where a loan was taken. Nothing here fills them. That is Step 4's decision and it is
argued in ``glue-refinery-path3.py``; this job's part in it is to not quietly undo it, which
means SMALLINT columns and no ``DEFAULT 0`` anywhere in the DDL.

WHAT THE COPY ASSUMES, STATED BECAUSE IT IS NOT CHECKED
--------------------------------------------------------

Both engines load a CSV **positionally**. The header line is skipped, not matched, so the
column order of the extract must equal the column order of the target table. It does, because
both come from the same list in section 2 of the build spec -- the extractor selects the table
whose columns are in that order, and the DDL declares them in that order. If someone reorders
one of the two, the load will not fail: the flag columns are all SMALLINT 0/1 and would shift
into each other's places without a type error. A column list on the COPY does not fix this,
because it would only rename the positions rather than match the header, so nothing here
pretends to.

ONE LOAD MUST FINISH BEFORE THE NEXT EXTRACT OF THE SAME TABLE STARTS
----------------------------------------------------------------------

The landing key is fixed -- ``.../<table>/data.csv``, overwritten every run -- so the raw landing
zone holds only the most recent extract of each table. If an extract of a table ran while this
job was loading the previous one, the COPY would read whichever version won the race. The Step
Functions definition is a sequential chain for that reason and not a ``Parallel`` state, and
this job is the half of that constraint which cannot enforce it.

THE ZERO-ROW RUN IS A NORMAL RUN
---------------------------------

The fourth run of the demonstration extracts 0 rows of ``mail_offers``, because the watermark
already holds the last wave. The extractor writes a header-only CSV rather than an empty object,
so the COPY here loads 0 rows and the MERGE matches nothing and inserts nothing. No branch is
needed for it and none is written; the row counts are logged, which is what makes the no-op
visible in CloudWatch as a no-op rather than as a silence.

WHAT THIS JOB DOES NOT DO
--------------------------

It does not create tables. ``redshift/redshift-create-tables.sql`` is applied first, in both
modes, and a loader holding CREATE rights is a loader holding a permission it uses on no normal
day. It does not create the staging tables either -- they are ``CREATE TABLE ... AS SELECT *``
copies declared in the same file, so their column order cannot drift from the target's.

It runs no step of the refinery framework, so it carries **no verdict badges**. APPLIES / N/A /
OVERRIDE / LIMIT / ENFORCE / BANNED are the vocabulary of the ten steps, and a row count wearing
one of them would read like a verdict about the data.

WHAT THE LOCAL RUN COVERS
--------------------------

Checked on DuckDB 1.5.5: that the generated MERGE parses and applies with the target qualified by
schema in the ON clause, that re-running it changes nothing, that a zero-row staging table merges
as a no-op, and the two transaction behaviours described above. The MERGE is the statement worth
rehearsing, because it is the one that can corrupt a table rather than refuse to load, and it is
rehearsed as the exact text a cluster would receive. The COPY is the one statement whose text
differs between the two engines, and the bucket and IAM role in it are arguments the deployer
supplies.

Local acceptance run, after ``build_source_db.py``, ``mysql-extraction.py`` and the DDL::

    python redshift-raw-ingestion.py --self-check

    python redshift-raw-ingestion.py --local --table_name mail_offers
    python redshift-raw-ingestion.py --local --table_name client_attributes
"""

import argparse
import logging
import textwrap

# The shared warehouse surface: --local and the connection arguments, the connection itself, and
# the statement runner. Deployment on Glue needs it uploaded alongside this script and named on
# the job definition:
#
#   --extra-py-files s3://<bucket>/jobs/warehouse_common.py
#
# Locally nothing is needed -- Python puts the running script's directory on sys.path, so the
# import resolves to the file next to this one.
from warehouse_common import add_local_arguments, connect, run

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("redshift_raw_ingestion")

RAW_SCHEMA = "raw_zone"
LANDING_PREFIX = "raw_landing_zone/credit_mailer_db"
DEFAULT_LOCAL_LANDING = "_localrun/" + LANDING_PREFIX

# The two tables, at the two grains the wide deposit was split into, with their columns in the
# order redshift-create-tables.sql declares them -- which is also the order the extract's header
# is in, and the COPY depends on the two agreeing (see the docstring).
#
# `keys` is the merge key and nothing else: mail_offers is one row per (client, wave) because a
# client can be mailed in more than one wave, client_attributes is one row per client because it
# is a snapshot with no event time at all.
TABLES = {
    "mail_offers": {
        "keys": ("client_id", "wave"),
        "columns": ("client_id", "wave", "offer4", "prize", "intshown", "dphoto_female",
                    "dphoto_none", "dphoto_black", "gender_match", "race_match",
                    "nspeakeligible", "speak_trt", "oneln_trt", "comploss_n", "use_any",
                    "stripany", "comp_n", "deadlinemed", "deadlinelong", "deadlong_elig",
                    "deadshort_elig", "deadlineshortext", "waved3", "applied", "tookup",
                    "amountbrw_unc", "badacct_last", "applied_2weeks", "tookup_after_short",
                    "tookup_after_med", "tookup_after_long", "tookup_outside_only"),
    },
    "client_attributes": {
        "keys": ("client_id",),
        "columns": ("client_id", "race", "risk", "female", "edhi", "dormancy", "trcount"),
    },
}


def _wrap(items, indent):
    """A comma-separated list wrapped to the file's line width, for a readable logged statement.

    run() logs every statement it executes. A 32-column INSERT list on one line is 300-odd
    characters of CloudWatch that nobody reads, and the whole point of logging the SQL is that
    somebody can read it.
    """
    return textwrap.fill(", ".join(items), width=96, subsequent_indent=indent)


def merge_sql(table):
    """The MERGE for one table, generated from its column list.

    The reference lab writes its three MERGEs out by hand, which is reasonable at five and
    seventeen columns and is not at thirty-two: the UPDATE list and the INSERT list would then
    be two hand-maintained copies of the same thirty names, and the failure when they disagree
    is a column that silently stops being updated. One list, used twice, cannot disagree with
    itself.

    The target is written out in full in the ON clause rather than aliased, because Redshift's
    MERGE does not accept an alias on the target table. DuckDB accepts the qualified form as
    well, which is what lets the same text run in both places.
    """
    spec = TABLES[table]
    target = "{0}.{1}".format(RAW_SCHEMA, table)
    staging = "{0}.{1}{2}".format(RAW_SCHEMA, "tmp_", table)
    # The keys are matched on and never updated: assigning a key to the value it was matched by
    # is at best a no-op and at worst rejected, and it makes the statement read as though the
    # grain were mutable.
    updates = [c for c in spec["columns"] if c not in spec["keys"]]
    return (
        "MERGE INTO {target}\n"
        "USING {staging} AS source\n"
        "ON {on}\n"
        "WHEN MATCHED THEN\n"
        "    UPDATE SET\n"
        "        {sets}\n"
        "WHEN NOT MATCHED THEN\n"
        "    INSERT ({cols})\n"
        "    VALUES ({vals})"
    ).format(
        target=target,
        staging=staging,
        on="\n   AND ".join("{0}.{1} = source.{1}".format(target, k) for k in spec["keys"]),
        sets=",\n        ".join("{0} = source.{0}".format(c) for c in updates),
        cols=_wrap(spec["columns"], " " * 12),
        vals=_wrap(["source." + c for c in spec["columns"]], " " * 12),
    )


def copy_sql(table, source, local, iam_role=None):
    """The bulk load into the staging table. The one statement whose text is not shared.

    EMPTYASNULL on the Redshift side is the difference from the reference lab's COPY, and it is
    there so the two engines agree about `race`: an empty CSV field is NULL to DuckDB and an
    empty string to Redshift for a VARCHAR column, which would have the 298 null races land
    differently in the two places.
    """
    staging = "{0}.{1}{2}".format(RAW_SCHEMA, "tmp_", table)
    if local:
        return "COPY {0} FROM '{1}' (FORMAT CSV, HEADER)".format(staging, source)
    return ("COPY {staging}\n"
            "FROM '{source}'\n"
            "IAM_ROLE '{role}'\n"
            "CSV\n"
            "IGNOREHEADER 1\n"
            "EMPTYASNULL").format(staging=staging, source=source, role=iam_role)


def landing_uri(table, local, bucket=None, local_landing=DEFAULT_LOCAL_LANDING):
    """Where the extract for this table was written.

    A fixed object key, not the prefix the reference lab COPYs from. Both work -- Redshift
    treats the FROM as a prefix either way -- but the key names exactly the one object the
    extractor wrote, so a stray file left in the prefix by anything else cannot join the load.
    """
    if local:
        return "{0}/{1}/data.csv".format(local_landing.rstrip("/"), table)
    return "s3://{0}/{1}/{2}/data.csv".format(bucket.strip("/"), LANDING_PREFIX, table)


def count_rows(cursor, table, log=LOG):
    """One count, logged by the caller. run() executes; the fetch is DB-API on both engines."""
    run(cursor, "SELECT count(*) FROM {0}".format(table), log=log)
    return cursor.fetchone()[0]


def self_check():
    """Assert the generated SQL, with no warehouse, no network and no files.

    Everything this job does at runtime is executed by the engine, so the only thing here that
    can be wrong on its own is the text sent to it -- and the ways it can be wrong are all
    quiet: a column missing from the UPDATE list stops being updated, an INSERT list that does
    not line up with its VALUES list loads every value into the wrong column.
    """
    # 1. The split from section 2 of the build spec: 32 columns at the event grain and 7 at the
    #    client grain, client_id in both as the minted join key, and nothing else shared.
    mail = TABLES["mail_offers"]["columns"]
    client = TABLES["client_attributes"]["columns"]
    assert len(mail) == 32, "mail_offers has {0} columns, not 32".format(len(mail))
    assert len(client) == 7, "client_attributes has {0} columns, not 7".format(len(client))
    assert len(set(mail)) == len(mail) and len(set(client)) == len(client), "a column repeats"
    assert set(mail) & set(client) == {"client_id"}, \
        "the two tables share a column other than the minted client_id"
    # 30 event columns + 6 attribute columns + client_id in both = the 37 source columns.
    assert len(mail) + len(client) - 2 == 37, "the split no longer accounts for 37 columns"

    for table, spec in TABLES.items():
        merge = merge_sql(table)
        target = "{0}.{1}".format(RAW_SCHEMA, table)

        # 2. Every key is matched on, and no key is assigned. A merge on a subset of the key
        #    overwrites rows that are not the same row: dropping `wave` would make every wave of
        #    a client fight over one target row, which is the failure that loses data without
        #    raising anything.
        for key in spec["keys"]:
            assert "{0}.{1} = source.{1}".format(target, key) in merge, \
                "{0} is not matched on in {1}'s MERGE".format(key, table)
            assert "        {0} = source.{0}".format(key) not in merge, \
                "{0} is a merge key and must not be in the UPDATE list".format(key)

        # 3. Every non-key column is updated. A column absent from the UPDATE list is loaded on
        #    the first insert and then frozen for ever, with no error at any point.
        for column in spec["columns"]:
            if column not in spec["keys"]:
                assert "{0} = source.{0}".format(column) in merge, \
                    "{0} is never updated by {1}'s MERGE".format(column, table)

        # 4. The INSERT list and the VALUES list agree, in order. This is the reason the
        #    statement is generated rather than typed, so it is the one thing worth pinning.
        head, _, tail = merge.partition("    INSERT (")
        cols, _, vals = tail.partition(")\n    VALUES (")
        assert head and vals, "{0}'s MERGE no longer has the expected INSERT shape".format(table)
        cols = [c.strip() for c in cols.replace("\n", " ").split(",")]
        vals = [v.strip() for v in vals.rstrip(")").replace("\n", " ").split(",")]
        assert cols == list(spec["columns"]), "{0}'s INSERT column list is wrong".format(table)
        assert vals == ["source." + c for c in spec["columns"]], \
            "{0}'s VALUES list does not line up with its INSERT column list".format(table)

        # 5. The staging table is a sibling of the target in the same schema. An unqualified
        #    name would resolve against search_path, which is not the same thing twice.
        assert "USING {0}.tmp_{1} AS source".format(RAW_SCHEMA, table) in merge
        assert merge.startswith("MERGE INTO {0}\n".format(target))

    # 6. The COPY. The local form must not carry an IAM role and the Redshift form must, and
    #    the local one reads a path while the other reads an s3:// URI.
    aws = copy_sql("mail_offers", landing_uri("mail_offers", False, bucket="a-bucket"), False,
                   iam_role="arn:aws:iam::000000000000:role/example")
    local = copy_sql("mail_offers", landing_uri("mail_offers", True), True)
    assert "IAM_ROLE" in aws and "EMPTYASNULL" in aws and "IGNOREHEADER 1" in aws
    assert "s3://a-bucket/{0}/mail_offers/data.csv".format(LANDING_PREFIX) in aws
    assert "IAM_ROLE" not in local and "(FORMAT CSV, HEADER)" in local
    assert "{0}/mail_offers/data.csv".format(DEFAULT_LOCAL_LANDING) in local
    assert "s3://" not in local, "the local COPY is reading S3"

    # 7. The MERGE takes no mode argument at all, which is what makes "the same text runs in
    #    both places" a property of the code rather than a promise in a comment. Checked the
    #    only way it can be: nothing engine-specific is in the text.
    for table in TABLES:
        merge = merge_sql(table)
        for foreign in ["s3://", "IAM_ROLE", "FORMAT CSV", "IGNOREHEADER"]:
            assert foreign not in merge, "{0} leaked into {1}'s MERGE".format(foreign, table)

    LOG.info("self-check passed: the 32/7 split accounts for all 37 source columns, both merge "
             "keys are matched and neither is assigned, every non-key column is updated, the "
             "INSERT and VALUES lists line up in order, and the COPY is the only statement that "
             "knows which engine it is talking to")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # Both spellings, because two callers disagree and both are right. Glue and the Step
    # Functions definition pass --table_name, which is what getResolvedOptions expects and what
    # the reference lab's job is named for; the hyphenated form is what a person typing it at a
    # shell will reach for. argparse takes both option strings and one dest.
    parser.add_argument("--table_name", "--table-name", dest="table_name",
                        choices=sorted(TABLES),
                        help="which landed extract to load. The choice sets the merge key: "
                             "(client_id, wave) for mail_offers, (client_id) for "
                             "client_attributes")
    parser.add_argument("--bucket",
                        help="S3 bucket holding the raw landing zone. Required unless --local; "
                             "it has no default because a default bucket name in a repository "
                             "is a name that eventually belongs to somebody else")
    parser.add_argument("--iam-role",
                        help="ARN of the role Redshift assumes to read the landing object. "
                             "Required unless --local. Redshift does the reading, not this job, "
                             "so the Glue role's own S3 access is irrelevant to the COPY")
    parser.add_argument("--local-landing", default=DEFAULT_LOCAL_LANDING,
                        help="directory the local extract was written under "
                             "(default: %(default)s)")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the generated SQL and exit; no warehouse, no network, no "
                             "files")
    # --local, and whatever connect() needs to reach a warehouse. Both live in warehouse_common
    # because three jobs need them and one place to change them is the point of the module.
    add_local_arguments(parser)
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run, and a strict parser exits 2 on them before the job starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return

    if not args.table_name:
        parser.error("--table_name is required unless --self-check")
    if not args.local:
        missing = [name for name, value in (("--bucket", args.bucket),
                                            ("--iam-role", args.iam_role)) if not value]
        if missing:
            parser.error("{0} required without --local: the COPY cannot be written without "
                         "them".format(" and ".join(missing)))

    table = args.table_name
    staging = "{0}.tmp_{1}".format(RAW_SCHEMA, table)
    target = "{0}.{1}".format(RAW_SCHEMA, table)
    source = landing_uri(table, args.local, args.bucket, args.local_landing)
    LOG.info("loading %s into %s from %s", table, target, source)

    # Bound BEFORE the try, which is fix (1) from the docstring: the reference lab binds it
    # inside and its finally clause then raises NameError over the top of whatever went wrong in
    # connect() -- a bad secret, a closed security group -- and CloudWatch keeps the wrong one.
    conn = None
    try:
        conn = connect(args.local, args.local_db, args.secret_name, args.region)
        # DuckDB's cursor() opens an independent connection over the same database, so a
        # transaction begun through it is not the one conn.commit() and conn.rollback() act on.
        # Locally the connection is its own cursor; redshift_connector's cursor() is a DB-API
        # cursor on the existing session and needs no such thing.
        cursor = conn if args.local else conn.cursor()
        if args.local:
            # redshift_connector has autocommit off and opens this implicitly. DuckDB does not,
            # and without it rollback() below would raise "cannot rollback - no transaction is
            # active" from inside the error handler, hiding the error it was handling.
            run(cursor, "BEGIN TRANSACTION", log=LOG)

        # Fix (2): before the COPY, not only after the merge. A run that died between the COPY
        # and the commit leaves rows here, and appending this run's extract to them would merge
        # a union of two extracts as though it were one.
        run(cursor, "TRUNCATE TABLE {0}".format(staging), log=LOG)
        run(cursor, copy_sql(table, source, args.local, args.iam_role), log=LOG)
        staged = count_rows(cursor, staging)
        if staged == 0:
            LOG.info("0 rows staged: the extract was header-only, so the watermark had already "
                     "reached the last wave in the source and the merge below is a no-op")

        run(cursor, merge_sql(table), log=LOG)
        run(cursor, "TRUNCATE TABLE {0}".format(staging), log=LOG)
        held = count_rows(cursor, target)
        conn.commit()
        LOG.info("%s: %s rows staged and merged, %s rows now in %s", table, staged, held, target)
    except Exception:
        if conn is not None:
            conn.rollback()
        # Re-raised rather than sys.exit(1): Glue fails the job on any non-zero exit, and the
        # traceback is the only thing in the log that says which of the five statements failed.
        raise
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
