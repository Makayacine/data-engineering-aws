"""Warehouse connection surface shared by the three jobs that talk to the warehouse.

Extracted at the THIRD copy, not the second, which is this repo's stated rule. Glue uploads one
file per job, so a shared module means ``--extra-py-files`` on every job definition that uses it
-- a real cost, paid once per deployment and again every time somebody adds a job and forgets.
At two copies that cost is not worth ~40 duplicated lines. At three it is:

    redshift-raw-ingestion.py     COPY into tmp, MERGE into raw_zone
    redshift-processed-layer.py   stage, MERGE into the star
    glue-refinery-path3.py        read the fact, write the two bandit tables

all open the same warehouse, and the credential path is exactly the kind of code where three
drifting copies eventually disagree about which secret key holds the port.

Deployment: upload this alongside the job scripts and add

    --extra-py-files s3://<bucket>/jobs/warehouse_common.py

to each Glue job. Locally nothing is needed -- Python puts the running script's directory on
sys.path, so ``import warehouse_common`` resolves to the file next to the job.

DELIBERATELY NOT IN HERE
------------------------
Anything that knows what a table is or what a step does. No table names, no MERGE text, no
column lists, no watermark logic, no zone names. A helper that knows that ``mail_offers`` merges
on ``(client_id, wave)`` is a helper that has to be edited when the processed layer changes,
which is the coupling that makes a shared module worse than the duplication it replaced. Four
things live here and the list is meant to stay four.

WHY THE DRIVER IMPORTS ARE INSIDE THE FUNCTIONS
-----------------------------------------------
``redshift_connector`` is not installed on a laptop and ``duckdb`` is not installed in a Glue
Python Shell container. Importing both at module scope would mean the module cannot be imported
at all in either place, and the failure would land on the ``import warehouse_common`` line of a
job that was never going to use the missing driver. Imported inside the branch that needs it,
each side pays only for the driver it actually opens, and the error -- if it comes -- names the
driver and the mode that asked for it.

DUCKDB IS THE LOCAL STAND-IN FOR REDSHIFT, AND THE SUBSTITUTION IS NOT FREE
---------------------------------------------------------------------------
It is chosen for one reason: DuckDB 1.4 added ``MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED``,
which is the single Redshift statement this pipeline's correctness actually rests on. So the
local run executes the same MERGE text rather than a rewritten equivalent, and the statement
that can corrupt data is the statement that gets exercised. What DuckDB does not model is
cluster physics rather than statement semantics: distribution and sort keys, COPY from S3, IAM
and WLM are Redshift's, and its planner is free to pick a different plan for the same text.

WHY ``run()`` DOES NOT COMMIT
------------------------------
The reference lab's equivalent helper commits after every statement AND catches the exception,
logs it, rolls back, and returns normally. The caller cannot tell that anything failed, so a
job whose MERGE raised still finishes green, with the COPY already committed and the merge
missing -- a partially loaded warehouse reported as a success. ``run()`` here does neither: it
executes, it logs, and it lets the exception out. Transaction control belongs to the job,
because only the job knows which group of statements is the unit that must be all-or-nothing.
"""

import argparse
import json
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("warehouse_common")

# The reference lab's secret name, kept. The secret is expected to be a JSON string holding
# host / dbname / username / password, and optionally port.
DEFAULT_SECRET = "dwh-credentials"

# Repo-relative, and _localrun/ is gitignored. Separate file from the source database that
# local-development/build_source_db.py writes: that one stands in for MySQL, this one stands in
# for Redshift, and collapsing them into one file would let a job read a table it should have
# had to load first.
DEFAULT_LOCAL_DB = os.path.join("_localrun", "warehouse.duckdb")

REDSHIFT_PORT = 5439

# How much of a statement reaches the log. A MERGE in this pipeline is a few hundred characters
# of column list; the first line of it identifies the statement, the rest is noise repeated on
# every run in CloudWatch, which is billed by ingested byte.
SQL_LOG_CHARS = 160


def get_secret(secret_name=DEFAULT_SECRET, region=None):
    """Read one Secrets Manager secret and parse its SecretString as JSON.

    Raises rather than calling ``sys.exit(1)`` the way the reference lab does. A library
    function that exits the process takes the decision away from every caller it will ever
    have, and it throws away the traceback -- which, for a credential failure, is the part that
    says whether the secret was missing, unparseable, or refused by IAM. An uncaught exception
    still exits non-zero, so Glue still marks the job FAILED; the difference is only in what
    CloudWatch is left holding.

    `region` defaults to the environment, which inside Glue is the region the job is running in.
    """
    import boto3  # preinstalled in Glue Python Shell; not needed by a --local run

    client = boto3.session.Session().client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    if "SecretString" not in response:
        raise ValueError("secret %r holds binary, not a string: this pipeline stores warehouse "
                         "credentials as a JSON string" % secret_name)
    return json.loads(response["SecretString"])


def connect(local, local_db=DEFAULT_LOCAL_DB, secret_name=DEFAULT_SECRET, region=None):
    """Open the warehouse and return a DB-API connection. The caller owns the transaction.

    ``local`` picks the driver and nothing else. Both sides return an object with ``cursor()``,
    ``commit()``, ``rollback()`` and ``close()``, so the calling job's body is the same text in
    both modes and only this line differs.

    On the DuckDB side ``commit()`` is a no-op when no explicit transaction is open, rather than
    an error, so a job that opens a connection and commits once at the end behaves the same
    locally as it does against Redshift. That is worth knowing rather than assuming: the
    alternative -- a driver that raises on a commit outside a transaction -- would make every
    job need a mode check around its own commit, and the point of this function is that they
    do not.

    The parent directory of `local_db` is created if it is missing, because ``_localrun/`` is
    gitignored and therefore does not exist in a fresh clone.
    """
    if local:
        import duckdb

        parent = os.path.dirname(local_db)
        if parent:
            os.makedirs(parent, exist_ok=True)
        LOG.info("local mode: DuckDB at %s", local_db)
        return duckdb.connect(local_db)

    import redshift_connector

    credentials = get_secret(secret_name, region)
    host = credentials["host"]
    database = credentials["dbname"]
    # The reference lab hardcodes 5439. Read it from the secret when it is there, so a cluster on
    # a non-default port is a secret change rather than a code change; fall back to the port
    # every Redshift cluster is created with.
    port = int(credentials.get("port", REDSHIFT_PORT))
    LOG.info("connecting to Redshift %s:%s database %s", host, port, database)
    return redshift_connector.connect(host=host, database=database, port=port,
                                      user=credentials["username"],
                                      password=credentials["password"])


def run(cursor, sql, log=LOG):
    """Log one statement and execute it. No commit, no exception handling. Returns the cursor.

    The logged form is the statement with its whitespace collapsed and truncated, so a triple
    quoted SQL block -- whose first physical line is empty -- still produces one readable line
    naming the operation and the table. The SQL that is EXECUTED is the string as passed,
    untouched: the truncation is applied to a copy for the log, never to the statement, because
    a helper that rewrote the SQL on its way to the driver would make the log a description of
    something other than what ran.

    `log` is a parameter so each job's lines carry that job's logger name in CloudWatch. The
    default is for a caller that has not got one, not the expected usage.
    """
    condensed = " ".join(sql.split())
    log.info("SQL %s%s", condensed[:SQL_LOG_CHARS],
             " ..." if len(condensed) > SQL_LOG_CHARS else "")
    cursor.execute(sql)
    # Not every driver populates rowcount for every statement -- a negative value means "the
    # driver did not say", which is different from "no rows", so it is not logged as zero.
    affected = getattr(cursor, "rowcount", -1)
    if affected is not None and affected >= 0:
        log.info("    %s row(s) affected", affected)
    return cursor


def add_local_arguments(parser):
    """Add the four flags every warehouse job takes, so ``connect()`` is one shared line.

    The jobs then call::

        conn = warehouse_common.connect(args.local, args.local_db, args.secret_name, args.region)

    which is the same line in all three. Keeping the flag NAMES identical is most of the value
    here: three jobs that each invented their own spelling of ``--local-db`` would be three jobs
    that cannot be driven by one shell script.

    Not added here: ``--table-name``, ``--dry-run``, ``--self-check``. They differ per job, or
    mean different things per job, and this module is not allowed to know which.

    A note for the caller rather than for this function: parse with ``parse_known_args()``, not
    ``parse_args()``. Glue appends ``--JOB_NAME`` and friends to sys.argv on every run, and a
    strict parser exits 2 on them before the job body starts.
    """
    parser.add_argument("--local", action="store_true",
                        help="run against the local DuckDB warehouse instead of Redshift; "
                             "needs no AWS account and no network")
    parser.add_argument("--local-db", default=DEFAULT_LOCAL_DB,
                        help="DuckDB warehouse file for --local (default: %(default)s)")
    parser.add_argument("--secret-name", default=DEFAULT_SECRET,
                        help="Secrets Manager secret holding the warehouse credentials as JSON "
                             "(default: %(default)s). Ignored by --local")
    parser.add_argument("--region", default=None,
                        help="AWS region; defaults to the environment, which on Glue is the "
                             "region the job runs in. Ignored by --local")
    return parser


def _self_check():
    """Assert the two things in this module that can break silently. No AWS, no network, no files.

    What is pinned:

    1.  ``run()`` hands the driver the EXACT string it was given. The log line is allowed to be
        condensed and truncated; the statement is not. A helper that normalised whitespace into
        the SQL it executed would still pass every functional test -- SQL does not care about
        whitespace -- right up to the first string literal containing two spaces.
    2.  ``run()`` does not commit. The stub cursor below has no ``commit`` and no ``connection``,
        so any attempt to reach transaction control from in here is an AttributeError rather
        than a behaviour nobody notices until a half-loaded warehouse reports success.

    The connection branches are not pinned: opening either driver needs that driver installed,
    and a check that asserts a mock was called is a check of the mock.
    """

    class StubCursor(object):
        def __init__(self):
            self.executed = []
            self.rowcount = -1

        def execute(self, sql):
            self.executed.append(sql)

    messy = "\n  MERGE INTO  a\n  USING b ON  a.k = b.k  -- note the  'two  spaces'\n"
    cursor = StubCursor()
    assert run(cursor, messy) is cursor
    assert cursor.executed == [messy], "run() must execute the statement it was handed, verbatim"

    long_sql = "SELECT " + ("x, " * 200) + "1"
    run(cursor, long_sql)
    assert cursor.executed[-1] == long_sql, "truncation is for the log only"

    parser = add_local_arguments(argparse.ArgumentParser())
    args, _ = parser.parse_known_args(["--JOB_NAME", "irrelevant"])
    assert args.local is False
    assert args.local_db == DEFAULT_LOCAL_DB
    assert args.secret_name == DEFAULT_SECRET
    assert args.region is None
    args, _ = parser.parse_known_args(["--local", "--local-db", "x.duckdb"])
    assert args.local is True and args.local_db == "x.duckdb"

    LOG.info("self-check passed: run() is verbatim and commit-free, and the flag fragment "
             "survives Glue's extra sys.argv entries")


if __name__ == "__main__":
    _self_check()
