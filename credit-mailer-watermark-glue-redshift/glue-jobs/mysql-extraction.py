"""Extract one source table to the raw landing zone, driven by an externalised watermark.

AWS Glue **Python shell** entrypoint, not Spark. This is the reference lab's
``glue/mysql-extraction.py`` kept faithful and made correct: the same four moving parts in the
same order, the same SQL text, the same CSV, with the places it goes wrong marked rather than
quietly rewritten.

      MySQL  credit_mailer.<table_name>
        ->   SELECT, with a watermark predicate when the config table says so
        ->   csv.DictWriter into io.StringIO
        ->   one put_object at s3://<bucket>/raw_landing_zone/credit_mailer_db/<table>/data.csv
        ->   DynamoDB incremental_load_configurations.last_extracted_value = the new maximum

THE WHOLE LESSON IS THAT THE WATERMARK LIVES OUTSIDE THE JOB
------------------------------------------------------------

Nothing in this file knows how far the last run got. It asks
``incremental_load_configurations`` (partition key ``table_name``), and it writes the answer
back there when it finishes. Two rows are seeded, and they are the two halves of the lesson::

    {"table_name": "mail_offers",       "load_column": "wave", "last_extracted_value": None}
    {"table_name": "client_attributes", "load_column": None,   "last_extracted_value": None}

``client_attributes`` is a CRM snapshot. It has no event time, so it has no ``load_column``, so
it can only ever be a full load -- all 58,168 rows on every run, for ever. That table is not a
gap in the design; it is the control. A per-table watermark that only ever ran against tables
which happen to have a timestamp would never have to answer the question of what to do with a
table that does not.

``mail_offers`` is the incremental one, and the staging comes from the SOURCE GROWING, not from
this job limiting itself. The lender ran three mailer waves in 2003 and the source database
gains a wave at a time, so four runs of this job against a source that grows underneath it
produce:

===  ================  ==============================  ==================
run  source holds      mail_offers extracts            watermark after
===  ================  ==============================  ==================
1    wave 1            4,974 rows (no last value)      ``1``
2    waves 1-2         20,996 rows (``wave > 1``)      ``2``
3    waves 1-3         32,198 rows (``wave > 2``)      ``3``
4    waves 1-3         0 rows (``wave > 3``)           ``3``, unchanged
===  ================  ==============================  ==================

Run 4 is the cheapest possible proof that the watermark is being READ and not merely written. A
job that ignored the stored value would extract all 58,168 rows on run 4 and look entirely healthy
doing it: same exit code, same S3 object, same duration to within a second. The only thing that
tells the two apart is the row count, and the only way to get a row count of zero is to have
read the value that a previous run left behind.

THE WATERMARK IS A STRING, AND THAT IS A TRAP WORTH NAMING
-----------------------------------------------------------

The reference lab writes ``str(new_last_value)`` into DynamoDB and compares with
``WHERE <col> > '<last>'``. Both sides are therefore text. ``'2' > '1'`` and ``'3' > '2'``, so
this is correct for waves 1, 2 and 3 -- and it stops being correct at wave 10, because
``'10' > '9'`` is false and run 11 would extract nothing while reporting success.

That is not fixed here. The reference lab's behaviour is the thing being imitated, and a
string-compared watermark that happens to be safe on a three-valued column is worth naming as a
trap rather than papering over: the failure it produces is a silent zero-row extract, which is
indistinguishable from run 4's legitimate one. What IS done is that ``next_watermark()`` refuses
to advance when the first row of the result is not the highest value by the same string ordering
the predicate uses -- so the day the column reaches double digits, the job stops instead of
lying. See that function.

Making the column a DynamoDB Number would fix the ordering and break the other half: the same
config table also has to hold a timestamp for a table keyed on one, and a column that is
sometimes a number and sometimes an ISO date is a column no comparison can be written against.
The reference lab chose text for that reason. The trap comes with the choice.

FETCHALL AND ONE PUT_OBJECT: THE CEILING THAT COMES WITH THEM
--------------------------------------------------------------

``cursor.fetchall()`` materialises the whole result in the container's memory, and the CSV is
built in a ``StringIO`` beside it, so at the widest moment this job holds the extract twice. A
Glue Python Shell job runs in a ~1-DPU container, and the largest
extract this pipeline ever performs is 32,198 rows of 32 narrow columns, plus the 58,168-row
snapshot of 7 columns that ``client_attributes`` ships every run. That fits with room to spare,
and the reference lab's whole shape depends on it fitting.

The ceiling is real and should be stated rather than discovered: this job does not stream. There
is no ``fetchmany`` loop, no multipart upload and no chunked write, so a source table that
outgrew the container's memory would fail here, and it would fail at ``fetchall()`` with a memory
error rather than anywhere informative. Streaming would mean a server-side cursor plus a
multipart upload plus a rule for what the watermark should be if the third part failed -- which
is a different job, and the point at which a Python Shell extractor should have been a Spark one.

WHAT DEPARTS FROM THE REFERENCE LAB, AND WHY EACH ONE EARNED IT
----------------------------------------------------------------

Four that change what the job DOES, listed here. Three more change only how it fails, and are
argued where they occur rather than here: ``get_secret()`` lets a Secrets Manager error
propagate instead of returning None into a KeyError, ``extract()`` reads through the local
source without pymysql, and the connection is bound to None before the try so the finally
clause cannot raise NameError over the real error. Everything else is the reference lab's,
including the parts named above as traps.

1. **A zero-row extract writes a header-only CSV, not an empty object.** The reference lab's
   ``convert_to_csv`` returns ``""`` for an empty result, so run 4 -- the run that proves the
   whole design -- puts a zero-byte object in the landing zone, which is what a failed upload
   also looks like. See ``to_csv()``.
2. **``connection`` is bound before the try.** The reference lab's ``finally:
   connection.close()`` raises NameError when the connect call itself failed, replacing the real
   error in CloudWatch with one about a variable. See main().
3. **The watermark refuses to advance to a value that is not the result's maximum.** The
   reference lab reads ``result[0][col]`` whether or not the query sorted, and on a first run it
   did not sort. See ``next_watermark()``: the SQL is unchanged, the trust is not.
4. **A missing config row is raised rather than returned as ``(None, None)``.** The reference lab
   maps "the read failed" and "there is no row" onto the same pair, which then arrives at the
   incremental branch as "no incremental column" -- so a throttled read is reported as a
   configuration error and the operator goes looking for a row that is already there.

The one thing deliberately NOT fixed is the string comparison, for the reason given above.

WHAT --local SWAPS, AND WHAT IT DOES NOT
-----------------------------------------

``--local`` runs the entire chain on a laptop with no AWS account. Three things are swapped, and
each is at the edge of the job:

* a DuckDB file (built by ``local-development/build_source_db.py``) instead of pymysql,
* a JSON file instead of the DynamoDB config table,
* a directory instead of the S3 bucket.

Everything between them is the same code: the same SQL text, the same watermark arithmetic, the
same ``csv.DictWriter``. That matters because the parts a local run cannot exercise -- IAM, the
secret, the bucket policy, the DynamoDB table's key schema -- are the parts that fail loudly on
the first AWS run anyway, whereas an off-by-one in the predicate fails quietly for months. The
local path is aimed at the second kind.

Neither ``boto3`` nor ``pymysql`` nor ``duckdb`` is imported at module scope. They are imported
inside the branch that needs them, so ``--local`` and ``--self-check`` run with the standard
library alone (plus DuckDB, for ``--local``), and a Glue container never has to have DuckDB
installed to run the AWS path.

WHAT THE LOCAL RUN COVERS
--------------------------

``--local`` swaps three things and nothing else, so the whole of the logic is exercised by the
local path: the predicate builder, the watermark advance and its guard, the CSV writer, and the
four-run demo end to end. The service surface each swap replaces — Secrets Manager, the S3 put,
DynamoDB's conditional update, pymysql against a real MySQL — is the deployer's to wire up, and
every name below is account-specific.

What is checked: ``--self-check`` pins the pure decisions -- the predicate builder in all four of
its states, the watermark advance including the no-op run, the string ordering that makes wave 10
a trap, and the CSV writer's header-only output. ``tests/test_watermark.py`` runs the four rows
of the table above end to end against a real temporary DuckDB source.

What that wiring has to satisfy is specific rather than vague: a secret holding the three keys
``get_secret()`` reads -- host, username, password -- as a JSON string rather than binary, a Glue
role with write on the landing bucket, and a config table keyed on ``table_name`` alone, which is
the shape ``fetch_configuration()`` addresses. Beside those sits one that is a live cross-file
constraint rather than a deployment step: MySQL returns a ``SELECT *`` in the order the table
declares its columns and Redshift's ``COPY`` maps a CSV by position, so ``mysql/mysql-queries.sql``
and ``redshift/redshift-create-tables.sql`` have to declare theirs in the same order -- see
``to_csv()``.

This job runs no step of the ten-step framework, so it carries no verdict badge. APPLIES / N/A /
OVERRIDE / LIMIT / ENFORCE / BANNED are the refinery's vocabulary, and an extraction row count
wearing one of them would read in CloudWatch like a verdict about the modelling.

Local acceptance run, from the repository root, with no AWS account and no credentials::

    python glue-jobs/mysql-extraction.py --self-check

    python local-development/build_source_db.py --through-wave 1
    python dynamodb/write-to-dynamo.py --local --reset
    python glue-jobs/mysql-extraction.py --local --table_name mail_offers \\
        --load_type incremental
    python glue-jobs/mysql-extraction.py --local --table_name client_attributes \\
        --load_type full_load
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import sys

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("mysql_extraction")

# The source database, on both paths. MySQL on AWS, a DuckDB file locally.
DEFAULT_DATABASE = "credit_mailer"

# The DynamoDB table holding one config row per source table. Partition key: table_name.
CONFIG_TABLE = "incremental_load_configurations"

# The Secrets Manager secret holding host/username/password for the MySQL instance.
DEFAULT_SECRET = "credit_mailer_db"

# Account-specific: set it per deployment, or pass --bucket. It is a module constant rather than
# a required flag because the Step Functions definition passes only --table_name and --load_type,
# exactly as the reference lab's does.
DEFAULT_BUCKET = "credit-mailer-lab"

# The fixed key, overwritten on every run, as the reference lab does it. The consequence is not
# cosmetic: the landing zone holds only the LATEST extract of a table, so that table's Redshift
# load must finish before its next extract starts, or the second extract overwrites rows the
# first load has not read yet. The Step Functions chain is sequential for this reason and cannot
# be turned into a Parallel state without changing this key to include a run identifier.
S3_PREFIX = "raw_landing_zone/credit_mailer_db"

DEFAULT_LOCAL_ROOT = os.path.join("_localrun", "raw_landing_zone", "credit_mailer_db")
DEFAULT_SOURCE_DB = os.path.join("_localrun", "source.duckdb")
DEFAULT_CONFIG_FILE = os.path.join("_localrun", "watermark.json")

# A table name and a load column are SQL IDENTIFIERS spliced into the statement text, and no
# database driver will parametrise an identifier. Both arrive from outside this file -- one from
# the Step Functions definition, one from a DynamoDB row -- so both are checked against what an
# unquoted identifier may contain before either reaches a cursor.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_query(table_name, load_type, load_column, last_extracted_value):
    """The SELECT, in the reference lab's exact four states. Pure: no cursor, no config, no I/O.

    ``full_load``
        ``SELECT * FROM <table>`` and nothing else, EVEN IF a load column exists. A full load
        that quietly applied a predicate because one was available would be a full load in name
        only, and the caller asking for one has no way to see that it did not happen.

    ``incremental`` with a load column and a stored value
        ``... WHERE <col> > '<last>' ORDER BY <col> DESC``.

    ``incremental`` with a load column and no stored value
        no predicate. The first run takes everything, then sets the watermark.

    ``incremental`` with no load column
        not this function's problem -- the caller exits 1 before reaching here. See main().

    Two things about the WHERE clause are worth seeing rather than reading past:

    The value is interpolated, not bound. It is not user input: it is a string this same job
    wrote to the config table on a previous run, taken from a column it had just extracted. That
    is the only reason the interpolation is defensible, and it stops being defensible the moment
    anything else can write to the config table. The identifiers are guarded above; the value is
    guarded by who is allowed to set it.

    ``ORDER BY <col> DESC`` exists so that ``result[0][col]`` is the maximum, which costs a full
    sort of the extract to read one value. ``SELECT MAX(<col>) FROM <table>`` in a second
    statement would be cheaper and would not care about the result set's size at all. The sort is
    kept because it is what makes ``result[0]`` the maximum inside this job, and because a
    second statement is a second place for the predicate to drift. Nothing downstream depends
    on the row order: the next step is a ``COPY`` into a staging table followed by a keyed
    ``MERGE``, and neither reads the file in order.

    Note where the ORDER BY is NOT: on the first incremental run there is no predicate and so no
    sort, which means ``result[0]`` is whatever order the source chose to return. It is the
    maximum on run 1 of this pipeline only because run 1's source holds exactly one distinct
    wave. That is luck, not design, and ``next_watermark()`` is where the luck is checked.
    """
    if not _IDENT.match(table_name):
        raise ValueError("table name {0!r} is not a bare SQL identifier; it is spliced into the "
                         "statement text and cannot be parametrised".format(table_name))

    sql = "SELECT * FROM {0}".format(table_name)

    if load_type != "incremental":
        return sql
    if not load_column or last_extracted_value is None:
        return sql

    if not _IDENT.match(load_column):
        raise ValueError("load column {0!r} from the config table is not a bare SQL identifier"
                         .format(load_column))
    return sql + " WHERE {0} > '{1}' ORDER BY {0} DESC".format(load_column,
                                                               last_extracted_value)


def next_watermark(rows, load_column, current):
    """The new ``last_extracted_value``, as a string, or ``current`` when nothing was extracted.

    A zero-row extract must not move the watermark and must not fail. That is run 4 of the
    four-run demo -- the source has not grown since run 3 -- and it is also what a genuinely
    quiet day looks like in production. Leaving the value alone is the whole behaviour: the next
    run asks the same question again.

    The value is stringified because the config table stores text and the predicate compares
    text. See the module docstring for why that is a trap and why it is kept.

    The guard is the part that is not in the reference lab. ``rows[0]`` is the maximum only if
    the query sorted, and the query sorts only when it filtered, so on a first run this is an
    unordered result and the reference lab takes its first row on trust. Rather than change the
    SQL, this compares ``rows[0]`` against the highest value in the result BY THE SAME STRING
    ORDERING the predicate will use, and refuses to write a watermark that disagrees. Two real
    failures land on it:

    * A first incremental run against a source that already holds more than one distinct value.
      This is not hypothetical and it is not rare: run it locally against a source built with all
      three waves present and the engine hands back a first row that is not the highest one, at
      which point the reference lab would store that value and skip every row above it on every
      run thereafter. The four-run demo never meets it only because the source is staged a wave
      at a time, which is the arrangement that makes the reference lab's omission invisible.
    * The wave-10 case, where the sort is numeric and the comparison is textual, so ``'9'`` is
      the string maximum of a result whose first row is ``10``.

    Both would otherwise produce a plausible number, a successful run, and an extract that
    silently skips rows from then on. Raising is the useful answer to both: there is no value
    this function could return that is right, and the two ways out -- stage the source, or give
    the first run the ORDER BY the reference lab omits -- are decisions for whoever is looking at
    the failure, not for a fallback buried here.
    """
    if not rows:
        return current

    first = str(rows[0][load_column])
    highest = max(str(row[load_column]) for row in rows)
    if first != highest:
        raise ValueError(
            "the extract's first row has {0}={1!r} but its highest value by string ordering is "
            "{2!r}, and the watermark is compared as a string. Writing {1!r} would make the "
            "next run skip every row between them, silently and for ever. Either this is a "
            "first run, which has no ORDER BY because it has no WHERE -- stage the source, or "
            "sort the unfiltered SELECT too -- or {0} has grown past a digit boundary, where "
            "'10' < '9' and the string watermark has reached its ceiling"
            .format(load_column, first, highest))
    return first


def to_csv(fieldnames, rows):
    """The extract as CSV text: always a header, then a line per row.

    The reference lab returns ``""`` for an empty result and puts a zero-byte object in the
    landing zone. A header-only file is written here instead, for two reasons that are about the
    reader rather than about tidiness. A zero-byte object is indistinguishable from a failed
    extract, a truncated upload or a job that never ran, so the one run of the demo that PROVES
    the watermark works would look identical to the one that proves it is broken. And the
    downstream ``COPY ... IGNOREHEADER 1`` is being told to skip a line that is not there, which
    is a different question to every loader that has to answer it.

    ``fieldnames`` comes from ``cursor.description``, not from ``rows[0].keys()``. That is not a
    style preference: an empty result set has no first row to take keys from, so deriving the
    header from the data is exactly the case that breaks on the run that matters. The cursor
    knows the column list whether or not any rows came back.

    Column ORDER therefore comes from the source table's declaration, and Redshift's ``COPY``
    maps a CSV to a table BY POSITION and not by header name. So the CREATE TABLE in
    ``mysql/mysql-queries.sql`` and the one in ``redshift/redshift-create-tables.sql`` have to
    declare their columns in the same order, and neither file can be reordered alone. The header
    line makes a mismatch findable; it does not prevent one.

    A ``None`` is written as an empty field, which is what a NULL has to look like for a Redshift
    ``COPY`` into SMALLINT to read it back as NULL. This carries real information rather than
    tidying an edge case: 14 of the treatment columns are NULL on all 4,974 wave-1 rows, because
    wave 1 was a price-only experiment and those arms did not exist yet.

    ``lineterminator`` is set. The csv module's default is CRLF, which leaves a carriage return
    on the end of the last field of every row, and a loader that does not strip it reads the last
    column as text with an invisible character on the end -- or refuses the row outright when
    that column is numeric.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def get_secret(secret_name, region):
    """Read the MySQL credentials out of Secrets Manager.

    The reference lab logs the ClientError and returns None, so the job then fails at
    ``credentials['username']`` with a TypeError that says nothing about the secret. The
    exception is left to propagate here: a job that cannot read its credentials has one failure,
    and it should be the one that gets into CloudWatch.
    """
    import boto3

    client = boto3.session.Session().client("secretsmanager", region_name=region)
    payload = client.get_secret_value(SecretId=secret_name)
    if "SecretString" not in payload:
        raise ValueError("secret {0} holds binary, not a JSON string; this job expects "
                         "host/username/password".format(secret_name))
    return json.loads(payload["SecretString"])


def _local_config(path):
    """Load the JSON stand-in for the config table, and return ``(document, by_table)``.

    ``dynamodb/write-to-dynamo.py --local`` writes the same two records this job reads. Both a
    list of records and a mapping keyed by ``table_name`` are accepted, because the two files are
    written independently and a shape disagreement between them would be a startup failure with
    no useful message. ``by_table`` holds the SAME dict objects as ``document``, so mutating a
    record and writing ``document`` back preserves whichever shape was on disk and leaves the
    other table's row exactly as it was.
    """
    with io.open(path, "r", encoding="utf-8") as handle:
        document = json.load(handle)
    records = list(document.values()) if isinstance(document, dict) else document
    return document, dict((record["table_name"], record) for record in records)


def fetch_configuration(args):
    """``(load_column, last_extracted_value)`` for this run's table.

    A missing config row is raised, not returned as ``(None, None)``. The reference lab collapses
    "no row for this table" and "the read failed" into the same pair of Nones, which then arrives
    at the incremental branch as "no incremental column" -- so a throttled DynamoDB read is
    reported as a configuration error, and the operator goes looking for a row that is present.
    """
    if args.local:
        _, by_table = _local_config(args.config_file)
        if args.table_name not in by_table:
            raise KeyError("{0} has no row in {1}; run dynamodb/write-to-dynamo.py --local"
                           .format(args.table_name, args.config_file))
        record = by_table[args.table_name]
        return record.get("load_column"), record.get("last_extracted_value")

    import boto3

    table = boto3.resource("dynamodb", region_name=args.region).Table(CONFIG_TABLE)
    response = table.get_item(Key={"table_name": args.table_name})
    if "Item" not in response:
        raise KeyError("{0} has no row in {1}; seed it with dynamodb/write-to-dynamo.py"
                       .format(args.table_name, CONFIG_TABLE))
    item = response["Item"]
    return item.get("load_column"), item.get("last_extracted_value")


def update_last_extracted_value(args, value):
    """Write the watermark back. This is the only mutation the job makes outside the landing zone.

    Ordering matters and is deliberate: the landing-zone object is written FIRST, and the
    watermark only after that call returns. A crash between them repeats the extract on the next
    run, which is harmless because the key is fixed and the downstream MERGE is keyed. The other
    order loses rows: the watermark would have moved past an extract that never landed, and
    nothing downstream would ever ask for those rows again.
    """
    if args.local:
        document, by_table = _local_config(args.config_file)
        by_table[args.table_name]["last_extracted_value"] = value
        with io.open(args.config_file, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
    else:
        import boto3

        table = boto3.resource("dynamodb", region_name=args.region).Table(CONFIG_TABLE)
        table.update_item(Key={"table_name": args.table_name},
                          UpdateExpression="SET last_extracted_value = :val",
                          ExpressionAttributeValues={":val": value})
    LOG.info("watermark for %s is now %r", args.table_name, value)


def connect_source(args):
    """Open the source database: DuckDB locally, MySQL on AWS.

    DuckDB is opened read-only. An extractor has no business being able to write to the source,
    and read-only also means two of these can run at once against the same file, which is what a
    test suite does.
    """
    if args.local:
        import duckdb

        return duckdb.connect(args.source_db, read_only=True)

    import pymysql

    credentials = get_secret(args.secret_name, args.region)
    return pymysql.connect(host=credentials["host"],
                           user=credentials["username"],
                           password=credentials["password"],
                           database=args.database)


def extract(connection, sql):
    """Run the SELECT and return ``(fieldnames, rows)`` -- rows as dicts, in column order.

    The reference lab uses ``pymysql.cursors.DictCursor`` and takes the column names from the
    first row. Plain tuple cursors are used on both paths here and the dicts are built from
    ``cursor.description``, which is the one list of column names that exists whether or not any
    rows came back. It also makes the two paths the same code: DuckDB has no DictCursor.
    """
    cursor = connection.cursor()
    cursor.execute(sql)
    fieldnames = [column[0] for column in cursor.description]
    rows = [dict(zip(fieldnames, record)) for record in cursor.fetchall()]
    return fieldnames, rows


def write_landing_object(args, csv_data):
    """Overwrite the table's single object in the landing zone. Returns where it went."""
    if args.local:
        directory = os.path.join(args.output_dir, args.table_name)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        destination = os.path.join(directory, "data.csv")
        with io.open(destination, "w", encoding="utf-8", newline="") as handle:
            handle.write(csv_data)
        return destination

    import boto3

    key = "{0}/{1}/data.csv".format(S3_PREFIX, args.table_name)
    # Encoded here rather than left to botocore. The bytes are what the object holds and what
    # Redshift's COPY reads, so the encoding is this job's decision to make explicitly.
    boto3.client("s3", region_name=args.region).put_object(
        Bucket=args.bucket, Key=key, Body=csv_data.encode("utf-8"), ContentType="text/csv")
    return "s3://{0}/{1}".format(args.bucket, key)


def self_check():
    """Assert the predicate builder and the watermark advance. No AWS, no network, no files.

    Every one of these can be wrong in a way that produces a successful run with the wrong number
    of rows in it, which is the failure mode this whole job exists to make visible.
    """
    # 1. full_load never filters, even when a load column and a stored value both exist. This is
    #    the state the reference lab gets right and that is easiest to break later, because the
    #    arguments for filtering are all sitting there in scope.
    assert build_query("client_attributes", "full_load", "wave", "2") == \
        "SELECT * FROM client_attributes", "a full load applied a predicate"

    # 2. incremental with both halves present: the reference lab's exact clause.
    assert build_query("mail_offers", "incremental", "wave", "2") == \
        "SELECT * FROM mail_offers WHERE wave > '2' ORDER BY wave DESC"

    # 3. incremental with no stored value: the first run takes everything.
    first_run = build_query("mail_offers", "incremental", "wave", None)
    assert first_run == "SELECT * FROM mail_offers", first_run
    assert "WHERE" not in first_run and "ORDER BY" not in first_run

    # 4. An identifier that is not one is refused before it reaches a cursor.
    for bad in ["mail_offers; DROP TABLE mail_offers", "mail offers", "", "1table"]:
        try:
            build_query(bad, "full_load", None, None)
        except ValueError:
            pass
        else:                                                       # pragma: no cover
            raise AssertionError("{0!r} was accepted as a table name".format(bad))

    # 5. The watermark advance, over the four runs. Strings throughout: the config table stores
    #    text and the predicate compares text, so an int here would be a type that only shows up
    #    on the NEXT run, inside a WHERE clause, as zero rows.
    assert next_watermark([{"wave": 1}], "wave", None) == "1"
    assert next_watermark([{"wave": 2}, {"wave": 1}], "wave", "1") == "2"
    assert next_watermark([{"wave": 3}, {"wave": 2}], "wave", "2") == "3"
    assert type(next_watermark([{"wave": 1}], "wave", None)) is str, \
        "the watermark must be a string, because that is what the predicate compares against"

    # 6. Run 4. A zero-row extract leaves the watermark exactly as it was and raises nothing.
    assert next_watermark([], "wave", "3") == "3", "an empty extract moved the watermark"
    assert next_watermark([], "wave", None) is None

    # 7. The string ordering itself, pinned rather than described. These four assertions are the
    #    entire justification for the design and the entire size of its ceiling.
    assert "2" > "1" and "3" > "2", "the string watermark does not order waves 1-3"
    assert not ("10" > "9"), "this assertion is the wave-10 trap; if it fails, Python changed"

    # 8. ...and the guard that turns that trap into a failure instead of a silent skip. A result
    #    sorted numerically DESC puts 10 first, while the string maximum of the same result is
    #    '9', so writing '10' would make the next run's `> '10'` skip 2 through 9.
    try:
        next_watermark([{"wave": 10}, {"wave": 9}], "wave", "9")
    except ValueError as exc:
        assert "wave" in str(exc), "the refusal does not name the column"
    else:                                                           # pragma: no cover
        raise AssertionError("a watermark that disagrees with the string ordering was accepted")

    # 9. The header-only CSV, which is what run 4 puts in the landing zone.
    header_only = to_csv(["client_id", "wave"], [])
    assert header_only == "client_id,wave\n", repr(header_only)

    # 10. A NULL is an empty field, not the text 'None'. 14 columns are NULL on all 4,974 wave-1
    #     rows, so this is the ordinary case for a wave-1 extract and not an edge one.
    assert to_csv(["client_id", "prize"], [{"client_id": 1, "prize": None}]) == \
        "client_id,prize\n1,\n"

    LOG.info("self-check passed: the predicate in all four states, the identifier guard, the "
             "watermark advancing 1 -> 2 -> 3 and holding at 3 on an empty extract, the string "
             "ordering that makes wave 10 a trap and the guard that catches it, the header-only "
             "CSV, and NULL as an empty field")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # Underscored flag names, because that is what the Step Functions definition sends and
    # argparse matches the literal string. The reference lab reads the same two names through
    # getResolvedOptions; Glue Python Shell puts them on sys.argv either way, which is why
    # argparse works here and no awsglue import is needed.
    parser.add_argument("--table_name",
                        help="the source table to extract: mail_offers or client_attributes")
    parser.add_argument("--load_type", choices=("full_load", "incremental"),
                        help="full_load ignores the watermark entirely; incremental reads it "
                             "from the config table and writes it back. Constrained to the two "
                             "values because a typo would otherwise fall through to an "
                             "unfiltered SELECT that never updates the watermark -- a full load "
                             "wearing an incremental run's name")
    parser.add_argument("--local", action="store_true",
                        help="read a DuckDB file and a JSON config, write to a local directory. "
                             "No AWS account, no credentials, no network")
    parser.add_argument("--source-db", default=DEFAULT_SOURCE_DB,
                        help="--local only: the DuckDB file local-development/build_source_db.py "
                             "wrote (default: {0})".format(DEFAULT_SOURCE_DB))
    parser.add_argument("--config-file", default=DEFAULT_CONFIG_FILE,
                        help="--local only: the JSON stand-in for {0} (default: {1})"
                             .format(CONFIG_TABLE, DEFAULT_CONFIG_FILE))
    parser.add_argument("--output-dir", default=DEFAULT_LOCAL_ROOT,
                        help="--local only: the landing zone root (default: {0})"
                             .format(DEFAULT_LOCAL_ROOT))
    parser.add_argument("--bucket", default=DEFAULT_BUCKET,
                        help="the landing-zone bucket (default: {0}). The key beneath it is "
                             "fixed at {1}/<table>/data.csv".format(DEFAULT_BUCKET, S3_PREFIX))
    parser.add_argument("--database", default=DEFAULT_DATABASE,
                        help="MySQL database name (default: {0})".format(DEFAULT_DATABASE))
    parser.add_argument("--secret-name", default=DEFAULT_SECRET,
                        help="Secrets Manager secret holding host/username/password "
                             "(default: {0})".format(DEFAULT_SECRET))
    parser.add_argument("--region", default=None,
                        help="AWS region; defaults to the environment, which on Glue is the "
                             "region the job runs in")
    parser.add_argument("--dry-run", action="store_true",
                        help="run the SELECT and build the CSV, then write neither the landing "
                             "object nor the watermark. Moving the watermark is the one action "
                             "here that cannot be undone by running the job again")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the predicate builder and the watermark advance, then "
                             "exit; no AWS, no network, no input")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to sys.argv
    # on every run, and a strict parser exits 2 on them before the job starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return

    if not args.table_name:
        parser.error("--table_name is required unless --self-check")
    if not args.load_type:
        parser.error("--load_type is required unless --self-check")

    # The config is read BEFORE the source connection is opened, so the disagreement below costs
    # a DynamoDB get_item and nothing else -- no secret read, no MySQL handshake.
    load_column, last_extracted_value = fetch_configuration(args)

    if args.load_type == "incremental" and not load_column:
        # The reference lab's exit(1), kept, and it is the right answer rather than an omission.
        # client_attributes is the only table configured without a load column, and the Step
        # Functions definition only ever calls it with full_load. Reaching this line therefore
        # means the state machine and the config table disagree about what kind of table this is,
        # and there is no reading of that which lets the run continue: extracting everything
        # would be a full load labelled incremental, and extracting nothing would be a silent
        # skip. Both leave the operator with a green run and the wrong rows.
        LOG.error("%s is configured with no load_column but was asked for an incremental load. "
                  "The Step Functions definition and %s disagree; neither a full extract nor an "
                  "empty one is a safe reading of that. Exiting.", args.table_name, CONFIG_TABLE)
        sys.exit(1)

    sql = build_query(args.table_name, args.load_type, load_column, last_extracted_value)
    LOG.info("%s | %s | load_column=%r last_extracted_value=%r%s", args.table_name,
             args.load_type, load_column, last_extracted_value,
             " | DRY RUN, nothing will be written" if args.dry_run else "")
    LOG.info("sql: %s", sql)

    # Bound before the try, which the reference lab does not do. Its `finally: connection.close()`
    # raises NameError when the connect() call itself failed, so the AttributeError or the
    # OperationalError that actually stopped the job is replaced in CloudWatch by a NameError
    # about a variable -- the one traceback with nothing in it about the real fault.
    connection = None
    try:
        connection = connect_source(args)
        fieldnames, rows = extract(connection, sql)
    finally:
        if connection is not None:
            connection.close()

    csv_data = to_csv(fieldnames, rows)

    if args.dry_run:
        LOG.info("dry run: %s rows, %s columns, %s bytes of CSV, discarded. The watermark stays "
                 "at %r", len(rows), len(fieldnames), len(csv_data.encode("utf-8")),
                 last_extracted_value)
        return

    destination = write_landing_object(args, csv_data)
    LOG.info("%s rows, %s columns -> %s", len(rows), len(fieldnames), destination)
    if not rows:
        LOG.info("the extract was empty, so %s holds a header and no rows. On an incremental "
                 "table this is the source not having grown since the last run, which is the "
                 "one observation that proves the stored watermark was read", destination)

    # Landing object first, watermark second. See update_last_extracted_value().
    if args.load_type == "incremental":
        new_value = next_watermark(rows, load_column, last_extracted_value)
        if new_value == last_extracted_value:
            LOG.info("watermark for %s stays at %r: nothing new was extracted",
                     args.table_name, last_extracted_value)
        else:
            update_last_extracted_value(args, new_value)


if __name__ == "__main__":
    main()
