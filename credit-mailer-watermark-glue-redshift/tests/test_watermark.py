"""The four runs of the watermark demo, executed end to end against a real DuckDB source.

There is one thing worth testing in this pipeline and this file tests it: that the extraction job
reads a watermark it did not compute, extracts exactly the rows above it, and writes it back --
four times, against a source database that grows a wave at a time underneath it.

Everything here is real. A temporary DuckDB file is built from the committed gzip by
``local-development/build_source_db.py``, the config file is written by
``dynamodb/write-to-dynamo.py``, and the extraction job's own ``main()`` is called with flags on
``sys.argv`` exactly as Glue Python Shell hands them to it. Nothing is stubbed, nothing is
patched, and no AWS client is constructed -- the job imports boto3 and pymysql inside the
branches that use them, so the ``--local`` path never reaches an import that is not installed.

WHY THIS IS NOT A MOCKED TEST
-----------------------------

``moto`` would let this file assert that ``update_item`` was called with the right arguments. It
would not catch the failure this pipeline is actually exposed to, which is not "the job called
DynamoDB wrongly" but "the job called DynamoDB correctly with a value that quietly skips rows".
Every interesting way the watermark can be wrong -- a first run whose result set was never
sorted, a predicate that filtered when it should not have, a zero-row extract that advanced the
value anyway -- produces a successful run, a green state machine and a smaller table. The only
assertion that separates those from a correct run is a row count taken from the object that
actually landed, so that is what is counted here.

Run 4 is the test that matters and it is the one that looks like it does nothing: the source has
not grown, the extract is empty, and the watermark must stay at 3. A job that ignored the stored
value entirely would pass runs 1 to 3 by luck -- their predicates all happen to select what an
unfiltered extract of a freshly grown source would -- and fail only here.

WHAT THIS FILE COVERS
---------------------

What the two paths share -- and what this file therefore covers -- is the SQL text,
the watermark arithmetic, the CSV writer and the order in which the landing object and the
watermark are written. What differs is four API calls at the edges: the secret read, the pymysql
connect, ``put_object`` and ``update_item``. Three of them have a local stand-in carrying the same
payload -- a DuckDB file for the source, a directory for the bucket, a JSON document for the config
table -- and the fourth, Secrets Manager, has none, because a DuckDB file takes no credentials to
open. The SQL, the CSV bytes and the watermark value handed to those calls are built by the same
code on both paths, and that code is what is asserted here.

Run from the project root::

    python -m pytest tests -q
"""

import csv
import importlib.util
import io
import json
import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_GZIP = os.path.join(PROJECT_ROOT, "data", "adcontentworth_qje.tab.gz")


def load_module(relative_path, module_name):
    """Import one of the job files by path.

    The job files carry hyphens in their names -- ``mysql-extraction.py``,
    ``write-to-dynamo.py`` -- because that is how the reference lab names its Glue scripts, and
    the whole point of this project is that its files can be diffed against that lab's. A hyphen
    is not a Python identifier, so ``import mysql_extraction`` cannot reach them at all. The
    names are kept and the cost is paid here, in one function, rather than by renaming the
    artefacts that get deployed.
    """
    path = os.path.join(PROJECT_ROOT, *relative_path.split("/"))
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


extraction = load_module("glue-jobs/mysql-extraction.py", "mysql_extraction")
seeder = load_module("dynamodb/write-to-dynamo.py", "write_to_dynamo")
builder = load_module("local-development/build_source_db.py", "build_source_db")

# Section 3 of the build spec, which is the contract this file exists to hold the job to. The
# staging is the SOURCE growing: runs 1 to 3 see a source holding waves 1, 1-2 and 1-3, and run 4
# sees the same source run 3 saw, because the lender only ever sent three mailer waves.
SOURCE_THROUGH_WAVE = (1, 2, 3, 3)
EXPECTED_ROWS = [4974, 20996, 32198, 0]
EXPECTED_WATERMARKS = ["1", "2", "3", "3"]

# Each run of the incremental table extracts exactly one wave, and run 4 extracts none.
EXPECTED_WAVES = [["1"], ["2"], ["3"], []]

# The published extract is one row per client, so client_attributes holds this on every run -- and
# so does a FULL load of mail_offers once the source holds all three waves, since every client was
# mailed exactly once.
SOURCE_ROW_COUNT = 58168


def _extract(argv):
    """Call the job's ``main()`` the way Glue does: flags on ``sys.argv`` and nothing else.

    Calling ``main()`` rather than the functions underneath it is deliberate. The ordering that
    makes a crash safe -- landing object first, watermark second -- lives in ``main()`` and
    nowhere else, and a test that called ``extract()`` and ``update_last_extracted_value()``
    itself would be asserting its own ordering rather than the job's.
    """
    saved = sys.argv
    sys.argv = ["mysql-extraction.py"] + argv
    try:
        extraction.main()
    finally:
        sys.argv = saved


def _read_landing(landing, table):
    """``(header, rows)`` from the object the run put in the landing zone."""
    path = os.path.join(landing, table, "data.csv")
    with io.open(path, "r", encoding="utf-8", newline="") as handle:
        parsed = list(csv.reader(handle))
    return parsed[0], parsed[1:]


def _configuration(config_path, table):
    """One row of the JSON stand-in for ``incremental_load_configurations``."""
    with io.open(config_path, "r", encoding="utf-8") as handle:
        records = json.load(handle)
    return dict((record["table_name"], record) for record in records)[table]


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    """Build the source once, then run the four rounds in order and record what each produced.

    Module-scoped because the four runs are one sequence, not four independent cases: run 2's
    result is only meaningful given the watermark run 1 wrote. Splitting them into four
    independent tests would mean either re-running the whole chain four times or asserting
    against a watermark this file had set itself, and the second of those tests nothing.

    The gzip is read and split ONCE and the wave cut applied per round, rather than shelling out
    to ``build_source_db.py`` three times. That decompresses, hashes and parses the committed
    file once instead of three times, and it exercises the same ``split`` / ``stage`` /
    ``write_duckdb`` functions the script's own ``main()`` calls, in the same order.
    """
    root = str(tmp_path_factory.mktemp("watermark"))
    database = os.path.join(root, "source.duckdb")
    config = os.path.join(root, "watermark.json")
    landing = os.path.join(root, "landing")

    seeder.write_local(config)
    tables = builder.split(builder.mint_and_cast(builder.read_source(SOURCE_GZIP)))

    built = None
    runs = []
    for through_wave in SOURCE_THROUGH_WAVE:
        if through_wave != built:
            builder.write_duckdb(database, builder.stage(tables, through_wave))
            built = through_wave
        _extract(["--local", "--table_name", "mail_offers", "--load_type", "incremental",
                  "--source-db", database, "--config-file", config, "--output-dir", landing])
        header, body = _read_landing(landing, "mail_offers")
        runs.append({
            "header": header,
            "rows": len(body),
            "waves": sorted(set(row[header.index("wave")] for row in body)),
            "bytes": os.path.getsize(os.path.join(landing, "mail_offers", "data.csv")),
            "watermark": _configuration(config, "mail_offers")["last_extracted_value"],
        })
    return {"database": database, "config": config, "landing": landing, "runs": runs}


def test_each_run_extracts_the_wave_the_watermark_leaves_it(pipeline):
    """4,974 -> 20,996 -> 32,198 -> 0. The fourth number is the one under test.

    The first three would also come out of a job that ignored the config table entirely and
    extracted whatever the source held, because the source is staged and each run's extract is
    the wave that had just been added. Run 4 is where the two behaviours diverge: an extractor
    reading its watermark takes nothing, and an extractor ignoring it takes all 58,168 rows
    again and merges the whole star a second time.
    """
    assert [run["rows"] for run in pipeline["runs"]] == EXPECTED_ROWS


def test_each_extract_holds_exactly_one_wave(pipeline):
    """The predicate selected a wave, rather than merely selecting the right NUMBER of rows.

    A row count alone cannot tell ``wave > '2'`` from a limit clause that happened to return
    32,198 rows of something else, and the two are not equally wrong: the second one would merge
    wave 2 into the fact table twice.
    """
    assert [run["waves"] for run in pipeline["runs"]] == EXPECTED_WAVES


def test_the_watermark_advances_and_then_holds(pipeline):
    """1 -> 2 -> 3 -> 3, as text.

    Text because that is what the config table stores and what the predicate compares. The type
    is asserted as well as the value: an integer here would be accepted by the JSON file and by
    DynamoDB, and would surface one run later as a comparison between a number and a string
    inside a WHERE clause -- which in MySQL is a silent coercion and in the local path is a
    different answer entirely.
    """
    watermarks = [run["watermark"] for run in pipeline["runs"]]
    assert watermarks == EXPECTED_WATERMARKS
    assert all(isinstance(value, str) for value in watermarks)


def test_the_empty_run_lands_a_header_and_not_a_zero_byte_object(pipeline):
    """Run 4's landing object holds the 32 column names and no rows.

    The reference lab writes ``""`` here. A zero-byte object in the landing zone is
    indistinguishable from a failed extract, a truncated upload or a job that never ran, so the
    one run of this demo that PROVES the watermark is read would look identical to the run that
    proves it is broken -- and the downstream ``COPY ... IGNOREHEADER 1`` would be told to skip a
    line that is not there.
    """
    final = pipeline["runs"][-1]
    assert final["rows"] == 0
    assert final["header"] == builder.MAIL_OFFERS_COLUMNS
    assert final["bytes"] > 0


def test_every_run_lands_the_same_columns_in_the_same_order(pipeline):
    """Column order is the CSV's only contract with Redshift.

    ``COPY`` maps a CSV to a table by POSITION and not by header name, so a run that emitted the
    same 32 columns in a different order would load every one of them into its neighbour without
    failing. The header line makes that findable; it does not prevent it.
    """
    for run in pipeline["runs"]:
        assert run["header"] == builder.MAIL_OFFERS_COLUMNS


def test_a_full_load_ignores_a_watermark_that_is_already_set(pipeline, tmp_path):
    """``full_load`` of mail_offers with the watermark at 3 takes all 58,168 rows, and leaves it.

    This is the state that is easiest to break later, because the arguments for filtering --
    a load column, a stored value -- are both sitting in scope by the time the query is built. A
    full load that quietly applied a predicate because one was available would be a full load in
    name only, and the caller asking for one has no way to see that it did not happen.

    It runs against the source the four rounds left behind, which holds all three waves, so the
    answer separates cleanly: 58,168 rows if the predicate was skipped, 0 if it was not.
    """
    landing = str(tmp_path)
    assert _configuration(pipeline["config"], "mail_offers")["last_extracted_value"] == "3"

    _extract(["--local", "--table_name", "mail_offers", "--load_type", "full_load",
              "--source-db", pipeline["database"], "--config-file", pipeline["config"],
              "--output-dir", landing])

    header, body = _read_landing(landing, "mail_offers")
    assert header == builder.MAIL_OFFERS_COLUMNS
    assert len(body) == SOURCE_ROW_COUNT
    # A full load must not write a watermark either. Advancing one here would make the NEXT
    # incremental run skip rows on the strength of an extract that was never incremental.
    assert _configuration(pipeline["config"], "mail_offers")["last_extracted_value"] == "3"


def test_client_attributes_ships_every_row_and_never_earns_a_watermark(pipeline, tmp_path):
    """The control table: no load column, so a full load on every run, for ever.

    58,168 rows each time. That is not a shortcoming of the design being tested around it -- it
    is the case a per-table watermark has to have an answer for, and a pipeline that only ever
    ran against tables which happen to carry a timestamp would never have to give one.
    """
    landing = str(tmp_path)
    _extract(["--local", "--table_name", "client_attributes", "--load_type", "full_load",
              "--source-db", pipeline["database"], "--config-file", pipeline["config"],
              "--output-dir", landing])

    header, body = _read_landing(landing, "client_attributes")
    assert header == builder.CLIENT_ATTRIBUTE_COLUMNS
    assert len(body) == SOURCE_ROW_COUNT
    assert _configuration(pipeline["config"], "client_attributes")["last_extracted_value"] is None


def test_incremental_against_a_table_with_no_load_column_exits_one(pipeline, tmp_path):
    """The reference lab's ``sys.exit(1)``, kept, and asserted rather than described.

    Reaching this branch means the Step Functions definition and the config table disagree about
    what kind of table this is. There is no reading of that which lets the run continue:
    extracting everything would be a full load wearing an incremental run's name, and extracting
    nothing would be a silent skip. Both hand the operator a green run and the wrong rows.

    The landing directory is asserted empty afterwards, which pins the ORDER of the check as well
    as its outcome -- the config is read before the source connection is opened, so a
    disagreement costs one ``get_item`` and never touches the landing zone.
    """
    landing = str(tmp_path)
    with pytest.raises(SystemExit) as caught:
        _extract(["--local", "--table_name", "client_attributes", "--load_type", "incremental",
                  "--source-db", pipeline["database"], "--config-file", pipeline["config"],
                  "--output-dir", landing])
    assert caught.value.code == 1
    assert os.listdir(landing) == []


def test_the_jobs_own_self_check_passes():
    """``--self-check`` is the assertion set the job ships to run on Glue, with no source at all.

    It is called from here so that it runs in CI rather than only when someone remembers to pass
    the flag, and because a job whose self-check has quietly stopped agreeing with its own code
    is worse than one that never had a self-check.
    """
    extraction.self_check()
