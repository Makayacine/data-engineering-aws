"""Build the local stand-in for the ``credit_mailer`` source database.

Reads the committed Harvard Dataverse extract, verifies its bytes, splits the one wide deposit
into the two tables the pipeline actually extracts from, and stages them into a DuckDB file --
optionally also as CSV, so the ``LOAD DATA LOCAL INFILE`` blocks in ``mysql/mysql-queries.sql``
have something to load on the AWS path.

    data/adcontentworth_qje.tab.gz          58,168 x 37, one wide table, no key
      ->  mail_offers        32 columns, event grain, one row per mailer, wave <= N
      ->  client_attributes   7 columns, CRM snapshot, 1:1 with the spine, all 58,168 rows

ONE SCRIPT AND NOT THREE, BECAUSE IT IS ONE DECISION
----------------------------------------------------

Verifying the download, minting the key and cutting the wide table in two look like three jobs
and are one: the normalisation. The column lists below are the whole of it. Splitting them
across three files would put the two halves of "no column is lost and none is duplicated" in
two places, and that invariant is the only thing standing between this script and a warehouse
that quietly holds 36 of the 37 published columns.

WHY THERE ARE TWO TABLES AT ALL
-------------------------------

The deposit is one rectangle with no identifier. The pipeline's lesson is a per-table
incremental watermark, and a watermark is only proved by a table that cannot have one. So the
rectangle is cut at its two real grains:

*   ``mail_offers`` is event grain. One row is one mailer sent in one wave, and ``wave`` is the
    load column -- an ordinal batch index, not a timestamp, which is the whole reason the
    watermark comparison in the extraction job is worth reading closely.
*   ``client_attributes`` is a CRM snapshot. It has no event time because a snapshot has none,
    so its ``load_column`` is NULL and every run of the pipeline ships all of it. That is not a
    limitation being worked around; it is the table that makes ``full_load`` mean something.

30 event columns + ``wave`` + 6 client columns = the 37 published columns. None dropped, none
carried twice. ``--self-check`` asserts exactly that, because it is an invariant a future edit
breaks by adding a column to one list and forgetting the other.

``client_id`` IS MINTED, AND THAT HAS TO BE SAID EVERY TIME IT APPEARS
----------------------------------------------------------------------

The deposit publishes no client identifier of any kind -- no account number, no household id,
nothing. ``client_id`` is the **1-based row number of the published extract** and it exists for
one reason: to give the two tables a join key. It is a surrogate over a fixed, hash-verified
file, so it is deterministic and any two people who run this script get the same ids. It is not
a lender account number, it carries no information, and nothing may be inferred from its
ordering -- the file is not sorted by wave, date or anything else, so a low ``client_id`` means
only that the row appears early in a file whose row order the depositors never claimed was
meaningful.

WHY DuckDB, AND WHY IT STANDS IN FOR BOTH ENDS OF THE PIPELINE
---------------------------------------------------------------

DuckDB is the local stand-in for MySQL *and* for Redshift. That sounds like laziness and is
actually the reason the local run is worth running: DuckDB 1.4 added
``MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED``, which is the one Redshift statement this
pipeline genuinely depends on. So the ingestion jobs execute the real MERGE text against DuckDB
rather than a rewritten local equivalent, and the statement that can silently corrupt the
warehouse is byte-identical in both places instead of being two statements that are believed to
agree. ``requirements.txt`` pins ``duckdb>=1.4`` for that reason and no other.

A file-backed DuckDB database, not in-memory, because the extraction job is a separate process
and has to open the same source the pipeline was built against.

THE COLUMN TYPES ARE DECLARED HERE, NOT INFERRED FROM THE FRAME
----------------------------------------------------------------

``CREATE TABLE ... AS SELECT * FROM <dataframe>`` would have been one line, and it would have
given ``offer4`` a DOUBLE. The offer rate is published with one or two decimals; through a
double it comes back out of the extractor as values like 9.6899999999999995, which then land in
Redshift's DECIMAL(5,2) after a round-trip nobody asked for and cannot be diffed against the
published file. So the types are declared: DECIMAL(5,2) for the rate, SMALLINT for the flags,
INTEGER for the amount borrowed, BIGINT for the minted key, VARCHAR(16) for the two strings.

SMALLINT and not BOOLEAN on the 0/1 flags, for a reason that shows up in this script and again
in the refinery: fourteen treatment columns are NULL for every wave-1 row, because wave 1 was a
price-only experiment and those arms did not exist yet. That NULL is a fact about the
experiment. It is preserved here as a NULL and never filled -- a nullable SMALLINT says
"unknown"; a BOOLEAN loaded from a CSV has to be told what an empty field means, and whatever
it is told will be a claim the data does not make.

WHAT "VERIFIED" COVERS HERE, AND WHAT IT DOES NOT
--------------------------------------------------

Covered: the uncompressed bytes hash to the recorded sha256 and the script raises if they do
not; the frame is 58,168 x 37; the two column lists reconstruct the 37 published names exactly
and share only the minted key; the per-wave row counts are the published ones; and the primary
keys are declared to DuckDB, so a duplicated ``client_id`` fails the load rather than becoming
a silent fan-out at the join.

Two things this script decides rather than proves:

*   The ``VARCHAR(16)`` headroom. DuckDB accepts the declaration and does not enforce the length,
    so the width that stops a Redshift ``COPY`` failing on an over-long value is a decision backed
    by the measured maxima of 8 (``coloured``) and 6 (``MEDIUM``).
*   The sha256 says the file is the one this was written against. It says nothing about whether
    the depositors' file is correct.

Run from the project root::

    python local-development/build_source_db.py --self-check
    python local-development/build_source_db.py --through-wave 1
    python local-development/build_source_db.py --through-wave 3 --csv-out mysql/data
"""

import argparse
import gzip
import hashlib
import io
import logging
import os

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("build_source_db")

# Repo-relative, and the script is run from the project root. os.path.join rather than a
# literal "data/..." so the same string works on a laptop and in a container.
DEFAULT_SOURCE = os.path.join("data", "adcontentworth_qje.tab.gz")
DEFAULT_DB = os.path.join("_localrun", "source.duckdb")

# Measured on the committed file: 5,980,352 uncompressed bytes, 58,169 lines (1 header + rows).
SOURCE_SHA256 = "6341d87b24abd1753e39e1c71f51ec09ebdb785970ed515f432d6b1bc0affd28"
SOURCE_COLUMN_COUNT = 37
SOURCE_ROW_COUNT = 58168

# Measured. Asserted rather than merely logged, because the four-run staging story in the README
# is only reproducible if a run through wave N really does hold these rows -- and the cheapest
# way for that to stop being true is a future edit to the split that drops rows on a filter.
WAVE_ROW_COUNTS = {1: 4974, 2: 20996, 3: 32198}

# The two tables, in the column order the CSVs are written in. That order is load-bearing:
# `LOAD DATA LOCAL INFILE` in mysql/mysql-queries.sql assigns fields positionally, so this list
# and the CREATE TABLE column order in that file are one decision recorded in two places, and
# changing either alone loads every column into its neighbour without an error.
MAIL_OFFERS_COLUMNS = [
    "client_id", "wave",
    "offer4", "prize", "intshown", "dphoto_female", "dphoto_none", "dphoto_black",
    "gender_match", "race_match", "nspeakeligible", "speak_trt", "oneln_trt", "comploss_n",
    "use_any", "stripany", "comp_n", "deadlinemed", "deadlinelong", "deadlong_elig",
    "deadshort_elig", "deadlineshortext", "waved3", "applied", "tookup", "amountbrw_unc",
    "badacct_last", "applied_2weeks", "tookup_after_short", "tookup_after_med",
    "tookup_after_long", "tookup_outside_only",
]

CLIENT_ATTRIBUTE_COLUMNS = [
    "client_id", "race", "risk", "female", "edhi", "dormancy", "trcount",
]

# The exceptions. Everything not named here is a 0/1 flag and gets DEFAULT_COLUMN_TYPE.
# Stated as exceptions rather than as a 39-entry map so that adding a flag needs no edit here,
# and adding something that is NOT a flag is a visible one.
COLUMN_TYPES = {
    "client_id": "BIGINT NOT NULL",
    "wave": "SMALLINT NOT NULL",
    # 3.25 to 14.75, published with one or two decimals. DECIMAL, not DOUBLE: see the docstring.
    "offer4": "DECIMAL(5,2)",
    # Rand borrowed, 0 to 10,000 -- outside SMALLINT's range.
    "amountbrw_unc": "INTEGER",
    # Measured maxima are 8 ("coloured") and 6 ("MEDIUM"). The width is headroom for a Redshift
    # COPY, which fails the whole load on a value one byte too long. DuckDB does not enforce it.
    "race": "VARCHAR(16)",
    "risk": "VARCHAR(16)",
}
DEFAULT_COLUMN_TYPE = "SMALLINT"

PRIMARY_KEYS = {
    "mail_offers": ("client_id", "wave"),
    "client_attributes": ("client_id",),
}

# The two columns that are genuinely not integers. Everything else in the deposit is an integer
# that Dataverse's TSV happens to render as "0.0", and reading those as floats would put a
# decimal point into every flag in the warehouse.
NON_INTEGER_COLUMNS = ("race", "risk", "offer4")

# MySQL's LOAD DATA reads an empty field into a numeric column as 0 -- with a warning, and the
# warning is not an error, so the load succeeds and the wave-1 treatment NULLs become zeros
# without anything failing. \N is the one token it reads as NULL. Those NULLs are the whole of
# the refinery's Step 4, so this is not a formatting preference.
CSV_NULL = "\\N"


def column_type(column):
    """The declared type for one column. Flags fall through to SMALLINT."""
    return COLUMN_TYPES.get(column, DEFAULT_COLUMN_TYPE)


def ddl(table, columns):
    """The CREATE statement for one table.

    ``CREATE OR REPLACE`` is what makes re-running with the same --through-wave idempotent:
    the tables are replaced, not appended to. A DROP + CREATE pair would leave the database
    with no ``mail_offers`` at all if the process died between them.

    The PRIMARY KEY is declared and not decorative. ``client_id`` is minted by this script, so
    a duplicate would be this script's own bug, and the join it feeds would fan out silently
    rather than fail.
    """
    body = ",\n".join("    {:<21}{}".format(c, column_type(c)) for c in columns)
    key = ", ".join(PRIMARY_KEYS[table])
    return "CREATE OR REPLACE TABLE {} (\n{},\n    PRIMARY KEY ({})\n);".format(table, body, key)


def read_source(path):
    """Read the committed gzip and refuse to go on if it is not the file this was written for.

    The whole uncompressed file is held in memory on purpose. It is under 6 MB; a streaming
    hash-then-reparse would read it twice to save nothing, and the point of hashing here is that
    the bytes that were verified are the same bytes that get parsed, which two passes do not
    guarantee.
    """
    with gzip.open(path, "rb") as handle:
        raw = handle.read()

    digest = hashlib.sha256(raw).hexdigest()
    if digest != SOURCE_SHA256:
        raise ValueError(
            "{} does not hash to the recorded sha256.\n  expected {}\n  measured {}\n"
            "Every row count, take-up rate and arm boundary in this project was measured on "
            "the expected file. A different file may be perfectly good data, but nothing "
            "downstream is entitled to call itself reproducible against it."
            .format(path, SOURCE_SHA256, digest))

    frame = pd.read_csv(io.BytesIO(raw), sep="\t")
    if frame.shape != (SOURCE_ROW_COUNT, SOURCE_COLUMN_COUNT):
        raise ValueError("{} parsed to {} -- expected ({}, {}). The bytes hashed correctly, so "
                         "this is a parser disagreement, not a different file."
                         .format(path, frame.shape, SOURCE_ROW_COUNT, SOURCE_COLUMN_COUNT))

    LOG.info("source verified: %s bytes, %s x %s, sha256 %s",
             len(raw), frame.shape[0], frame.shape[1], digest)
    return frame


def mint_and_cast(frame):
    """Add ``client_id`` and put every flag back into an integer type, nulls intact.

    Dataverse's ingested TSV renders the Stata numerics as floats, so a 0/1 flag arrives as
    "0.0" and a column with any missing value arrives as float64 whatever it holds. Cast to
    pandas' nullable Int64: 0.0 becomes 0, and a missing value stays missing instead of becoming
    NaN, which is a float and would be written to CSV as the string "nan".

    The cast raises on a fractional value rather than truncating one. That is not a
    hypothetical courtesy -- silently flooring a rate into an integer flag column is the exact
    shape of corruption that survives every downstream check because the result is still 0 or 1.
    """
    frame = frame.copy()
    for column in frame.columns:
        if column in NON_INTEGER_COLUMNS:
            continue
        try:
            frame[column] = frame[column].astype("Int64")
        except (TypeError, ValueError) as exc:
            raise ValueError("{} holds a value that is not a whole number ({}). Every column "
                             "except {} is an integer in the published deposit, so this means "
                             "the file or the parse has changed."
                             .format(column, exc, ", ".join(NON_INTEGER_COLUMNS)))

    # 1-based row number of the published extract. insert() at position 0 rather than an
    # assignment, so the frame's own column order shows the key first and the two SELECT lists
    # below read the way the tables do.
    frame.insert(0, "client_id", range(1, len(frame) + 1))
    return frame


def split(frame):
    """The normalisation: one wide frame -> ``{table_name: frame}`` at two grains.

    Column selection by an explicit list, never by "everything except". A drop-list quietly
    absorbs a new column into whichever table it was not dropped from; a keep-list makes the
    same edit fail the self-check.
    """
    missing = [c for c in MAIL_OFFERS_COLUMNS + CLIENT_ATTRIBUTE_COLUMNS
               if c not in frame.columns]
    if missing:
        raise ValueError("the source frame has no {} -- this is not the published extract "
                         "with client_id minted onto it".format(missing))
    return {"mail_offers": frame[MAIL_OFFERS_COLUMNS].copy(),
            "client_attributes": frame[CLIENT_ATTRIBUTE_COLUMNS].copy()}


def stage(tables, through_wave):
    """Apply the wave cut. ``client_attributes`` is never cut, and that is the point.

    The staging is the SOURCE growing a wave at a time, which is what happened in 2003 -- not
    the extractor limiting what it takes. So the mailer table holds waves 1..N and the extractor
    is left free to ask for everything; whether it takes 4,974 rows or none is decided by the
    watermark, which is the behaviour the pipeline exists to demonstrate.
    """
    tables = dict(tables)
    mail_offers = tables["mail_offers"]
    tables["mail_offers"] = mail_offers[mail_offers["wave"] <= through_wave].copy()

    expected = sum(WAVE_ROW_COUNTS[w] for w in range(1, through_wave + 1))
    if len(tables["mail_offers"]) != expected:
        raise ValueError("mail_offers through wave {} holds {} rows, expected {}"
                         .format(through_wave, len(tables["mail_offers"]), expected))
    if len(tables["client_attributes"]) != SOURCE_ROW_COUNT:
        raise ValueError("client_attributes holds {} rows and must always hold all {} -- it is "
                         "a snapshot, and a snapshot of some of the clients is not one"
                         .format(len(tables["client_attributes"]), SOURCE_ROW_COUNT))
    return tables


def write_duckdb(db_path, tables):
    """Replace both tables in the DuckDB source database."""
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    connection = duckdb.connect(db_path)
    try:
        for table, frame in tables.items():
            connection.execute(ddl(table, list(frame.columns)))
            # register() exposes the frame to SQL without copying it through a file. The INSERT
            # is what applies the declared types -- SELECT * from the frame straight into a
            # CREATE TABLE AS would have taken pandas' types instead, which is the DOUBLE
            # problem described in the docstring.
            connection.register("staged_frame", frame)
            connection.execute("INSERT INTO {} SELECT * FROM staged_frame".format(table))
            connection.unregister("staged_frame")
            held = connection.execute("SELECT count(*) FROM {}".format(table)).fetchone()[0]
            LOG.info("%-18s %6s rows, %2s columns -> %s", table, held, len(frame.columns),
                     db_path)
    finally:
        connection.close()


def write_csv(directory, tables, through_wave):
    """Write the CSVs the MySQL ``LOAD DATA`` blocks read, one file per wave.

    One file per wave rather than one file per table, because the AWS path stages the source the
    same way the local one does: ``mysql/mysql-queries.sql`` has three LOAD DATA blocks and
    loads them one at a time, so that the four pipeline runs are reproducible against MySQL and
    not only against DuckDB. A single cumulative mail_offers.csv cannot do that -- loading it
    twice would insert wave 1 twice.
    """
    os.makedirs(directory, exist_ok=True)
    written = []
    mail_offers = tables["mail_offers"]
    for wave in range(1, through_wave + 1):
        path = os.path.join(directory, "mail_offers_wave{}.csv".format(wave))
        written.append(_to_csv(mail_offers[mail_offers["wave"] == wave], path))
    written.append(_to_csv(tables["client_attributes"],
                           os.path.join(directory, "client_attributes.csv")))
    return written


def _to_csv(frame, path):
    frame.to_csv(
        path,
        index=False,
        na_rep=CSV_NULL,
        # offer4 is the only float left after mint_and_cast, and DECIMAL(5,2) is what receives
        # it at both ends. Pinning the scale here keeps every rate two decimals wide, so the
        # column is diffable against the published file and lands in DECIMAL(5,2) without the
        # loader having to decide anything. The published file writes 9.7, not 9.70.
        float_format="%.2f",
        # Explicit, because pandas would otherwise use the platform's line ending, and MySQL is
        # told LINES TERMINATED BY '\n'. On a CRLF file every final field arrives with a
        # carriage return glued to it: the numeric columns then load as 0 and the load succeeds.
        lineterminator="\n",
    )
    LOG.info("%-28s %6s rows", path, len(frame))
    return path


def self_check(source_path):
    """Assert the split is lossless. No writes; the only read is the committed gzip's header.

    Two of these are invariants that a plausible future edit breaks without any test noticing,
    because the result is a warehouse that loads cleanly and is missing a column.
    """
    # 1. The two lists share exactly the minted key and nothing else. A source column in both
    #    tables would be two copies of one fact, free to disagree after the first update.
    shared = set(MAIL_OFFERS_COLUMNS) & set(CLIENT_ATTRIBUTE_COLUMNS)
    assert shared == {"client_id"}, \
        "these columns are in both tables: {}".format(sorted(shared - {"client_id"}))
    for name, columns in (("mail_offers", MAIL_OFFERS_COLUMNS),
                          ("client_attributes", CLIENT_ATTRIBUTE_COLUMNS)):
        assert len(columns) == len(set(columns)), "{} lists a column twice".format(name)

    # 2. Together they account for every published column, and invent none.
    declared = (set(MAIL_OFFERS_COLUMNS) | set(CLIENT_ATTRIBUTE_COLUMNS)) - {"client_id"}
    assert len(declared) == SOURCE_COLUMN_COUNT, \
        "the two tables account for {} source columns, not {}".format(len(declared),
                                                                      SOURCE_COLUMN_COUNT)
    with gzip.open(source_path, "rt") as handle:
        published = handle.readline().rstrip("\n").split("\t")
    assert len(published) == SOURCE_COLUMN_COUNT, \
        "{} has a {}-column header".format(source_path, len(published))
    assert set(published) == declared, \
        "dropped {}, invented {}".format(sorted(set(published) - declared),
                                         sorted(declared - set(published)))

    # 3. The real mint/split/stage functions over a two-row frame -- one wave 1, one wave 2 --
    #    with every published column present. Values, not just names: a keep-list that selected
    #    the right names in the wrong order would pass check 2 and load every column into its
    #    neighbour.
    row1 = dict.fromkeys(published, 0)
    row1.update({"wave": 1, "race": "coloured", "risk": "MEDIUM", "offer4": 7.5, "trcount": 3})
    row2 = dict(row1)
    row2.update({"wave": 2, "race": None, "risk": "HIGH", "offer4": 11.0, "prize": 1})
    # stage() is deliberately not exercised here: it asserts the published per-wave row counts,
    # which a two-row frame cannot satisfy, and weakening that assertion to make a synthetic
    # frame pass would remove the only check that the wave cut does not also drop rows.
    frame = mint_and_cast(pd.DataFrame([row1, row2], columns=published))
    tables = split(frame)

    assert list(tables["mail_offers"].columns) == MAIL_OFFERS_COLUMNS
    assert list(tables["client_attributes"].columns) == CLIENT_ATTRIBUTE_COLUMNS
    assert tables["mail_offers"]["client_id"].tolist() == [1, 2], "client_id is not 1-based"
    assert tables["client_attributes"]["risk"].tolist() == ["MEDIUM", "HIGH"]
    assert tables["mail_offers"]["offer4"].tolist() == [7.5, 11.0]
    assert tables["mail_offers"]["wave"].tolist() == [1, 2]

    # 4. A missing value survives the integer cast as a missing value. If this ever became 0,
    #    wave 1 would claim it was shown treatments that did not exist yet, and every count in
    #    the refinery's Step 4 would still add up.
    assert pd.isna(tables["client_attributes"]["race"].iloc[1]), "a NULL string became a value"
    assert str(tables["mail_offers"]["prize"].dtype) == "Int64", "a flag is not a nullable int"

    # 5. A fractional value in a flag column is refused, not floored.
    try:
        mint_and_cast(pd.DataFrame([dict(row1, tookup=0.5)], columns=published))
    except ValueError as exc:
        assert "tookup" in str(exc), "the rejection does not name the column"
    else:                                                       # pragma: no cover
        raise AssertionError("a fractional flag value was accepted and silently floored")

    # 6. The declared types. Two of these are arguments made in the docstring, and an edit that
    #    reverted either would be invisible until Redshift rounded a rate or truncated a name.
    assert column_type("offer4") == "DECIMAL(5,2)", "the offer rate is not a decimal"
    assert column_type("amountbrw_unc") == "INTEGER", "the amount borrowed does not fit SMALLINT"
    assert column_type("tookup") == DEFAULT_COLUMN_TYPE, "a flag is not a nullable SMALLINT"
    assert "PRIMARY KEY (client_id, wave)" in ddl("mail_offers", MAIL_OFFERS_COLUMNS)
    assert "PRIMARY KEY (client_id)" in ddl("client_attributes", CLIENT_ATTRIBUTE_COLUMNS)

    LOG.info("self-check passed: %s source columns split across two tables sharing only the "
             "minted client_id, order and values preserved, nulls preserved as nulls, "
             "fractional flags refused, and the declared types intact",
             SOURCE_COLUMN_COUNT)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default=DEFAULT_SOURCE,
                        help="the committed gzip TSV (default: {}). Its sha256 is checked and a "
                             "mismatch raises".format(DEFAULT_SOURCE))
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="DuckDB file to build (default: {}). Replaced, not appended to"
                             .format(DEFAULT_DB))
    parser.add_argument("--through-wave", type=int, choices=[1, 2, 3], default=3,
                        help="mail_offers holds waves 1..N; client_attributes always holds all "
                             "rows. Run this with 1, then 2, then 3 to stage the source the way "
                             "it grew (default: 3)")
    parser.add_argument("--csv-out", default=None,
                        help="also write mail_offers_wave<N>.csv per wave and "
                             "client_attributes.csv into this directory, for the MySQL "
                             "LOAD DATA blocks in mysql/mysql-queries.sql")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the split is lossless and exit; reads only the source "
                             "file's header line and writes nothing")
    # parse_args and not parse_known_args: this is a local development script and never runs on
    # Glue, so there are no injected --JOB_NAME/--TempDir arguments to tolerate. An unrecognised
    # flag here is a typo, and exiting 2 on it is the useful answer.
    args = parser.parse_args()

    if args.self_check:
        self_check(args.source)
        return

    frame = read_source(args.source)
    tables = stage(split(mint_and_cast(frame)), args.through_wave)

    write_duckdb(args.db, tables)
    if args.csv_out:
        write_csv(args.csv_out, tables, args.through_wave)

    LOG.info("source database staged through wave %s: mail_offers %s rows, client_attributes "
             "%s rows. client_id is the 1-based row number of the published extract and is "
             "not a lender identifier", args.through_wave,
             len(tables["mail_offers"]), len(tables["client_attributes"]))


if __name__ == "__main__":
    main()
