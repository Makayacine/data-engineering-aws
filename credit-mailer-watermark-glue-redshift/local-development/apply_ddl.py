"""Apply ``redshift/redshift-create-tables.sql`` to the local DuckDB warehouse.

On AWS this script does not exist. The DDL is run once by a human with CREATE rights, in two
sessions, because Redshift cannot create a database and the objects inside it from one
connection: unqualified names resolve against the database the connection is attached to, so you
run ``create database db_credit_mailer``, reconnect to it, and run the rest. None of the three
Glue jobs holds CREATE rights and none of them should.

Locally there is no database-level statement to run -- the DuckDB file IS the database -- so
this script skips exactly one line of the DDL and executes every remaining statement unchanged.
That is the whole of it, and the reason it is worth saying out loud: the local warehouse is not
built from a translated schema kept alongside the real one. Two schema files drift, and they
drift silently, because the only thing that would notice is a column the local run never
exercises. There is one file, it is the one you deploy to Redshift, and the local run reads it.

WHAT "UNCHANGED" MEANS, AND WHERE THE TWO ENGINES DIFFER

Exactly one statement is removed, `create database db_credit_mailer;`, and nothing else -- its
comment is left in place, because a comment is a comment to DuckDB too. Everything below it --
both schemas, four tables, two staging copies, the four processed-zone tables and the
18-row `dim_offer_arm` seed -- is handed to DuckDB as written. Where the two engines agree on the
text, the local run is evidence about the real DDL. Where they do not, it is evidence about
DuckDB only, and this file cannot tell you which is which: DuckDB accepts `DECIMAL(10,8)` and
`SMALLINT` and `VARCHAR(16)` with Redshift's meaning, but it also accepts things Redshift would
reject and it enforces PRIMARY KEY, which Redshift does not. The local run therefore pins the
text: every statement in the file you deploy to Redshift -- both schemas, every column, every
default, the 18 seeded rows -- parses, builds, and comes back countable, which is why this
script ends by listing the tables it found and the arm count it read rather than by reporting
success.

    python apply_ddl.py                       # build/rebuild _localrun/warehouse.duckdb
    python apply_ddl.py --drop                # start from an empty file first
"""

import argparse
import logging
import os

import duckdb

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("apply_ddl")

# Repo-relative, resolved from this file rather than from the caller's working directory, so the
# script runs the same from the project root and from local-development/.
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
DEFAULT_DDL = os.path.join(PROJECT, "redshift", "redshift-create-tables.sql")
DEFAULT_DB = os.path.join(PROJECT, "_localrun", "warehouse.duckdb")

# The one statement that is skipped. Matched on its text rather than by line number, so
# reordering the DDL cannot silently start skipping something else.
DATABASE_STATEMENT = "create database db_credit_mailer;"


def local_ddl(text):
    """Return the DDL with the single database-level statement removed.

    Raises if it is not there. A silent no-op would mean this script quietly stopped doing the
    one transformation it exists to do -- and the failure mode of NOT removing it is a DuckDB
    parse error at statement one, which is loud, so the only dangerous direction is the DDL
    having been rewritten such that the match fails and something else gets skipped instead.
    """
    if DATABASE_STATEMENT not in text:
        raise ValueError(
            "{!r} is not in the DDL. Either the database name changed -- in which case change "
            "it here too -- or the statement was removed, in which case delete this function "
            "rather than leaving it matching nothing".format(DATABASE_STATEMENT))
    return text.replace(DATABASE_STATEMENT, "", 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ddl", default=DEFAULT_DDL,
                        help="the schema file to apply (default: the deployed one)")
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="DuckDB warehouse file to create")
    parser.add_argument("--drop", action="store_true",
                        help="delete the warehouse file first, so the run starts empty")
    args = parser.parse_args()

    if args.drop and os.path.exists(args.db):
        os.remove(args.db)
        LOG.info("removed %s", args.db)

    parent = os.path.dirname(args.db)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(args.ddl, encoding="utf-8") as handle:
        text = handle.read()

    connection = duckdb.connect(args.db)
    try:
        connection.execute(local_ddl(text))
        tables = connection.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "ORDER BY table_schema, table_name").fetchall()
        arms = connection.execute(
            "SELECT COUNT(*) FROM processed_zone.dim_offer_arm").fetchone()[0]
    finally:
        connection.close()

    for schema, table in tables:
        LOG.info("   %s.%s", schema, table)
    LOG.info("applied %s to %s: %s tables, dim_offer_arm seeded with %s arms",
             os.path.relpath(args.ddl, PROJECT), os.path.relpath(args.db, PROJECT),
             len(tables), arms)


if __name__ == "__main__":
    main()
