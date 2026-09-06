"""Seed the externalised watermark table: two rows, one per source table.

The pipeline's incremental state does not live in the extraction job, in a file beside the data,
or in a Redshift column. It lives in a DynamoDB table, ``incremental_load_configurations``,
partition key ``table_name``, and this script puts the two rows there. That externalisation is
the whole lesson of the reference lab, and it is worth stating what it buys before the code
starts: the extraction job becomes stateless, so it can be re-run, run out of order, or run from
a different account without carrying a memory of the last run; and the state itself is one small
item a human can read, edit and reset without touching a job or redeploying anything.

    table_name          load_column    last_extracted_value
    mail_offers         wave           NULL, until the first extract sets it
    client_attributes   NULL           NULL, and it stays that way

THE TWO ROWS ARE THE NORMALISATION, NOT A CONFIGURATION CHOICE
--------------------------------------------------------------

The published deposit is one wide table with no key. It is split into two at their real grains
before any of this runs: ``mail_offers`` at event grain, one row per mailer, and
``client_attributes`` as a CRM snapshot, 58,168 rows, one per client. The two rows below are
those two grains, and their ``load_column`` values are a consequence of the grains rather than a
setting somebody picked:

*   ``mail_offers.load_column = 'wave'``. A mailer belongs to wave 1, 2 or 3, the source table
    gains a wave at a time as the 2003 campaign did, and ``wave`` is therefore the column an
    incremental extract can filter on.
*   ``client_attributes.load_column = NULL``. A CRM snapshot has no event time. There is no
    column in it that means "when this row happened", because the rows did not happen; they are
    what the lender believed about a client. A NULL ``load_column`` is how this table says so,
    and the extraction job reads that NULL and does a full load.

That second row is the point of the exercise and not a leftover. An incremental watermark that
has only ever been tried on tables that can be incremental has not been tested; it has been
demonstrated. The reference lab makes the same point with an attributes table that has no
timestamp, and the same shape is kept here because the argument is the same one.

The alternative was to mint a ``created_at`` on ``client_attributes`` so that every table could
be incremental and the code path with no ``load_column`` would never run. That was rejected: a
fabricated timestamp on a snapshot is a claim about when something happened, made up, and it
would delete the one case this pipeline exists to handle honestly.

WHAT IS TAKEN FROM THE REFERENCE LAB, AND THE TWO THINGS DROPPED
----------------------------------------------------------------

Taken: ``boto3.resource('dynamodb')``, ``Table(...)``, a literal list of the configuration rows,
and ``batch_writer()``. Two rows do not need a batch writer and one ``put_item`` each would be
shorter, but the shape is the reference lab's and it is the shape that stops mattering at two
rows and starts mattering at twenty.

Dropped, with reasons, because both are things a reader would otherwise assume were considered:

1.  **The ``uuid4()`` ``id`` attribute.** The reference lab stamps a fresh
    ``str(uuid.uuid4())`` onto every configuration row. The partition key here is ``table_name``
    alone, so that attribute is not part of the key schema; what it is instead is a field that
    takes a different value every time the seeder runs, on a row whose entire purpose is to be
    stable and addressable. And if the name were ever taken at face value and ``id`` promoted
    into the key, the extraction job's ``get_item(Key={'table_name': ...})`` and its
    ``update_item`` on that same key would both stop resolving, because a partition key alone
    does not address an item in a table with a composite key. A random attribute on a keyed
    configuration row has no reader and one clear way to break the thing that does read it.
2.  **The ``{k: v if v is not None else None}`` comprehension**, which the reference lab
    comments as converting None to DynamoDB's NULL. It does nothing: the expression returns ``v``
    for every input, None included. The conversion it describes is real, but boto3's serialiser
    already does it -- a Python ``None`` is encoded as ``{'NULL': True}`` on the way to the wire.
    So the items below are handed to the batch writer exactly as written. That NULL is
    load-bearing: it is what the extraction job checks to decide that a first incremental run has
    no predicate and takes everything.

SEED AND RESET ARE THE SAME WRITE, AND THIS FILE OVERWRITES
------------------------------------------------------------

``--reset`` exists because the four-run demo has to be re-runnable, and the runs only mean
anything from a known starting state:

    run 1   source holds wave 1     mail_offers extracts  4,974 rows   watermark -> 1
    run 2   source holds waves 1-2  mail_offers extracts 20,996 rows   watermark -> 2
    run 3   source holds waves 1-3  mail_offers extracts 32,198 rows   watermark -> 3
    run 4   source holds waves 1-3  mail_offers extracts      0 rows   watermark unchanged

``--reset`` does not, however, run different code. The seeded state and the reset state are the
same two items, because a seed is by definition ``last_extracted_value = NULL``; the flag changes
the log line and nothing else, and pretending otherwise by writing a second code path would
create two places for the declared configuration to live.

The consequence, said plainly rather than guarded against: **this script overwrites.**
``batch_writer`` issues ``PutItem``, which replaces the whole item, and there is no conditional
put and no create-if-absent here. Running it in the middle of the demo puts the demo back to run
1. A ``ConditionExpression`` guard is not available through the batch writer at all, so adding
one would mean abandoning the reference lab's shape to defend against a mistake this paragraph
and the log line already name.

--local WRITES THE SAME TWO RECORDS TO A JSON FILE
---------------------------------------------------

So the whole chain runs on a laptop with no AWS account. The file is a JSON **list** of the same
two dicts -- not an object keyed by table name -- so that it stays a literal picture of what
DynamoDB holds rather than a re-indexed version of it. At two rows, a caller wanting the
``get_item`` equivalent scans the list for its ``table_name``, and that scan is the whole index.
The extraction job's ``--local`` path reads this file and rewrites it in full when it advances
the watermark, which is the local stand-in for ``PutItem``.

Note for anyone reading the file afterwards: ``last_extracted_value`` is written back by the
extraction job as a **string**, because the reference lab writes ``str(new_last_value)``. Seeded,
it is null and has no type at all. The string comparison that follows is safe over waves 1 to 3
and is not safe in general; the extraction job is where that trap is named, because that is where
the comparison is made.

WHAT THE LOCAL PATH COVERS
--------------------------

The declared configuration itself: two items, their shape, and the reset that puts both
``last_extracted_value`` back to None so the four-run demo can be run again. That is what the
demo and the tests exercise. The deployment side is a table the operator creates once, and it
has to match what is written here in one respect that matters: the partition key is
``table_name`` alone, so an item is addressable by the name the extraction job looks it up by.

Local usage, with no credentials and no region configured::

    python dynamodb/write-to-dynamo.py --local
    python dynamodb/write-to-dynamo.py --local --reset
"""

import argparse
import json
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("write_to_dynamo")

DEFAULT_TABLE = "incremental_load_configurations"
# Forward slashes: this is a repo-relative path and open() takes it on every platform.
DEFAULT_LOCAL_PATH = "_localrun/watermark.json"

# The declared configuration. One item per source table, and the only place these two rows are
# written down -- the extraction job reads them, it does not know them.
CONFIGURATIONS = [
    {"table_name": "mail_offers", "load_column": "wave", "last_extracted_value": None},
    {"table_name": "client_attributes", "load_column": None, "last_extracted_value": None},
]


def write_local(path):
    """Write the two records to a JSON file, replacing whatever was there.

    Whole-file replacement rather than a merge, for the same reason the DynamoDB path uses
    PutItem: this file is the declaration, so a stale key surviving in it would be a value
    nobody declared.
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(CONFIGURATIONS, handle, indent=2)
        handle.write("\n")


def write_dynamo(table_name, region):
    """Write the two records to DynamoDB. None is encoded as NULL by boto3, not by this file.

    boto3 is imported here and not at module scope so that --local needs no AWS SDK at all.
    Glue Python Shell ships boto3 preinstalled, so the AWS path never notices; a laptop
    installing only the local requirements would otherwise fail on the import before reaching
    the branch that does not use it.
    """
    import boto3

    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    with table.batch_writer() as batch:
        for item in CONFIGURATIONS:
            batch.put_item(Item=item)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--local", action="store_true",
                        help="write the two records to a JSON file instead of DynamoDB, so the "
                             "chain runs with no AWS account")
    parser.add_argument("--path", default=DEFAULT_LOCAL_PATH,
                        help="where --local writes (default: %(default)s)")
    parser.add_argument("--table", default=DEFAULT_TABLE,
                        help="DynamoDB table name (default: %(default)s). The table is not "
                             "created here: a seeder that creates tables holds IAM permissions "
                             "it has no other use for")
    parser.add_argument("--region", default=None,
                        help="AWS region; defaults to the environment")
    parser.add_argument("--reset", action="store_true",
                        help="put both rows back to last_extracted_value = NULL so the four-run "
                             "demo can be run again. Same write as a plain seed -- the seeded "
                             "state IS the reset state -- so this only changes the log line")
    # parse_args, and deliberately not the parse_known_args the Glue jobs in this repo use: this
    # is an operator script run once before the state machine starts, not a Glue job, so nothing
    # appends --JOB_NAME and --TempDir to its argv and a strict parser costs nothing.
    args = parser.parse_args()

    action = "reset" if args.reset else "seeded"
    if args.local:
        write_local(args.path)
        LOG.info("%s: %s configurations written to %s", action, len(CONFIGURATIONS),
                 args.path)
    else:
        write_dynamo(args.table, args.region)
        LOG.info("%s: %s configurations written to DynamoDB table %s", action,
                 len(CONFIGURATIONS), args.table)

    for item in CONFIGURATIONS:
        LOG.info("   %-18s load_column=%-6s last_extracted_value=%s -> %s",
                 item["table_name"], item["load_column"], item["last_extracted_value"],
                 "incremental" if item["load_column"] else "full_load on every run")
    LOG.info("the next run of mail_offers has no watermark, so it extracts every wave the "
             "source currently holds")


if __name__ == "__main__":
    main()
