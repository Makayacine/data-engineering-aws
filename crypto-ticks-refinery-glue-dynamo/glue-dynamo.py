"""Publish the three path ledgers to DynamoDB.

AWS Glue **Python shell** entrypoint, not Spark. The three artifacts this job loads are 18, 72
and 3 rows -- about 12 KB of Parquet in total -- and a Spark job would spend two and a half
minutes starting a cluster to move them. So: no SparkSession, no JVM, no ``--extra-py-files``,
and the Glue 4.0 strip test that gates the three path jobs does not apply here, because nothing
in this file imports pyspark.

      <path1>/coefficients   18 rows   the Elastic Net ledger, one row per candidate feature
      <path2>/coefficients   72 rows   24 features x 3 classes, multinomial logistic
      <path3>/arms            3 rows   the bandit posterior, one row per arm
  ->  one DynamoDB table:  pk = "<run-id>#<path>",  sk = the grain within that path

THIS FILE IS DESIGNED, NOT LIFTED, SO HERE IS THE BAR IT IS HELD TO
------------------------------------------------------------------

The three path jobs can each say "reproduces the notebook to the last digit", because
``refinery-walkthrough.ipynb`` executed every one of their steps first. The walkthrough stops at
the three paths. There is no prototype of this job, no recorded output to diff against, and
therefore no oracle. What replaces it is narrower and worth stating exactly, because "verified"
would be the wrong word for it:

1.  **Every item is encoded by the real encoder before anything is sent.** ``--dry-run`` builds
    every item from the real artifacts and passes each one through
    ``boto3.dynamodb.types.TypeSerializer`` -- not a mock and not a re-implementation, but the
    exact code the DynamoDB client runs on the way to the wire. If an item would be rejected for
    its *types*, it is rejected on a laptop with no AWS account, before any of the work is paid
    for.
2.  **The keys are proved unique at build time.** A duplicate ``(pk, sk)`` is the one failure on
    this path that destroys data without raising anything: ``batch_writer`` de-duplicates
    nothing, a repeated key inside one batch is a ValidationException, and a repeated key
    *across* batches is a silent overwrite that turns 72 rows into 24 items with no error and no
    warning. So the keys are counted before the first write, not trusted. This is not
    hypothetical: ``imbalance`` is a feature name in Path 1's ledger AND in Path 2's -- the only
    name the two share -- so a key built from the feature name alone would have those two rows
    fighting over one item today. The path is in the PARTITION key, which is what keeps them
    apart, and that is the reason for it rather than a pleasant side effect.
3.  **``--self-check`` pins the pure decisions with no AWS, no network and no files.**

What none of that proves: that the table exists, that its key schema matches, that the IAM role
can write to it, that the region is right, or that the throughput holds. Those need an account.

**Why not moto or localstack.** Both were considered and neither is used. moto reimplements
DynamoDB in Python, so a green moto run is evidence about moto; the parts it would add on top of
the serializer -- does the table exist, is its key schema the one this job assumes -- are exactly
the parts a fake table cannot vouch for, because the fake table is created by the test. That is a
new test dependency bought for a weaker guarantee than the one boto3 already ships.

THE TABLE SCHEMA IS THE DECISION, AND IT IS A JUDGEMENT CALL
------------------------------------------------------------

Three artifacts arrive at three different grains -- one row per feature, one row per
(feature, class), one row per arm -- and they have to land somewhere. They land in **one table**,
keyed::

    pk = "<run-id>#<path>"     "2025-01#path1"
    sk = "#model"              the model-level header, one per path
         "feature#<name>"                       path 1
         "feature#<name>#class_name#<class>"    path 2
         "symbol#<symbol>"                      path 3

One partition holds exactly one path's result from exactly one run, which makes the query anyone
would actually write -- *give me Path 1's ledger for 2025-01* -- a single ``Query`` on the
partition key, and makes ``begins_with(sk, "feature#imbalance#")`` return one Path 2 feature's
three class rows. The sort key alternates the artifact's own column names with their values, so
it reads back against the Parquet it came from without a translation table. ``"#model"`` sorts
before every grain key -- '#' is 0x23 and every grain prefix starts with a letter -- so a Query
returns the header first without being asked to.

Three things about that are choices, not deductions, and a reader should be able to see them as
choices:

*   **The run id is in the partition key**, so a rerun of the same run is idempotent -- same
    keys, same overwrite -- while a *new* run writes a new partition instead of mutating the old
    one. The alternative, a table holding only "current" with history left in S3, is smaller and
    defensible; it was rejected because a month with fewer surviving features than the last one
    leaves the deleted features behind as items that still claim to be current. That failure is
    not avoided by the key alone -- ``put_item`` cannot delete -- so ``sweep_partition()`` reads
    each partition back after writing it and removes whatever this run did not produce. Without
    that the word "idempotent" would be doing work the code does not do.
*   **The run id is SUPPLIED, not derived.** Path 3 derives its arms and its bar width from the
    input on the grounds that a flag is a second place for them to be wrong. That argument cannot
    be made here: the three artifacts carry no month, no calendar and no run identity of any
    kind, so there is nothing to derive it from. ``--run-id`` is therefore required and has no
    default, which at least makes the second place for it to be wrong a visible one.
*   **The model-level columns are lifted onto a header item.** ``reg_param``,
    ``elastic_net_param``, ``intercept``, ``cv_rmse``, ``target_sd`` and ``n_rows`` are identical
    on all 18 Path 1 rows, and ``reg_param``, ``elastic_net_param`` and ``baseline_accuracy`` on
    all 72 Path 2 rows. Parquet repeats them because Parquet is rectangular and has nowhere else
    to put them; DynamoDB is not rectangular and does. Copying one model's ``cv_rmse`` onto 18
    items would create eighteen places for it to live, and the one thing that can be guaranteed
    about eighteen copies of a number is that one day seventeen of them will agree.

DYNAMODB REJECTS FLOAT, AND IT REJECTS IT AT WRITE TIME
-------------------------------------------------------

The same shape of trap as the ``numpy.float64`` that killed Path 2's first write, and the same
cost: the type error is raised after every step of the refinery has already run. Measured against
boto3 1.39.11's ``TypeSerializer``::

    0.1                       TypeError    "Float types are not supported. Use Decimal types"
    Decimal(0.1)              Inexact      the exact binary value is 55 significant digits and
                                           DYNAMODB_CONTEXT has prec=38
    Decimal(str(0.1))         {'N': '0.1'} correct
    numpy.float64(0.1)        TypeError    a float subclass, caught by the float check
    numpy.int64(212)          TypeError    "Unsupported type"
    numpy.bool_(True)         TypeError    "Unsupported type" -- numpy 2.x bool is NOT a bool
    Decimal(str(nan))         TypeError    "Infinity and NaN not supported"

Two consequences run right through this file.

**The Parquet is read with pyarrow and not with pandas.** ``pyarrow.Table.to_pylist()`` returns
native ``str`` / ``int`` / ``float`` / ``bool`` and ``None`` for a null. ``pandas.read_parquet``
returns numpy scalars, every one of which the table above rejects -- and worse, it turns a null
float64 into ``NaN``, which is a *different* rejection with a different message. Path 1's
``mu``, ``sigma`` and ``bp_per_sd`` are null on the 12 features Step 9 zeroed, so the pandas
route would hit that on the very first artifact.

**A null becomes an absent attribute, not a NULL and not a zero.** DynamoDB has a NULL type and
boto3 will happily write it, but an item that says ``sigma: null`` and an item with no ``sigma``
read back differently for no reason that means anything here: the feature has no sigma because
Step 9 zeroed it and Step 10 never scaled it. Absent is the honest encoding, and it is also the
one that costs nothing to store.

NOTHING HERE HAS RUN ON AWS
---------------------------

There is no AWS account behind this repository. This job has never opened a connection to
DynamoDB, and no table named below has ever existed. Everything asserted above about item shapes
and types was measured locally against the real artifacts and boto3's own encoder; everything
about a real table is untested.

This job runs no framework step, so it carries **no verdict badges**. APPLIES / N/A / OVERRIDE /
LIMIT / ENFORCE / BANNED are the vocabulary of the ten steps, and a load count wearing one of
them would read in CloudWatch like a verdict about the refinery.

Local acceptance run. The three path jobs run first (see the README); then, with no AWS account,
no credentials and no region configured::

    python glue-dynamo.py --self-check

    python glue-dynamo.py --dry-run --run-id 2025-01-sample \\
        --path1 _localrun/path1 \\
        --path2 _localrun/path2 \\
        --path3 _localrun/path3
"""

import argparse
import logging
from decimal import Decimal
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
# Both submodules are imported by name. `boto3.dynamodb.conditions.Key` after a plain
# `import boto3` is an AttributeError -- boto3.resource("dynamodb") happens to import the
# submodule as a side effect, so the attribute access works only if a resource was built
# first, which is a dependency on statement order that no test on this machine can reach.
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("glue_dynamo")

DEFAULT_TABLE = "crypto_ticks_refinery"

# The encoder the real client uses. Held at module scope because --dry-run, --self-check and the
# live write all have to run items through the SAME one -- a dry run that validated with
# different code than the write would be theatre.
SERIALIZER = TypeSerializer()

# DynamoDB's documented number range, as base-10 exponents of the leading digit: 1E-130 to
# 9.9999...E125. Decimal.adjusted() is exactly that exponent.
DDB_MIN_EXP = -130
DDB_MAX_EXP = 125

# What each path publishes, and how one of its rows becomes a sort key.
#
# `artifact` is the sub-prefix the path job wrote beneath its --output; `grain` is the columns
# that identify one row, in key order; `model` is the columns that are constant across the whole
# artifact and are lifted onto the header item. That last list is asserted at load time against
# the data rather than trusted -- see build_items().
PATHS = {
    "path1": {"artifact": "coefficients",
              "grain": ["feature"],
              "model": ["reg_param", "elastic_net_param", "intercept",
                        "cv_rmse", "target_sd", "n_rows"]},
    "path2": {"artifact": "coefficients",
              "grain": ["feature", "class_name"],
              "model": ["reg_param", "elastic_net_param", "baseline_accuracy"]},
    "path3": {"artifact": "arms",
              "grain": ["symbol"],
              "model": []},
}

# Sorts before every grain key, so a Query on the partition returns the header row first:
# '#' is 0x23 and the grain prefixes all start with a letter.
MODEL_SK = "#model"


def sort_key(path, row):
    """The sort key for one artifact row: alternating field name and value.

    ``feature#imbalance#class_name#down`` rather than ``imbalance#down``. The names cost a few
    bytes and buy two things: an item read back is self-describing without the caller holding a
    schema, and ``begins_with(sk, "feature#imbalance#")`` means one Path 2 feature's three class
    rows and cannot accidentally mean a *different* feature whose name starts the same way.
    """
    parts = []
    for field in PATHS[path]["grain"]:
        value = row[field]
        # A '#' inside a value would forge a key boundary -- `feature#a#class#b` could then be
        # produced by two different rows. Nothing in these artifacts contains one (the values are
        # Python identifiers and Binance symbols), which is exactly why it is checked here rather
        # than assumed forever.
        if not isinstance(value, str) or not value or "#" in value:
            raise ValueError(f"{path} row has {field}={value!r}: a grain value must be a "
                             f"non-empty string with no '#', which is the key separator")
        parts += [field, value]
    return "#".join(parts)


def attribute(name, value):
    """One Parquet cell -> one DynamoDB attribute value, or None meaning "omit this attribute".

    Everything DynamoDB refuses is refused HERE, with the column name attached, rather than 90
    items later inside boto3 with only the offending value to go on.
    """
    if value is None:
        return None

    # BEFORE the int branch, because bool IS a subclass of int: check int first and
    # survived_step9=True is written as the number 1. The item is valid, the write succeeds, and
    # a consumer reads 1 where it expected true. Nothing raises, ever.
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if value != value:
            raise ValueError(f"{name} is NaN -- DynamoDB has no NaN, and boto3 raises "
                             f"'Infinity and NaN not supported' at write time. A null in "
                             f"Parquet arrives here as None and is omitted; a NaN means "
                             f"something upstream computed one, or the file was read with "
                             f"pandas, which turns nulls into NaN")
        if value in (float("inf"), float("-inf")):
            raise ValueError(f"{name} is {value} -- DynamoDB has no infinity")
        # str(), not Decimal(value). Python's float repr has been the shortest string that
        # round-trips since 3.1, so Decimal(str(x)) loses nothing and float() returns the
        # identical double; Decimal(value) instead takes the exact binary expansion -- 55
        # significant digits for 0.1 -- and DYNAMODB_CONTEXT (prec=38) raises Inexact on it.
        encoded = Decimal(str(value))
        # The magnitude band boto3 does NOT police. Measured: 1E-131, 1E-140 and 1E126 all
        # serialize cleanly, and DynamoDB rejects every one of them -- its range is 1E-130 to
        # 9.9999...E125. That band is the one place --dry-run would come out green and the real
        # write would fail, which is the single thing this file exists to make impossible.
        # Unreachable on today's artifacts (they span 1.36e-08 to 4301.0); two lines anyway,
        # because "if it would be rejected, it is rejected on a laptop" is either true or it is
        # not. `if encoded` skips zero, whose adjusted() is 0 and means nothing.
        if encoded and not (DDB_MIN_EXP <= encoded.adjusted() <= DDB_MAX_EXP):
            raise ValueError(f"{name} is {value!r}, outside DynamoDB's number range of 1E-130 "
                             f"to 9.9E125 -- boto3 encodes it without complaint and the "
                             f"service refuses it")
        return encoded

    if isinstance(value, str):
        return value

    raise TypeError(f"{name} is {type(value).__name__} -- expected the native Python types "
                    f"pyarrow.to_pylist() produces. A numpy scalar here means the Parquet was "
                    f"read with pandas, and boto3 rejects every one of them")


def read_rows(uri):
    """Read a Parquet prefix -- one Spark part-file or several -- as plain Python dicts.

    boto3 lists and fetches the objects itself rather than handing pyarrow an S3 filesystem.
    The files are single-digit KB (each path job writes ``.coalesce(1)``), and this way the job
    needs nothing beyond boto3 and pyarrow: no s3fs, no awswrangler, and no dependency on the
    installed pyarrow having been built with S3 support.
    """
    if not uri.startswith("s3://"):
        # pyarrow reads a directory as a dataset and ignores Spark's _SUCCESS and .crc files.
        return pq.read_table(uri).to_pylist()

    bucket, _, prefix = uri[len("s3://"):].partition("/")
    s3 = boto3.client("s3")
    keys = []
    # Paginated: list_objects_v2 returns at most 1000 keys and silently truncates without it.
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        raise ValueError(f"no .parquet objects under {uri} -- the path job either did not run "
                         f"or wrote somewhere else")
    tables = [pq.read_table(BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read()))
              for k in sorted(keys)]
    return pa.concat_tables(tables).to_pylist()


def build_items(path, rows, run_id):
    """One artifact -> its header item and its grain items, keys proved unique.

    Returns ``(items, model)`` where `model` is the constant columns lifted out of every row.
    """
    spec = PATHS[path]
    if not rows:
        raise ValueError(f"{path} artifact holds no rows -- there is nothing to publish")

    missing = [c for c in spec["grain"] + spec["model"] if c not in rows[0]]
    if missing:
        raise ValueError(f"{path} artifact is missing {missing} -- this is not the "
                         f"{spec['artifact']}/ frame that glue-refinery-{path}.py writes")

    # The model columns are only safe to lift because they are constant, so that is checked
    # rather than believed. If a future run made one of them per-row, lifting it would publish
    # one row's value as if it were the model's and delete the other 17 without a word.
    model = {}
    for column in spec["model"]:
        values = {row[column] for row in rows}
        if len(values) != 1:
            raise ValueError(f"{path}.{column} takes {len(values)} distinct values across "
                             f"{len(rows)} rows -- it is not a model-level constant and must "
                             f"not be lifted onto the header item")
        model[column] = values.pop()

    pk = f"{run_id}#{path}"
    header = {"pk": pk, "sk": MODEL_SK, "run_id": run_id, "path": path,
              "artifact": spec["artifact"], "artifact_rows": len(rows)}
    for column, value in model.items():
        encoded = attribute(f"{path}.{column}", value)
        if encoded is not None:
            header[column] = encoded

    items = [header]
    for row in rows:
        item = {"pk": pk, "sk": sort_key(path, row), "run_id": run_id, "path": path}
        for column, value in row.items():
            if column in spec["model"]:
                continue                      # lives on the header item now
            encoded = attribute(f"{path}.{column}", value)
            if encoded is None:
                continue                      # a null is an absent attribute, not a NULL
            item[column] = encoded
        items.append(item)

    # The failure that loses rows in silence. batch_writer de-duplicates nothing: a repeated key
    # inside one batch is a ValidationException, and a repeated key across two batches is an
    # overwrite that raises nothing at all and leaves a short table nobody counts.
    keys = {(i["pk"], i["sk"]) for i in items}
    if len(keys) != len(items):
        raise ValueError(f"{path} produced {len(items)} items with only {len(keys)} distinct "
                         f"keys -- {spec['grain']} does not identify a row of "
                         f"{spec['artifact']}/, and the duplicates would overwrite each other")

    for item in items:
        SERIALIZER.serialize(item)            # the real encoder, before anything is sent

    return items, model


def report_type_disagreements(built):
    """Warn where two paths give the same column name two different DynamoDB types.

    This job is the first place all three artifacts meet, so it is the only place a
    cross-path inconsistency is visible at all. It reports and does not repair: retyping a
    column here would make the table disagree with the Parquet it came from, and if a path's
    encoding is wrong then the path is what needs fixing.

    It found exactly one hit when it was written: ``survived_step9`` was a genuine boolean on
    Path 1 and the strings "True"/"False" on Path 2. That was fixed where it belonged --
    ``StringType`` -> ``BooleanType`` in ``glue-refinery-path2.py``'s COEFFICIENT_SCHEMA -- so
    it now reports nothing, which is the honest state for it and not a reason to delete it: a
    fourth path, or a schema edit to any of the three, has no other place to be caught.
    """
    types = {}
    for path, items in built.items():
        for item in items[1:]:                # grain items; the header is this job's own shape
            for column, value in item.items():
                types.setdefault(column, {})[path] = type(value).__name__
    for column, by_path in sorted(types.items()):
        if len(set(by_path.values())) > 1:
            LOG.warning("%s is published with different types by different paths (%s) -- the "
                        "artifacts disagree, and this job ships them as written rather than "
                        "retyping one to match the other", column, by_path)


def write_items(table, items):
    """Write with the batch writer, deliberately WITHOUT overwrite_by_pkeys.

    boto3's ``overwrite_by_pkeys`` would collapse a duplicate key inside a batch instead of
    letting DynamoDB reject it. build_items() has already proved the keys unique, so a duplicate
    reaching this point means that proof is wrong -- and the useful behaviour then is a loud
    ValidationException, not a quiet last-one-wins.
    """
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def sweep_partition(table, pk, written):
    """Read back one partition and delete whatever this run did not write.

    Returns ``(held, orphans)``. Without this the job's idempotency claim is not true, only
    nearly true: ``put_item`` overwrites and cannot delete, so a rerun whose Step 8 keeps fewer
    features than the last one under the same ``--run-id`` leaves the dropped features sitting
    in the partition, still carrying last run's coefficient, and a Query returns them alongside
    the current ones with nothing to mark them.

    An earlier version of this function only counted and logged a warning. That is the shape of
    fix that reads as diligence and changes nothing: the job still exits 0, Airflow still goes
    green, and the stale item is still served. The partition is keyed ``<run-id>#<path>``, so
    everything in it was written by a previous run of THIS job for THIS run id and path --
    which is what makes deleting from it a republish rather than a guess. Every deletion is
    logged by name.
    """
    held = set()
    query = {"KeyConditionExpression": Key("pk").eq(pk),
             # `sk` is not a reserved word, but the projection is expressed through a name
             # placeholder anyway: the reserved-word list is 570 entries long and growing, and
             # finding out you are on it costs a ValidationException in production.
             "ProjectionExpression": "#s", "ExpressionAttributeNames": {"#s": "sk"},
             "ConsistentRead": True}
    while True:
        page = table.query(**query)
        held.update(item["sk"] for item in page["Items"])
        if "LastEvaluatedKey" not in page:
            break
        query["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    orphans = sorted(held - written)
    if orphans:
        with table.batch_writer() as batch:
            for sk in orphans:
                batch.delete_item(Key={"pk": pk, "sk": sk})
    return held, orphans


def self_check():
    """Assert the decisions that would go wrong silently. No AWS, no network, no files.

    Everything heavy here is boto3's, and boto3 is tested upstream. What is NOT tested upstream
    is this file's type boundary and its key construction -- and each way those can be wrong
    produces a valid item, a successful write and a wrong table.
    """
    # 1. bool is checked before int. `isinstance(True, int)` is True, so an attribute() that
    #    asked about int first would write survived_step9 as the number 1: a valid item, a
    #    successful write, and a consumer reading 1 where it expected true. `== True` cannot
    #    catch it either, because 1 == True in Python. Only the type can.
    assert type(attribute("b", True)) is bool, "a bool was encoded as something else"
    assert SERIALIZER.serialize(attribute("b", False)) == {"BOOL": False}
    assert type(attribute("n", 212)) is int, "an int64 must stay an integer, not become Decimal"
    assert SERIALIZER.serialize(attribute("n", 212)) == {"N": "212"}

    # 2. A float becomes Decimal(str(x)) and comes back the IDENTICAL double. Decimal(x) does
    #    not: the exact binary expansion of 0.1 is 55 significant digits and DYNAMODB_CONTEXT
    #    has prec=38, so boto3 raises Inexact -- at write time, after the whole refinery has run.
    for value in [0.1, 1.359882056121006e-08, -0.12208986818822024, 4301.0, 0.0]:
        encoded = attribute("f", value)
        assert type(encoded) is Decimal, f"{value} was not converted to Decimal"
        assert float(encoded) == value, f"{value} did not round-trip: {encoded}"
        SERIALIZER.serialize(encoded)
    try:
        SERIALIZER.serialize(Decimal(0.1))
    except Exception:
        pass
    else:                                                       # pragma: no cover
        raise AssertionError("Decimal(0.1) serialized -- the str() conversion is now pointless "
                             "and this file's reason for it is wrong")

    # 3. NaN and infinity are refused HERE, with a column name, not 90 items later inside boto3.
    #    This is the one pandas would hand over: Path 1's mu/sigma/bp_per_sd are null on 12 of
    #    18 rows and pandas reads a null float64 as NaN.
    for bad in [float("nan"), float("inf"), float("-inf")]:
        try:
            attribute("mu", bad)
        except ValueError as exc:
            assert "mu" in str(exc), "the rejection does not name the column"
        else:                                                   # pragma: no cover
            raise AssertionError(f"{bad} was accepted")

    # 4. A null is an absent attribute, not a NULL and not a zero.
    assert attribute("sigma", None) is None

    # 4b. The magnitude band boto3 waves through and the service refuses. MEASURED: 1E-131,
    #     1E-140 and 1E126 all serialize cleanly here, so without this guard --dry-run would
    #     report success on an item the real write rejects -- the one way this file's whole
    #     premise could be false. Zero is not a magnitude and must survive.
    assert attribute("f", 0.0) == Decimal("0")
    for edge in [1e-130, 9.9e125, -9.9e125, -1e-130]:
        SERIALIZER.serialize(attribute("f", edge))          # inside the range, must pass
    #     (5e-324 is NOT in this list: boto3 raises Underflow on it, because a denormal's
    #      exponent falls below DYNAMODB_CONTEXT's Etiny. It is outside the range AND caught,
    #      so it says nothing about the gap.)
    for outside in [1e-131, 1e-140, 1e126, -1e126]:
        SERIALIZER.serialize(Decimal(str(outside)))         # boto3 itself does NOT object...
        try:
            attribute("coefficient", outside)               # ...so this file has to
        except ValueError as exc:
            assert "coefficient" in str(exc), "the rejection does not name the column"
        else:                                               # pragma: no cover
            raise AssertionError(f"{outside} is outside DynamoDB's range and was accepted")

    # 5. A type pyarrow does not produce is refused HERE, by name. numpy.int64 and numpy.bool_
    #    are the two boto3 rejects as "Unsupported type", and neither can arrive through
    #    pyarrow -- which is the point of the check: if it ever fires, somebody swapped the
    #    reader for pandas. (numpy.float64 is the one numpy scalar this boundary would let
    #    through, because it is a float subclass and converts correctly. boto3 would reject it
    #    downstream anyway, so nothing here depends on catching it.)
    for bad in [object(), b"x", [1], {"a": 1}]:
        try:
            attribute("coefficient", bad)
        except TypeError:
            pass
        else:                                                   # pragma: no cover
            raise AssertionError(f"{type(bad).__name__} was accepted as an attribute")

    # 6. The keys. Every path's grain must identify a row -- a grain that does not silently
    #    overwrites, which is the only failure here that destroys data without raising.
    p1 = [{"feature": f, "coefficient": 0.0, "reg_param": 1e-6, "elastic_net_param": 1.0,
           "intercept": 0.0, "cv_rmse": 1.0, "target_sd": 1.0, "n_rows": 3.0, "mu": None}
          for f in ["open", "close", "ret1"]]
    p2 = [{"feature": f, "class_name": c, "coefficient": 0.0, "reg_param": 0.01,
           "elastic_net_param": 0.5, "baseline_accuracy": 0.35}
          for f in ["imbalance", "log_range"] for c in ["down", "flat", "up"]]
    p3 = [{"symbol": s, "pulls": 1, "posterior_mean": 0.5} for s in ["BTCUSDT", "ETHUSDT"]]

    items1, model1 = build_items("path1", p1, "R")
    items2, _ = build_items("path2", p2, "R")
    items3, _ = build_items("path3", p3, "R")
    assert [len(items1), len(items2), len(items3)] == [4, 7, 3], "header item count changed"
    assert model1["cv_rmse"] == 1.0 and "cv_rmse" not in items1[1], \
        "a model-level column was left on a grain item as well as lifted"
    assert items2[1]["sk"] == "feature#imbalance#class_name#down", \
        f"path 2 sort key is {items2[1]['sk']!r}"
    assert items1[0]["sk"] == MODEL_SK and sorted(i["sk"] for i in items1)[0] == MODEL_SK, \
        "the header item does not sort first, so a Query would not return it first"
    assert "mu" not in items1[1], "a null was written instead of omitted"

    # Path 2's grain is (feature, class_name) and NOT feature alone. Dropping class_name maps
    # all three classes of a feature to one key: 72 rows become 24 items, no error, no warning.
    collapsed = {sort_key("path1", r) for r in p2}
    assert len(collapsed) == 2 < len(p2), \
        "feature alone should collide on path 2 -- if it does not, this check proves nothing"

    # 7. A '#' in a grain value would forge a key boundary.
    for bad in ["a#b", "", None, 7]:
        try:
            sort_key("path1", {"feature": bad})
        except ValueError:
            pass
        else:                                                   # pragma: no cover
            raise AssertionError(f"{bad!r} was accepted as a grain value")

    # 8. A model column that is not actually constant must refuse to be lifted.
    try:
        build_items("path1", [dict(p1[0], cv_rmse=v) for v in (1.0, 2.0)], "R")
    except ValueError as exc:
        assert "cv_rmse" in str(exc)
    else:                                                       # pragma: no cover
        raise AssertionError("a non-constant column was lifted onto the header item")

    LOG.info("self-check passed: bool-before-int, Decimal(str(x)) round-tripping over 5 values, "
             "NaN/inf refused by column name, the 1E%s..9.9E%s band boto3 does not police, "
             "nulls omitted, the %s-part path 2 grain, the '#' guard, and the "
             "constant-column proof",
             DDB_MIN_EXP, DDB_MAX_EXP, len(PATHS["path2"]["grain"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for path in PATHS:
        parser.add_argument(f"--{path}",
                            help=f"the --output prefix glue-refinery-{path}.py wrote; "
                                 f"{PATHS[path]['artifact']}/ is read from beneath it "
                                 f"(s3:// or a local path). At least one path is required")
    parser.add_argument("--run-id",
                        help="identifies this run in the partition key, e.g. the month the bars "
                             "cover. SUPPLIED, not derived: unlike Path 3's arms and bar width "
                             "there is nothing in the artifacts to derive it from, so it has no "
                             "default and a rerun must pass the same one to stay idempotent")
    parser.add_argument("--table", default=DEFAULT_TABLE,
                        help=f"DynamoDB table name (default: {DEFAULT_TABLE}). The table is not "
                             f"created here -- see the README; a loader that creates tables is a "
                             f"loader holding IAM permissions it has no other use for")
    parser.add_argument("--region", default=None,
                        help="AWS region; defaults to the environment, which on Glue is the "
                             "region the job runs in")
    parser.add_argument("--dry-run", action="store_true",
                        help="build and encode every item, print them, write nothing. Needs no "
                             "credentials and no region -- and for a local --path*, no network")
    parser.add_argument("--self-check", action="store_true",
                        help="assert the type boundary and the key construction, then exit; no "
                             "AWS, no network, no input")
    # parse_known_args, not parse_args: Glue appends --JOB_NAME, --TempDir and friends to
    # sys.argv on every run and a strict parser exits 2 on them before the job starts.
    args, _ = parser.parse_known_args()

    if args.self_check:
        self_check()
        return

    given = [p for p in PATHS if getattr(args, p)]
    # Checked here rather than by required=True so that --self-check needs neither of them.
    if not given:
        parser.error("at least one of --path1/--path2/--path3 is required unless --self-check")
    if not args.run_id:
        parser.error("--run-id is required: it is half the partition key and has no default")
    if "#" in args.run_id:
        parser.error("--run-id must not contain '#' -- it is the partition key separator")

    LOG.info("run-id %s | table %s | paths %s%s", args.run_id, args.table, given,
             " | DRY RUN, nothing will be written" if args.dry_run else "")

    built = {}
    for path in given:
        uri = f"{getattr(args, path).rstrip('/')}/{PATHS[path]['artifact']}"
        rows = read_rows(uri)
        items, model = build_items(path, rows, args.run_id)
        built[path] = items
        LOG.info("%s: %s rows from %s -> %s items (1 header + %s grain)%s",
                 path, len(rows), uri, len(items), len(items) - 1,
                 f" | model {model}" if model else "")

    report_type_disagreements(built)

    if args.dry_run:
        for path, items in built.items():
            for item in items:
                LOG.info("%s %s", path, SERIALIZER.serialize(item))
        LOG.info("dry run complete: %s items encoded by boto3's own serializer and discarded. "
                 "This proves their TYPES, and nothing about a table that has never existed",
                 sum(len(i) for i in built.values()))
        return

    table = boto3.resource("dynamodb", region_name=args.region).Table(args.table)
    LOG.info("   %-8s %7s %7s %7s", "path", "written", "found", "swept")
    for path, items in built.items():
        write_items(table, items)
        held, orphans = sweep_partition(table, items[0]["pk"], {i["sk"] for i in items})
        LOG.info("   %-8s %7s %7s %7s", path, len(items), len(held), len(orphans))
        for sk in orphans:
            LOG.warning("%s: deleted %s -- a previous run under --run-id %s wrote it and this "
                        "one did not, so it was serving a stale value", path, sk, args.run_id)
    LOG.info("published %s items to %s under run-id %s; each partition now holds exactly what "
             "this run produced", sum(len(i) for i in built.values()), args.table, args.run_id)


if __name__ == "__main__":
    main()
