"""Crypto ticks refinery -- the monthly Glue workflow.

    validate_raw_ticks >> check_validation >> [ingest_bars, end_dag]
    ingest_bars >> [refine_path1, refine_path2, refine_path3] >> load_dynamo

One Glue Spark job builds the bars, THREE run the path-isolated sub-refineries, and one Glue
Python shell job publishes their ledgers to DynamoDB. The three path tasks are parallel and
that is not a scheduling optimisation: the fork is the project's whole thesis, the paths share
step numbers and nothing else, and a DAG that ran them in a chain would draw a dependency
between them that the code deliberately does not have.

THE MONTH IS SUPPLIED ONCE AND HAS TO REACH THREE PLACES
--------------------------------------------------------
``glue-dynamo.py`` says in its own docstring that its ``--run-id`` is supplied rather than
derived, because the three artifacts carry no month, no calendar and no run identity to derive
one from. This DAG is where it comes from: ``data_interval_start`` on an ``@monthly`` schedule,
formatted once into ``MONTH`` and passed to the entryway's ``--month``, into the S3 prefix every
path job writes under, and into the loader's ``--run-id``.

Those three have to agree, and only this file can make them. Every job writes
``mode("overwrite")`` and none of them partitions, so a second month pointed at the same prefix
destroys the first -- which would leave DynamoDB holding a run-scoped history whose S3 artifacts
no longer exist. Month-scoping the prefixes here is what keeps the table's key and the bucket
telling the same story.

FOUR THINGS THE REFERENCE LAB DOES THAT THIS DOES NOT
-----------------------------------------------------
``Lab2-Airflow-Spark-Dynamo/dag-glue-workflow.py`` is the lab this project imitates. Four of its
choices do not survive contact with a current Airflow or with a job that can fail:

1.  **It hand-rolls the Glue poller with boto3, and the poller passes on failure.** Its loop
    exits when the state is no longer ``RUNNING``/``STARTING``/``STOPPING`` and then logs that
    the job "has finished" -- so ``FAILED``, ``TIMEOUT`` and ``STOPPED`` all leave the loop and
    the task goes green. A Glue job that died would hand a green Airflow task to the next task,
    which would then run against last month's artifacts. ``GlueJobOperator`` raises on a
    terminal state that is not ``SUCCEEDED``.
2.  **It polls the wrong run.** ``get_job_runs(JobName=..., MaxResults=1)`` returns the most
    recent run of that job *name*, and the ``JobRunId`` that ``start_job_run`` returned is
    discarded. Any concurrent or manually-started run is what gets watched instead.
    ``GlueJobOperator`` keeps the run id it started and polls that one.
3.  **It imports from module paths that are on their way out.** ``airflow.operators``'s
    ``dummy_operator``, ``dummy`` and ``python_operator``, and ``provide_context=True`` on the
    operator. This was worth measuring rather than repeating, because the widely-repeated claim
    -- that these break DAG parsing on Airflow 2.4+ -- is **wrong**: on 2.9.3 all three import
    fine and Lab 2's DAG parses into a DagBag with no import errors at all, emitting a
    ``DeprecationWarning`` each. What is true is that they are shims scheduled for removal in
    Airflow 3, alongside ``provide_context`` (``RemovedInAirflow3Warning``, already ignored) and
    ``days_ago`` (same warning, on call). So this file uses ``airflow.operators.empty`` and
    ``airflow.operators.python`` because the old spellings are deprecated, not because they are
    broken today.

    ``days_ago(1)`` is dropped for a second reason as well as the deprecation: a *dynamic*
    ``start_date`` moves every time the scheduler re-parses the file, and this DAG derives its
    S3 prefixes and its DynamoDB partition key from the run's data interval. A key that depends
    on when a file was last parsed is not a key. ``start_date`` is the fixed first month the
    archives cover.
4.  **It is scheduled ``*/5 * * * *``** for jobs that take minutes, with nothing stopping runs
    from overlapping. This one is ``@monthly`` -- the cadence of the source archives -- with
    ``max_active_runs=1``, because two concurrent runs would write the same S3 prefixes.

``GlueJobOperator`` comes from ``apache-airflow-providers-amazon``, which this repository
already depends on for the sibling project's ``EmrServerlessStartJobOperator``. Preferring the
provider operator over hand-rolled boto3 is that project's precedent, and both of the polling
bugs above are the kind the provider exists to have solved once.

WHAT IS CHECKED, AND WHERE
---------------------------
``test_dag_workflow.py`` next to this file checks the DAG as a graph: that the file parses into
a DagBag with no import errors, that the task graph is the one drawn above, that every Glue task
waits for completion, and that ``script_args`` is a template field -- which is what lets
``MONTH`` reach the jobs at all. That last one is the check worth having, because without it the
jobs receive the literal Jinja string as their ``--month``.

Whether the five Glue jobs exist under the names below, and whether the scheduler's role may
start them, is settled when the jobs are created; the names here are the deployment's to set.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import boto3
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # A FIXED date, not days_ago(1). See the docstring: a dynamic start_date moves on every
    # parse, and the month it produces is this DAG's S3 prefix and its DynamoDB partition key.
    # 2025-01 is the first month the archives cover.
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # The entryway is 341M ticks and measured 6 min 56 s locally; a retry that lands while the
    # previous attempt's Glue run is still winding down is worse than waiting.
    'retry_delay': timedelta(minutes=15),
}

# Constants for S3 bucket and prefixes
BUCKET_NAME = 'nl-aws-de-labs'
RAW_PREFIX = 'crypto_ticks/unzipped/'
SCRIPTS_PREFIX = 'crypto_ticks/scripts/'
BARS_PREFIX = 'crypto_ticks/curated/bars/'
CURATED_PREFIX = 'crypto_ticks/curated/'

AWS_REGION = 'us-east-1'
DYNAMO_TABLE = 'crypto_ticks_refinery'

# The three symbols the archives cover, and the bar width. 5s is not a default anybody should
# change casually -- it is the only width where Path 2's flat class is material and Step 3 has
# any empty bars to flag at all. See the README's "Why 5-second bars".
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
BAR_INTERVAL = '5s'

# Glue job NAMES, not script paths: the jobs are created once by the README's deploy steps and
# this DAG only runs them. GlueJobOperator is given no script_location and no create_job_kwargs
# on purpose, so a missing job fails the task instead of being quietly created here with
# whatever defaults the operator would pick.
INGEST_JOB = 'crypto-ticks-ingest-bars'
PATH_JOBS = {
    'path1': 'crypto-ticks-refinery-path1',
    'path2': 'crypto-ticks-refinery-path2',
    'path3': 'crypto-ticks-refinery-path3',
}
LOAD_JOB = 'crypto-ticks-load-dynamo'

# The one templated value in the DAG. `data_interval_start` on an @monthly schedule is the
# first instant of the month being processed, so this renders to e.g. "2025-01". It is a Jinja
# string rather than a Python value because it is only knowable per run, and it reaches the
# Glue jobs through GlueJobOperator.script_args, which is a template field.
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

# Every path job's artifacts live under the month that produced them. This is the prefix that
# has to agree with glue-dynamo.py's --run-id; both are MONTH.
MONTH_PREFIX = f'{CURATED_PREFIX}{MONTH}/'

# Poll every 30 s rather than the operator's 6 s default. These are multi-minute jobs -- the
# entryway measured 6 min 56 s on a full month locally -- so a 6 s poll is a few hundred
# GetJobRun calls per run to learn nothing.
POLL_INTERVAL = 30


def raw_objects_for(month):
    """The three raw archives this run expects, by key.

    The source files are ``<SYMBOL>-trades-<month>.csv`` -- headerless, seven columns, and the
    entryway binds them BY POSITION, so there is no header here to validate the way the sibling
    bank-marketing DAG validates its CSV. Existence and non-emptiness is the whole check that
    is available before Spark starts.
    """
    return {symbol: f'{RAW_PREFIX}{symbol}-trades-{month}.csv' for symbol in SYMBOLS}


def validate_raw_ticks(data_interval_start=None, **_):
    """Are all three symbols present and non-empty for this month?

    The month comes from the run's data interval rather than from a Jinja string, because a
    PythonOperator is handed the real datetime and does not need one.
    """
    month = data_interval_start.strftime('%Y-%m')
    s3 = boto3.client('s3')
    results = {}

    for symbol, key in raw_objects_for(month).items():
        try:
            size = s3.head_object(Bucket=BUCKET_NAME, Key=key)['ContentLength']
        except Exception as exc:
            # A missing object is a legitimate answer to "is this month ready", not an error:
            # the branch below turns it into a skip. Anything else -- no credentials, no
            # bucket, denied -- is a broken deployment and is re-raised.
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
                logging.warning(f"{key} is not present yet")
                results[symbol] = False
                continue
            logging.error(f"Failed to head {key}: {exc}")
            raise
        results[symbol] = size > 0
        logging.info(f"{key} is {size} bytes")

    logging.info(f"raw tick archives for {month}: {results}")
    return results


def check_validation(ti):
    """All three symbols, or none of the pipeline.

    Not two of three. Every path job derives its symbol list from the bars frame and refuses an
    input whose grid is not dense across every symbol, so a two-symbol month would either be
    rejected by load_bars() three tasks later or -- worse -- silently produce a two-arm bandit
    and an 18-feature ledger that look exactly like a complete run.
    """
    results = ti.xcom_pull(task_ids='validate_raw_ticks')
    if results and all(results.values()):
        return 'ingest_bars'
    return 'end_dag'


with DAG('crypto_ticks_refinery',
         default_args=default_args,
         description='Binance ticks -> 5s bars -> three path-isolated refineries -> DynamoDB',
         schedule_interval='@monthly',
         catchup=False,
         # Two concurrent runs would write the same S3 prefixes with mode("overwrite").
         max_active_runs=1) as dag:

    validate_raw_ticks = PythonOperator(
        task_id='validate_raw_ticks',
        python_callable=validate_raw_ticks
    )

    # No provide_context=True: Airflow 2 rejects unknown operator kwargs, and it injects ti
    # into the callable on its own.
    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=check_validation
    )

    # script_args keys carry their leading dashes: Glue passes them to the job's argv, where
    # argparse reads them. --local is deliberately absent from all five -- on Glue the master
    # comes from the cluster.
    ingest_bars = GlueJobOperator(
        task_id='ingest_bars',
        job_name=INGEST_JOB,
        region_name=AWS_REGION,
        wait_for_completion=True,
        job_poll_interval=POLL_INTERVAL,
        script_args={
            '--input': f's3://{BUCKET_NAME}/{RAW_PREFIX}',
            '--bars-output': f's3://{BUCKET_NAME}/{BARS_PREFIX}{MONTH}/',
            '--month': MONTH,
            '--bar-interval': BAR_INTERVAL,
        }
    )

    # The fork. Three tasks, no edges between them, converging on the loader.
    refine = {}
    for path, job_name in PATH_JOBS.items():
        refine[path] = GlueJobOperator(
            task_id=f'refine_{path}',
            job_name=job_name,
            region_name=AWS_REGION,
            wait_for_completion=True,
            job_poll_interval=POLL_INTERVAL,
            script_args={
                '--input': f's3://{BUCKET_NAME}/{BARS_PREFIX}{MONTH}/',
                '--output': f's3://{BUCKET_NAME}/{MONTH_PREFIX}{path}/',
            }
        )

    # --run-id is the same MONTH the path prefixes carry. glue-dynamo.py cannot derive it from
    # the artifacts, so this is the single place it is decided.
    load_dynamo = GlueJobOperator(
        task_id='load_dynamo',
        job_name=LOAD_JOB,
        region_name=AWS_REGION,
        wait_for_completion=True,
        job_poll_interval=POLL_INTERVAL,
        script_args={
            '--run-id': MONTH,
            '--table': DYNAMO_TABLE,
            '--path1': f's3://{BUCKET_NAME}/{MONTH_PREFIX}path1/',
            '--path2': f's3://{BUCKET_NAME}/{MONTH_PREFIX}path2/',
            '--path3': f's3://{BUCKET_NAME}/{MONTH_PREFIX}path3/',
        }
    )

    end_dag = EmptyOperator(
        task_id='end_dag'
    )

    validate_raw_ticks >> check_validation >> [ingest_bars, end_dag]
    ingest_bars >> list(refine.values()) >> load_dynamo
