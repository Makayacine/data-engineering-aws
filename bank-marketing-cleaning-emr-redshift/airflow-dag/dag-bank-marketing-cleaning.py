"""Bank marketing cleaning + KPI DAG.

Validates the raw CSV in S3, runs the cleaning/preparation job on EMR Serverless,
loads the two KPI tables it emits into Redshift, then archives the raw object.

    validate_raw_dataset >> check_validation >> [submit_spark_job, end_dag]
    submit_spark_job >> load_segment_kpis >> load_monthly_kpis >> archive_processed_files

The heavy work happens in spark-jobs/bank_marketing_clean_prep.py on the cluster.
This DAG only validates, submits, and moves small aggregate CSVs into Redshift.
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
# The reference lab imports DummyOperator from airflow.operators.dummy, which was removed
# in Airflow 2.4 -- EmptyOperator is the current name for the same do-nothing operator.
from airflow.operators.empty import EmptyOperator
# The reference lab imports PostgresHook from airflow.hooks.postgres_hook, the deprecated
# pre-provider path; the Postgres provider package is where it lives now.
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
import pandas as pd
import boto3
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'nl-aws-de-labs'
RAW_FILE_PATH = 'bank_marketing/raw/bank-additional-full.csv'
ARCHIVE_PREFIX = 'bank_marketing/archived/'
CURATED_PREFIX = 'bank_marketing/curated/'
KPI_PREFIX = 'bank_marketing/kpis/'
SEGMENT_KPI_PREFIX = KPI_PREFIX + 'segment_level_kpis/'
MONTHLY_KPI_PREFIX = KPI_PREFIX + 'monthly_kpis/'

# PLACEHOLDERS -- nothing AWS-side has been created yet. Replace both with the real
# application id and the ARN of a job role that can read the raw prefix and write the
# curated/KPI/log prefixes, or the first run fails inside EMR with a ValidationException.
EMR_APPLICATION_ID = '<emr-serverless-application-id>'
EMR_EXECUTION_ROLE_ARN = 'arn:aws:iam::<account-id>:role/<emr-serverless-job-role>'
SPARK_ENTRY_POINT = f's3://{BUCKET_NAME}/bank_marketing/scripts/bank_marketing_clean_prep.py'
EMR_LOG_URI = f's3://{BUCKET_NAME}/bank_marketing/emr-logs/'

# The raw file is the source distribution: semicolon separated, and four of its headers are
# dotted (emp.var.rate ...). The Spark job renames them to underscores via its schema, so the
# dotted spelling only ever appears here, in the header check.
REQUIRED_COLUMNS = {
    'raw_bank_marketing': ['age', 'job', 'marital', 'education', 'default', 'housing',
                           'loan', 'contact', 'month', 'day_of_week', 'duration',
                           'campaign', 'pdays', 'previous', 'poutcome', 'emp.var.rate',
                           'cons.price.idx', 'cons.conf.idx', 'euribor3m', 'nr.employed',
                           'y'],
    # Both KPI lists are also the Redshift column ORDER: the upsert ends with
    # INSERT INTO <table> SELECT * FROM tmp_<table>, so the frame must match the DDL.
    'segment_level_kpis': ['contact_date', 'sector', 'education', 'contacts', 'subscribed',
                           'subscribe_rate', 'avg_age', 'avg_campaign'],
    'monthly_kpis': ['contact_date', 'contacts', 'unique_jobs', 'subscribed',
                     'subscribe_rate', 'avg_euribor3m', 'top_sector', 'pct_cellular'],
}

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket that match the prefix """
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}")
        return files
    except Exception as e:
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

def read_s3_csv(file_name, bucket=BUCKET_NAME, **read_csv_kwargs):
    """ Helper function to read a CSV file from S3 """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'], **read_csv_kwargs)
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")

def s3_object_size(file_name, bucket=BUCKET_NAME):
    """ Byte size of a single S3 object, without downloading it """
    s3 = boto3.client('s3')
    try:
        size = s3.head_object(Bucket=bucket, Key=file_name)['ContentLength']
        logging.info(f"{file_name} is {size} bytes")
        return size
    except Exception as e:
        logging.error(f"Failed to head {file_name} in S3: {str(e)}")
        raise Exception(f"Failed to head {file_name} in S3: {str(e)}")

def read_kpi_csv(prefix, table_name):
    """ Read back a KPI directory the Spark job wrote as coalesce(1) CSV with a header.

    The prefix is a directory: one part-*.csv plus a zero-byte _SUCCESS marker, which
    list_s3_files already filters out by extension.
    """
    files = list_s3_files(prefix)
    if not files:
        raise Exception(f"No KPI CSV part files found under {prefix}")

    kpis = pd.concat([read_s3_csv(file) for file in files], ignore_index=True)
    # Spark writes the date as YYYY-MM-DD; psycopg2 needs a date object for a DATE column.
    kpis['contact_date'] = pd.to_datetime(kpis['contact_date']).dt.date
    return kpis[REQUIRED_COLUMNS[table_name]]

def validate_raw_dataset():
    validation_results = {}

    # Validate the raw bank marketing dataset
    try:
        # nrows=0 reads the header alone -- there is no reason to pull 5.8 MB over the wire
        # to check twenty-one column names.
        header = read_s3_csv(RAW_FILE_PATH, sep=';', nrows=0)
        # ORDER matters, not just membership: the Spark job reads with an explicit
        # StructType, which binds CSV fields by POSITION. The same 21 names in a different
        # order would pass a set check and then load every value into the wrong column.
        expected = REQUIRED_COLUMNS['raw_bank_marketing']
        found = list(header.columns)
        if found == expected:
            validation_results['columns'] = True
            logging.info("All required columns present, in order, in raw_bank_marketing")
        else:
            validation_results['columns'] = False
            logging.warning(f"raw_bank_marketing header mismatch: expected {expected}, "
                            f"found {found} (missing: {set(expected) - set(found)}, "
                            f"unexpected: {set(found) - set(expected)})")

        size = s3_object_size(RAW_FILE_PATH)
        if size > 0:
            validation_results['non_empty'] = True
            logging.info(f"raw_bank_marketing is non-empty ({size} bytes)")
        else:
            validation_results['non_empty'] = False
            logging.warning("raw_bank_marketing is a zero-byte object")
    except Exception as e:
        validation_results['columns'] = False
        validation_results['non_empty'] = False
        logging.error(f"Failed to read or validate raw_bank_marketing from S3: {e}")
        raise

    return validation_results

def branch_task(ti):
    validation_results = ti.xcom_pull(task_ids='validate_raw_dataset')

    if all(validation_results.values()):
        return 'submit_spark_job'
    else:
        return 'end_dag'

def upsert_to_redshift(df, table_name, id_columns):
    redshift_hook = PostgresHook(postgres_conn_id="redshift_default")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try:
        # psycopg2 has no adapter for numpy.int64/float64, and a NaN top_sector from the
        # left join must go in as NULL rather than a float nan -- .item() unwraps the numpy
        # scalars, pd.isna() catches the missing values.
        data_tuples = [tuple(None if pd.isna(v) else getattr(v, 'item', lambda: v)()
                             for v in row)
                       for row in df.to_numpy()]

        # Create insert query for the temporary table
        cols = ', '.join(list(df.columns))
        vals = ', '.join(['%s'] * len(df.columns))
        tmp_table_query = f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})"

        cursor.executemany(tmp_table_query, data_tuples)

        # Create the merge (upsert) query
        delete_condition = ' AND '.join([f'tmp_{table_name}.{col} = {table_name}.{col}' for col in id_columns])
        merge_query = f"""
        BEGIN;
        DELETE FROM reporting_schema.{table_name}
        USING reporting_schema.tmp_{table_name}
        WHERE {delete_condition};

        INSERT INTO reporting_schema.{table_name}
        SELECT * FROM reporting_schema.tmp_{table_name};

        TRUNCATE TABLE reporting_schema.tmp_{table_name};
        COMMIT;
        """

        cursor.execute(merge_query)
        conn.commit()
        logging.info(f"Data ingested and merged successfully into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to ingest and merge data into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def load_segment_kpis():
    segment_kpis = read_kpi_csv(SEGMENT_KPI_PREFIX, 'segment_level_kpis')

    logging.info("Segment-level KPIs:")
    logging.info(segment_kpis.columns)

    upsert_to_redshift(segment_kpis, 'segment_level_kpis',
                       ['contact_date', 'sector', 'education'])

def load_monthly_kpis():
    monthly_kpis = read_kpi_csv(MONTHLY_KPI_PREFIX, 'monthly_kpis')

    logging.info("Monthly KPIs:")
    logging.info(monthly_kpis.columns)

    upsert_to_redshift(monthly_kpis, 'monthly_kpis', ['contact_date'])

def move_processed_files():
    s3 = boto3.client('s3')
    try:
        copy_source = {'Bucket': BUCKET_NAME, 'Key': RAW_FILE_PATH}
        destination_key = RAW_FILE_PATH.replace('bank_marketing/raw/', ARCHIVE_PREFIX)
        s3.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key=destination_key)
        s3.delete_object(Bucket=BUCKET_NAME, Key=RAW_FILE_PATH)
        logging.info(f"Moved {RAW_FILE_PATH} to {destination_key}")
    except Exception as e:
        logging.error(f"Failed to move {RAW_FILE_PATH} to {ARCHIVE_PREFIX}: {str(e)}")
        raise

with DAG('bank_marketing_cleaning_and_kpis', default_args=default_args,
         schedule_interval='@daily', catchup=False) as dag:
    validate_raw_dataset = PythonOperator(
        task_id='validate_raw_dataset',
        python_callable=validate_raw_dataset
    )

    # No provide_context=True: Airflow 2 rejects unknown operator kwargs, and it injects
    # ti into the callable on its own.
    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task
    )

    # entryPointArguments binds positionally to the job's argparse flags; --local is
    # deliberately absent so EMR supplies the master.
    submit_spark_job = EmrServerlessStartJobOperator(
        task_id='submit_spark_job',
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        name='bank-marketing-clean-prep',
        job_driver={
            'sparkSubmit': {
                'entryPoint': SPARK_ENTRY_POINT,
                'entryPointArguments': [
                    '--input', f's3://{BUCKET_NAME}/{RAW_FILE_PATH}',
                    '--curated-output', f's3://{BUCKET_NAME}/{CURATED_PREFIX}',
                    '--kpi-output', f's3://{BUCKET_NAME}/{KPI_PREFIX}',
                ],
            }
        },
        configuration_overrides={
            'monitoringConfiguration': {
                's3MonitoringConfiguration': {'logUri': EMR_LOG_URI}
            }
        }
    )

    load_segment_kpis = PythonOperator(
        task_id='load_segment_kpis',
        python_callable=load_segment_kpis
    )

    load_monthly_kpis = PythonOperator(
        task_id='load_monthly_kpis',
        python_callable=load_monthly_kpis
    )

    archive_processed_files = PythonOperator(
        task_id='archive_processed_files',
        python_callable=move_processed_files
    )

    end_dag = EmptyOperator(
        task_id='end_dag'
    )

    validate_raw_dataset >> check_validation >> [submit_spark_job, end_dag]
    submit_spark_job >> load_segment_kpis >> load_monthly_kpis >> archive_processed_files
