# bank-marketing-cleaning-emr-redshift

Cleaning and preparation pipeline for the UCI Bank Marketing dataset
(`bank-additional-full.csv`, 41,188 rows x 21 columns), productionised out of
`local-development/cleaning_and_prep_pipeline_spark.ipynb`.

The Spark job does the work: it cleans the raw CSV, engineers the features, writes a curated
model-ready Parquet frame to S3, and emits two flat KPI CSVs. Airflow validates the raw file,
submits the job to EMR Serverless, upserts the two KPI files into Redshift, and archives the
raw object.

## Architecture

```
 S3 raw/                       EMR Serverless                    S3 curated/
 bank-additional-full.csv  ->  bank_marketing_clean_prep.py  ->  Parquet (model matrix,
 (41,188 x 21)                 clean -> engineer                  keeps the ML vectors)
        ^                            |
        |                            +-------------------------> S3 kpis/
        |                                                          segment_level_kpis/*.csv
 Airflow                                                           monthly_kpis/*.csv
 validate_raw_dataset                                                    |
 check_validation (branch) ---- end_dag                                  v
 submit_spark_job                                        Redshift reporting_schema
 load_segment_kpis                                       tmp_<table> load -> DELETE/INSERT
 load_monthly_kpis                                       upsert -> segment_level_kpis
 archive_processed_files -> S3 archived/                              monthly_kpis
```

Task graph:

```
validate_raw_dataset >> check_validation >> [submit_spark_job, end_dag]
submit_spark_job >> load_segment_kpis >> load_monthly_kpis >> archive_processed_files
```

## S3 layout

The bucket and every prefix are module-level constants at the top of
`airflow-dag/dag-bank-marketing-cleaning.py` — point them at your own bucket before deploying.

```
s3://<bucket>/bank_marketing/
├── raw/bank-additional-full.csv          input, validated by the DAG (header only)
├── archived/bank-additional-full.csv     where the DAG moves the raw object after a run
├── curated/                              Parquet written by the Spark job (--curated-output)
├── kpis/
│   ├── segment_level_kpis/part-*.csv     coalesce(1), header row (--kpi-output)
│   └── monthly_kpis/part-*.csv
├── emr-logs/                             EMR Serverless driver/executor logs (EMR_LOG_URI)
└── scripts/bank_marketing_clean_prep.py  the EMR Serverless entrypoint
```

The two KPI subdirectory names are fixed in the job (`SEGMENT_KPI_DIR`, `MONTHLY_KPI_DIR`) and
are created under whatever `--kpi-output` points at. Both are written with `coalesce(1)` and a
header row because the DAG reads them back with pandas to feed the Redshift upsert; neither
contains a vector, array or struct column.

## Running the Spark job locally

Needs Python 3.11 and `pyspark==3.5.5` (see `requirements.txt`) plus a Java 11 or 17 JDK on
`JAVA_HOME`. Spark does not support Java 21+.

From the project root:

```bash
python spark-jobs/bank_marketing_clean_prep.py \
    --local \
    --input data/bank-additional-sample.csv \
    --curated-output _localrun/curated \
    --kpi-output _localrun/kpis
```

On PowerShell use a backtick for the line continuation and backslashes in the paths.

`--local` is what adds `.master("local[*]")` and drops `spark.sql.shuffle.partitions` to 8;
without it the session is built with no master so EMR supplies one. The job is pure PySpark —
no pandas, no boto3 — so a plain filesystem path works for all three path arguments.

`data/bank-additional-sample.csv` is a 2,000-row sample committed for a quick smoke run. It
will not reproduce the row counts below: the year reconstruction and the IQR fence both need
the full file. For that, download `bank-additional-full.csv` from the UCI Bank Marketing
dataset (<https://archive.ics.uci.edu/dataset/222/bank+marketing>, inside
`bank-additional.zip`) and point `--input` at it.

### Verified run

| Output | Result |
|---|---|
| raw read | 41,188 rows x 21 columns (+ `row_id`) |
| after `dropDuplicates(subset=DATA_COLS)` | 41,176 rows (12 removed) |
| curated Parquet | 41,176 rows x 26 columns, 8 of them `VectorUDT` |
| year reconstruction | 2008 = 27,682 / 2009 = 11,436 / 2010 = 2,058 |
| `segment_level_kpis` CSV | 741 rows, header first |
| `monthly_kpis` CSV | 26 rows, header first |

Every figure matches the notebook.

## Deploying to EMR Serverless

1. Upload the job:
   `aws s3 cp spark-jobs/bank_marketing_clean_prep.py s3://<bucket>/bank_marketing/scripts/`
2. Create (once) an EMR Serverless application of type SPARK, and an IAM job-execution role
   with read/write on the bucket.
3. Put the application id and the role ARN into the constants at the top of the DAG.

The job has no third-party dependencies beyond PySpark itself, so no `--py-files`, no packaged
virtualenv and no `--jars` are needed. A manual submission looks like:

```bash
aws emr-serverless start-job-run \
  --application-id <application-id> \
  --execution-role-arn arn:aws:iam::<account>:role/<emr-serverless-job-role> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://<bucket>/bank_marketing/scripts/bank_marketing_clean_prep.py",
      "entryPointArguments": [
        "--input", "s3://<bucket>/bank_marketing/raw/bank-additional-full.csv",
        "--curated-output", "s3://<bucket>/bank_marketing/curated/",
        "--kpi-output", "s3://<bucket>/bank_marketing/kpis/"
      ]
    }
  }'
```

Note the absence of `--local`: on EMR the master comes from the cluster.

## Redshift bootstrap

Run `redshift/redshift-tables.sql` once against the cluster before the first DAG run,
connected to the database `redshift_default` will point at — the script creates no database
of its own (Redshift cannot switch databases mid-script), so `reporting_schema`, the two KPI
tables and their `tmp_` twins all land in whichever database you ran it against. The upsert
does `INSERT INTO <table> SELECT * FROM tmp_<table>`, so the two column lists must stay in the
same order — edit both halves together or not at all.

The upsert helper loads the DataFrame into `reporting_schema.tmp_<table>`, then runs one
`BEGIN; DELETE ... USING tmp ...; INSERT ...; TRUNCATE tmp; COMMIT;` block, rolling back on
failure. Delete keys:

| Table | Key columns |
|---|---|
| `segment_level_kpis` | `contact_date`, `sector`, `education` |
| `monthly_kpis` | `contact_date` |

Longest observed string values, for the `VARCHAR` widths: `sector` / `top_sector` is
`"Business owner"` (14 chars), `education` is `"professional.course"` (19).

## Airflow requirements

- Connection `redshift_default` — a Postgres-type connection pointing at the Redshift cluster
  (host, port 5439, schema/database, user, password). Used by
  `PostgresHook(postgres_conn_id="redshift_default")`.
- AWS credentials for boto3 and the EMR Serverless call — either the `aws_default` connection
  or, on MWAA, the environment's execution role. The role needs `s3:GetObject`, `s3:PutObject`,
  `s3:DeleteObject` and `s3:ListBucket` on the bucket, plus `emr-serverless:StartJobRun` and
  `emr-serverless:GetJobRun`.
- The bucket, prefixes, EMR application id and job-role ARN are module-level constants in the
  DAG, not Airflow Variables.
- Install `requirements.txt` on the scheduler. `airflow.operators.dummy` was removed in Airflow
  2.4, so the short-circuit branch uses `airflow.operators.empty.EmptyOperator` rather than the
  reference lab's `DummyOperator` import, which would break DAG parsing on a current install.

## KPIs produced

`segment_level_kpis` — one row per `(contact_date, sector, education)`; 741 rows on the full
file.

| # | Column | Type | Meaning |
|---|---|---|---|
| 1 | `contact_date` | `date` | first of the reconstructed contact month |
| 2 | `sector` | `string` | job mapped through the 11-row sector lookup |
| 3 | `education` | `string` | education level, `"unknown"` imputed to the mode |
| 4 | `contacts` | `bigint` | rows in the segment |
| 5 | `subscribed` | `bigint` | `sum(y)` — term deposits opened |
| 6 | `subscribe_rate` | `double` | `avg(y)` |
| 7 | `avg_age` | `double` | mean age |
| 8 | `avg_campaign` | `double` | mean of `campaign_capped`, so one 56-call outlier cannot move it |

`monthly_kpis` — one row per `contact_date`; 26 rows on the full file (May 2008 to Nov 2010).

| # | Column | Type | Meaning |
|---|---|---|---|
| 1 | `contact_date` | `date` | first of the contact month |
| 2 | `contacts` | `bigint` | rows in the month |
| 3 | `unique_jobs` | `bigint` | `countDistinct(job)` |
| 4 | `subscribed` | `bigint` | `sum(y)` |
| 5 | `subscribe_rate` | `double` | `avg(y)` |
| 6 | `avg_euribor3m` | `double` | mean 3-month Euribor |
| 7 | `top_sector` | `string` | most-contacted sector, argmax via `max(struct(n, sector))` |
| 8 | `pct_cellular` | `double` | `avg(contacted_by_cell)` |

The curated Parquet is not loaded into Redshift — it holds `VectorUDT` columns and stays on S3
as the model matrix.

## Repository layout

```
airflow-dag/dag-bank-marketing-cleaning.py   single-file DAG, module-level functions
data/bank-additional-sample.csv              2,000-row sample for a smoke run
local-development/                           the notebook this job was lifted from
redshift/redshift-tables.sql                 schema + tables + tmp_ twins
spark-jobs/bank_marketing_clean_prep.py      the EMR Serverless entrypoint
requirements.txt
```

## Notes and known gaps

Three things in the job are load-bearing and easy to break:

- `row_id` is stamped with `monotonically_increasing_id()` **immediately after the read**. A
  Spark DataFrame has no row order, the file is chronological, and the year reconstruction
  (`lag` + a running sum of month rollovers over `Window.orderBy("row_id")`) depends on it.
  Capture it after any shuffle and the order is gone for good.
- `dropDuplicates` must be passed `subset=DATA_COLS`. `row_id` is unique by construction, so a
  bare `dropDuplicates()` compares it too and removes 0 rows instead of the 12 real repeats.
- The two `localCheckpoint(eager=True)` calls — after the encoders and after the window stage —
  are not optimisations. Without them the plan grows past what Catalyst will re-analyse and the
  job fails.

Carried over from the notebook but deliberately **not** productionised: the bucketizers
(`age_band`, `campaign_quartile`, `econ_climate`), the `MinMaxScaler`, the
`edu_level`/`edu_detail`/`edu_digits` split columns, and the text-normalisation pass. None of
them feed the curated frame or either KPI, and the notebook verified the normalisation is a
no-op on this source (every string column is already trimmed and lower-cased). Add them back
only if a downstream model asks for them.
