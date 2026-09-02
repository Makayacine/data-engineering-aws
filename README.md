# data_engineering_aws

AWS data-engineering projects. Each subfolder is a self-contained pipeline: the exploratory
notebook it grew out of, the production job, the orchestration DAG, and the warehouse DDL.

## Layout

Every project follows the same shape, so a reader who has seen one can navigate the rest:

```
<project>/
├── airflow-dag/        the DAG that orchestrates the pipeline
├── spark-jobs/         the compute job submitted to EMR Serverless or Glue
├── redshift/           database, schema and table DDL
├── local-development/  the notebook the logic was prototyped in
├── data/               a committed sample so the project runs after clone
├── requirements.txt
└── README.md           architecture, deployment steps, and what the job produces
```

The notebook is kept deliberately. It is where the logic was worked out, and it carries the
executed outputs, so the production job can be diffed against something that is known to be
correct rather than against a description of it.

## Projects

### `bank-marketing-cleaning-emr-redshift`

Cleaning and feature-preparation pipeline for the UCI Bank Marketing dataset — 41,188 rows of
Portuguese bank term-deposit campaign records, May 2008 to November 2010.

A PySpark job cleans the raw CSV and engineers features, writing a curated model-ready Parquet
frame to S3 and two flat KPI CSVs alongside it. Airflow validates the raw file, submits the job
to EMR Serverless, upserts the KPIs into Redshift through a `tmp_` staging table, and archives
the processed object.

```
S3 raw/  ->  EMR Serverless (PySpark)  ->  S3 curated/  (Parquet, keeps the ML vectors)
                                       ->  S3 kpis/     (CSV)  ->  Redshift reporting_schema
```

| Output | Grain | Contents |
| --- | --- | --- |
| curated Parquet | one row per contact | 26 columns, 8 of them `VectorUDT` feature vectors |
| `segment_level_kpis` | date × sector × education | contacts, subscribed, subscribe rate, avg age, avg campaign |
| `monthly_kpis` | date | contacts, unique jobs, subscribed, subscribe rate, avg euribor, top sector, % cellular |

The interesting problems in it were Spark-specific rather than pipeline-specific:

- **Row order has to be captured before it is needed.** The source file has `month` but no year.
  The year is recoverable by counting month rollovers in file order — but a Spark DataFrame has
  no row order, and by the time the reconstruction runs the frame has been through a shuffle.
  `monotonically_increasing_id()` called at that point numbers rows in *post-shuffle* order and
  produces years from 2013 to 2027. The id has to be stamped immediately after the read.
- **A surrogate key silently disables `dropDuplicates()`.** Once `row_id` exists, a bare
  `dropDuplicates()` compares it too, finds every row unique, and removes nothing. It needs an
  explicit `subset` of the real columns.
- **Plan depth fails before data volume does.** Fifty chained transformations plus six fitted ML
  models grew the logical plan past what Catalyst would re-analyse, and the job died with a
  several-hundred-line plan dump that pointed at the wrong stage. Two `localCheckpoint(eager=True)`
  calls fix it; the pipeline itself runs in well under a minute.

Full detail, deployment steps and the verified run figures are in the project README.

## Running anything here

Each project's README carries its own prerequisites. In general you need Python 3.11,
`pyspark==3.5.5`, and a Java 11 or 17 JDK on `JAVA_HOME` — Spark 3.5 does not support Java 21+.
The Spark jobs take a `--local` flag that runs them against plain filesystem paths, so every
project can be exercised end to end without an AWS account.

The AWS-side values in each DAG — EMR Serverless application id, IAM execution role ARN, S3
bucket, and the `redshift_default` Airflow connection — are account-specific and must be set to
your own before deploying.
