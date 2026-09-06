# data_engineering_aws

AWS data-engineering projects. Each subfolder is a self-contained pipeline: the exploratory
notebook it grew out of, the production jobs, the orchestration DAG, and whatever the
destination store needs — Redshift DDL for one, a DynamoDB key schema for the other.

## Layout

Every project is a self-contained pipeline and they share a shape, so a reader who has seen one
can navigate the rest:

```
<project>/
├── airflow-dag/        the DAG that orchestrates the pipeline
├── <engine>-jobs/      the compute jobs: spark-jobs/ for EMR Serverless, glue-jobs/ for Glue
├── data/               a committed sample so the project runs after clone
├── the prototype notebook, with its executed outputs
└── README.md           architecture, deployment steps, and what the job produces
```

Beyond that each project carries only what its stack needs, and the differences are the point
rather than drift. `bank-marketing-cleaning-emr-redshift` adds `redshift/` for the warehouse DDL,
`local-development/` for its notebook and a `requirements.txt`.
`crypto-ticks-refinery-glue-dynamo` adds `tests/` and a `local-docker-development.sh` that runs
the whole chain inside AWS's own Glue 4.0 image, and keeps its notebook at the project root
because every path in it is written relative to there.
`credit-mailer-watermark-glue-redshift` has **no `airflow-dag/` at all** — it is orchestrated by
Step Functions, so `step-functions/` takes that slot — and adds `mysql/` for the source database
it extracts from and `dynamodb/` for the one control-plane table that holds its load state.

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

### `crypto-ticks-refinery-glue-dynamo`

A ten-step statistical refinery over Binance spot tick data — 340,971,834 trades across BTCUSDT,
ETHUSDT and SOLUSDT for January 2025, aggregated into 5-second OHLCV bars.

The point of the project is the refinery's structure rather than the bars. Steps 1–3 are a
shared, *path-blind* entryway, because they finish before the pipeline knows what kind of target
it is handling. The moment data leaves Step 3 the pipeline reads the geometry of `y` and forks
into three path-isolated sub-refineries that share step *numbers* and little else — and on the
bandit path three of the seven steps are **forbidden**, each ban existing because a named
downstream engine would break if the step ran.

```
S3 raw/  ->  Glue: glue-ingest-bars.py  ->  S3 curated/ bars  (1,607,040 x 18)
             Steps 1-3, path-blind                  |
                           ============== THE FORK ==============
                                     |              |              |
                              path1 continuous  path2 categorical  path3 bandit
                              next-bar return   up/flat/down       Thompson Sampling
                                     |              |              |
                                     +--------------+--------------+
                                                    |
                                  Glue Python shell: glue-dynamo.py  ->  DynamoDB
```

| Output | Grain | Contents |
| --- | --- | --- |
| `bars/` | symbol × 5s slot | 18 columns, decimal money, a declared calendar rather than an observed one |
| `path{1,2,3}/` | per path | the scaled feature matrix, Step 6's dependency topology, and the model or posterior ledger |
| DynamoDB | `<run-id>#<path>` / grain | 96 items — one per surviving feature, (feature, class) cell and bandit arm |

Verified run: 340,971,834 ticks to 1,607,040 bars in 6 min 56 s. The interesting problems were
again about arithmetic and determinism rather than plumbing:

- **`time` is epoch microseconds, not milliseconds.** Sixteen digits. Reading it as milliseconds
  returns a perfectly valid timestamp in the year 56971 and raises nothing. Binance moved spot
  trade archives to microsecond resolution for 2025 data, and the download page does not say so.
- **Money is `DecimalType`, never `Double`, because a Double pipeline is not idempotent.**
  Summing 2M real `quote_qty` values under 3, 11 and 29 shuffle partitions gives three different
  doubles and one identical decimal — float addition is not associative and Spark promises no
  stable merge order, so the same job over byte-identical input would emit different bars.
- **`open` and `close` come from `min_by`/`max_by` on `trade_id`**, not `first()`/`last()` and
  not an ordering on the timestamp. `first()`/`last()` over a `groupBy` return whatever reached
  the partition first, and the timestamp is only *non-decreasing*: 75% of ticks share a
  microsecond with their predecessor, up to 1,110 in a single microsecond, and 24,851 of those
  tie groups have a non-constant price. An `event_time` ordering leaves open and close genuinely
  undefined on a large fraction of bars.

Full detail, the per-path step tables and every verified figure are in the project README.

### `credit-mailer-watermark-glue-redshift`

Incremental ETL over a South African consumer-lender field experiment — 58,168 direct mail loan
offers sent in three waves in 2003, each at a randomly assigned interest rate, from
Bertrand et al.'s QJE 2010 replication deposit (CC0).

The lesson is the watermark, and what makes it a lesson is the asymmetry: the load state lives in
DynamoDB, one row per table, and one of the two source tables **cannot** be loaded incrementally.
A pipeline where every table has a timestamp would not need this — a hard-coded predicate would
do. `client_attributes` is a CRM snapshot with no event column anywhere in it, so its
`load_column` is `None` and it ships all 58,168 rows on every run.

```
MySQL  ->  Glue Python Shell  ->  S3 landing  ->  Redshift raw_zone  ->  processed_zone star
           (watermark in DynamoDB)                                              |
                                                                        Path 3: the bandit
```

Everything is a Glue **Python Shell** job — the standard library, boto3 and numpy, no pandas and
no Spark. The largest extract is 58,168 rows of 7 columns, and a cluster would spend longer
starting than the whole chain takes.

| Run | Source holds | `mail_offers` extracts | Watermark after |
| --- | --- | --- | --- |
| 1 | wave 1 | 4,974 | `1` |
| 2 | waves 1-2 | 20,996 | `2` |
| 3 | waves 1-3 | 32,198 | `3` |
| 4 | waves 1-3 | **0** | `3`, unchanged |

Run 4 is the point. A job that ignored the stored value would extract all 58,168 rows and look
healthy doing it — same exit code, same object, same duration. The row count is the only thing
that separates them.

The analytical half is Path 3 of the same ten-step framework, and only Path 3: the geometry of
`y` allows nothing else. `amountbrw_unc` is zero on 92.47% of rows, and `badacct_last` exists only
for the 4,381 people who got a loan. What the file does have is an action that was genuinely
randomised, which makes the off-policy evaluation causal rather than decorative.

- **The file reproduces the paper exactly.** Waves 2 and 3 are 53,194 rows, which is the paper's
  published N; mean rate 793 basis points against its "793"; 87.2% of applications became loans
  against its "87%"; and the maximum rate offered in each risk band is the lender's own standard
  schedule to the cent. That is the provenance check, and it is cheaper than trusting a filename.
- **One of six off-policy comparisons clears zero**, and it is the band where the arms separate:
  HIGH risk on wave 3, +11.78%, CI95 [+0.0025, +0.0091], with best and worst arms 6.7 posterior
  standard deviations apart. Take-up falls from 5.76% on the cheapest arm to 4.08% on the
  dearest, though not monotonically — arms 3 and 4 are inverted by well under one posterior sd.
- **One came out negative.** LOW risk on wave 3, -9.62% — the policy trained on the earlier waves
  did worse than the lender's own randomisation. LOW's top three arms are within one standard
  deviation of each other and its greedy arm never settles, moving 1 -> 2 -> 0 across the rounds.
  A confident policy over arms that do not separate is an overconfident one.

Full detail, the verified figures and the three banned steps are in the project README.


## Running anything here

Each project's README carries its own prerequisites. For the two Spark projects you need
Python 3.11, `pyspark==3.5.5`, and a Java 11 or 17 JDK on `JAVA_HOME` — Spark 3.5 does not support
Java 21+. `credit-mailer-watermark-glue-redshift` needs none of that: it is Python Shell
throughout, so `pip install -r requirements.txt` and a `duckdb>=1.4` is the whole prerequisite.
The Spark jobs take a `--local` flag that runs them against plain filesystem paths, so every
project can be exercised end to end without an AWS account.

`crypto-ticks-refinery-glue-dynamo` can also be run against the runtime it actually deploys to,
without installing any of the above:

```bash
cd crypto-ticks-refinery-glue-dynamo && ./local-docker-development.sh
```

That runs all five jobs inside `amazon/aws-glue-libs:glue_libs_4.0.0_image_01` — Spark 3.3.0,
Python 3.10, Java 8 — which is a stricter check than local Spark 3.5.5, because it is the API
surface the jobs are written against rather than a superset of it.

The AWS-side values in each DAG are account-specific and must be set to your own before
deploying: EMR Serverless application id, IAM execution role ARN, S3 bucket and the
`redshift_default` Airflow connection for the bank-marketing pipeline; S3 bucket, Glue job names
and the DynamoDB table for the crypto-ticks one. Each project README states exactly what its own
verification covers.
