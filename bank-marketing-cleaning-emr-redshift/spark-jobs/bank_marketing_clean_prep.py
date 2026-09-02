"""Bank marketing cleaning + preparation job.

EMR Serverless / Glue entrypoint. Productionises the thirteen stages of
``cleaning_and_prep_pipeline_spark.ipynb``:

  raw CSV (41,188 x 21)
      -> clean   (impute, de-duplicate, drop leakage, cap outliers)  -> 41,176 rows
      -> engineer(encode, derive, reconstruct the year, join sectors)
      -> curated Parquet (model matrix, keeps the ML vector columns)
      -> two flat KPI CSVs for the Redshift upsert

Pure PySpark: no pandas, no boto3. This runs on the cluster, not in the scheduler.

Local acceptance run::

    python spark-jobs/bank_marketing_clean_prep.py --local \\
        --input data/bank-additional-sample.csv \\
        --curated-output _localrun/curated \\
        --kpi-output _localrun/kpis
"""

import argparse
import logging

from pyspark.ml.feature import (OneHotEncoder, StandardScaler, StringIndexer,
                                VectorAssembler)
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOG = logging.getLogger("bank_marketing_clean_prep")

APP_NAME = "BankMarketingCleanPrep"

# An explicit schema skips the extra read pass inferSchema needs and fails loudly if the
# file shape changes. The dotted source names (emp.var.rate, cons.price.idx, cons.conf.idx,
# nr.employed) are declared here with underscores so nothing downstream needs backticks.
SCHEMA = StructType([
    StructField("age", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("marital", StringType(), True),
    StructField("education", StringType(), True),
    StructField("default", StringType(), True),
    StructField("housing", StringType(), True),
    StructField("loan", StringType(), True),
    StructField("contact", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("campaign", IntegerType(), True),
    StructField("pdays", IntegerType(), True),
    StructField("previous", IntegerType(), True),
    StructField("poutcome", StringType(), True),
    StructField("emp_var_rate", DoubleType(), True),
    StructField("cons_price_idx", DoubleType(), True),
    StructField("cons_conf_idx", DoubleType(), True),
    StructField("euribor3m", DoubleType(), True),
    StructField("nr_employed", DoubleType(), True),
    StructField("y", StringType(), True),
])

# The 21 real columns, i.e. everything except the row_id we add ourselves.
DATA_COLS = [f.name for f in SCHEMA.fields]

UNKNOWN = "unknown"
PDAYS_SENTINEL = 999          # "never previously contacted", 39,673 rows
# 'default' is deliberately absent: 8,597 unknowns (21%) is too many to impute, and the
# refusal to answer is itself signal, so it keeps 'unknown' as a level.
IMPUTE_COLS = ["job", "marital", "education", "housing", "loan"]

ENCODE_COLS = ["job", "marital", "education", "contact", "poutcome", "age_group"]
VECTOR_COLS = [f"{c}_vec" for c in ENCODE_COLS]

MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
BASE_YEAR = 2008              # the campaign starts in May 2008
EPOCH_START = "2008-05-01"    # months_elapsed is measured from the first contact month

EDU_YEARS = {"illiterate": 0, "basic.4y": 4, "basic.6y": 6, "basic.9y": 9,
             "high.school": 12, "professional.course": 13, "university.degree": 16}

SECTOR_ROWS = [("admin.", "White collar"), ("blue-collar", "Manual"),
               ("entrepreneur", "Business owner"), ("housemaid", "Service"),
               ("management", "White collar"), ("retired", "Not working"),
               ("self-employed", "Business owner"), ("services", "Service"),
               ("student", "Not working"), ("technician", "Technical"),
               ("unemployed", "Not working")]

NUMERIC_FEATURES = ["age", "campaign_capped", "previous", "contacts_total",
                    "emp_var_rate", "cons_price_idx", "cons_conf_idx",
                    "euribor3m", "nr_employed", "edu_years", "months_elapsed"]
FLAG_FEATURES = ["was_contacted_before", "contacted_by_cell", "known_customer",
                 "is_university", "is_q4", "prev_success_rate"]

SEGMENT_KPI_DIR = "segment_level_kpis"
MONTHLY_KPI_DIR = "monthly_kpis"


def build_session(local):
    """Local runs need a master and a sane shuffle width; EMR supplies both itself."""
    builder = SparkSession.builder.appName(APP_NAME)
    if local:
        # local[*] uses every core on the machine; the 200-partition default is far too
        # many for a 41k-row frame and turns every shuffle into scheduling overhead.
        builder = (builder.master("local[*]")
                          .config("spark.sql.shuffle.partitions", "8"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    LOG.info("Spark %s | parallelism %s | local=%s",
             spark.version, spark.sparkContext.defaultParallelism, local)
    return spark


def read_raw(spark, path):
    """Read the semicolon-delimited source and stamp file order onto every row."""
    raw = (spark.read
           .option("header", "true")
           .option("sep", ";")
           .schema(SCHEMA)
           .csv(path))

    # A Spark DataFrame has NO row order. The file is chronological and the year
    # reconstruction in engineer() depends on that fact, so capture it HERE -- immediately
    # after the read, before any filter, join or dropDuplicates can shuffle the rows.
    # After a shuffle the original order is unrecoverable.
    raw = raw.withColumn("row_id", func.monotonically_increasing_id()).cache()

    LOG.info("read %s rows x %s columns (+ row_id) from %s",
             raw.count(), len(DATA_COLS), path)
    return raw


def _mode_of(frame, column):
    """Spark has no .mode(): groupBy/count/orderBy and take the top row.

    The column name is the tie-breaker so the result is deterministic across runs.
    """
    return (frame.filter(func.col(column) != UNKNOWN)
                 .groupBy(column).count()
                 .orderBy(func.desc("count"), func.asc(column))
                 .first()[0])


def clean(df):
    """Target to 0/1, impute the disguised missings, de-duplicate, drop leakage, cap."""
    df = df.withColumn("y", func.when(func.col("y") == "yes", 1).otherwise(0))

    # Compute every mode against the same frame before rewriting any of them.
    modes = {c: _mode_of(df, c) for c in IMPUTE_COLS}
    for column, mode in modes.items():
        df = df.withColumn(column, func.when(func.col(column) == UNKNOWN, func.lit(mode))
                                       .otherwise(func.col(column)))
        LOG.info("imputed %s: '%s' -> '%s'", column, UNKNOWN, mode)

    # The 999 sentinel becomes a real null plus an explicit flag, so "never contacted"
    # stops being read as "contacted 999 days ago".
    df = (df.withColumn("was_contacted_before",
                        func.when(func.col("pdays") != PDAYS_SENTINEL, 1).otherwise(0))
            .withColumn("pdays",
                        func.when(func.col("pdays") == PDAYS_SENTINEL, None)
                            .otherwise(func.col("pdays"))))

    # Whitespace/case normalisation is a verified no-op on this source: every string column
    # is already trimmed and lower-cased, so there is nothing to repair here.

    rows_before = df.count()
    # subset=DATA_COLS is mandatory. row_id is unique by construction, so a bare
    # dropDuplicates() would compare it too, make every row its own group and remove 0 rows
    # instead of the 12 genuine repeats.
    df = df.dropDuplicates(subset=DATA_COLS)
    rows_after = df.count()
    LOG.info("de-duplicated: %s -> %s rows (%s removed)",
             rows_before, rows_after, rows_before - rows_after)

    # 'duration' is target leakage: the call length is only known once the call has ended,
    # by which point the outcome is known too. Dropping the column is the fix.
    df = df.drop("duration")

    # Cap rather than delete: least() is Series.clip(upper=...). The IQR fence lands on 6.
    q1, q3 = df.approxQuantile("campaign", [0.25, 0.75], 0.001)
    cap = q3 + 1.5 * (q3 - q1)
    df = df.withColumn("campaign_capped", func.least(func.col("campaign"), func.lit(cap)))
    LOG.info("capped 'campaign' at the IQR fence (%s) into 'campaign_capped'", cap)

    return df.cache()


def engineer(df):
    """Encode the categoricals, derive the features, rebuild the calendar, join sectors."""
    df = (df
          .withColumn("age_group", func.when(func.col("age") < 30, "young")
                                       .when(func.col("age") < 60, "middle")
                                       .otherwise("senior"))
          .withColumn("contacted_by_cell",
                      func.when(func.col("contact") == "cellular", 1).otherwise(0)))

    # StringIndexer: text -> frequency-ordered index. OneHotEncoder: index -> sparse
    # vector, dropLast=True being the drop_first=True of pandas.get_dummies.
    for column in ENCODE_COLS:
        df = (StringIndexer(inputCol=column, outputCol=f"{column}_idx",
                            handleInvalid="keep")
              .fit(df).transform(df))
    df = (OneHotEncoder(inputCols=[f"{c}_idx" for c in ENCODE_COLS],
                        outputCols=VECTOR_COLS, dropLast=True)
          .fit(df).transform(df))

    # Seven fitted models now sit in this plan. Materialise once, or Catalyst re-analyses
    # the whole tree on every later action and the job eventually dies on a plan dump.
    df = df.localCheckpoint(eager=True)

    years_map = func.create_map([func.lit(x) for kv in EDU_YEARS.items() for x in kv])
    df = (df
          .withColumn("contacts_total", func.col("campaign") + func.col("previous"))
          .withColumn("prev_success_rate",
                      func.when(func.col("previous") > 0,
                                func.when(func.col("poutcome") == "success", 1)
                                    .otherwise(0))
                          .otherwise(0))
          .withColumn("known_customer",
                      func.when(func.col("previous") > 0, 1).otherwise(0))
          .withColumn("is_university",
                      func.when(func.col("education").contains("university"), 1)
                          .otherwise(0))
          # regexp_extract on the education string leaves 70% of rows blank, so the years
          # come from the schooling map instead.
          .withColumn("edu_years", years_map[func.col("education")].cast("double")))

    # The source has a month name but no year. Rebuild it by counting the points where the
    # month number goes backwards, in file order.
    month_map = func.create_map([func.lit(x) for kv in MONTHS.items() for x in kv])
    df = df.withColumn("month_num", month_map[func.col("month")])

    # Order by the row_id captured at read time, NOT by a fresh monotonically_increasing_id()
    # which would describe the current post-shuffle order. A Window with no partitionBy puts
    # every row on one executor -- acceptable only because the recovery is inherently
    # sequential and this is a 41k-row frame.
    order = Window.orderBy("row_id")
    running = Window.orderBy("row_id").rowsBetween(Window.unboundedPreceding, 0)
    df = (df
          .withColumn("prev_month", func.lag("month_num").over(order))
          .withColumn("rollover",
                      func.when(func.col("month_num") < func.col("prev_month"), 1)
                          .otherwise(0))
          .withColumn("year", BASE_YEAR + func.sum("rollover").over(running))
          .withColumn("contact_date",
                      func.make_date(func.col("year"), func.col("month_num"),
                                     func.lit(1))))

    # Second break in the lineage, for the same reason as the first: the single-partition
    # window on top of the encoder stage is exactly where the plan becomes unmanageable.
    df = df.localCheckpoint(eager=True)

    df = (df
          .withColumn("quarter", func.quarter("contact_date"))
          .withColumn("is_q4",
                      func.when(func.quarter("contact_date") == 4, 1).otherwise(0))
          .withColumn("months_elapsed",
                      func.months_between(func.col("contact_date"),
                                          func.lit(EPOCH_START).cast("date")).cast("int")))

    # 11-row lookup: broadcast it so the join stays map-side.
    sectors = df.sparkSession.createDataFrame(SECTOR_ROWS, ["job", "sector"])
    rows_before = df.count()
    df = df.join(func.broadcast(sectors), on="job", how="left").cache()

    rows_after = df.count()
    unmatched = df.filter(func.col("sector").isNull()).count()
    LOG.info("sector join: %s -> %s rows, %s unmatched jobs",
             rows_before, rows_after, unmatched)
    if rows_after != rows_before:
        raise ValueError(f"sector join changed the row count: {rows_before} -> {rows_after}")

    assert df.filter(func.col("contact_date").isNull()).count() == 0, \
        "year reconstruction produced null contact_date -- row order was lost"
    return df


def model_matrix(df):
    """The curated, model-ready frame. Keeps the ML vectors, so it can only go to Parquet."""
    model_df = df.select(NUMERIC_FEATURES + FLAG_FEATURES + VECTOR_COLS + ["y"])

    assembled = (VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features",
                                 handleInvalid="skip")
                 .transform(model_df))
    # withMean=True is NOT the default: without it the output has unit variance but is not
    # centred, which quietly breaks anything assuming zero-mean input.
    scaled = (StandardScaler(inputCol="features", outputCol="features_std",
                             withMean=True, withStd=True)
              .fit(assembled).transform(assembled))

    return scaled.select(*NUMERIC_FEATURES, *FLAG_FEATURES, *VECTOR_COLS,
                         "features", "features_std", "y")


def segment_kpis(df):
    """One row per (contact_date, sector, education). Flat scalars only -- written as CSV."""
    return (df.groupBy("contact_date", "sector", "education")
              .agg(func.count("*").alias("contacts"),
                   func.sum("y").alias("subscribed"),
                   func.avg("y").alias("subscribe_rate"),
                   func.avg("age").alias("avg_age"),
                   # the capped column, so a single 56-call outlier cannot move the mean
                   func.avg("campaign_capped").alias("avg_campaign"))
              .orderBy("contact_date", "sector", "education"))


def monthly_kpis(df):
    """One row per contact_date. Flat scalars only -- written as CSV."""
    base = (df.groupBy("contact_date")
              .agg(func.count("*").alias("contacts"),
                   func.countDistinct("job").alias("unique_jobs"),
                   func.sum("y").alias("subscribed"),
                   func.avg("y").alias("subscribe_rate"),
                   func.avg("euribor3m").alias("avg_euribor3m"),
                   func.avg("contacted_by_cell").alias("pct_cellular")))

    # max() over a struct orders by its first field, so this is an argmax on the count
    # without a second shuffle. Projecting .sector back out keeps the column a plain string.
    top = (df.groupBy("contact_date", "sector")
             .agg(func.count("*").alias("n"))
             .groupBy("contact_date")
             .agg(func.max(func.struct("n", "sector")).alias("best"))
             .select("contact_date", func.col("best.sector").alias("top_sector")))

    return (base.join(top, on="contact_date", how="left")
                .select("contact_date", "contacts", "unique_jobs", "subscribed",
                        "subscribe_rate", "avg_euribor3m", "top_sector", "pct_cellular")
                .orderBy("contact_date"))


def write_kpi_csv(df, base_path, name):
    """coalesce(1) + header: the DAG reads these back with pandas for the Redshift upsert."""
    path = f"{base_path.rstrip('/')}/{name}"
    # Count BEFORE the write: counting the same frame afterwards is a second full
    # evaluation of the branch (both groupBys and the join, for monthly_kpis).
    rows = df.count()
    (df.coalesce(1).write.mode("overwrite")
       .option("header", "true").csv(path))
    LOG.info("wrote %s rows -> %s", rows, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", required=True,
                        help="raw bank-additional-full.csv (s3:// or a local path)")
    parser.add_argument("--curated-output", required=True,
                        help="destination for the curated Parquet")
    parser.add_argument("--kpi-output", required=True,
                        help="parent path for the two KPI CSV directories")
    parser.add_argument("--local", action="store_true",
                        help="run against the local filesystem with master local[*]")
    args = parser.parse_args()

    spark = build_session(args.local)
    try:
        raw = read_raw(spark, args.input)
        cleaned = clean(raw)
        enriched = engineer(cleaned)

        curated = model_matrix(enriched)
        # Same reason as write_kpi_csv: count first, or the StandardScaler transform runs
        # a second time just to log a number.
        curated_rows = curated.count()
        curated.write.mode("overwrite").parquet(args.curated_output)
        LOG.info("wrote %s rows x %s columns -> %s",
                 curated_rows, len(curated.columns), args.curated_output)

        write_kpi_csv(segment_kpis(enriched), args.kpi_output, SEGMENT_KPI_DIR)
        write_kpi_csv(monthly_kpis(enriched), args.kpi_output, MONTHLY_KPI_DIR)
        LOG.info("job complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
