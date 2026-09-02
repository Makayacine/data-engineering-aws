-- Bootstrap for the bank-marketing KPI warehouse.
-- Run once, connected to the cluster's default database, before the DAG's first run.
-- Everything lives in that database: Redshift cannot switch databases mid-script, so
-- redshift_default must point at the SAME database this script was run against.
--
-- Column ORDER here must match the KPI CSVs the Spark job writes, because the DAG's
-- upsert runs `INSERT INTO <table> SELECT * FROM tmp_<table>` with no column list.
-- Each table therefore has an identical tmp_ twin.

create schema reporting_schema;

-- segment_level_kpis: one row per (contact_date, sector, education).
-- 741 rows for the whole 2008-2010 campaign, so DISTSTYLE ALL -- a full copy on every
-- node is cheaper than any distribution key and makes the upsert's DELETE ... USING
-- join, and every join back to monthly_kpis, node-local.
-- SORTKEY is the upsert's id_columns in order, so the DELETE and the usual
-- "one month, one sector" query both hit a narrow zone-map range.
-- VARCHAR widths are sized to the data: longest sector 'Business owner' (14),
-- longest education 'professional.course' (19).
CREATE TABLE reporting_schema.segment_level_kpis (
    contact_date DATE NOT NULL,
    sector VARCHAR(32) NOT NULL,
    education VARCHAR(32) NOT NULL,
    contacts BIGINT,
    subscribed BIGINT,
    subscribe_rate FLOAT,
    avg_age FLOAT,
    avg_campaign FLOAT
)
DISTSTYLE ALL
SORTKEY (contact_date, sector, education);

CREATE TABLE reporting_schema.tmp_segment_level_kpis (
    contact_date DATE NOT NULL,
    sector VARCHAR(32) NOT NULL,
    education VARCHAR(32) NOT NULL,
    contacts BIGINT,
    subscribed BIGINT,
    subscribe_rate FLOAT,
    avg_age FLOAT,
    avg_campaign FLOAT
)
DISTSTYLE ALL
SORTKEY (contact_date, sector, education);

-- monthly_kpis: one row per contact_date, 26 campaign months.
-- Same reasoning as above, only more so at 26 rows. top_sector stays nullable: it comes
-- from a left join in the Spark job.
CREATE TABLE reporting_schema.monthly_kpis (
    contact_date DATE NOT NULL,
    contacts BIGINT,
    unique_jobs BIGINT,
    subscribed BIGINT,
    subscribe_rate FLOAT,
    avg_euribor3m FLOAT,
    top_sector VARCHAR(32),
    pct_cellular FLOAT
)
DISTSTYLE ALL
SORTKEY (contact_date);

CREATE TABLE reporting_schema.tmp_monthly_kpis (
    contact_date DATE NOT NULL,
    contacts BIGINT,
    unique_jobs BIGINT,
    subscribed BIGINT,
    subscribe_rate FLOAT,
    avg_euribor3m FLOAT,
    top_sector VARCHAR(32),
    pct_cellular FLOAT
)
DISTSTYLE ALL
SORTKEY (contact_date);
