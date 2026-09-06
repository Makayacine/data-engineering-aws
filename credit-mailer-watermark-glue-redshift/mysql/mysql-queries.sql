-- ---------------------------------------------------------------------------------------------
-- credit_mailer -- the source database the Glue extraction job reads.
--
-- This is the AWS-side twin of the DuckDB file that local-development/build_source_db.py
-- builds. Same two tables, same column order, same CSVs; a different engine. Run the client
-- from the project root, because every path below is repo-relative:
--
--     python local-development/build_source_db.py --through-wave 3 --csv-out mysql/data
--     mysql --local-infile=1 -h {mysql-endpoint} -P 3306 -u admin -p
--
-- --local-infile=1 is on the client line for a reason worth knowing before it costs an hour:
-- LOAD DATA LOCAL INFILE needs the capability enabled at BOTH ends. The client flag above is
-- one end. The other is the server variable local_infile, which is OFF by default in MySQL 8
-- and, on RDS, is set in the parameter group rather than by SET GLOBAL. Without it the load
-- fails with "Loading local data is disabled", which names neither end.
-- ---------------------------------------------------------------------------------------------

create database credit_mailer;

use credit_mailer;


-- ---------------------------------------------------------------------------------------------
-- THE TWO TABLES, AND WHY THE WIDE DEPOSIT IS NOT ONE TABLE
--
-- The published extract is one rectangle of 37 columns with no key. It is cut at its two real
-- grains, because the pipeline's subject is a per-table incremental watermark and a watermark
-- is only demonstrated by putting it next to a table that cannot have one:
--
--   mail_offers        event grain, one row per mailer per wave. Its load column is `wave`.
--   client_attributes  a CRM snapshot, 1:1 with the mailing list. It has no event time,
--                      because a snapshot does not have one, so its load column is NULL and
--                      every run of the pipeline ships all of it.
--
-- client_id IS MINTED. The deposit publishes no client identifier of any kind. client_id is the
-- 1-based row number of the published extract and exists only to give these two tables a join
-- key. It is deterministic -- the file is byte-verified by sha256 before the split -- but it is
-- not a lender account number, and nothing may be read into its ordering.
--
-- COLUMN ORDER IS PART OF THE CONTRACT. LOAD DATA assigns fields by POSITION. The CSV carries a
-- header and IGNORE 1 ROWS throws it away rather than using it to map columns, so if the order
-- below ever stops matching MAIL_OFFERS_COLUMNS in build_source_db.py, every column loads into
-- its neighbour and MySQL reports nothing worse than a truncation warning.
--
-- SMALLINT and not BOOLEAN on the 0/1 flags. Fourteen treatment columns are NULL for every
-- wave-1 row, because wave 1 was a price-only experiment and those arms did not exist yet. That
-- NULL is a fact about the experiment and is carried all the way to the fact table. A nullable
-- SMALLINT says "unknown"; a BOOLEAN column loaded from a CSV has to be told what an empty
-- field means, and every available answer is a claim the data does not make.
--
-- VARCHAR(16) on race and risk, whose measured maxima are 8 ("coloured") and 6 ("MEDIUM"). The
-- width is headroom, and it is headroom for Redshift rather than for MySQL: a Redshift COPY
-- fails the entire load on one value a byte too long, and these two columns keep their width
-- unchanged from here to the raw zone so that the two schemas can be diffed.
-- ---------------------------------------------------------------------------------------------

CREATE TABLE mail_offers (
    client_id            BIGINT NOT NULL,
    wave                 SMALLINT NOT NULL,
    offer4               DECIMAL(5,2),
    prize                SMALLINT,
    intshown             SMALLINT,
    dphoto_female        SMALLINT,
    dphoto_none          SMALLINT,
    dphoto_black         SMALLINT,
    gender_match         SMALLINT,
    race_match           SMALLINT,
    nspeakeligible       SMALLINT,
    speak_trt            SMALLINT,
    oneln_trt            SMALLINT,
    comploss_n           SMALLINT,
    use_any              SMALLINT,
    stripany             SMALLINT,
    comp_n               SMALLINT,
    deadlinemed          SMALLINT,
    deadlinelong         SMALLINT,
    deadlong_elig        SMALLINT,
    deadshort_elig       SMALLINT,
    deadlineshortext     SMALLINT,
    waved3               SMALLINT,
    applied              SMALLINT,
    tookup               SMALLINT,
    amountbrw_unc        INT,
    badacct_last         SMALLINT,
    applied_2weeks       SMALLINT,
    tookup_after_short   SMALLINT,
    tookup_after_med     SMALLINT,
    tookup_after_long    SMALLINT,
    tookup_outside_only  SMALLINT,
    PRIMARY KEY (client_id, wave)
);

CREATE TABLE client_attributes (
    client_id  BIGINT NOT NULL,
    race       VARCHAR(16),
    risk       VARCHAR(16),
    female     SMALLINT,
    edhi       SMALLINT,
    dormancy   SMALLINT,
    trcount    SMALLINT,
    PRIMARY KEY (client_id)
);


-- ---------------------------------------------------------------------------------------------
-- THE SNAPSHOT. LOADED ONCE, IN FULL, AND NEVER STAGED.
--
-- 58,168 rows -- one per client on the mailing list, all of them, on the first load and for
-- every run afterwards. This table is what makes full_load mean something: it has no load
-- column, so there is nothing for a watermark to filter on, and the extraction job ships the
-- whole table on all four runs. That is the control against which the mailer table's four
-- decreasing extracts are read.
--
-- \N is the NULL token. It is not decoration: MySQL loads an EMPTY field into a numeric column
-- as 0 and raises a warning, and a warning is not an error, so a CSV using empty-for-null would
-- load cleanly and turn 298 clients of unrecorded race into clients of race "". The CSVs are
-- written with \N by build_source_db.py for exactly this reason.
-- ---------------------------------------------------------------------------------------------

LOAD DATA LOCAL INFILE "mysql/data/client_attributes.csv" INTO TABLE credit_mailer.client_attributes FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


-- ---------------------------------------------------------------------------------------------
-- THE MAILER TABLE, ONE WAVE AT A TIME.
--
-- Three separate loads, and this is the whole staging mechanism. The pipeline's four runs are
-- not produced by an extractor that limits itself -- the extractor always asks for everything.
-- They are produced by the SOURCE growing, which is what actually happened in 2003: the lender
-- posted wave 1, then wave 2, then wave 3.
--
--   after block 1  the source holds wave 1        run 1 extracts 4,974 rows, watermark -> 1
--   after block 2  the source holds waves 1-2     run 2 extracts 20,996 rows, watermark -> 2
--   after block 3  the source holds waves 1-3     run 3 extracts 32,198 rows, watermark -> 3
--   no fourth block                               run 4 extracts 0 rows, watermark stays 3
--
-- Run 4 is the point of the exercise and it is why nothing is loaded before it: an empty
-- extract is the cheapest possible proof that the watermark is being read rather than ignored.
-- A pipeline whose extract shrinks each round could be doing that for a dozen reasons; one that
-- then takes nothing at all, and leaves the stored watermark untouched, is doing exactly one.
--
-- So: run the pipeline between these blocks, not after all three.
-- ---------------------------------------------------------------------------------------------

-- Wave 1 -- 4,974 rows. The price-only experiment: the 14 treatment columns that the later
-- waves randomise are NULL on every one of these rows, and they must arrive as NULL.
LOAD DATA LOCAL INFILE "mysql/data/mail_offers_wave1.csv" INTO TABLE credit_mailer.mail_offers FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Wave 2 -- 20,996 rows. Advertising layout and offer deadline are randomised from here on.
LOAD DATA LOCAL INFILE "mysql/data/mail_offers_wave2.csv" INTO TABLE credit_mailer.mail_offers FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Wave 3 -- 32,198 rows. The source is now complete at 58,168 mailers.
LOAD DATA LOCAL INFILE "mysql/data/mail_offers_wave3.csv" INTO TABLE credit_mailer.mail_offers FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


-- ---------------------------------------------------------------------------------------------
-- Check a load rather than assume it. SHOW WARNINGS is worth reading after each block above:
-- LOAD DATA reports type coercions as warnings and still returns success, so a run that
-- silently turned every NULL into a 0 looks identical to a clean one in the client's output.
--
-- Expected after all three mailer blocks: 4,974 / 20,996 / 32,198, and 4,974 NULL prize values,
-- all of them in wave 1.
-- ---------------------------------------------------------------------------------------------

SELECT wave, count(*) AS rows_loaded, sum(prize IS NULL) AS null_prize
FROM credit_mailer.mail_offers
GROUP BY wave
ORDER BY wave;

SELECT count(*) AS rows_loaded, count(DISTINCT client_id) AS distinct_clients
FROM credit_mailer.client_attributes;
