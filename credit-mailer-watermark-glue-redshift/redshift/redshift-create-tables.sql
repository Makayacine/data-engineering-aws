-- =============================================================================================
-- credit-mailer-watermark-glue-redshift -- the warehouse schema, and the arm grid it seeds.
--
-- Two zones, and the rule that separates them:
--
--   raw_zone        mirrors the published extract's column names VERBATIM. `offer4`, `edhi`,
--                   `waved3`, `amountbrw_unc` -- names nobody would choose -- are kept exactly
--                   as the deposit spells them, so a reader can diff a raw_zone row against the
--                   source file and see character-for-character that nothing was renamed on the
--                   way in. A rename in the landing zone is a rename you cannot audit later,
--                   because by then the only surviving evidence of the old name is the loader.
--   processed_zone  uses business names. `offer4` becomes `offer_rate`, `edhi` becomes
--                   `is_more_educated`. This is the only place the translation happens, and it
--                   happens once, in a MERGE whose column list is the mapping.
--
-- Run order on Redshift. `create database` and the objects inside it cannot be created in one
-- session: Redshift resolves unqualified names against the database the connection is attached
-- to, so run the first statement, RECONNECT to db_credit_mailer, then run the rest. On the local
-- DuckDB stand-in there is no database-level statement at all -- the file is the database -- so
-- the first line is skipped there and everything from `create schema` down runs unchanged.
--
-- A note that belongs everywhere client_id appears, including here: CLIENT_ID IS MINTED. The
-- deposit publishes no client identifier of any kind. `client_id` is the 1-based row number of
-- the published extract and exists only to give the two source tables a join key. It is
-- deterministic and reproducible because the file it counts is fixed and published; it is not a
-- lender account number, it is not an account opening order, and nothing may be inferred from
-- its ordering.
-- =============================================================================================

create database db_credit_mailer;

-- ---------------------------------------------------------------------------------------------
-- RAW ZONE
-- ---------------------------------------------------------------------------------------------
create schema raw_zone;

-- Two tables, because the deposit is one wide table with no key and the two halves have
-- different grains. mail_offers is event grain -- one row per mailer, so a client who was mailed
-- in more than one wave has more than one row. client_attributes is a CRM snapshot, 1:1 with the
-- spine, and is structurally timestamp-free: a snapshot has no event time to be incremental on.
-- That is the whole reason this table exists in this shape. Its watermark config carries
-- load_column = NULL and it ships every one of the 58,168 rows on every run.

-- SMALLINT, NOT BOOLEAN, ON THE 0/1 FLAGS -- and this is the load-bearing decision in this file.
-- Fourteen of the treatment columns are 100% NULL on all 4,974 wave-1 rows, because wave 1 was a
-- price-only experiment and those arms did not exist yet. That NULL is data. It is Step 4's
-- latent state and it survives all the way into processed_zone.fact_mailer unfilled. BOOLEAN
-- would still hold NULL, but a CSV COPY into a BOOLEAN column has to be told what an empty field
-- means before it will load one, and being asked that question is how a NULL quietly becomes a
-- FALSE. SMALLINT is never asked the question: an empty CSV field is a NULL and a 0 is a 0.
CREATE TABLE raw_zone.mail_offers (
  client_id            BIGINT   NOT NULL,
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
  amountbrw_unc        INTEGER,
  badacct_last         SMALLINT,
  applied_2weeks       SMALLINT,
  tookup_after_short   SMALLINT,
  tookup_after_med     SMALLINT,
  tookup_after_long    SMALLINT,
  tookup_outside_only  SMALLINT,
  PRIMARY KEY (client_id, wave)
);

-- VARCHAR(16) ON TWO COLUMNS WHOSE MEASURED MAXIMA ARE 8 AND 6.
--   race  longest value is 'coloured', 8 characters
--   risk  longest value is 'MEDIUM',   6 characters
-- The width is headroom and the maxima are written down here so that the headroom is visible as
-- a decision rather than looking like a number somebody guessed. The reason it is worth paying
-- for: a Redshift COPY does not truncate an over-long value and does not skip the row, it fails
-- the entire load. Sizing a VARCHAR to today's maximum means one new category spelled
-- 'unspecified' takes the pipeline down, and takes it down at COPY time, in the landing zone,
-- where the failure looks like a loader bug rather than a source change.
CREATE TABLE raw_zone.client_attributes (
  client_id  BIGINT NOT NULL,
  race       VARCHAR(16),
  risk       VARCHAR(16),
  female     SMALLINT,
  edhi       SMALLINT,
  dormancy   SMALLINT,
  trcount    SMALLINT,
  PRIMARY KEY (client_id)
);

-- Staging copies, made by CTAS exactly as the reference lab makes them: the COPY lands here, the
-- MERGE reads from here, and the ingestion job truncates before the COPY as well as after the
-- MERGE. Two things about CTAS on Redshift that are easy to be wrong about, and neither hurts
-- here: it does not carry the PRIMARY KEY across, and Redshift does not enforce a PRIMARY KEY
-- anyway -- the constraint is planner metadata, and duplicate rows are kept out by the MERGE's
-- ON clause, not by the declaration above.
CREATE TABLE raw_zone.tmp_mail_offers       AS SELECT * FROM raw_zone.mail_offers;
CREATE TABLE raw_zone.tmp_client_attributes AS SELECT * FROM raw_zone.client_attributes;

-- ---------------------------------------------------------------------------------------------
-- PROCESSED ZONE
-- ---------------------------------------------------------------------------------------------
create schema processed_zone;

-- NO IDENTITY COLUMN ANYWHERE IN THIS SCHEMA, which is where this file departs from the
-- reference lab's fact table. The fact's grain is (client_id, wave) and that pair is a natural
-- key: a client is mailed at most once per wave. An IDENTITY surrogate would not replace that
-- key, it would sit beside it, and the MERGE would then have two candidate things to match on --
-- one of which is generated fresh on every insert and therefore matches nothing. That is the
-- shape of a MERGE that silently inserts a second copy of every row on the second run.
CREATE TABLE processed_zone.dim_client (
  client_id         BIGINT PRIMARY KEY,
  risk_band         VARCHAR(16),
  race              VARCHAR(16),
  is_female         BOOLEAN,
  is_more_educated  BOOLEAN,
  months_dormant    SMALLINT,
  prior_loans       SMALLINT
);

-- THIS TABLE IS SEED DATA, NOT PIPELINE OUTPUT. It is declared and populated by this file, and
-- no job writes to it. The processed-layer job asserts it holds 18 rows and fails if it does
-- not, because an empty dim_offer_arm would drop every fact row on an inner join and produce a
-- successful run over nothing.
--
-- Why the cut points are declared constants and never quantiles of the arriving data: the
-- posterior in bandit_posterior accumulates (pulls, rewards) per arm ACROSS waves. If the grid
-- were recomputed per run from whatever had arrived, round 1's "arm 3" would be a different
-- price band from round 3's "arm 3", and the accumulation would be adding counts from two
-- different arms into one row while every column still looked plausible. This is the same
-- argument as the declared bar calendar in crypto-ticks-refinery-glue-dynamo, and it is the
-- precedent being followed rather than a fresh idea.
--
-- An arm is  rate_floor <= offer_rate < rate_ceil,  with the top arm of each band open above.
-- arm_id = base + arm_index + 1, base 0 for HIGH, 6 for MEDIUM, 12 for LOW.
CREATE TABLE processed_zone.dim_offer_arm (
  arm_id      SMALLINT PRIMARY KEY,
  risk_band   VARCHAR(16)  NOT NULL,
  arm_index   SMALLINT     NOT NULL,
  rate_floor  DECIMAL(5,2) NOT NULL,
  rate_ceil   DECIMAL(5,2),            -- NULL on the top arm: open above
  arm_label   VARCHAR(32)  NOT NULL
);

-- The bottom arm of each band is floored at 0.00 rather than at the lowest rate the source
-- happens to contain. rate_floor is NOT NULL, so the bottom arm needs a number; anchoring it to
-- an observed minimum would leave the grid with a gap underneath it, and a mailer priced below
-- that minimum would then match no arm at all. The declared floors are the five cut points per
-- band; 0.00 is the closed end of a band that is conceptually open below.
INSERT INTO processed_zone.dim_offer_arm
  (arm_id, risk_band, arm_index, rate_floor, rate_ceil, arm_label)
VALUES
  ( 1, 'HIGH',   0,  0.00,  5.50, 'HIGH 0 [0.00, 5.50)'),
  ( 2, 'HIGH',   1,  5.50,  7.50, 'HIGH 1 [5.50, 7.50)'),
  ( 3, 'HIGH',   2,  7.50,  9.00, 'HIGH 2 [7.50, 9.00)'),
  ( 4, 'HIGH',   3,  9.00, 10.00, 'HIGH 3 [9.00, 10.00)'),
  ( 5, 'HIGH',   4, 10.00, 11.00, 'HIGH 4 [10.00, 11.00)'),
  ( 6, 'HIGH',   5, 11.00,  NULL, 'HIGH 5 [11.00, +inf)'),
  ( 7, 'MEDIUM', 0,  0.00,  5.00, 'MEDIUM 0 [0.00, 5.00)'),
  ( 8, 'MEDIUM', 1,  5.00,  6.75, 'MEDIUM 1 [5.00, 6.75)'),
  ( 9, 'MEDIUM', 2,  6.75,  7.50, 'MEDIUM 2 [6.75, 7.50)'),
  (10, 'MEDIUM', 3,  7.50,  8.25, 'MEDIUM 3 [7.50, 8.25)'),
  (11, 'MEDIUM', 4,  8.25,  9.25, 'MEDIUM 4 [8.25, 9.25)'),
  (12, 'MEDIUM', 5,  9.25,  NULL, 'MEDIUM 5 [9.25, +inf)'),
  (13, 'LOW',    0,  0.00,  4.50, 'LOW 0 [0.00, 4.50)'),
  (14, 'LOW',    1,  4.50,  5.50, 'LOW 1 [4.50, 5.50)'),
  (15, 'LOW',    2,  5.50,  6.00, 'LOW 2 [5.50, 6.00)'),
  (16, 'LOW',    3,  6.00,  6.75, 'LOW 3 [6.00, 6.75)'),
  (17, 'LOW',    4,  6.75,  7.50, 'LOW 4 [6.75, 7.50)'),
  (18, 'LOW',    5,  7.50,  NULL, 'LOW 5 [7.50, +inf)');

-- risk_band is denormalised onto the fact even though dim_client already carries it. It is the
-- bandit's context, and the context is part of the arm's identity -- (band, arm_index) is what
-- the posterior is keyed on. Carrying it here means the refinery reads one table, and means a
-- later correction to a client's risk grade in dim_client cannot retroactively move a mailer
-- into a different arm than the one it was actually priced under.
--
-- bad_account is NULL unless took_up = 1, and is never coalesced to 0. A client who was never
-- lent to has no repayment status; a 0 would claim they repaid. In the source, badacct_last is
-- non-null on exactly the 4,381 rows where tookup = 1.
CREATE TABLE processed_zone.fact_mailer (
  client_id            BIGINT   NOT NULL,
  wave                 SMALLINT NOT NULL,
  arm_id               SMALLINT NOT NULL,
  risk_band            VARCHAR(16) NOT NULL,   -- the bandit's context, denormalised
  offer_rate           DECIMAL(5,2) NOT NULL,
  applied              SMALLINT NOT NULL,
  took_up              SMALLINT NOT NULL,
  amount_borrowed      INTEGER  NOT NULL,
  bad_account          SMALLINT,               -- NULL unless took_up = 1
  -- the treatment flags, carried with their native missingness (Step 4)
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
  PRIMARY KEY (client_id, wave)
);

-- The refinery's two output tables. through_wave and eval_wave are ordinal batch indices, not
-- dates: this pipeline's notion of time is the mailer wave number, and there is no event
-- timestamp anywhere in the deposit to make it anything else.
--
-- alpha and beta are DECIMAL(12,2) rather than integers because they are Beta parameters, which
-- start at the Beta(1,1) prior and are only integral because every update here adds whole
-- counts. Storing them as INTEGER would be storing today's arithmetic rather than the quantity.
CREATE TABLE processed_zone.bandit_posterior (
  run_id        VARCHAR(32)  NOT NULL,
  through_wave  SMALLINT     NOT NULL,
  arm_id        SMALLINT     NOT NULL,
  risk_band     VARCHAR(16)  NOT NULL,
  arm_index     SMALLINT     NOT NULL,
  pulls         INTEGER      NOT NULL,
  rewards       INTEGER      NOT NULL,
  alpha         DECIMAL(12,2) NOT NULL,
  beta          DECIMAL(12,2) NOT NULL,
  posterior_mean DECIMAL(10,8) NOT NULL,
  posterior_sd   DECIMAL(10,8) NOT NULL,
  ts_probability DECIMAL(10,8) NOT NULL,   -- P(this arm wins a Thompson draw)
  PRIMARY KEY (run_id, through_wave, arm_id)
);

-- diff, ci_low and ci_high are signed: the evaluated policy is allowed to come out WORSE than
-- the mailer's own randomisation, and when it does that is a result, not a fault to be clamped.
CREATE TABLE processed_zone.bandit_policy_value (
  run_id           VARCHAR(32) NOT NULL,
  eval_wave        SMALLINT    NOT NULL,
  risk_band        VARCHAR(16) NOT NULL,
  n_rows           INTEGER     NOT NULL,
  logging_value    DECIMAL(10,8) NOT NULL,
  snips_value      DECIMAL(10,8) NOT NULL,
  diff             DECIMAL(10,8) NOT NULL,
  ci_low           DECIMAL(10,8) NOT NULL,
  ci_high          DECIMAL(10,8) NOT NULL,
  p_diff_positive  DECIMAL(10,8) NOT NULL,
  PRIMARY KEY (run_id, eval_wave, risk_band)
);

-- ---------------------------------------------------------------------------------------------
-- Seed check. Run this after the INSERT above; it is the same count the processed-layer job
-- asserts before it merges the fact.
--   expected:  18 arms, 3 bands, 6 arms per band, and exactly 3 open-topped arms.
-- ---------------------------------------------------------------------------------------------
SELECT risk_band,
       count(*)                                           AS arms,
       sum(CASE WHEN rate_ceil IS NULL THEN 1 ELSE 0 END) AS open_topped,
       min(rate_floor)                                    AS lowest_floor,
       max(rate_floor)                                    AS highest_floor
FROM processed_zone.dim_offer_arm
GROUP BY risk_band
ORDER BY risk_band;
