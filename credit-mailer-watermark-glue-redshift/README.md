# credit-mailer-watermark-glue-redshift

An incremental ETL pipeline over a South African consumer-lender field experiment — 58,168 direct
mail offers sent in three waves in 2003, each at a **randomly assigned** interest rate — landing
in a Redshift star and ending in a contextual bandit.

The point of the project is the watermark, not the bandit:

> **The load state lives outside the code, one row per table, and the table that proves it is the
> one that CANNOT be loaded incrementally.**

A pipeline where every table has a timestamp does not need an externalised watermark; a hard-coded
`WHERE updated_at > ...` would do. This one has two source tables and only one of them has an
ordering column at all. The other is a CRM snapshot with no event time anywhere in it, so its
`load_column` is `None` and it is reloaded in full on every run — and that asymmetry is the entire
reason the configuration is a DynamoDB row rather than a constant.

Everything is an **AWS Glue Python Shell** job: the standard library, boto3 in the extractor and
numpy in the refinery. No Spark, no JVM, no `SparkSession` — and no pandas either, because a job
whose largest read is 58,168 rows of 7 columns has nothing to give a DataFrame to do. A Spark
cluster would spend longer starting than the whole chain takes to run.

## Architecture

```
 MySQL (RDS)                    AWS Glue Python Shell               Redshift
 credit_mailer                                                      db_credit_mailer
 ├── mail_offers        ──►  mysql-extraction.py            ──►  S3 raw_landing_zone/
 │     load_column=wave        reads the watermark from             credit_mailer_db/
 │     INCREMENTAL             DynamoDB, writes it back             <table>/data.csv
 └── client_attributes                                                    │
       load_column=None   ┌── DynamoDB ───────────────────┐               ▼
       FULL LOAD          │ incremental_load_configurations│    redshift-raw-ingestion.py
                          │  mail_offers        wave       │      COPY → tmp_<table>
                          │  client_attributes  None       │      MERGE → raw_zone.<table>
                          └────────────────────────────────┘               │
                                                                           ▼
                                                            redshift-processed-layer.py
                                                              dim_client · fact_mailer
                                                                           │
                                                      ══════════ THE FORK ══════════
                                                                           │
                                                              glue-refinery-path3.py
                                                              Steps 4–10, Path 3 only
                                                              Beta–Bernoulli · SNIPS
                                                                           ▼
                                                              bandit_posterior (54)
                                                              bandit_policy_value (6)
```

Step Functions chains the six job runs. It is a chain and not a `Parallel` state for a reason
given below: every table lands on one fixed S3 key that each run overwrites.

## Status


| File                                    | State                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------- |
| `glue-jobs/mysql-extraction.py`         | **done** — the watermark. Four-run demo verified, including the zero-row run |
| `glue-jobs/redshift-raw-ingestion.py`   | **done** — COPY + MERGE + TRUNCATE, two reference-lab bugs fixed             |
| `glue-jobs/redshift-processed-layer.py` | **done** — the star: `dim_client` and `fact_mailer`                          |
| `glue-jobs/glue-refinery-path3.py`      | **done** — Steps 4–10, reproduces the notebook's posterior exactly           |
| `glue-jobs/warehouse_common.py`         | **done** — connection, statement runner, the `--local` flags                 |
| `redshift/redshift-create-tables.sql`   | **done** — 9 tables, `dim_offer_arm` seeded with 18 arms                     |
| `local-development/build_source_db.py`  | **done** — the normalisation, and the staged local source                    |
| `local-development/apply_ddl.py`        | **done** — applies the deployed DDL to the local warehouse                   |
| `dynamodb/write-to-dynamo.py`           | **done** — seeds and resets the two config rows                              |
| `step-functions/step-functions.json`    | **done** — six states, one chain                                             |
| `refinery-walkthrough.ipynb`            | **done** — 58 cells, executed, the 13-section clean plus the fork            |
| `tests/`                                | **done** — 18 tests, no AWS, no network, no mocks                            |




## The data

Bertrand, Karlan, Mullainathan, Shafir & Zinman, *"What's Advertising Content Worth? Evidence from
a Consumer Credit Marketing Field Experiment"*, **Quarterly Journal of Economics 125(1), 2010**.
Harvard Dataverse (IPA Dataverse), `[doi:10.7910/DVN/II4HDS](https://doi.org/10.7910/DVN/II4HDS)`,
**CC0 1.0**.

A lender mailed former clients a pre-approved loan offer. The interest rate was randomised, and in
waves 2 and 3 the advertising layout and the offer deadline were randomised too. Because the
*price* was assigned at random, this is the rare observational-looking file that supports a
genuinely causal off-policy evaluation.

`data/adcontentworth_qje.tab.gz` is the whole deposit, committed:


|              |                                                                    |
| ------------ | ------------------------------------------------------------------ |
| uncompressed | 5,980,352 bytes, 58,169 lines (1 header + 58,168 rows), 37 columns |
| sha256       | `6341d87b24abd1753e39e1c71f51ec09ebdb785970ed515f432d6b1bc0affd28` |
| gzip -9      | 547,778 bytes                                                      |


At 535 KB there is no reason to commit a sample, so the project runs on the complete published
data straight after a clone. The two things that read it — `build_source_db.py` and the notebook —
verify that hash first. The Glue jobs never touch it; they read the source database.

**Downloading it yourself.** The deposit is CC0 and the file is not access-restricted, but it sits
behind a Dataverse guestbook, so the plain API returns HTTP 400. The file-level DDI is public and
needs no guestbook, which is enough to read the schema before downloading anything:

```bash
curl -s "https://dataverse.harvard.edu/api/access/datafile/2668657/metadata/ddi"
```

For the bytes, accept the guestbook once in a browser on the dataset page.

### The file reproduces the paper, exactly

Restricting to waves 2 and 3 gives the paper's analysis sample. This is the provenance check, and
it is cheap: if the columns meant something other than what the paper says they mean, these would
not line up. Section 10 of the notebook recomputes all of it.


| paper                                                                     | this file, waves 2–3                               |
| ------------------------------------------------------------------------- | -------------------------------------------------- |
| "direct mail solicitations to **53,194** former clients"                  | 53,194                                             |
| "sample mean interest rate of **793 basis points**"                       | 793                                                |
| "Rates varied from **3.25%** per month to **11.75%**"                     | 3.25 – 11.75                                       |
| "**97%** of the offers were at lower-than-standard rates"                 | 96.8%                                              |
| "an average discount of **3.1 percentage points**"                        | 3.07                                               |
| "**87%** of applications resulted in a loan"                              | 87.2%                                              |
| "the **4,000 or so** individuals that obtained a loan"                    | 3,944                                              |
| standard schedule low **7.75** / medium **9.75** / high **11.75** %/month | the maximum rate offered in each band, to the cent |


**Wave 1 is a pilot the paper excludes**, and the file says so from the other side. Its 4,974 rows
carry NULL in **14** of the 19 treatment flags — 69,636 null cells — and take **exactly one**
distinct treatment combination, against 9,667 across the whole file. It randomised price and
nothing else. The paper's footnote 26 calls it "a pilot wave of mailers that did not include the
content randomizations".

That wave is kept. It is a third of the watermark demo, it is where the framework's Step 4
OVERRIDE has something real to act on, and the price arms — the only thing this project's bandit
pulls — exist in it.

## Two tables out of one

The deposit is one wide table with no key. It holds two grains, and separating them is what gives
the pipeline its lesson:


| table               | grain                            | `load_column` | rows                          |
| ------------------- | -------------------------------- | ------------- | ----------------------------- |
| `mail_offers`       | one row per mailer               | `wave`        | grows 4,974 → 25,970 → 58,168 |
| `client_attributes` | CRM snapshot, 1:1 with the spine | `None`        | 58,168 on every run           |


31 event columns + 6 client columns = the 37 published columns, none dropped and none duplicated;
`client_id` is minted on top and appears in both tables as the join key. The split is asserted,
not assumed, in `build_source_db.py --self-check`.

`client_id` **is minted.** The deposit publishes no client identifier of any kind. `client_id` is
the **1-based row number of the published extract**, and it exists only to give the two tables a
join key. It is deterministic because the file it counts is fixed and published. It is not a lender
account number, it is not an ordering of anything, and nothing may be inferred from it.

**What was rejected:** minting a synthetic `created_at` so `client_attributes` "has to" be
`full_load`. That would be a fabricated timestamp on a snapshot that genuinely has no event time —
different in kind from a row number, which claims nothing about when anything happened. The table
is legitimately timestamp-free, which is the whole point of it.

### The 134 duplicate rows are kept

`mail_offers` has 134 exact duplicate rows across all 37 columns — 121 pairs, 5 triples and one
group of four. With no published client id, a repeated row is genuinely ambiguous: the same mailer
recorded twice, or two people who match on every published column.

The published sample size settles it. Dropping them would leave waves 2+3 at **53,175** against the
paper's **53,194** — and this file reproduces that figure to the row. They are two clients the
deposit cannot tell apart, and the minted `client_id` is what keeps them distinct.

## The watermark, and the run that proves it

Extraction semantics are the reference lab's, unchanged: `SELECT *`, plus
`WHERE <load_column> > '<last>' ORDER BY <load_column> DESC` when the load is incremental and a
stored value exists, then `last_extracted_value` is set from the first row of the descending
result.

The staging comes from **the source growing**, not from the extractor limiting — which is what
actually happened in 2003. `build_source_db.py --through-wave N` adds a wave to the source, and the
pipeline runs again:


| run | source holds | `mail_offers` extracts                     | watermark after |
| --- | ------------ | ------------------------------------------ | --------------- |
| 1   | wave 1       | **4,974** (no stored value → no predicate) | `1`             |
| 2   | waves 1–2    | **20,996** (`wave > '1'`)                  | `2`             |
| 3   | waves 1–3    | **32,198** (`wave > '2'`)                  | `3`             |
| 4   | waves 1–3    | **0** (`wave > '3'`)                       | `3`, unchanged  |


`client_attributes` ships all 58,168 rows on all four.

**Run 4 is the point.** It is the cheapest available proof that the stored value is being *read*
and not merely written. A job that ignored it would extract all 58,168 rows and look entirely
healthy doing it — same exit code, same S3 object, same duration to within a second. The only thing
that separates the two is the row count, and the only way to get zero is to have read what the
previous run left behind.

Three details that are traps rather than decisions:

- **The watermark is a string.** The reference lab writes `str(new_last_value)`, so the predicate
compares `'2' > '1'` lexically. That is correct for waves 1–3 and would break at wave 10, where
`'10' > '9'` is false. It is kept and named rather than quietly fixed, because the reference lab's
behaviour is the thing being imitated and a watermark that happens to be safe on a three-value
column is worth pointing at.
- **A zero-row extract writes a header-only CSV**, not an empty object. The reference lab writes
`""`. An empty object makes the downstream `COPY ... IGNOREHEADER 1` ambiguous and is
indistinguishable from a failed extract.
- `load_type='incremental'` **with** `load_column=None` **exits 1.** `client_attributes` is only ever
called with `full_load`, so reaching that branch means the state machine and the DynamoDB config
disagree about what the table is, and failing loudly is the right answer.

**The landing zone is one fixed key per table, overwritten every run.** That is the reference lab's
design and it is kept, but it has a consequence worth stating: a table's Redshift load must finish
before its next extract starts. The Step Functions definition is a sequential chain, which is what
makes that safe.

## The warehouse

`raw_zone` mirrors the published extract's column names **verbatim** — `offer4`, `edhi`, `waved3`,
`amountbrw_unc`, names nobody would choose — so a landed row can be diffed against the source file
character for character. `processed_zone` uses business names, and the MERGE column list is the
only place the translation happens.

```
raw_zone.mail_offers            32 cols   PK (client_id, wave)
raw_zone.client_attributes       7 cols   PK (client_id)
raw_zone.tmp_*                            CREATE TABLE AS SELECT * — column order cannot drift

processed_zone.dim_client       58,168 rows
processed_zone.dim_offer_arm        18 rows   seed data, not pipeline output
processed_zone.fact_mailer      58,168 rows   grain (client_id, wave)
processed_zone.bandit_posterior     54 rows   18 arms × 3 through-waves
processed_zone.bandit_policy_value   6 rows   2 eval waves × 3 bands
```

The largest single extract is `client_attributes` at 58,168 rows of 7 columns; `mail_offers`
peaks at 32,198 rows of 32. Either way it is about a megabyte of CSV, which is what makes the
reference lab's `fetchall()`-into-`StringIO`-into-one-`put_object` shape safe here.

`SMALLINT` and not `BOOLEAN` on the 0/1 flags, because 14 of them are genuinely NULL for all 4,974
wave-1 rows and that NULL is load-bearing — it is Step 4's latent state. No `IDENTITY` column
anywhere: `(client_id, wave)` is a natural key and a surrogate would only give the MERGE a second
thing to match on.

The 19 treatment flags sit on the fact rather than in a junk dimension. They take **9,667 distinct
combinations** over 58,168 rows — a dimension one sixth the size of its own fact is not a dimension.

### The arm grid is declared, never derived

Six price arms inside each of the three risk bands, cut points **declared as constants and seeded
by the DDL**:


| band   | cuts                               | the band's standard rate | highest rate its top arm holds |
| ------ | ---------------------------------- | ------------------------ | ------------------------------ |
| LOW    | 4.50 · 5.50 · 6.00 · 6.75 · 7.50   | 7.75                     | 11.75                          |
| MEDIUM | 5.00 · 6.75 · 7.50 · 8.25 · 9.25   | 9.75                     | 13.75                          |
| HIGH   | 5.50 · 7.50 · 9.00 · 10.00 · 11.00 | 11.75                    | 14.75                          |


**Every top arm is open above** — `rate_ceil` is NULL in `dim_offer_arm` — and it has to be. In
waves 2 and 3 the standard rate *is* the highest rate offered, so a closed ceiling there would be
harmless. Wave 1's pilot grid ran above it on 635 offers, and a closed ceiling would drop every
one of them on the join.

A grid recomputed per run as quantiles of the arriving data would make round 1's "arm 3" a
different price band from round 3's, so the posterior would accumulate counts across arms that are
not the same arm. This is the same argument the sibling `crypto-ticks-refinery-glue-dynamo` makes
for a declared bar calendar rather than an observed one.

Positivity holds: the smallest of the 54 (band, wave, arm) cells is **79 rows**. Arm shares within
a band run **0.0999 to 0.2786**, so the logging policy is genuinely non-uniform — which is the only
reason the importance weights below are ever anything but 1.

## Path 3

Steps 1–3 are path-blind — dedup, normalisation and the missingness flags — and they are carried
by the three jobs that fill the star rather than by any one of them: the split and the type
coercion happen on extract, the keyed MERGE is the dedup, and the NULLs reach `fact_mailer`
unfilled. None of those jobs prints a step verdict, because a row count wearing a framework badge
would read as a verdict about the data. `refinery-walkthrough.ipynb` is where Steps 1–3 are walked
with their badges. The fork then fires on the geometry of `y`:

- **Path 1** would need a money measure. `amountbrw_unc` is 0 on **92.47%** of rows — the absence of
a loan wearing a rand sign, not a continuous target.
- **Path 2** would be `badacct_last`. It is defined on only the **4,381** rows that got a loan, 517
positives, self-selected on a randomised price *and* through a loan-officer screen with no
unselected comparison group in the file.
- **Path 3** takes `tookup` against an action that was actually randomised.

`glue-refinery-path3.py` runs Steps 4–10 and prints a verdict for each. Three are forbidden, and
each ban names the engine that would break:


| Step             |              | Substance, measured on the run that prints it                                                                                                                                                                                                              |
| ---------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 4 Imputation     | **OVERRIDE** | native missingness kept as a latent state: 14 columns × 4,974 wave-1 rows, not filled, not dropped, not coalesced. NULL means "this arm did not exist yet", and every fill would assert that it did                                                        |
| 5 Diagnostics    | APPLIES      | exposure and reward for 54 cells; smallest is 79 rows (MEDIUM, wave 1, arm 3)                                                                                                                                                                              |
| 6 Topology       | **N/A**      | cyclical encoding needs a period. `wave` has 3 values and no period, and none of `fact_mailer`'s 28 columns is time-typed — read out of `information_schema` on the run, not asserted from the DDL                                                         |
| 7 Feature Eng    | APPLIES      | the interaction frame is the (band × arm) cell; the pairing *is* the model                                                                                                                                                                                 |
| 8 Pruning        | **BANNED**   | a one-hot arm played a fraction *p* of the time has variance *p*(1−*p*), so a variance threshold deletes arms in ascending order of exposure — the thinnest arm first, at a share of 0.0999, which is exactly the arm the posterior is least certain about |
| 9 Regularisation | **BANNED**   | L1 shrinks thin arms back onto the prior, and the distance between an arm's posterior and the prior *is* the explore signal                                                                                                                                |
| 10 Scaling       | **BANNED**   | mean-centring puts **22 of 36** Beta shape parameters at or below zero, where the density does not exist — numpy's sampler raises rather than degrading                                                                                                    |


**The engine.** Batched contextual Beta–Bernoulli Thompson sampling. Context = risk band, action =
price arm, reward = `took_up`, prior Beta(1, 1). One round per wave, because a round is exactly
what one incremental extract delivers.

**No γ discount**, although the framework's Path 3 carries γ = 0.97 and the sibling crypto-ticks
project uses it. Each client is solicited exactly once, so no arm ever receives a second reward
from the same unit and there is no non-stationarity for a discount to track. Applying one here
would only throw away wave 1.

**This is an off-policy replay of a completed cross-section — the regime SNIPS is built for.**
Every mailer was sent and its outcome recorded before any of this code existed, so the logged
assignment is fixed and the posterior is scored against it rather than steering it.

## What it found

Posterior mean take-up per arm, after all three waves:


| band   | arm 0       | arm 1       | arm 2   | arm 3   | arm 4   | arm 5   | argmax | best − worst |
| ------ | ----------- | ----------- | ------- | ------- | ------- | ------- | ------ | ------------ |
| HIGH   | **0.05762** | 0.05080     | 0.04998 | 0.04471 | 0.04580 | 0.04075 | arm 0  | **6.7 sd**   |
| MEDIUM | 0.15573     | **0.16546** | 0.15835 | 0.12863 | 0.15179 | 0.15299 | arm 1  | 3.3 sd       |
| LOW    | **0.17793** | 0.16865     | 0.16786 | 0.14031 | 0.15455 | 0.15030 | arm 0  | 3.6 sd       |


Off-policy evaluation — train on the earlier waves, evaluate on this one. The policy is stochastic
(π(a|x) = P(arm *a* wins a posterior draw)), the propensity is the realised arm frequency within
(band, eval wave), and the interval is a percentile bootstrap on the difference:


| eval wave | band     | n      | logging | SNIPS   | lift        | CI95 on the difference   | P(diff>0) |
| --------- | -------- | ------ | ------- | ------- | ----------- | ------------------------ | --------- |
| 2         | HIGH     | 15,662 | 0.04731 | 0.04944 | +4.50%      | [−0.00163, +0.00622]     | 0.858     |
| 2         | MEDIUM   | 1,911  | 0.13658 | 0.14152 | +3.62%      | [−0.01345, +0.02287]     | 0.691     |
| 2         | LOW      | 3,423  | 0.16973 | 0.17852 | +5.18%      | [−0.00435, +0.02313]     | 0.894     |
| 3         | **HIGH** | 24,845 | 0.04882 | 0.05457 | **+11.78%** | **[+0.00249, +0.00908]** | 1.000     |
| 3         | MEDIUM   | 3,590  | 0.15348 | 0.15727 | +2.47%      | [−0.01219, +0.01981]     | 0.696     |
| 3         | **LOW**  | 3,763  | 0.15865 | 0.14340 | **−9.62%**  | [−0.03289, +0.00233]     | 0.045     |


**One of six intervals clears zero**, and it is the band where the arms separate. HIGH risk carries
43,660 of the 58,168 mailers and its best and worst arms are 6.7 posterior standard deviations
apart, so the learned policy beats the lender's own randomisation by a margin the bootstrap can
see. Its price response also falls the way an economist would predict, from 5.76% on the cheapest
arm to 4.08% on the dearest — though not monotonically: arms 3 and 4 are inverted, 0.04471 against
0.04580, a gap of well under one posterior standard deviation of ~0.0026.

**One cell came out negative.** LOW risk on wave 3, −9.62%: the policy trained on waves 1–2 did
*worse* than randomising. The reason is in the posterior — LOW's top three arms sit at 0.1779,
0.1687 and 0.1679 against standard deviations near 0.010, under one standard deviation apart — and
its greedy arm moves 1 → 2 → 0 across the three rounds without ever settling. A confident policy
over arms that do not separate is an overconfident policy, and that is what it costs.

The remaining four cannot be distinguished from the logging policy at all. Reporting only the HIGH
number would be describing a different experiment.

**What none of this establishes.** The propensities are **estimated** from the logged assignment
frequencies, because the randomisation design document is not published — the paper says only that
a "target distribution of interest rates" was set per risk category. The rate was genuinely
randomised, so the estimate is unbiased, but SNIPS with an estimated propensity is not the
guarantee that SNIPS with a known one would be.

## Running it

Python 3.9+ and `pip install -r requirements.txt` — `duckdb>=1.4`, pandas, numpy, pytest. No AWS
account, no Docker, no Spark, no JVM.

```bash
python local-development/apply_ddl.py --drop
python dynamodb/write-to-dynamo.py --local --reset
```

Then one run of the pipeline, for each wave in turn:

```bash
python local-development/build_source_db.py --through-wave 1
python glue-jobs/mysql-extraction.py --local --table_name mail_offers --load_type incremental
python glue-jobs/redshift-raw-ingestion.py --local --table_name mail_offers
python glue-jobs/mysql-extraction.py --local --table_name client_attributes --load_type full_load
python glue-jobs/redshift-raw-ingestion.py --local --table_name client_attributes
python glue-jobs/redshift-processed-layer.py --local
```

Repeat with `--through-wave 2`, then `3`, then `3` again for the zero-row run, and finish with:

```bash
python glue-jobs/glue-refinery-path3.py --local
```

All four runs plus the refinery take **about 15 s** end to end on a laptop (14.6 s and 15.4 s on
two measured runs). Every job also has
`--self-check`, which pins its pure decisions with no database, no network and no files.

**DuckDB stands in for both MySQL and Redshift**, and it earns that because DuckDB 1.4 added
`MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED`. The MERGE is generated once from the column list
and the same string is sent to whichever engine is connected — so the local run rehearses the exact
statement the cluster would receive. The `COPY` is the one statement whose text differs, and that
is the right way round: a COPY that is wrong fails to load, while a MERGE that is wrong loads the
wrong thing.

`apply_ddl.py` runs `redshift/redshift-create-tables.sql` itself, skipping exactly one line
(`create database`, which DuckDB has no equivalent for). There is no second, translated schema
kept alongside the real one, because two schema files drift and they drift silently.

## What the local run covers

**Covered end to end on a laptop.** The four-run watermark demo including the zero-row run; the
split accounting for all 37 source columns; the MERGE text parsing and applying, being idempotent
on re-run, and no-opping on an empty staging table; the arm binner against all 18 declared bands
and both sides of every cut point; SNIPS against a hand-computed two-arm case; that the Thompson
and bootstrap generators do not share a stream; and every figure in the tables above, each of which
is printed by a run in this repository. Four numbers here are measured outside a run, and this
sentence is where they are declared: the gzip size (547,778 bytes), the file's line count (58,169),
the 9,667 distinct treatment combinations, and the ~15 s wall-clock. `refinery-walkthrough.ipynb`
reproduces the production job's posterior to five decimal places, independently.

**Two engine differences to carry into the deploy.** DuckDB accepts `VARCHAR(16)` without
enforcing the length, so the width headroom that keeps a Redshift `COPY` from rejecting a row is
a design decision here. And **Redshift's** `TRUNCATE` **commits the transaction it runs in**,
where DuckDB's does not — so the load is one atomic unit locally and three on the cluster,
argued at length in `redshift-raw-ingestion.py`.

## Deploying it

1. Run `redshift/redshift-create-tables.sql` — first statement, reconnect to `db_credit_mailer`,
  then the rest. Redshift cannot create a database and its contents from one session.
2. Create the DynamoDB table `incremental_load_configurations`, partition key `table_name`, and
  seed it with `python dynamodb/write-to-dynamo.py`.
3. Load the source database with `mysql/mysql-queries.sql`, whose `LOAD DATA` blocks are split by
  wave so the four runs above are reproducible on AWS too. Generate its CSVs with
   `python local-development/build_source_db.py --through-wave 3 --csv-out mysql/data`.
4. Upload the four job scripts in `glue-jobs/` to S3 and create one Glue **Python Shell** job
  each, plus `warehouse_common.py` alongside them. boto3 is preinstalled in the Python Shell
   runtime; nothing else is, and the two groups need different things.
  - `mysql-extraction` — `--additional-python-modules pymysql`. It does not import
  `warehouse_common`, so it needs no `--extra-py-files`.
  - the three warehouse jobs — `--extra-py-files s3://<bucket>/jobs/warehouse_common.py` and
  `--additional-python-modules redshift_connector`.
   `redshift-raw-ingestion` **requires** `--bucket` and `--iam-role` whenever `--local` is absent,
   because the COPY cannot be written without them. The state machine passes both as placeholders;
   set them there, or as the Glue job's default arguments.
5. Store the RDS and Redshift credentials in Secrets Manager as `credit_mailer_db` and
  `dwh-credentials`.
6. Create the state machine from `step-functions/step-functions.json`.

Every AWS value in these files — bucket, IAM role ARN, secret names, region — is a placeholder by
nature and must be set to your own.