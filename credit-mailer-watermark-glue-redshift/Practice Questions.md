**Question 1**: How do you prove a stored watermark is being read rather than merely written?

**Before Execution** (`[run 3 finished]` | `[_localrun/watermark.json]`):

```text
landing object after run 3: 2,268,647 bytes, 32,199 lines
watermark.json: {"table_name": "mail_offers", "load_column": "wave", "last_extracted_value": "3"}

```

**After Execution** (`[run 4, source unchanged]` | `[same object, same file]`):

```text
run 4  source holds waves 1-3  sql: SELECT * FROM mail_offers WHERE wave > '3' ORDER BY wave DESC
                                   0 rows, 32 columns   watermark stays at '3'

the same run with the stored value ignored (--load_type full_load, dry):
  58168 rows, 32 columns, 4,021,982 bytes of CSV

```

**Code**:

```python
def next_watermark(rows, load_column, current):
    # A zero-row extract must not move the watermark and must not fail.
    if not rows:
        return current
```

**Answer:** Take a fourth run against a source that has not grown: `WHERE wave > '3'` extracts 0 rows, where a job ignoring the stored value would re-pull all 58,168.

**Notes**:

* **Core Execution Mechanic:** One DynamoDB item keyed on `table_name` holds the watermark, read by `get_item` before the source connection opens and written by `update_item` after the landing object is put.


* **Core Execution Mechanic (Dummy Translation):** Every run the job asks a small lookup table where it got to last time, then asks for newer rows only.


* **Boundary / Memory Constraint:** Run 3 is the widest extract at 32,198 rows and 2,268,647 bytes, which `cursor.fetchall()` materialises beside a `StringIO` copy in a 1-DPU Glue Python Shell container.


* **Boundary / Memory Constraint (Dummy Translation):** The biggest single pull is about two megabytes, held in memory twice over while the file is built on one small machine.


* **Failure Mode / Downstream Impact:** A watermark written but never read fails silently, because the fixed landing key is overwritten and the downstream keyed MERGE re-matches all 58,168 rows, changing nothing.


* **Failure Mode / Downstream Impact (Dummy Translation):** It would quietly re-pull the whole table every night, no alarm would fire, and you would just keep paying for it.

---

**Question 2**: How do you make an incremental-load framework work when one of the source tables has no ordering column at all?

**Before Execution** (`[the published deposit]` | `[column scan, 37 columns]`):

```text
columns with a datetime dtype                                  : 0
columns whose name matches date|time|dt|day|month|year|stamp   : 0

```

**After Execution** (`[two rows in the config table]` | `[both paths run]`):

```text
table_name          load_column   last_extracted_value   what the job does
mail_offers         wave          '3'                    WHERE wave > '3' ORDER BY wave DESC
client_attributes   None          None                   SELECT * FROM client_attributes

client_attributes --load_type full_load    -> 58168 rows, 7 columns, 1,488,707 bytes, every run
client_attributes --load_type incremental  -> exit code 1, nothing written

```

**Code**:

```python
CONFIGURATIONS = [
    {"table_name": "mail_offers", "load_column": "wave", "last_extracted_value": None},
    {"table_name": "client_attributes", "load_column": None, "last_extracted_value": None},
]

# The reference lab's exit(1), kept
if args.load_type == "incremental" and not load_column:
    sys.exit(1)
```

**Answer:** You give the table a `load_column` of NULL and let that NULL mean full load, shipping all 58,168 rows and 1,488,707 bytes every run and forcing the configuration out of the source file.

**Notes**:

* **Core Execution Mechanic:** `fetch_configuration()` returns `(load_column, last_extracted_value)` and `build_query()` branches on both, so a NULL column never filters and never sorts.


* **Core Execution Mechanic (Dummy Translation):** Each table's lookup row names the column to count forward on, and a blank means take everything.


* **Boundary / Memory Constraint:** The snapshot costs 1,488,707 bytes per run where a wave of `mail_offers` costs between 379 and 2,268,647.


* **Boundary / Memory Constraint (Dummy Translation):** Re-shipping the whole snapshot nightly costs about a megabyte and a half, cheap only while the table stays small.


* **Failure Mode / Downstream Impact:** The disagreement exits 1 before the secret is read or the source connection opened, costing one `get_item`.


* **Failure Mode / Downstream Impact (Dummy Translation):** If the schedule ever asks this table for only what is new, the job stops instead of reporting success on wrong rows.

---

**Question 3**: How do you keep a string-typed watermark from silently skipping rows once the load column crosses a digit boundary?

**Before Execution** (`[3-valued wave column]` | `[Python and DuckDB, waves 1-3]`):

```text
'2' > '1'  -> True
'3' > '2'  -> True

```

**After Execution** (`[the same column reaching 12]` | `[SMALLINT vs VARCHAR, same job]`):

```text
'10' > '9' -> False

SMALLINT wave > '9' ->  3 rows  [12,11,10]
VARCHAR  wave > '9' ->  0 rows  []

guard on the 12-value result: first='12'  highest by string ordering='9'   disagree -> raises

```

**Code**:

```python
first = str(rows[0][load_column])
highest = max(str(row[load_column]) for row in rows)
if first != highest:
    raise ValueError(...)

```

**Answer:** The watermark is stored as text, and text ordering agrees with numeric ordering only while every value has the same digit count, so `next_watermark()` refuses to write a value the predicate would misread.

**Notes**:

* **Core Execution Mechanic:** `ORDER BY <col> DESC` makes `rows[0]` the maximum by the engine's ordering, while the guard re-derives it with `max(str(...))`, which is always lexicographic.


* **Core Execution Mechanic (Dummy Translation):** Sorted as words, "10" comes before "9" the way "apple" comes before "banana", so the job checks its answer a second way and stops if they disagree.


* **Boundary / Memory Constraint:** The ceiling is exactly one digit boundary: safe for values 1 through 9, unsafe from 10.


* **Boundary / Memory Constraint (Dummy Translation):** It works for the numbers one to nine and stops working at ten, and this dataset never gets past three.


* **Failure Mode / Downstream Impact:** Without the guard the job writes a plausible watermark, reports success, and skips every row between it and the true maximum on every run thereafter.


* **Failure Mode / Downstream Impact (Dummy Translation):** Left alone, the job would pull nothing every night while claiming everything went fine, like a quiet day with no new data.

---

**Question 4**: How do you write a zero-row extract to the landing zone so that it cannot be mistaken for a failed one?

**Before Execution** (`[reference lab: return ""]` | `[COPY ... FORMAT CSV, HEADER]`):

```text
convert_to_csv over an empty result -> ""    -> object of 0 bytes

COPY ... (FORMAT CSV, HEADER) on empty_string.csv -> InvalidInputException

```

**After Execution** (`[header-only CSV]` | `[run 4 landed and loaded]`):

```text
to_csv(fieldnames, []) -> 379 bytes, 1 line, ends with '\n', no CR anywhere

COPY ... (FORMAT CSV, HEADER) on header_only.csv -> 0 rows
mail_offers: 0 rows staged and merged, 58168 rows now in raw_zone.mail_offers

```

**Code**:

```python
# fieldnames comes from cursor.description, not rows[0].keys()
buffer = io.StringIO()
writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
writer.writeheader()
for row in rows:
    writer.writerow(row)
```

**Answer:** Write the header unconditionally from `cursor.description`, so an empty result lands as a 379-byte one-line file that `COPY` reads as 0 rows, not a zero-byte object indistinguishable from a failed extract.

**Notes**:

* **Core Execution Mechanic:** `csv.DictWriter.writeheader()` runs before the row loop, so the file always carries a first line, built from `cursor.description` rather than from the rows.


* **Core Execution Mechanic (Dummy Translation):** The column-names line comes from the database's description of the table, so it is there even with no rows.


* **Boundary / Memory Constraint:** The header line is 379 bytes against 4,021,982 for a full extract.


* **Boundary / Memory Constraint (Dummy Translation):** The extra line costs almost nothing next to the megabytes the job writes.


* **Failure Mode / Downstream Impact:** A zero-byte object raises `InvalidInputException` on the local loader and looks identical to a failed extract, while the header-only file stages 0 rows and leaves 58,168 rows in `raw_zone.mail_offers`.


* **Failure Mode / Downstream Impact (Dummy Translation):** With an empty file, the night nothing arrived is the night the loader crashes, but with the header it loads nothing and moves on.

---

**Question 5**: How do you keep an error handler from destroying the error it is handling?

**Before Execution** (`[reference lab: no pre-binding]` | `[connect raises, CPython 3.12.11]`):

```text
ConnectionRefusedError: Can't connect to MySQL server on 'credit-mailer.rds' (111)

NameError: name 'connection' is not defined

exception that escapes main(): NameError
```

**After Execution** (`[connection = None before the try]` | `[same failure, same interpreter]`):

```text
exception that escapes main(): ConnectionRefusedError

18 passed in 4.15s
```

**Code**:

```python
    # Bound before the try, which the reference lab does not do
    connection = None
    try:
        connection = connect_source(args)
    finally:
        if connection is not None:
            connection.close()
```

**Answer:** Bind the name to `None` before the `try` and guard the close with `if connection is not None`, so a failed connect leaves the `finally` clause nothing to trip over.

**Notes**:

* **Core Execution Mechanic:** An exception raised inside a `finally` block replaces the in-flight one as the propagating exception, demoting the original to `__context__`.


* **Core Execution Mechanic (Dummy Translation):** The cleanup code runs even when things went wrong, and if the cleanup trips over, its complaint drowns out the real problem.


* **Boundary / Memory Constraint:** The whole fix is `connection = None` plus one `is not None` check, and it costs nothing at runtime.


* **Boundary / Memory Constraint (Dummy Translation):** Two extra lines that cost no speed, since the guard just checks whether anything is open.


* **Failure Mode / Downstream Impact:** Any handler, filter or retry keyed on the exception type sees `NameError`, so a transient connection refusal is classified as a code bug and is not retried.


* **Failure Mode / Downstream Impact (Dummy Translation):** You are told a variable is missing instead of that the database refused the connection, so an automatic retry will not recognise a flaky connection as one.

---

**Question 6**: How do you make a staging table's contents a function of one run alone?

**Before Execution** (`[TRUNCATE after the merge only]` | `[Crashed Prior Run]`):

```text
  raw_zone.tmp_mail_offers      20,996    <- wave 2's extract, left by the run that died
  staging after the COPY        53,194    of which wave 2: 20,996   wave 3: 32,198
  wave 2 in the target          20,996 rows, sum(applied) = 1,923    <- reverted, no error

```

**After Execution** (`[TRUNCATE before the COPY as well]` | `[Same Crashed State]`):

```text
  staging after the COPY        32,198    exactly the extract, nothing else
  wave 2 in the target          20,996 rows, sum(applied) = 20,996   <- the correction survives

```

**Code**:

```python
        # Before the COPY, not only after the merge.
        run(cursor, "TRUNCATE TABLE {0}".format(staging), log=LOG)
        run(cursor, copy_sql(table, source, args.local, args.iam_role), log=LOG)
        run(cursor, merge_sql(table), log=LOG)
        run(cursor, "TRUNCATE TABLE {0}".format(staging), log=LOG)

```

**Answer:** Truncate the staging table before the COPY as well as after the merge, so its contents are a function of this run alone, whatever the previous run did.

**Notes**:

* **Core Execution Mechanic:** A COPY appends, so a staging table holding two extracts merges as their union on `(client_id, wave)`, restating non-key columns from the older file.


* **Core Execution Mechanic (Dummy Translation):** Loading a file adds rows rather than replacing them, so a dead run's leftovers merge in too and stale values overwrite corrected ones.


* **Boundary / Memory Constraint:** The staging tables never exceed 58,168 rows, so truncating is a metadata operation costing nothing worth measuring.


* **Boundary / Memory Constraint (Dummy Translation):** The loading area never holds more than about fifty-eight thousand rows, so emptying it is instant.


* **Failure Mode / Downstream Impact:** The run exits 0 with the right row count while the corrupted column moves from 20,996 to 1,923, and downstream reads the reverted values.


* **Failure Mode / Downstream Impact (Dummy Translation):** Nothing fails and the totals look right; the only clue is a log line saying more rows loaded than the file held.

---

**Question 7**: How do you find out whether TRUNCATE, COPY, MERGE, TRUNCATE, commit is really one all-or-nothing unit?

**Before Execution** (`[DuckDB 1.5.5, local stand-in]` | `[the five statements, then ROLLBACK]`):

```text
before                                    tmp = 0        target = 25,970

BEGIN TRANSACTION
  after MERGE + TRUNCATE (uncommitted)    tmp = 0        target = 58,168
ROLLBACK
  after ROLLBACK                          tmp = 0        target = 25,970

```

**After Execution** (`[Redshift]` | `[a documented difference in transaction semantics]`):

```text
  TRUNCATE tmp [commit] -> COPY, MERGE -> TRUNCATE tmp [commit] -> conn.commit() (no-op)

```

**Code**:

```python
        cursor = conn if args.local else conn.cursor()
        if args.local:
            run(cursor, "BEGIN TRANSACTION", log=LOG)

```

**Answer:** On DuckDB, yes — the whole sequence rolls back to where it started. On Redshift, no: TRUNCATE commits the transaction it runs in, making the five statements three committed units.

**Notes**:

* **Core Execution Mechanic:** DuckDB versions TRUNCATE like any other write, so ROLLBACK restores the truncated rows, measured at 25,970 in the target afterwards.


* **Core Execution Mechanic (Dummy Translation):** Emptying a table is an ordinary undoable change on the laptop database, but on the cloud warehouse it permanently saves everything done so far.


* **Boundary / Memory Constraint:** DuckDB 1.5.5 is the local stand-in because DuckDB 1.4 added `MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED`, so the MERGE is the same text in both engines.


* **Boundary / Memory Constraint (Dummy Translation):** The local database understands the one statement this pipeline's correctness rests on, so that statement can be rehearsed rather than rewritten.


* **Failure Mode / Downstream Impact:** `ROLLBACK` with no open transaction raises `TransactionException` from inside the `except` block and replaces the error it was handling, which is why the local path opens one explicitly.


* **Failure Mode / Downstream Impact (Dummy Translation):** The dangerous case is the error handler itself breaking: it tries to undo work that was never started, and its own error hides the real one.

---

**Question 8**: How do you decide whether two table loads may run in parallel, when every table lands on one fixed object key?

**Before Execution** (`[one fixed landing key per table]` | `[four runs, same key]`):

```text
the race, run 3's extract firing while run 2's load is still queued:
  raw_zone.mail_offers by wave      1=4,974   3=32,198        <- wave 2 is not there

```

**After Execution** (`[the chain as declared]` | `[step-functions.json]`):

```text
state machine: 6 states, StartAt=ExtractMailOffers
  Parallel/Map states: 0

same four runs run in that order: raw_zone.mail_offers  1=4,974  2=20,996  3=32,198  = 58,168

```

**Code**:

```json
{
  "Comment": "Chain, not Parallel: each table has one fixed S3 key, overwritten every run.",
  "StartAt": "ExtractMailOffers",
  "States": {
    "ExtractMailOffers": {
      "Next": "RawIngestMailOffers"
    },
    "RawIngestMailOffers": {
      "Next": "ExtractClientAttributes"
    }
  }
}
```

**Answer:** Because each table lands on one object, `.../<table>/data.csv`, overwritten every run, its extract and load share a mutable slot that a `Parallel` state would let the COPY read mid-overwrite.

**Notes**:

* **Core Execution Mechanic:** Extract and load are separate Glue jobs with no shared lock, so Step Functions' `Next` is the only synchronisation available.


* **Core Execution Mechanic (Dummy Translation):** Each table gets one drop-box that every run empties and refills, so the two jobs sharing it take turns.


* **Boundary / Memory Constraint:** Chaining costs wall-clock time on files of at most 2.3 MB and 58,168 rows, where parallelism would save seconds.


* **Boundary / Memory Constraint (Dummy Translation):** Running the two tables side by side would save seconds on files of a couple of megabytes.


* **Failure Mode / Downstream Impact:** All six states succeed, yet 20,996 wave-2 mailers are missing and the watermark has advanced to '3', so the next extract returns 0 rows.


* **Failure Mode / Downstream Impact (Dummy Translation):** Every step goes green and twenty-one thousand mailers are simply not there, with nothing to tell "we have everything" from "we skipped a batch".

---

**Question 9**: How does the incremental predicate on the warehouse side differ from the one on the source side?

**Before Execution** (`[source side, in DynamoDB]` | `[WHERE wave > 'last_extracted_value']`):

```text
run | watermark on entry | rows extracted | rows in the landed CSV
 4  | '3'                |              0 |      0      header-only file, 379 bytes

```

**After Execution** (`[warehouse side, read off the fact]` | `[WHERE mo.wave >= COALESCE(MAX(wave), 0)]`):

```text
run | MAX(wave) in the fact | rows staged | fact rows after
 3  |                     2 |      53,194 |          58,168
 4  |                     3 |      32,198 |          58,168

```

**Code**:

```python
# Read out of the table it describes, so it cannot drift
WAVE_WATERMARK = "(SELECT COALESCE(MAX(wave), 0) FROM processed_zone.fact_mailer)"

# The source side, in mysql-extraction.py:
    return sql + " WHERE {0} > '{1}' ORDER BY {0} DESC".format(load_column,
                                                               last_extracted_value)

```

**Answer:** The warehouse watermark is `MAX(wave)` read off the fact it filters, and compares with `>=` not `>`, so a partly merged wave is re-merged, not skipped.

**Notes**:

* **Core Execution Mechanic:** An empty fact gives NULL and `wave >= NULL` matches no rows, so `COALESCE(MAX(wave), 0)` makes the first run a full load.


* **Core Execution Mechanic (Dummy Translation):** On the first run the table is empty, so comparing against nothing matches nothing and the run that must load everything loads nothing, unless a zero stands in for "nothing yet".


* **Boundary / Memory Constraint:** `>=` re-stages run 4's 32,198 rows, affordable only because the merge key `(client_id, wave)` makes the re-merge a no-op.


* **Boundary / Memory Constraint (Dummy Translation):** Re-processing the newest batch every run is safe only because each row is matched on its own identity and rewritten unchanged.


* **Failure Mode / Downstream Impact:** With `>` a wave extracted but not merged is skipped permanently, because an advanced watermark is indistinguishable from an up-to-date one.


* **Failure Mode / Downstream Impact (Dummy Translation):** A bookmark that only moves forward loses a batch fetched but never filed, and the loss looks exactly like success.

---

**Question 10**: How do you stop a dimension join from silently deleting fact rows?

**Before Execution** (`[inner join to dim_offer_arm]` | `[the CRM respells MEDIUM]`):

```text
what an INNER JOIN stages, out of 32,198 source rows:
  the CRM starts spelling MEDIUM as 'MED'     28,608     3,590 rows gone, no error

```

**After Execution** (`[LEFT JOIN + two assertions]` | `[the same respelling]`):

```text
'MED' respelling               LEFT JOIN stages 32,198, arm_id IS NULL on 3,590
  ValueError: 3590 of 32198 staged mailers matched no arm in dim_offer_arm ...

```

**Code**:

```python
# Two assertions: the grid before staging, the nulls after it.
held = scalar(cursor, "SELECT COUNT(*) FROM processed_zone.dim_offer_arm;")
if held != EXPECTED_ARMS:
    raise ValueError(...)

missed = scalar(cursor, "SELECT COUNT(*) FROM stage_fact_mailer WHERE arm_id IS NULL;")
if missed:
    raise ValueError(...)

```

**Answer:** Join with a LEFT JOIN so unmatched rows survive as a NULL `arm_id`, then assert twice before the MERGE: the grid holds 18 rows, and no staged mailer missed an arm.

**Notes**:

* **Core Execution Mechanic:** A LEFT JOIN keeps unmatched rows with a NULL `arm_id`, so a row an inner join would delete becomes a countable value.


* **Core Execution Mechanic (Dummy Translation):** A plain join throws away whatever it cannot pair up, so you keep those rows with a blank and count the blanks.


* **Boundary / Memory Constraint:** The grid is seed data: 18 arms written by `redshift-create-tables.sql`, never by this pipeline.


* **Boundary / Memory Constraint (Dummy Translation):** The eighteen price brackets are set up once by the schema, and no run can restore one that goes missing.


* **Failure Mode / Downstream Impact:** Silent and worse than a crash: a partly wrong grid leaves 4,139 rows quietly missing from 32,198, all of them the highest-priced HIGH-risk mailers, in a complete-looking, biased fact table.


* **Failure Mode / Downstream Impact (Dummy Translation):** The run finishes green with the most expensive offers missing, and everything downstream reports on that partial picture with full confidence.

---

**Question 11**: How do you decide whether 134 duplicate rows are two people or one record written twice?

**Before Execution** (`[df.duplicated(), no key]` | `[58,168 x 37 as published]`):

```text
non-first occurrences     : 134
distinct rows             : 58,034 of 58,168

```

**After Execution** (`[the paper's N as arbiter]` | `[waves 2+3, kept vs dropped]`):

```text
QJE analysis sample as published : 53,194
this file, waves 2+3, as kept    : 53,194        <- to the row
this file, waves 2+3, deduped    : 53,175        <- short by 19

```

**Code**:

```python
paper_n = 53194
w23 = df[df.wave.isin([2, 3])]
would_lose = int(w23.duplicated().sum())

# 1-based row number of the published extract
frame.insert(0, "client_id", range(1, len(frame) + 1))

```

**Answer:** You decide it from an external count: kept, waves 2+3 hold exactly the paper's 53,194 rows and deduplicated they hold 53,175, so all 134 duplicates stay.

**Notes**:

* **Core Execution Mechanic:** `df.duplicated()` hashes the whole 37-column tuple, so its 134 non-first occurrences become a prediction the published sample size can test.


* **Core Execution Mechanic (Dummy Translation):** With no customer number in the file, you count it both ways: keeping the repeats matches the paper, dropping them falls 19 short.


* **Boundary / Memory Constraint:** Only 13,517 of 84,000 possible demographic-and-rate cells are occupied by 58,168 rows, far too coarse a surface to make a collision improbable.


* **Boundary / Memory Constraint (Dummy Translation):** With only a few coarse facts per person — risk grade, race, the rate offered — two different people can match on all of them.


* **Failure Mode / Downstream Impact:** A `DISTINCT` or `drop_duplicates()` anywhere in the load fails silently — no error, no key violation, just 134 fewer mailers and 19 fewer rows in the analysis sample.


* **Failure Mode / Downstream Impact (Dummy Translation):** Removing repeated rows quietly deletes 134 real customers while the data still looks healthy, and only an outside number catches it.

---

**Question 12**: How do you land a 0/1 flag column that contains NaN as `SMALLINT` rather than `1.0`?

**Before Execution** (`[pd.read_csv, no cast]` | `[flags as float64]`):

```text
  prize          float64  nulls  4,974   values [0.0, 1.0]
  tookup         int64    nulls      0   values [0, 1]

to_csv() straight off that frame:
wave,prize,intshown,tookup
1,,,0
2,1.0,1.0,0

```

**After Execution** (`[astype("Int64")]` | `[same frame, same table]`):

```text
to_csv() after the nullable-integer cast:
wave,prize,intshown,tookup
1,,,0
2,1,1,0

```

**Code**:

```python
# Everything else in the deposit is an integer that Dataverse's TSV happens to render as "0.0"
NON_INTEGER_COLUMNS = ("race", "risk", "offer4")
for column in frame.columns:
    if column in NON_INTEGER_COLUMNS:
        continue
    frame[column] = frame[column].astype("Int64")

```

**Answer:** Cast every column but the non-integer ones to pandas' nullable `Int64`, so a missing value never widens it to `float64` and `to_csv` keeps writing `1`. It also refuses a fractional value instead of flooring it.

**Notes**:

* **Core Execution Mechanic:** `Int64` is a pandas extension dtype, an `int64` array plus a boolean mask, so absence sits beside the value instead of widening the column to a float.


* **Core Execution Mechanic (Dummy Translation):** A plain whole-number column cannot write "blank", so one blank turns every 1 into 1.0, while the nullable kind notes blanks separately.


* **Boundary / Memory Constraint:** 25 of the 37 columns parse as `float64` and only 10 as `int64`, costing 7,668,417 bytes against 5,123,713 for the same 58,168 x 37 file.


* **Boundary / Memory Constraint (Dummy Translation):** Two thirds of the columns come out as decimals, and dropping the decimal point cuts a third off every uploaded file.


* **Failure Mode / Downstream Impact:** DuckDB 1.5.5 takes `'1.0'` into `SMALLINT` and stores the right number anyway, so the defect stays silent locally and surfaces only as a divergence with Redshift.


* **Failure Mode / Downstream Impact (Dummy Translation):** Nothing complains on your own machine, so the mismatch only shows up when the real warehouse reads the file.

---

**Question 13**: How do you tell apart two columns that are full of NULLs for opposite reasons?

**Before Execution** (`[flat NULL census]` | `[fact_mailer, 58,168 rows]`):

```text
columns with any NULL in processed_zone.fact_mailer:
   bad_account        53,787
   prize               4,974
count of NULL-bearing columns: 15 of 28
```

**After Execution** (`[NULL rate by wave]` | `[same 58,168 rows]`):

```text
 wave    bad_account          prize       intshown  dphoto_female         comp_n
    1         0.9121         1.0000         1.0000         0.0000         0.0000
    2         0.9246         0.0000         0.0000         0.0000         0.0000
    3         0.9267         0.0000         0.0000         0.0000         0.0000

WHERE (bad_account IS NULL) <> (took_up = 0)  ->  0 rows disagree
```

**Code**:

```python
nulls = ", ".join("SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}".format(c=col)
                  for col in TREATMENT_FLAGS)
cursor.execute("SELECT wave, COUNT(*) AS n_rows, {} FROM processed_zone.fact_mailer "
               "GROUP BY wave ORDER BY wave".format(nulls))
```

**Answer:** Count the nulls by wave, not overall: the 14 treatment flags are NULL on 100% of wave 1 and 0% of waves 2 and 3, `bad_account` on exactly the rows where `took_up = 0`.

**Notes**:

* **Core Execution Mechanic:** `GROUP BY wave` returns rates in {0, 1} for a design-level absence and rates strictly inside (0, 1) for a row-level one.


* **Core Execution Mechanic (Dummy Translation):** Counted wave by wave, one column is blank for a whole wave, the other for whoever never borrowed.


* **Boundary / Memory Constraint:** Wave 1's five surviving flags are a constant 0, not NULL, against means of 0.0356 to 0.8002 in waves 2 and 3.


* **Boundary / Memory Constraint (Dummy Translation):** Wave 1 has a zero in five treatment columns and a blank in fourteen, and the file's authors meant those differently.


* **Failure Mode / Downstream Impact:** A zero fill on `bad_account` fails silently, reporting the portfolio default rate as 0.008888 instead of the measured 0.11801.


* **Failure Mode / Downstream Impact (Dummy Translation):** Writing 0 for "never borrowed" says 53,787 people who were never lent a rand did not default, and nothing breaks.

---

**Question 14**: How do you keep "arm 3" meaning the same price band in wave 3 as in wave 1?

**Before Execution** (`[grid recomputed per run]` | `[wave 1 rows, re-binned]`):

```text
what "MEDIUM arm 3" is under each grid:
  from wave 1 alone    [7.250, 8.500)   144 wave-1 mailers, take-up 0.1736
  from waves 1+2+3     [7.500, 8.190)    79 wave-1 mailers, take-up 0.1519
```

**After Execution** (`[declared CUTS, checked against dim_offer_arm]` | `[the same wave-1 rows]`):

```text
MEDIUM arm 3 = [7.500, 8.250) in every run: 79 wave-1 mailers, take-up 0.1519
```

**Code**:

```python
# Declared cuts, not quantiles of the arriving data
CUTS = {
    "HIGH": (5.50, 7.50, 9.00, 10.00, 11.00),
    "MEDIUM": (5.00, 6.75, 7.50, 8.25, 9.25),
    "LOW": (4.50, 5.50, 6.00, 6.75, 7.50),
}
```

**Answer:** Declare the cut points in the job as constants and check them against the seeded `dim_offer_arm` rows, rather than deriving them from the rates in the current extract.

**Notes**:

* **Core Execution Mechanic:** `bisect_right(CUTS[band], rate)` depends only on the constant tuple and the one rate, while a quantile grid makes that index depend on the whole sample.


* **Core Execution Mechanic (Dummy Translation):** The bands are written down once, never worked out from the data, so a price always falls in the same box.


* **Boundary / Memory Constraint:** Fifteen cut points, all multiples of 0.25 and exact as doubles, so the `rate == cut` comparison has no rounding case.


* **Boundary / Memory Constraint (Dummy Translation):** Fifteen numbers, all of them round to the nearest quarter, so nothing lands ambiguously on an edge.


* **Failure Mode / Downstream Impact:** A drifting grid raises nothing and drops no rows: it makes `alpha`/`beta` a sum over two different price intervals sharing one `arm_id`.


* **Failure Mode / Downstream Impact (Dummy Translation):** Nothing crashes and nothing goes missing, but the totals under "arm 3" mix two prices and name a rate nobody ever charged.

---

**Question 15**: How do you keep the reported policy value from moving when you change how many bootstrap replicates you ask for?

**Before Execution** (`[one shared default_rng(seed)]` | `[2,000 vs 8,000 replicates]`):

```text
  thompson draw 2, after 500 bootstrap draws           0.050496651126229
  thompson draw 2, with no bootstrap draws at all      0.380195735019618
```

**After Execution** (`[SeedSequence(seed).spawn(2)]` | `[the same two replicate counts]`):

```text
  thompson draw 2, after 500 bootstrap draws           0.233117269451900
  thompson draw 2, with no bootstrap draws at all      0.233117269451900
```

**Code**:

```python
# The two generators must not share a stream
def make_generators(seed):
    thompson, bootstrap = np.random.SeedSequence(seed).spawn(2)
    return np.random.default_rng(thompson), np.random.default_rng(bootstrap)
```

**Answer:** Derive the two streams with `np.random.SeedSequence(seed).spawn(2)`, one for `run_rounds` and one for `evaluate_policy`, so how many draws the bootstrap takes cannot change where the Thompson generator is standing. Sharing one generator, with the evaluation inside the round loop, drifts the stored `ts_probability` by up to 2.15e-03 between 2,000 and 8,000 replicates.

**Notes**:

* **Core Execution Mechanic:** `SeedSequence.spawn(n)` gives each child a distinct spawn key mixed with the parent entropy, so advancing one `default_rng` leaves the other's position untouched.


* **Core Execution Mechanic (Dummy Translation):** Both jobs need random numbers, and from one shared queue the one that takes more leaves the other starting somewhere else, so spawning gives each its own queue.


* **Boundary / Memory Constraint:** A probability from 200,000 Thompson draws carries a standard error of at most 1.1e-03, the same order as the 2.15e-03 shift.


* **Boundary / Memory Constraint (Dummy Translation):** The wobble the bug adds is the size of the wobble the method already has, so you cannot spot it by eye.


* **Failure Mode / Downstream Impact:** Nothing raises, no column leaves its range, and each run still repeats itself exactly for a fixed set of flags.


* **Failure Mode / Downstream Impact (Dummy Translation):** The only way to notice is to ask for a tighter error bar and find the number in the middle moved too.

---

**Question 16**: How do you tell whether the importance-weighted estimator is doing anything a plain average would not?

**Before Execution** (`[pi = e, the logging policy against itself]` | `[wave 3 LOW]`):

```text
   wave 3 LOW    plain 0.1586500133   SNIPS 0.1586500133   difference 2.8e-17
```

**After Execution** (`[stochastic Thompson policy]` | `[the same cell]`):

```text
  wave  band       n       plain mean    SNIPS(Thompson)   pi/e weight range
   3    LOW      3,763     0.15865001      0.14339523      0.0008 .. 4.3058
```

**Code**:

```python
def snips(pi, rewards, pulls):
    # e = the realised arm frequency in the cell
    played = pulls > 0
    weight = pi[played].sum()
    return float((pi[played] * (rewards[played] / pulls[played])).sum() / weight)
```

**Answer:** SNIPS earns its name only when the evaluated policy is stochastic and the logging shares are not uniform, and with `e` estimated from the same rows it is a direct method in importance-weighted clothing.

**Notes**:

* **Core Execution Mechanic:** With `e_a = n_a / n` the `n` cancels between the two sums and the ratio reduces to `sum_a pi_a * (k_a / n_a)`.


* **Core Execution Mechanic (Dummy Translation):** Because each price's chance of being offered is worked out from the same rows, the weighting cancels and leaves each price's own take-up rate, averaged using the new policy's preferences as the weights.


* **Boundary / Memory Constraint:** A `GROUP BY wave, risk_band, arm_id` reduces each cell to six pairs of integers, so the estimator reads 54 rows instead of 58,168.


* **Boundary / Memory Constraint (Dummy Translation):** The whole calculation runs on six pairs of counts per group, not tens of thousands of rows.


* **Failure Mode / Downstream Impact:** A greedy policy's SNIPS looks like an off-policy estimate but is a subgroup mean over one arm, 591 rows where the header says 3,763.


* **Failure Mode / Downstream Impact (Dummy Translation):** The number always looks reasonable, so nothing tells you it came from a small slice of the rows.

---

**Question 17**: How do you decide that a refinery step is forbidden on a path rather than merely skipped?

**Before Execution** (`[Step 8 on this run's numbers]` | `[final posterior, waves 1-3]`):

```text
STEP 8 -- a variance threshold over the one-hot arm dummies removes ascending p(1-p):

  band   arm    pulls   share p   p(1-p)    posterior sd   ts_probability
  LOW     5        823   0.0999   0.089922    0.012434       0.01713   <- deleted first
  LOW     4      2,295   0.2786   0.200976    0.007541       0.00511
```

**After Execution** (`[verdict(..., banned=True)]` | `[the run's own log]`):

```text
eight BANNED verdicts printed in total; verdict() returns False for every one of them
```

**Code**:

```python
    label = ("BANNED   -- " if banned
             else "OVERRIDE -- " if override
             else "APPLIES  -- " if applies
             else "N/A      -- ")
    LOG.info("%s%s", label, message)
    return bool(applies) and not banned
```

**Answer:** A step is banned rather than skipped when it would have acted and the acting is destructive, so the run prices that damage in its own numbers and prints `BANNED`.

**Notes**:

* **Core Execution Mechanic:** `p(1-p)` is strictly increasing on `[0, 0.5]`, so ordering arms by dummy variance orders them by exposure.


* **Core Execution Mechanic (Dummy Translation):** The threshold ranks prices by how often they were tried and cuts the least-tried one first, the one still being learned.


* **Boundary / Memory Constraint:** Eighteen arms carrying between 823 and 8,362 mailers give posterior standard deviations spanning 0.002252 to 0.012434.


* **Boundary / Memory Constraint (Dummy Translation):** The busiest price was tried ten times as often as the quietest, and that difference is the only reason the method knows which prices it is still unsure about.


* **Failure Mode / Downstream Impact:** Steps 8 and 9 fail SILENTLY: the run completes and the only change is which arm the argmax names.


* **Failure Mode / Downstream Impact (Dummy Translation):** Nothing crashes and no row goes missing -- the run just recommends a different price nobody can check.
