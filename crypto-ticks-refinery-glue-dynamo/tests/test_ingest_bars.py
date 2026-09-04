"""Acceptance check for glue-ingest-bars.py: every bar, rebuilt without Spark.

Runs the job against the committed two-hour sample, then recomputes all 4,320 bars from the
same raw CSVs in pure ``decimal.Decimal`` -- no Spark, no float -- and asserts the two agree
field for field.

The point is not coverage, it is the three traps that are silent when wrong:

  * ``event_time`` is epoch MICROseconds. Read as milliseconds it yields a valid-looking
    timestamp in the year 56971 and every bar lands in one bucket.
  * ``open``/``close`` must be ordered by ``trade_id``. 75% of ticks share a microsecond with
    their predecessor, so an ``event_time`` ordering leaves them genuinely undefined -- and a
    wrong tie-break still produces plausible OHLC that no eyeball check would catch.
  * ``is_buyer_maker = True`` means the buyer was the MAKER, so the trade is an aggressive
    SELL. Inverting it flips the sign of a predictive feature while leaving the global
    buy/sell ratio at ~50/50, so no ratio sanity-check would notice.

Run it from the project root::

    python test_ingest_bars.py

Needs the same Java 11/17 JDK and pyspark the job needs; it shells out to the job itself, so
it tests the shipped artifact rather than a copy of its logic.
"""

import glob
import gzip
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from decimal import Decimal

import pyarrow.parquet as pq

# Paths are relative to the PROJECT ROOT, which is where this is run from.
JOB = "glue-jobs/glue-ingest-bars.py"
SAMPLE_DIR = "data/sample"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MONTH = "2025-01"
INTERVAL_LABEL = "5s"
INTERVAL_US = 5_000_000
START_US = 1_735_689_600_000_000            # 2025-01-01T00:00:00Z
END_US = START_US + 2 * 60 * 60 * 1_000_000  # the sample is exactly two hours


def reference_bars():
    """Rebuild every bar from the raw CSV text with exact decimal arithmetic."""
    bars = {}
    for symbol in SYMBOLS:
        rows = defaultdict(list)
        path = f"{SAMPLE_DIR}/{symbol}-trades-{MONTH}.csv.gz"
        with gzip.open(path, "rt") as handle:
            for line in handle:
                tid, price, qty, quote, event_us, buyer_maker, best = line.rstrip("\n").split(",")
                event_us = int(event_us)
                # Half-open [bar_us, bar_us + interval), same arithmetic as the job.
                rows[event_us - event_us % INTERVAL_US].append(
                    (int(tid), Decimal(price), Decimal(qty), Decimal(quote),
                     buyer_maker == "True", best == "True"))

        for bar_us, ticks in rows.items():
            ticks.sort(key=lambda t: t[0])          # by trade_id, never by event_us
            prices = [t[1] for t in ticks]
            bars[(symbol, bar_us)] = dict(
                open=ticks[0][1], close=ticks[-1][1],
                high=max(prices), low=min(prices),
                volume=sum(t[2] for t in ticks),
                quote_volume=sum(t[3] for t in ticks),
                n_ticks=len(ticks),
                first_trade_id=ticks[0][0], last_trade_id=ticks[-1][0],
                # not is_buyer_maker -> the buyer was the taker -> an aggressive BUY
                taker_buy_qty=sum((t[2] for t in ticks if not t[4]), Decimal(0)),
                taker_buy_quote_qty=sum((t[3] for t in ticks if not t[4]), Decimal(0)),
                all_best_match=all(t[5] for t in ticks),
            )
    return bars


def job_bars(out_dir):
    """Run the job and read back what it wrote."""
    cmd = [sys.executable, JOB, "--local",
           "--input", SAMPLE_DIR, "--bars-output", out_dir,
           "--bar-interval", INTERVAL_LABEL,
           "--calendar-start", "2025-01-01T00:00:00",
           "--calendar-end", "2025-01-01T02:00:00"]
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, check=True)

    table = pq.read_table(glob.glob(f"{out_dir}/*.parquet"))
    return {(r["symbol"], r["bar_us"]): r for r in table.to_pylist()}


def main():
    out_dir = tempfile.mkdtemp(prefix="ingest_bars_test_")
    try:
        produced = job_bars(out_dir)
        expected = reference_bars()

        n_slots = (END_US - START_US) // INTERVAL_US
        assert len(produced) == n_slots * len(SYMBOLS), (
            f"expected a dense grid of {n_slots * len(SYMBOLS)} bars, got {len(produced)}")

        empty = compared = 0
        for key, row in sorted(produced.items()):
            reference = expected.get(key)
            if reference is None:
                # A slot no tick landed in: Step 3 must FLAG it, never fill it.
                assert row["is_missing_bar"] == 1, f"{key} has no ticks but is not flagged"
                assert row["n_ticks"] == 0, f"{key} is empty but n_ticks={row['n_ticks']}"
                assert row["close"] is None, (
                    f"{key} is empty but close={row['close']} -- the entryway filled a gap, "
                    f"which is Step 4 and belongs after the fork")
                empty += 1
                continue

            assert row["is_missing_bar"] == 0, f"{key} has ticks but is flagged missing"
            for field, want in reference.items():
                got = row[field]
                if isinstance(want, Decimal):
                    got = Decimal(str(got))
                assert got == want, f"{key} {field}: job {got!r} != reference {want!r}"
            compared += 1

        # Guard the guard: if the sample ever stopped exercising the argmin/argmax, every
        # ordering bug would pass silently. A bar whose close is strictly inside (low, high)
        # and differs from open can only be right if open/close were resolved by trade_id.
        discriminating = sum(
            1 for k, r in produced.items()
            if r["is_missing_bar"] == 0 and r["open"] != r["close"]
            and r["low"] < r["close"] < r["high"])
        assert discriminating > 100, (
            f"only {discriminating} bars discriminate an ordering bug -- this check has gone "
            f"blind and would pass with first()/last() or an event_time tie-break")

        print(f"\nOK  {compared} non-empty bars match the decimal reference field for field")
        print(f"OK  {empty} empty bars flagged and left unfilled")
        print(f"OK  {discriminating} bars would have caught an open/close ordering bug")
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
