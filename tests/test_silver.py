"""Unit tests for the silver building blocks: dedupe, PII masking, quarantine."""
from datetime import datetime

from silver import dedupe_latest, mask_email, mask_phone, parse_legacy_date, split_valid
from pyspark.sql import functions as F


def test_dedupe_keeps_latest_version_and_drops_exact_duplicates(spark):
    t1, t2 = datetime(2025, 1, 1), datetime(2025, 6, 1)
    rows = [
        (1, "Pune", t1, t1),      # old version of customer 1
        (1, "Mumbai", t2, t2),    # newer version (CDC update) -> should win
        (2, "Delhi", t1, t1),     # customer 2 ...
        (2, "Delhi", t1, t1),     # ... exact duplicate from the legacy system
    ]
    df = spark.createDataFrame(rows, "customer_id int, city string, updated_at timestamp, _ingested_at timestamp")
    out = {r.customer_id: r.city for r in dedupe_latest(df, "customer_id").collect()}
    assert out == {1: "Mumbai", 2: "Delhi"}


def test_email_is_hashed_consistently_and_not_readable(spark):
    df = spark.createDataFrame([("Asha.Rao@Gmail.com ",), ("asha.rao@gmail.com",), (None,)], "email string")
    hashes = [r.h for r in df.select(mask_email(F.col("email")).alias("h")).collect()]
    assert hashes[0] == hashes[1]                       # same person -> same hash (joinable)
    assert len(hashes[0]) == 64 and "@" not in hashes[0]  # SHA-256 hex, no raw email
    assert hashes[2] is None                            # NULL stays NULL


def test_phone_masked_to_last_four_digits_for_all_legacy_formats(spark):
    df = spark.createDataFrame([("+91-9876543210",), ("09876543210",), ("+91 98765 43210",), ("123",)], "phone string")
    out = [r.p for r in df.select(mask_phone(F.col("phone")).alias("p")).collect()]
    assert out == ["XXXXXX3210", "XXXXXX3210", "XXXXXX3210", None]


def test_legacy_dates_parse_both_formats_and_reject_garbage(spark):
    df = spark.createDataFrame([("2025-03-01 10:00:00",), ("01/03/2025 10:00",), ("2025-02-30 10:00:00",), ("N/A",)],
                               "order_date string")
    out = [r.ts for r in df.select(parse_legacy_date("order_date").alias("ts")).collect()]
    assert out[0] == out[1] == datetime(2025, 3, 1, 10, 0)
    assert out[2] is None and out[3] is None


def test_invalid_rows_are_quarantined_with_reason_not_dropped(spark):
    df = spark.createDataFrame([(1, 2), (2, -1), (3, 0)], "id int, quantity int")
    valid, bad = split_valid(df, [(F.col("quantity") <= 0, "non-positive quantity")])
    assert [r.id for r in valid.collect()] == [1]
    assert {r.id: r._reject_reason for r in bad.collect()} == {2: "non-positive quantity", 3: "non-positive quantity"}
    assert valid.count() + bad.count() == df.count()     # nothing silently lost
