# test_silver_transform.py
# Unit tests for the four Silver transform functions, run against tiny
# hand-built DataFrames so each rule is checked in isolation. Works with
# pytest (`pytest test_silver_transform.py`) or standalone
# (`python test_silver_transform.py`).

from pyspark.sql import SparkSession
from silver_transform_databricks import (
    dedupe_readings, reconcile_weather, resolve_tariff, join_weather,
)

spark = SparkSession.builder.appName("silver-tests").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def test_dedupe_keeps_latest_ingest_and_nulls_missing():
    rows = [
        ("MTR-1", "2026-01-05T00:00:00+00:00", 1.0, "GOOD", "2026-01-08T00:00:00Z"),
        ("MTR-1", "2026-01-05T00:00:00+00:00", 1.0, "GOOD", "2026-01-08T01:00:00Z"),  # re-delivered dup, later ingest
        ("MTR-1", "2026-01-05T00:15:00+00:00", 0.0, "MISSING", "2026-01-08T00:00:00Z"),
    ]
    df = spark.createDataFrame(rows, ["meter_id", "reading_ts", "interval_kwh", "quality", "_ingested_at"])
    out = dedupe_readings(df).collect()

    assert len(out) == 2, f"expected 2 rows after dedup, got {len(out)}"
    kept = [r for r in out if r.reading_ts.isoformat().startswith("2026-01-05T00:00")][0]
    assert kept._ingested_at == "2026-01-08T01:00:00Z", "dedup did not keep the latest ingest"
    missing = [r for r in out if r.quality == "MISSING"][0]
    assert missing.interval_kwh is None, "MISSING reading should have interval_kwh nulled, not left at 0.0"
    print("PASS  dedupe_readings: keeps latest duplicate, nulls MISSING interval_kwh")


def test_reconcile_weather_prefers_history_on_overlap():
    hist = spark.createDataFrame(
        [("2026-01-06T00:00", 59.91, 10.75, -1.0), ("2026-01-06T01:00", 59.91, 10.75, -1.2)],
        ["weather_ts", "latitude", "longitude", "temperature_2m"])
    fcst = spark.createDataFrame(
        [("2026-01-06T00:00", 59.91, 10.75, 4.4),   # overlaps hist -- forecast's guess, should lose
         ("2026-01-06T02:00", 59.91, 10.75, -0.8)],  # forecast-only hour, should survive
        ["weather_ts", "latitude", "longitude", "temperature_2m"])
    out = {r.weather_ts.isoformat(): r.temperature_2m for r in reconcile_weather(hist, fcst).collect()}

    assert len(out) == 3, f"expected 3 distinct hours (2 hist + 1 forecast-only), got {len(out)}"
    assert out["2026-01-06T00:00:00"] == -1.0, "overlap should keep history's value, not forecast's"
    print("PASS  reconcile_weather: history wins on overlap, forecast-only hours still included")


def test_resolve_tariff_as_of_join():
    hist = spark.createDataFrame(
        [("MTR-1", "A", "2026-01-01", "2026-01-05"),
         ("MTR-1", "B", "2026-01-06", "2026-01-10")],
        ["meter_id", "tariff_code", "valid_from", "valid_to"])
    readings = spark.createDataFrame(
        [("MTR-1", "2026-01-03T12:00:00+00:00"),   # inside A's range
         ("MTR-1", "2026-01-06T00:00:00+00:00"),   # exactly on B's valid_from boundary
         ("MTR-1", "2026-01-10T23:00:00+00:00")],  # inside B's inclusive valid_to day
        ["meter_id", "reading_ts"]
    ).withColumn("reading_ts", __import__("pyspark.sql.functions", fromlist=["F"]).to_timestamp("reading_ts"))
    out = sorted(resolve_tariff(readings, hist).collect(), key=lambda r: r.reading_ts)

    assert [r.tariff_code for r in out] == ["A", "B", "B"], \
        f"as-of join resolved wrong tariffs: {[r.tariff_code for r in out]}"
    print("PASS  resolve_tariff: as-of join resolves the tariff that was actually active at read time")


def test_join_weather_truncates_to_hour():
    readings = spark.createDataFrame(
        [("MTR-1", "2026-01-06T14:45:00+00:00")], ["meter_id", "reading_ts"]
    ).withColumn("reading_ts", __import__("pyspark.sql.functions", fromlist=["F"]).to_timestamp("reading_ts"))
    weather = spark.createDataFrame(
        [("2026-01-06T14:00", 59.91, 10.75, 3.3, 88)],
        ["weather_ts", "latitude", "longitude", "temperature_2m", "relative_humidity_2m"]
    ).withColumn("weather_ts", __import__("pyspark.sql.functions", fromlist=["F"]).to_timestamp("weather_ts"))
    out = join_weather(readings, weather).collect()[0]

    assert out.temperature_2m == 3.3, "a 14:45 reading should join the 14:00 weather row"
    print("PASS  join_weather: 15-min reading correctly truncates to its containing hour")


if __name__ == "__main__":
    test_dedupe_keeps_latest_ingest_and_nulls_missing()
    test_reconcile_weather_prefers_history_on_overlap()
    test_resolve_tariff_as_of_join()
    test_join_weather_truncates_to_hour()
    print("\nAll tests passed.")
    spark.stop()