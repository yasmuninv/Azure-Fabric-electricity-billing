# silver_transform_databricks.py
#
# Databricks-native Silver-layer transform for the smart-meter platform.
# Companion to the Fabric-based plan in Azure_Data_Platform_Project: this
# reads Bronze straight from ADLS Gen2 -- the same storage account that
# Fabric's OneLake shortcut points at, so Bronze is written once and read
# by both engines -- applies the cleaning/conforming rules, and writes
# Silver back out. The transform logic below is plain PySpark: point it
# at a Fabric Lakehouse "Files" path instead of abfss:// and it runs
# unchanged inside a Fabric notebook too.
#
# Tested locally against a small synthetic sample (bronze_sample/, see
# README) using plain Spark + Parquet, since Delta needs Maven access
# this sandbox doesn't have. Set FILE_FORMAT = "delta" for the real
# Databricks/Fabric run -- nothing else below changes.

from pyspark.sql import SparkSession, functions as F, Window

# ----------------------------------------------------------------------
# Config -- the only section you should need to touch per environment.
# ----------------------------------------------------------------------
FILE_FORMAT = "delta"  # "delta" on Databricks/Fabric; local test run uses "parquet"
BRONZE_ROOT = "abfss://bronze@<storage_account>.dfs.core.windows.net"
REF_ROOT    = "abfss://reference@<storage_account>.dfs.core.windows.net"
SILVER_ROOT = "abfss://silver@<storage_account>.dfs.core.windows.net"


def dedupe_readings(readings):
    """Bronze can contain re-delivered/duplicate events from the stream;
    keep the most recently ingested copy of each (meter_id, reading_ts)."""
    w = Window.partitionBy("meter_id", "reading_ts").orderBy(F.col("_ingested_at").desc())
    return (
        readings
        .withColumn("reading_ts", F.to_timestamp("reading_ts"))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        # MISSING readings carry a sentinel 0.0 upstream; null it out so
        # it isn't silently averaged into real consumption downstream.
        .withColumn("interval_kwh", F.when(F.col("quality") == "MISSING", None)
                                      .otherwise(F.col("interval_kwh")))
    )


def reconcile_weather(history, forecast):
    """History and forecast overlap for a few days at the boundary
    (forecast's past_days bridges the archive's ~5-day lag). Prefer the
    confirmed history row wherever both exist for the same hour+location."""
    tagged = (
        history.withColumn("_priority", F.lit(1))
        .unionByName(forecast.withColumn("_priority", F.lit(2)), allowMissingColumns=True)
        .withColumn("weather_ts", F.to_timestamp("weather_ts"))
    )
    w = Window.partitionBy("weather_ts", "latitude", "longitude").orderBy("_priority")
    return (
        tagged.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn", "_priority")
    )


def resolve_tariff(readings, tariff_history):
    """As-of join: pick the tariff_code that was actually valid at the
    moment of each reading (SCD2), instead of a static meter_id join that
    would be wrong for the ~12% of meters that switch tariff mid-year."""
    hist = (
        tariff_history
        .withColumn("valid_from", F.to_timestamp("valid_from"))
        .withColumn("valid_to", F.to_timestamp(F.date_add(F.col("valid_to"), 1)))  # exclusive upper bound
    )
    return (
        readings.alias("r")
        .join(
            hist.alias("t"),
            (F.col("r.meter_id") == F.col("t.meter_id"))
            & (F.col("r.reading_ts") >= F.col("t.valid_from"))
            & (F.col("r.reading_ts") <  F.col("t.valid_to")),
            "left",
        )
        .select("r.*", F.col("t.tariff_code"))
    )


def join_weather(readings, weather):
    """15-min reads join hourly weather by truncating to the hour."""
    return (
        readings
        .withColumn("weather_hour", F.date_trunc("hour", F.col("reading_ts")))
        .join(
            weather.select(
                F.col("weather_ts").alias("weather_hour"),
                "temperature_2m", "relative_humidity_2m",
            ),
            on="weather_hour", how="left",
        )
        .drop("weather_hour")
    )


def main(spark):
    readings     = spark.read.format(FILE_FORMAT).load(f"{BRONZE_ROOT}/meter_readings")
    weather_hist = spark.read.format(FILE_FORMAT).load(f"{BRONZE_ROOT}/weather_history")
    weather_fcst = spark.read.format(FILE_FORMAT).load(f"{BRONZE_ROOT}/weather_forecast")
    tariff_hist  = spark.read.format(FILE_FORMAT).load(f"{REF_ROOT}/dim_meter_tariff")

    clean    = dedupe_readings(readings)
    weather  = reconcile_weather(weather_hist, weather_fcst)
    tariffed = resolve_tariff(clean, tariff_hist)
    enriched = join_weather(tariffed, weather)

    (enriched.write.format(FILE_FORMAT).mode("overwrite")
     .partitionBy("meter_id")
     .save(f"{SILVER_ROOT}/meter_readings_enriched"))
    return enriched


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-transform").getOrCreate()
    main(spark)