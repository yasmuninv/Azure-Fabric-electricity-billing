import pandas as pd
from deltalake import DeltaTable, write_deltalake
from azure.identity import InteractiveBrowserCredential
from datetime import datetime
import pyarrow as pa
import pyarrow.compute as pc

# Authenticate
token = InteractiveBrowserCredential().get_token(
    "https://storage.azure.com/.default"
).token

opts = {"bearer_token": token, "use_fabric_endpoint": "true"}

BASE = ("abfss://electrisity%20billing@onelake.dfs.fabric.microsoft.com/"
        "energy_lakehouse.Lakehouse/Tables/")

# ---------------------------------------------------------
# Helper: read a Delta table into a Pandas DataFrame
# ---------------------------------------------------------

def read_delta(path):
    dt = DeltaTable(path, storage_options=opts)
    return dt.to_pandas()

# ---------------------------------------------------------
# Phase 3 — SILVER
# ---------------------------------------------------------

# -----------------------------
# Cell 1 — meter readings
# -----------------------------
print("Loading Bronze meter readings...")
bronze = read_delta(BASE + "meter_readings_schema/meter_readings")

# Cast
bronze["reading_ts"] = pd.to_datetime(bronze["reading_ts"])
bronze["interval_kwh"] = bronze["interval_kwh"].astype(float)
bronze["voltage"] = bronze["voltage"].astype(float)

# Filter window
W_START = pd.Timestamp("2025-06-29")
W_END   = pd.Timestamp("2026-06-30")
bronze = bronze[(bronze["reading_ts"] >= W_START) & (bronze["reading_ts"] < W_END)]

# Deduplicate
bronze = bronze.drop_duplicates(subset=["meter_id", "reading_ts"])

# Quarantine
quarantine = bronze[bronze["quality"] != "GOOD"]
write_deltalake(BASE + "dbo/meter_readings_quarantine",quarantine,mode="overwrite",storage_options=opts)
print("meter_readings_quarantine:", len(quarantine))

# GOOD readings
good = bronze[bronze["quality"] == "GOOD"]

# Tariff join
tariff = read_delta(BASE + "dbo/dim_meter_tariff_bronze")
tariff["valid_from"] = pd.to_datetime(tariff["valid_from"])
tariff["valid_to"]   = pd.to_datetime(tariff["valid_to"])

# As-of join
def find_tariff(row):
    ts = row["reading_ts"].date()
    subset = tariff[(tariff["meter_id"] == row["meter_id"]) & (tariff["valid_from"] <= ts) & (tariff["valid_to"] >= ts)]
    if len(subset) == 0:
        return None
    return subset.iloc[0]["tariff_code"]

good["tariff_code"] = good.apply(find_tariff, axis=1)

# hour_ts
good["hour_ts"] = good["reading_ts"].dt.floor("H")

# Write Silver
write_deltalake(BASE + "dbo/meter_readings_silver",good,mode="overwrite",storage_options=opts)
print("meter_readings_silver:", len(good))

# -----------------------------
# Cell 2 — weather_silver
# -----------------------------
print("Loading weather Bronze...")
hist = read_delta(BASE + "dbo/weather_history_bronze")
fc   = read_delta(BASE + "dbo/weather_forecast_bronze")

hist["src"] = 1
fc["src"] = 2

w = pd.concat([hist, fc], ignore_index=True)
w["weather_ts"] = pd.to_datetime(w["weather_ts"])

w = w[(w["weather_ts"] >= W_START) & (w["weather_ts"] < W_END)]

# Pick history over forecast when overlapping
w = w.sort_values(["weather_ts", "src"])
w = w.drop_duplicates(subset=["weather_ts"], keep="first")
w = w.drop(columns=["src"])

write_deltalake(BASE + "dbo/weather_silver", w, mode="overwrite", storage_options=opts)
print("weather_silver:", len(w))

# -----------------------------
# Cell 3 — promote dimensions
# -----------------------------
dims = [  "dim_location",  "dim_customer",  "dim_meter",  "dim_meter_tariff",  "dim_tariff",  "dim_tariff_rate","dim_date"]

for d in dims:
    bronze_dim = read_delta(BASE + f"dbo/{d}_bronze")
    write_deltalake(BASE + f"dbo/{d}_silver", bronze_dim, mode="overwrite", storage_options=opts)
    print("promoted", d + "_silver")

print("SILVER COMPLETE")
