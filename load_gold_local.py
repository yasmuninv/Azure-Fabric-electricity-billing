# # build_gold_local.py -- Phase 4 Gold, no Spark, no Fabric compute
import pandas as pd
from azure.identity import InteractiveBrowserCredential
from deltalake import DeltaTable, write_deltalake

token = InteractiveBrowserCredential().get_token(
    "https://storage.azure.com/.default").token
opts = {"bearer_token": token, "use_fabric_endpoint": "true"}
BASE = ("abfss://electrisity%20billing@onelake.dfs.fabric.microsoft.com/"
        "energy_lakehouse.Lakehouse/Tables/dbo/")

silver = DeltaTable(BASE + "meter_readings_silver", storage_options=opts).to_pandas()
rate   = DeltaTable(BASE + "dim_tariff_rate_silver", storage_options=opts).to_pandas()

ts = pd.to_datetime(silver["reading_ts"], utc=True)
silver["local_hour"] = ts.dt.tz_convert("Europe/Oslo").dt.hour
silver["date_key"]   = ts.dt.date.astype(str)

fact = silver.merge(rate, on="tariff_code")
fact = fact[(fact.local_hour >= fact.start_hour) &
            (fact.local_hour <  fact.end_hour)].copy()
fact["cost"] = (fact.interval_kwh * fact.unit_rate).round(6)
fact = fact[["meter_id", "reading_ts", "hour_ts", "date_key",
             "interval_kwh", "voltage", "tariff_code",
             "period_name", "unit_rate", "cost"]]

daily = (fact.groupby(["meter_id", "date_key", "tariff_code"], as_index=False)
             .agg(kwh=("interval_kwh", "sum"),
                  cost=("cost", "sum"),
                  avg_voltage=("voltage", "mean")))

write_deltalake(BASE + "fact_meter_reading_gold", fact,  mode="overwrite", storage_options=opts)
write_deltalake(BASE + "fact_daily_usage_gold",  daily, mode="overwrite", storage_options=opts)
print(f"fact: {len(fact):,}  daily: {len(daily):,}")

# For the Power BI mastery build (weather at daily grain):
weather = DeltaTable(BASE + "weather_silver", storage_options=opts).to_pandas()
weather["date_key"] = pd.to_datetime(weather["weather_ts"]).dt.date.astype(str)
wd = (weather.groupby("date_key", as_index=False)
             .agg(avg_temp_c=("temperature_2m", "mean"),
                  min_temp_c=("temperature_2m", "min"),
                  max_temp_c=("temperature_2m", "max"),
                  avg_cloud_pct=("cloud_cover", "mean"),
                  avg_radiation=("shortwave_radiation", "mean")))
write_deltalake(BASE + "weather_daily_gold", wd, mode="overwrite", storage_options=opts)
print(f"weather_daily_gold: {len(wd):,}")