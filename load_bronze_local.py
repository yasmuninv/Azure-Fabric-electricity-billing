# load_bronze_local.py -- run in VS Code, no Fabric compute used
import pandas as pd
from azure.identity import InteractiveBrowserCredential
from deltalake import write_deltalake

token = InteractiveBrowserCredential().get_token(
    "https://storage.azure.com/.default").token
opts = {"bearer_token": token, "use_fabric_endpoint": "true"}

BASE = ("abfss://electrisity%20billing@onelake.dfs.fabric.microsoft.com/"
        "energy_lakehouse.Lakehouse/Tables/dbo/")


for csv, table in [
    ("dim_meter_tariff.csv", "dim_meter_tariff_bronze"),
    ("dim_tariff_rate.csv",  "dim_tariff_rate_bronze"),
    ("dim_date.csv",         "dim_date_bronze"),
]:
    df = pd.read_csv(csv)
    write_deltalake(BASE + table, df, mode="overwrite", storage_options=opts)
    print(f"wrote {table}: {len(df)} rows")
