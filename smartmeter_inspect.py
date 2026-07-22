# smartmeter_inspect.py -- quick sanity check of the generated data
import pandas as pd
df = pd.read_json("sample_readings.jsonl", lines=True)
print(df.shape)
print(df.head())
print(df["quality"].value_counts())
# Average load curve by hour of day (should show morning + evening peaks)
df["hour"] = pd.to_datetime(df["reading_ts"]).dt.hour
print(df.groupby("hour")["interval_kwh"].mean())