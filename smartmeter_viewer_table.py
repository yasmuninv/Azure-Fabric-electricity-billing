import json
import pandas as pd

FILE = "sample_readings.jsonl"

def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            rows.append(json.loads(line))
    return pd.DataFrame(rows)

def main():
    df = load_jsonl(FILE)

    # Convert timestamps
    df["reading_ts"] = pd.to_datetime(df["reading_ts"])

    print("\n--- DATA TABLE ---")
    print(df)

    print("\n--- FIRST 10 ROWS ---")
    print(df.head(10))

    print("\n--- SUMMARY ---")
    print(df.describe(include="all"))

if __name__ == "__main__":
    main()
