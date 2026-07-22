import json
import pandas as pd
import plotly.graph_objects as go

FILE = "sample_readings.jsonl"

def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            rows.append(json.loads(line))
    return pd.DataFrame(rows)

def main():
    df = load_jsonl(FILE)

    print("\n--- BASIC INFO ---")
    print(df.head())
    print("\nColumns:", df.columns.tolist())
    print("\nTotal rows:", len(df))

    # Convert timestamps
    df["reading_ts"] = pd.to_datetime(df["reading_ts"])

    # Plot consumption
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=df["reading_ts"],
        y=df["interval_kwh"],
        mode="lines",
        name="kWh per interval"
    ))
    fig1.update_layout(
        title="Smart Meter Consumption (15‑min intervals)",
        xaxis_title="Time",
        yaxis_title="kWh"
    )
    fig1.show()

    # Plot voltage
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df["reading_ts"],
        y=df["voltage"],
        mode="lines",
        name="Voltage"
    ))
    fig2.update_layout(
        title="Voltage Over Time",
        xaxis_title="Time",
        yaxis_title="Voltage (V)"
    )
    fig2.show()

    # Show anomalies
    anomalies = df[df["quality"] != "GOOD"]
    print("\n--- ANOMALIES ---")
    print(anomalies.head())
    print("Total anomalies:", len(anomalies))

if __name__ == "__main__":
    main()
