# weather_source.py
# Pulls weather from Open-Meteo and reshapes it into tidy hourly rows.
# Imports location + date window from smartmeter_generator so the weather
# always matches the meters' coordinates and time frame.
#This endpoint gives historical weather data:
#ARCHIVE_URL: temperature, wind, humidity, precipitation, solar radiation, soil moisture, etc. 
#hourly or daily
#FORECAST_URL: This endpoint gives current + future weather forecasts:
#temperature, humidity, wind, precipitation, clouds, pressure, etc. 
#hourly, daily, and even 15‑min data 

import time
import requests
import pandas as pd

from smartmeter_generator import LOCATION, WINDOW_START, WINDOW_END

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/archive"

# Variables chosen for their relevance to energy demand and solar.
HOURLY_VARS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "cloud_cover",
    "wind_speed_10m",
    "shortwave_radiation",
]

def _get(url, params, retries=4):
    """GET with a simple exponential backoff on rate limits."""
    resp = None
    for attempt in range(retries):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:        # too many requests
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
    resp.raise_for_status()

def _to_frame(payload, lat, lon):
    # payload["hourly"] holds parallel arrays incl. "time", so a
    # DataFrame falls straight out of it.
    df = pd.DataFrame(payload["hourly"]).rename(
        columns={"time": "weather_ts"})
    df["latitude"] = lat
    df["longitude"] = lon
    return df

def fetch_history(lat, lon, start_date, end_date, hourly=HOURLY_VARS):
    payload = _get(ARCHIVE_URL, {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "hourly": ",".join(hourly),
        "timezone": "UTC",      # align with UTC meter timestamps
    })
    return _to_frame(payload, lat, lon)

def fetch_forecast(lat, lon, forecast_days=7, past_days=7, hourly=HOURLY_VARS):
    # past_days bridges the ~5-day lag of the archive up to today.
    payload = _get(FORECAST_URL, {
        "latitude": lat, "longitude": lon,
        "forecast_days": forecast_days,
        "past_days": past_days,
        "hourly": ",".join(hourly),
        "timezone": "UTC",
    })
    return _to_frame(payload, lat, lon)

if __name__ == "__main__":
    lat = LOCATION["latitude"]
    lon = LOCATION["longitude"]

    # History for the exact meter window (matches WINDOW_START/END).
    hist = fetch_history(lat, lon, WINDOW_START, WINDOW_END)
    hist.to_csv("weather_history.csv", index=False)
    print(f"history:  {len(hist)} hourly rows -> weather_history.csv "
          f"({WINDOW_START} -> {WINDOW_END})")

    # Recent + near-future, to bridge any archive lag at the window's tail.
    fc = fetch_forecast(lat, lon, forecast_days=7, past_days=7)
    fc.to_csv("weather_forecast.csv", index=False)
    print(f"forecast: {len(fc)} hourly rows -> weather_forecast.csv")