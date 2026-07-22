# reference_builder.py
# Builds the static reference (dimension) tables for the platform.
# Imports shared config from smartmeter_generator so EVERYTHING reconciles:
# same fleet (IDs), same location (coords), same date window (dim_date).
import random
import pandas as pd
from datetime import date, timedelta

from smartmeter_generator import (
    build_fleet, LOCATION, WINDOW_START, WINDOW_END, FLEET_SEED, TARIFFS,
)

def build_dim_location():
    return pd.DataFrame([LOCATION])

def build_dim_customer(fleet, seed=FLEET_SEED):
    rng = random.Random(seed + 1)
    rows = []
    for m in fleet:
        join = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1400))
        rows.append({
            "customer_id": m["customer_id"],
            "segment": "residential",
            "occupants": m["occupants"],
            "join_date": join.isoformat(),
        })
    return pd.DataFrame(rows)

def build_dim_meter(fleet):
    # Note: tariff is deliberately NOT here -- it's not a static meter
    # attribute, it's a customer contract that can change mid-window.
    # See build_dim_meter_tariff() below for the SCD2 history.
    rows = []
    for m in fleet:
        rows.append({
            "meter_id": m["meter_id"],
            "customer_id": m["customer_id"],
            "location_id": LOCATION["location_id"],
            "has_solar": m["has_solar"],
            "meter_model": "SM-" + ("S" if m["has_solar"] else "B"),
        })
    return pd.DataFrame(rows)

def build_dim_meter_tariff(fleet, seed=FLEET_SEED, change_rate=0.12):
    """
    SCD2 history of each meter's tariff across the window. ~change_rate
    of meters switch tariff once, at a random point in the middle 60% of
    the window, so Silver has a real reason to do an as-of join
    (reading_ts BETWEEN valid_from AND valid_to) instead of a static one
    on meter_id. Everyone else gets a single row spanning the full window.
    """
    rng = random.Random(seed + 3)  # own stream: fleet uses seed, dim_customer uses seed+1
    w_start, w_end = date.fromisoformat(WINDOW_START), date.fromisoformat(WINDOW_END)
    total_days = (w_end - w_start).days

    rows, sk = [], 1
    for m in fleet:
        original = m["tariff"]
        if rng.random() < change_rate:
            offset = rng.randint(int(total_days * 0.2), int(total_days * 0.8))
            change_date = w_start + timedelta(days=offset)
            new_tariff = rng.choice([t for t in TARIFFS if t != original])
            rows += [
                {"meter_tariff_sk": sk,     "meter_id": m["meter_id"], "tariff_code": original,
                 "valid_from": w_start.isoformat(), "valid_to": (change_date - timedelta(days=1)).isoformat(),
                 "is_current": False},
                {"meter_tariff_sk": sk + 1, "meter_id": m["meter_id"], "tariff_code": new_tariff,
                 "valid_from": change_date.isoformat(), "valid_to": w_end.isoformat(),
                 "is_current": True},
            ]
            sk += 2
        else:
            rows.append({"meter_tariff_sk": sk, "meter_id": m["meter_id"], "tariff_code": original,
                         "valid_from": w_start.isoformat(), "valid_to": w_end.isoformat(),
                         "is_current": True})
            sk += 1
    return pd.DataFrame(rows)

def build_dim_tariff():
    return pd.DataFrame([
        {"tariff_code": "standard",   "tariff_name": "Standard Flat",
         "standing_charge": 0.45, "currency": "EUR"},
        {"tariff_code": "economy7",   "tariff_name": "Economy 7",
         "standing_charge": 0.48, "currency": "EUR"},
        {"tariff_code": "ev_special", "tariff_name": "EV Overnight",
         "standing_charge": 0.55, "currency": "EUR"},
        {"tariff_code": "green",      "tariff_name": "100% Green",
         "standing_charge": 0.50, "currency": "EUR"},
    ])

def build_dim_tariff_rate():
    # Time-of-use windows: [start_hour, end_hour) in local time.
    rows = [
        ("standard",   "all_day",   0, 24, 0.28),
        ("economy7",   "night",     0,  7, 0.14),
        ("economy7",   "day",       7, 24, 0.31),
        ("ev_special", "ev_window", 0,  5, 0.09),
        ("ev_special", "day",       5, 24, 0.30),
        ("green",      "all_day",   0, 24, 0.32),
    ]
    return pd.DataFrame(rows, columns=[
        "tariff_code", "period_name", "start_hour",
        "end_hour", "unit_rate"])

def build_dim_date(start=WINDOW_START, end=WINDOW_END):
    seasons = {12: "winter", 1: "winter", 2: "winter",
               3: "spring", 4: "spring", 5: "spring",
               6: "summer", 7: "summer", 8: "summer",
               9: "autumn", 10: "autumn", 11: "autumn"}
    rows = []
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        rows.append({
            "date": d.isoformat(), "year": d.year,
            "month": d.month, "day": d.day,
            "day_of_week": d.isoweekday(),       # 1=Mon..7=Sun
            "is_weekend": d.isoweekday() >= 6,
            "season": seasons[d.month],
        })
        d += timedelta(days=1)
    return pd.DataFrame(rows)

if __name__ == "__main__":
    fleet = build_fleet()  # same fleet as the generator
    tables = {
        "dim_location":    build_dim_location(),
        "dim_customer":    build_dim_customer(fleet),
        "dim_meter":       build_dim_meter(fleet),
        "dim_meter_tariff": build_dim_meter_tariff(fleet),
        "dim_tariff":      build_dim_tariff(),
        "dim_tariff_rate": build_dim_tariff_rate(),
        "dim_date":        build_dim_date(),
    }
    for name, df in tables.items():
        df.to_csv(f"{name}.csv", index=False)
        print(f"{name}: {len(df)} rows -> {name}.csv")