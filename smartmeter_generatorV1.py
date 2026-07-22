# smartmeter_generator.py
# Synthetic residential smart-meter reading generator.
# This file is the SINGLE SOURCE OF TRUTH for the shared config below;
# reference_builder.py and weather_source.py import from it so the three
# stay aligned (same fleet, same location, same date window).

import math
import time
import random
from datetime import datetime, date, timedelta, timezone

# ===================== SHARED CONFIG =====================
N_HOUSEHOLDS = 50
FLEET_SEED = 42

# Inclusive date window. Meter readings, weather, and dim_date all use this.
WINDOW_START = "2025-06-29"; WINDOW_END   = "2026-06-29"

# Coordinates here MUST match what weather_source.py queries.
LOCATION = { "location_id": 1, "city": "Oslo", "country": "NO", "latitude": 59.91, "longitude": 10.75}

TARIFFS = ["standard", "economy7", "ev_special", "green"]
# =========================================================

# ---------- 1. Define a fleet of households (meters) ----------
def build_fleet(n_households=N_HOUSEHOLDS, seed=FLEET_SEED):
    rng = random.Random(seed)
    fleet = []
    for i in range(n_households):
        fleet.append({
            "meter_id": f"MTR-{i:05d}",
            "customer_id": f"CUST-{i:05d}",
            "base_load_kw": round(rng.uniform(0.15, 0.6), 3),  # always-on draw
            "peak_factor": round(rng.uniform(1.5, 4.0), 2),    # how spiky usage is
            "occupants": rng.randint(1, 5),
            "has_solar": rng.random() < 0.2,
            "tariff": rng.choice(TARIFFS),
        })
    return fleet

# ---------- 2. Shape functions that make usage realistic ----------
def daily_shape(hour):
    # Two peaks: morning (~7.5h) and a larger evening one (~19.5h)
    morning = math.exp(-((hour - 7.5) ** 2) / 4.0)
    evening = math.exp(-((hour - 19.5) ** 2) / 6.0)
    overnight = 0.15
    return overnight + 0.6 * morning + 1.0 * evening

def seasonal_factor(month):
    # Higher in winter/summer (heating + cooling), milder in between
    return 1.0 + 0.4 * math.cos((month - 1) / 12.0 * 2 * math.pi)

def solar_offset(hour, has_solar):
    if not has_solar:
        return 0.0
    # Solar production lowers net consumption around midday
    return -0.8 * math.exp(-((hour - 13) ** 2) / 6.0)

# ---------- 3. Build one reading for one meter at one time ----------
def reading_for(meter, ts, rng):
    hour = ts.hour + ts.minute / 60.0
    weekend_boost = 1.15 if ts.weekday() >= 5 else 1.0

    shape = daily_shape(hour) * meter["peak_factor"] * weekend_boost
    season = seasonal_factor(ts.month)

    kw = meter["base_load_kw"]
    kw += shape * 0.4 * season * (0.6 + 0.1 * meter["occupants"])
    kw += solar_offset(hour, meter["has_solar"]) * season
    kw *= rng.uniform(0.9, 1.1)  # measurement noise
    kw = max(kw, 0.0)

    interval_kwh = round(kw * 0.25, 4)  # 15-min interval => kW * 0.25

    return {
        "meter_id": meter["meter_id"],
        "customer_id": meter["customer_id"],
        "tariff": meter["tariff"],
        "reading_ts": ts.astimezone(timezone.utc).isoformat(),
        "interval_kwh": interval_kwh,
        "voltage": round(rng.gauss(230, 3), 1),
        "quality": "GOOD",
    }

# ---------- 4. Occasionally inject realistic faults ----------
def maybe_inject_anomaly(reading, rng, rate=0.01):
    if rng.random() < rate:
        kind = rng.choice(["spike", "dropout", "stuck"])

        if kind == "spike":
            reading["interval_kwh"] = round(
                reading["interval_kwh"] * rng.uniform(5, 12), 4)
            reading["quality"] = "SUSPECT_HIGH"

        elif kind == "dropout":
            reading["interval_kwh"] = 0.0
            reading["voltage"] = 0.0
            reading["quality"] = "MISSING"

        else:  # stuck meter
            reading["quality"] = "STUCK"

    return reading

# ---------- 5. Endless generator of readings ----------
def stream_readings(fleet, start=None, step_minutes=15,
                    speedup=None, anomaly_rate=0.01, seed=7):

    rng = random.Random(seed)
    # Default to a clean, grid-aligned start (top of the current hour, UTC).
    if start is None:
        start = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0)
    sim_time = start

    while True:
        for meter in fleet:
            r = reading_for(meter, sim_time, rng)
            yield maybe_inject_anomaly(r, rng, anomaly_rate)

        sim_time += timedelta(minutes=step_minutes)

        if speedup:  # real-time pacing for demos
            time.sleep(step_minutes * 60 / speedup)

def window_slot_count(start_date=WINDOW_START, end_date=WINDOW_END,
                      n_households=N_HOUSEHOLDS, step_minutes=15):
    """Total readings needed to cover the inclusive date window."""
    days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    slots_per_day = (24 * 60) // step_minutes
    return days * slots_per_day * n_households

# ---------- 6. Generate the full window to a file ----------
if __name__ == "__main__":
    import json, itertools

    fleet = build_fleet()  # uses N_HOUSEHOLDS, FLEET_SEED

    start = datetime.combine(
        date.fromisoformat(WINDOW_START), datetime.min.time(),
        tzinfo=timezone.utc)
    total = window_slot_count()
    gen = stream_readings(fleet, start=start, step_minutes=15)

    # NOTE: the full year is ~1.76M rows (a few hundred MB). For a quick
    # test, replace `total` with e.g. N_HOUSEHOLDS * 96 (a single day).
    with open("sample_readings.jsonl", "w") as f:
        for r in itertools.islice(gen, total):
            f.write(json.dumps(r) + "\n")

    print(f"wrote sample_readings.jsonl ({total} rows, {N_HOUSEHOLDS} meters, "
          f"{WINDOW_START} -> {WINDOW_END})")