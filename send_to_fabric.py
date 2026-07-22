# send_to_fabric.py -- streams generated readings into a Fabric Eventstream
#
# FIXED to match the real smartmeter_generator API:
#   - build_fleet() takes n_households (not num_households); calling it with
#     no args uses the shared N_HOUSEHOLDS / FLEET_SEED config, which keeps
#     the stream consistent with reference_builder.py and weather_source.py.
#   - stream_readings() is a SYNCHRONOUS generator that yields ONE reading
#     at a time (not an async generator of lists), and its kwarg is
#     step_minutes (not interval_minutes). So we pull readings with next()
#     instead of "async for".

import os
import sys
import json
import time
import asyncio
from datetime import datetime, date, timezone
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from smartmeter_generator import (
    build_fleet, stream_readings, WINDOW_START, window_slot_count,
)
 
CONNECTION_STR = os.environ["FABRIC_ES_CONNECTION_STRING"]
 
async def replay(batch_size=500):
    """Stream the whole historical window, then stop."""
    fleet = build_fleet()
    start = datetime.combine(date.fromisoformat(WINDOW_START),
                             datetime.min.time(), tzinfo=timezone.utc)
    gen = stream_readings(fleet, start=start, step_minutes=15)
    total = window_slot_count()
 
    producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)
    sent, t0 = 0, time.time()
    async with producer:
        while sent < total:
            batch = await producer.create_batch()
            for _ in range(min(batch_size, total - sent)):
                batch.add(EventData(json.dumps(next(gen))))
            await producer.send_batch(batch)
            sent += min(batch_size, total - sent)
            if sent % 100_000 < batch_size:
                rate = sent / (time.time() - t0)
                eta = (total - sent) / rate / 60
                print(f"{sent:,}/{total:,} sent ({rate:,.0f}/s, ~{eta:.0f} min left)")
    print(f"done: {sent:,} readings covering the full window")
 
async def demo(minutes=2, events_per_second=20, batch_size=50):
    """Live-feed simulation starting now. Auto-stops after `minutes`."""
    fleet = build_fleet()
    gen = stream_readings(fleet, step_minutes=15)
    deadline = time.time() + minutes * 60
 
    producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)
    async with producer:
        while time.time() < deadline:
            batch = await producer.create_batch()
            for _ in range(batch_size):
                batch.add(EventData(json.dumps(next(gen))))
            await producer.send_batch(batch)
            print(f"sent {batch_size} events")
            await asyncio.sleep(batch_size / events_per_second)
    print(f"demo finished after {minutes} min -- remember these timestamps "
          f"are ahead of the wall clock; exclude them in Silver")
 
if __name__ == "__main__":
    if "--demo" in sys.argv:
        mins = 2
        if "--minutes" in sys.argv:
            mins = float(sys.argv[sys.argv.index("--minutes") + 1])
        asyncio.run(demo(minutes=mins))
    else:
        asyncio.run(replay())