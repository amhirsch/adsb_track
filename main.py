import asyncio
import os
from adsb_track.stream import FlightRecorder

gs_lat = float(os.environ.get('ADSB_LATITUDE', '34'))
gs_lon = float(os.environ.get('ADSB_LONGITUDE', '-118.2'))
flights = FlightRecorder('pi.lan.xanderhirsch.us', 'test2.sqlite3', gs_lat, gs_lon)

async def start_program():
    await asyncio.gather(flights.print_status(), flights.record())

asyncio.run(start_program())

