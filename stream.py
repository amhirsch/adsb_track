import os
import sqlite3
import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient

from adsb_track.aircraft import Aircraft
from adsb_track.database import DBSQLite


TC_POS = tuple(range(9, 19)) + tuple(range(20, 23))


class FlightRecorder(TcpClient):
    def __init__(self, host, db, gs_lat, gs_lon, port=30005, rawtype='beast', buffer=25):
        super(FlightRecorder, self).__init__(host, port, rawtype)
        self.gs_lat = gs_lat
        self.gs_lon = gs_lon
        self.airspace = {}
        self.db = DBSQLite(db, buffer)

    def process_msg(self, msg, ts, icao, tc):
        if tc in TC_POS:
            self.process_position(msg, ts, icao, tc)
        elif tc == 19:
            self.process_velocity(msg, ts, icao)
        elif tc in tuple(range(1,5)):
            self.process_ident(msg, ts, icao, tc)

    def process_position(self, msg, ts, icao, tc):
        alt_src = 'BARO' if tc < 19 else 'GNSS'
        alt = pms.adsb.altitude(msg)
        lat, lon = pms.adsb.position_with_ref(msg, self.gs_lat, self.gs_lon)

        self.db.record_position(ts, icao, lat, lon, alt, alt_src)

        if icao not in self.airspace:
            self.airspace[icao] = Aircraft(icao)
        self.airspace[icao].update_position(ts, lat, lon, alt)

    def process_velocity(self, msg, ts, icao):
        velocity = pms.adsb.velocity(msg, True)
        heading = velocity[1]
        speed = velocity[0]
        vertical_speed = velocity[2]

        self.db.record_velocity(ts, icao, *velocity)

        if icao not in self.airspace:
            self.airspace[icao] = Aircraft(icao)
        self.airspace[icao].update_velocity(ts, heading, speed, vertical_speed)

    def process_ident(self, msg, ts, icao, tc):
        callsign = pms.adsb.callsign(msg).strip('_')
        category = pms.adsb.category(msg)

        self.db.record_ident(ts, icao, callsign, tc, category)

        if icao not in self.airspace:
            self.airspace[icao] = Aircraft(icao)
        self.airspace[icao].update_callsign(ts, callsign)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if not all((len(msg)==28, pms.df(msg)==17, pms.crc(msg)==0)):
                continue

            icao = pms.adsb.icao(msg)
            tc = pms.adsb.typecode(msg)

            self.process_msg(msg, ts, icao, tc)

    def record(self):
        try:
            self.run()
        except KeyboardInterrupt:
            self.db.commit()


if __name__ == '__main__':
    gs_lat = float(os.environ.get('ADSB_LATITUDE', '34'))
    gs_lon = float(os.environ.get('ADSB_LONGITUDE', '-118.2'))
    # client = ADSBClient('pi.lan.xanderhirsch.us')
    # client.run()
    client = FlightRecorder('pi.lan.xanderhirsch.us', 'test.db', gs_lat, gs_lon)
    client.record()
