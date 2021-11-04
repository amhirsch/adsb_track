from datetime import datetime as dt

import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient

from adsb_track.aircraft import Airspace
from adsb_track.database import DBSQLite
from adsb_track.session import session_hash


def current_timestamp():
    return dt.now().timestamp()


TC_POS = tuple(range(9, 19)) + tuple(range(20, 23))
TC_IDENT = tuple(range(1, 5))


class FlightRecorder(TcpClient):

    def __init__(self,
                 host,
                 db,
                 gs_lat,
                 gs_lon,
                 port=30005,
                 rawtype='beast',
                 buffer=25):
        super(FlightRecorder, self).__init__(host, port, rawtype)
        now = current_timestamp()
        self.session_id = session_hash(host, port, rawtype, now)
        self.gs_lat = gs_lat
        self.gs_lon = gs_lon
        self.airspace = Airspace()
        self.db = DBSQLite(db, buffer)
        self.db.record_session_start(self.session_id, host, port, now)

    def process_msg(self, msg, ts, icao, tc):
        if tc in TC_POS:
            self.process_position(msg, ts, icao, tc)
        elif tc == 19:
            self.process_velocity(msg, ts, icao)
        elif tc in TC_IDENT:
            self.process_ident(msg, ts, icao, tc)

    def process_position(self, msg, ts, icao, tc):
        alt_src = 'BARO' if tc < 19 else 'GNSS'
        alt = pms.adsb.altitude(msg)
        lat, lon = pms.adsb.position_with_ref(msg, self.gs_lat, self.gs_lon)

        self.db.record_position(ts, icao, lat, lon, alt, alt_src)
        self.airspace.update_position(icao, ts, lat, lon, alt)

    def process_velocity(self, msg, ts, icao):
        velocity = pms.adsb.velocity(msg, True)
        heading = velocity[1]
        speed = velocity[0]
        vertical_speed = velocity[2]

        self.db.record_velocity(ts, icao, *velocity)
        self.airspace.update_velocity(icao, ts, heading, speed, vertical_speed)

    def process_ident(self, msg, ts, icao, tc):
        callsign = pms.adsb.callsign(msg).strip('_')
        category = pms.adsb.category(msg)

        self.db.record_ident(ts, icao, callsign, tc, category)
        self.airspace.update_callsign(icao, ts, callsign)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) == 28 and pms.df(msg) == 17 and pms.crc(msg) == 0:
                icao = pms.adsb.icao(msg)
                tc = pms.adsb.typecode(msg)
                self.process_msg(msg, ts, icao, tc)

    def record(self):
        try:
            self.run()
        except KeyboardInterrupt:
            self.db.record_session_stop(self.session_id, current_timestamp())
            self.db.commit()
