from datetime import datetime as dt
import hashlib
import struct

import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient

from adsb_track.database import Database


class FlightRecorder(TcpClient):

    self.TC_POS = tuple(range(9, 19)) + tuple(range(20, 23))
    self.TC_IDENT = tuple(range(1, 5))

    @staticmethod
    def create_session_hash(host: str, port: int, rawtype: str, start: float):
        """Creates a unique has for the session.

        Args:
            host (str): Session host
            port (int): Session port
            rawtype (str): Session rawtype
            start (float): Session start time.
        Returns:
            str: SHA-1 hash of the session.
        """
        h = hashlib.sha1()
        h.update(str.encode(host))
        h.update(int.to_bytes(port, 2, 'big'))
        h.update(str.encode(rawtype))
        h.update(bytes(struct.pack('d', start)))
        return h.hexdigest()

    def __init__(self,
                 host,
                 db,
                 gs_lat,
                 gs_lon,
                 port=30002,
                 rawtype='raw',
                 buffer=25):
        super(FlightRecorder, self).__init__(host, port, rawtype)
        now = dt.now()
        self.session_hash = self.create_session_hash(host, port, rawtype,
                                                     now.timestamp())
        self.gs_lat = gs_lat
        self.gs_lon = gs_lon
        self.db = Database('sqlite', db)
        self.db.record_session_start(self.session_hash, host, port, now)

    def process_msg(self, msg, ts, icao, tc):
        if tc in self.TC_POS:
            self.process_position(msg, ts, icao, tc)
        elif tc == 19:
            self.process_velocity(msg, ts, icao)
        elif tc in self.TC_IDENT:
            self.process_ident(msg, ts, icao, tc)

    def process_position(self, msg, ts, icao, tc):
        alt_src = 'BARO' if tc < 19 else 'GNSS'
        alt = pms.adsb.altitude(msg)
        lat, lon = pms.adsb.position_with_ref(msg, self.gs_lat, self.gs_lon)

        self.db.record_position(ts, icao, lat, lon, alt, alt_src)

    def process_velocity(self, msg, ts, icao):
        velocity = pms.adsb.velocity(msg, True)

        self.db.record_velocity(ts, icao, *velocity)

    def process_ident(self, msg, ts, icao, tc):
        callsign = pms.adsb.callsign(msg).strip('_')
        category = pms.adsb.category(msg)

        self.db.record_ident(ts, icao, callsign, tc, category)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) == 28 and pms.df(msg) == 17 and pms.crc(msg) == 0:
                icao = pms.adsb.icao(msg)
                tc = pms.adsb.typecode(msg)
                self.process_msg(msg, dt.fromtimestamp(ts), icao, tc)

    def record(self):
        try:
            self.run()
        except KeyboardInterrupt:
            self.db.record_session_stop(self.session_hash, dt.now())
            self.db.close_session()
