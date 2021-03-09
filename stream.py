import sqlite3
import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient

import adsb_track.database as database


class Database:
    def __init__(self, name, buffer=25):
        self.con = sqlite3.connect(name)
        self.cur = self.con.cursor()
        self.max_buffer = buffer
        self.buffer = 0
        database.initialize(self.con, self.cur)


    def commit(self):
        print('buffer cleared')
        self.con.commit()
        self.buffer = 0


    def log(func):
        def wrapper(self, *args, **kwargs):
            self.buffer += 1

            func(self, *args, **kwargs)

            if self.buffer >= self.max_buffer:
                self.commit()

        return wrapper


    @log
    def log_ident(self, msg, ts, icao, tc):
        callsign = pms.adsb.callsign(msg).strip('_')
        category = pms.adsb.category(msg)
        database.insert_ident(self.cur, ts, icao, callsign, tc, category)

    @log
    def log_velocity(self, msg, ts, icao):
        velocity = pms.adsb.velocity(msg, True)
        database.insert_velocity(self.cur, ts, icao, *velocity)


class FlightRecorder(TcpClient):
    def __init__(self, host, db, port=30005, rawtype='beast', buffer=25):
        super(FlightRecorder, self).__init__(host, port, rawtype)
        self.db = Database(db, buffer)


    def process_msg(self, msg, ts, icao, tc):
        if tc in tuple(range(1,5)):
            self.db.log_ident(msg, ts, icao, tc)
        if tc == 19:
            print(pms.adsb.velocity(msg, True))
            self.db.log_velocity(msg, ts, icao)


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
    # client = ADSBClient('pi.lan.xanderhirsch.us')
    # client.run()
    client = FlightRecorder('pi.lan.xanderhirsch.us', 'test.db')
    client.record()
