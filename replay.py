from datetime import datetime as dt
from adsb_track.const import *
from adsb_track.database import DBSQL
from adsb_track.aircraft import Airspace

def recreate_airspace_from_messages(messages):
    airspace = Airspace()
    for msg in messages:
        type_ = msg[0]
        msg = msg[1:]
        ts = msg[DBSQL.TIMESTAMP_INDEX]
        icao = msg[DBSQL.ICAO_INDEX]
        if type_ == IDENT:
            callsign = msg[DBSQL.IDENT_INDICES[CALLSIGN]]
            airspace.update_callsign(icao, ts, callsign)
        elif type_ == VELOCITY:
            heading, speed, vs = [msg[DBSQL.VELOCITY_INDICES[x]]
                                  for x in (ANGLE, SPEED, VERTICAL_SPEED)]
            airspace.update_velocity(icao, ts, heading, speed, vs)
        elif type_ == POSITION:
            lat, lon, alt = [msg[DBSQL.POSITION_INDICES[x]]
                             for x in (LATITUDE, LONGITUDE, ALTITUDE)]
            airspace.update_position(icao, ts, lat, lon, alt)
    return airspace


def recreate_airspace_from_session(db, session_uuid):
    return recreate_airspace_from_messages(db.replay_session(session_uuid))


def recreate_airspace_from_timestamp(db, datetime=None, previous=60):
    stop = datetime if isinstance(datetime, (int, float)) else None
    if datetime is None:
        stop = dt.now().timestamp()
    elif isinstance(datetime, str):
        if datetime.lower() == 'last':
            stop = db.last_message()
        else:
            stop = dt.fromisoformat(datetime).timestamp()
    start = stop - previous
    return recreate_airspace_from_messages(db.replay_messages(start, stop))


if __name__ == '__main__':
    from adsb_track.database import DBSQLite
    db = DBSQLite('test.sqlite3')
    ac = recreate_airspace(db, 'last')