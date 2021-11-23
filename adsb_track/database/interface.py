from numpy import positive
import adsb_track.const as const

import pandas as pd
from sqlalchemy import create_engine, select, between
from sqlalchemy.orm import Session

from adsb_track.const import *
from adsb_track.database.schema import (Base, RecordingSession, Ident, Velocity,
                                        Position)


class Database:

    def record_session_start(self, session_hash, host, port, start):
        """Records the session socket and start time

        Args:
            session_hash (str): Session SHA-1 hash
            host (str): Session host
            port (int): Session port
            start (datetime.datetime): Session start time
        """
        self.session.add(
            RecordingSession(session_hash=session_hash,
                             host=host,
                             port=port,
                             start=start))

    def record_session_stop(self, session_hash, stop):
        """Records the session end time

        Args:
            session_hash (str): Active session SHA-1 hash
            stop (datetime.datetime): Active session end time
        """
        self.session.execute(
            select(RecordingSession).filter_by(
                session_hash=session_hash)).scalar_one().stop = stop

    def record_ident(self, ts, icao, callsign, tc, cat):
        """Records an identification message

        Args:
            ts (datetime.datetime): Message timestamp
            icao (str): Aircraft ICAO24 code
            callsign (str): Aircraft callsign
            tc (int): Aircraft typecode
            cat (int): Aircraft category
        """
        self.session.add(
            Ident(timestamp=ts,
                  icao=icao,
                  callsign=callsign,
                  typecode=tc,
                  category=cat))

    # Order meant to match pyModeS return
    def record_velocity(self, ts, icao, spd, angle, vs, spd_type, angle_src,
                        vs_src):
        """Records a velocity message

        Args:
            ts (datetime.datetime): Message timestamp
            icao (str): Aircraft ICAO24 code
            spd (int): Aircraft speed
            angle (float): Aircraft heading
            vs (int): Aircraft vertical speed
            spd_type (str): Type of speed recorded
            angle_src (str): Source of heading measurement
            vs_src (str): Source of vertical speed measurement
        """
        self.session.add(
            Velocity(timestamp=ts,
                     icao=icao,
                     speed=spd,
                     speed_type=spd_type,
                     vertical_speed=vs,
                     vertical_speed_src=vs_src,
                     angle=angle,
                     angle_src=angle_src))

    def record_position(self, ts, icao, lat, lon, alt, alt_src):
        """Records a position message
        
        Args:
            ts (datetime.datetime): Message timestamp
            icao (str): Aircraft ICAO24 code
            lat (float): Aircraft latitude
            lon (float): Aircraft longitude
            alt (int): Aircraft altitude
            alt_src (str): Source of altitude measurement
        """
        self.session.add(
            Position(timestamp=ts,
                     icao=icao,
                     latitude=lat,
                     longitude=lon,
                     altitude=alt,
                     altitude_src=alt_src))

    def replay_messages(self, start, stop):
        """Replays the message in a given time duration

        Args:
            start (datetime.datetime): Start time, seconds since UNIX epoch
            stop (datetime.datetime): Stop time, seconds since UNIX epoch
        
        Returns:
            pandas.DataFrame: The messages captured in the time duration
        """
        return tuple([
            pd.read_sql_query(
                select(x).where(between(x.timestamp, start, stop)),
                self.engine,
                index_col='id',
            ) for x in (Ident, Velocity, Position)
        ])

    def list_sessions(self):
        """Lists the recording sessions of the receiver
        
        Returns:
            pandas.DataFrame: The sessions found in the database.
        """

        df = pd.read_sql_table(RecordingSession.__tablename__,
                               self.engine,
                               index_col='id')
        string_col = [const.SESSION_HASH, const.HOST]
        df.loc[:, string_col] = df.loc[:, string_col].convert_dtypes()
        df[const.DURATION] = df[const.STOP] - df[const.START]
        return df

    def replay_session(self, session_hash):
        """Replays the messages of a specified session.
        
        Args:
            session_hash (str): Session SHA-1 hash
        
        Returns:
            pandas.DataFrame: the messages in the given session.
        """

        hash_sql_prefix = RecordingSession.session_hash.ilike(
            f'{session_hash}%')

        all_sessions = [
            x[0] for x in self.session.execute(
                select(RecordingSession.session_hash).where(hash_sql_prefix))
        ]

        if not all_sessions:  # No matches found
            raise ValueError('No matching sessions hashes found')
        if len(all_sessions) > 1:
            raise ValueError(
                f'Ambiguous SHA-1 hash prefix. Found sessions: {all_sessions}')

        timestamps = [
            x for x in self.session.execute(
                select(RecordingSession.start, RecordingSession.stop).where(
                    hash_sql_prefix))
        ][0]

        return self.replay_messages(timestamps.start, timestamps.stop)

    def close_session(self):
        self.session.commit()
        self.session.close()

    def __init__(self, dialect, url):
        if dialect == 'sqlite':
            database_url = f'sqlite:///{url}'
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)
