from abc import ABC, abstractmethod
from adsb_track.const import DURATION
from copy import deepcopy
from functools import wraps
import sqlite3

import pandas as pd

from adsb_track.const import *

_NAME = 'name'
_COLUMNS = 'columns'

_CREATE_TABLE, _CREATE_VIEW = [
    f'CREATE {x} IF NOT EXISTS' for x in ('TABLE', 'VIEW')
]

_UNIVERSAL_COLUMNS = {
    TIMESTAMP: None,
    ICAO: None,
}


def _column_declaration(name, dtype, not_null=True):
    return f"{name} {dtype}{' NOT NULL' if not_null else ''}"


def _sql_create(table_def, pk_sql, universal_columns=None):
    if universal_columns is None:
        universal_columns = {}
    column_definitions = deepcopy(universal_columns)
    column_definitions.update(table_def[_COLUMNS])
    sql_columns = [pk_sql]

    for col in column_definitions:
        params = column_definitions[col]
        # Only a datatype is specified
        if isinstance(params, str):
            sql_columns.append(_column_declaration(col, params))
        # Datatype and other parameters declared as tuple
        elif isinstance(params, (list, tuple)):
            sql_columns.append(_column_declaration(col, *params))
        # Datatype and other parameters declared in key-value pairs
        elif isinstance(params, dict):
            sql_columns.append(_column_declaration(col, **params))
        else:
            raise ValueError(f'Bad column definition for {col}')

    return f"{_CREATE_TABLE} {table_def[_NAME]} ({', '.join(sql_columns)})"


def _sql_view(table_def, sql_datetime, limit=20):
    view_name = table_def[_NAME] + '_recent'
    datetime_col = sql_datetime.format(TIMESTAMP)
    columns = ', '.join([datetime_col, ICAO] + list(table_def[_COLUMNS].keys()))
    return (
        f'{_CREATE_VIEW} {view_name} AS SELECT {columns} FROM {table_def[_NAME]} '
        f'ORDER BY {TIMESTAMP} DESC LIMIT {limit}')


def _sql_insert(table_def, universal_columns=True):
    all_columns = list(_UNIVERSAL_COLUMNS.keys()) if universal_columns else []
    all_columns += list(table_def[_COLUMNS].keys())
    columns = ', '.join([x for x in all_columns])
    values = ', '.join(['?'] * len(all_columns))
    return f"INSERT INTO {table_def[_NAME]} ({columns}) VALUES ({values})"


def _sql_col_indices(table_def, universal_columns=True):
    columns = list(_UNIVERSAL_COLUMNS.keys()) if universal_columns else []
    columns += list(table_def[_COLUMNS].keys())
    index_map = {}
    for i in range(len(columns)):
        index_map[columns[i]] = i
    return index_map


class DBSQL(ABC):

    TIMESTAMP_INDEX = 0
    ICAO_INDEX = 1

    SESSION_TABLE = {
        _NAME: SESSION,
        _COLUMNS: {
            SESSION_UUID: None,
            HOST: None,
            PORT: None,
            START: None,
            STOP: [None, False],
        }
    }
    SESSION_INDICES = _sql_col_indices(SESSION_TABLE, False)

    IDENT_TABLE = {
        _NAME: IDENT,
        _COLUMNS: {
            CALLSIGN: None,
            TYPECODE: None,
            CATEGORY: None,
        }
    }
    IDENT_INSERT = _sql_insert(IDENT_TABLE)
    IDENT_INDICES = _sql_col_indices(IDENT_TABLE)

    VELOCITY_TABLE = {
        _NAME: VELOCITY,
        _COLUMNS: {
            SPEED: None,
            SPEED_TYPE: None,
            VERTICAL_SPEED: None,
            VERTICAL_SPEED_SRC: None,
            ANGLE: None,
            ANGLE_SRC: None,
        }
    }
    VELOCITY_INSERT = _sql_insert(VELOCITY_TABLE)
    VELOCITY_INDICES = _sql_col_indices(VELOCITY_TABLE)

    POSITION_TABLE = {
        _NAME: POSITION,
        _COLUMNS: {
            LATITUDE: [None, False],
            LONGITUDE: [None, False],
            ALTITUDE: None,
            ALTITUDE_SRC: None,
        }
    }
    POSITION_INSERT = _sql_insert(POSITION_TABLE)
    POSITION_INDICES = _sql_col_indices(POSITION_TABLE)

    def initialize(self, commit=True):
        """Initializes the database
        
        Args:
            commit (bool): Commits the changes
        """
        self.cur.execute(self.SESSION_CREATE)
        self.cur.execute(self.IDENT_CREATE)
        self.cur.execute(self.VELOCITY_CREATE)
        self.cur.execute(self.POSITION_CREATE)
        if commit:
            self.con.commit()

    def commit(self):
        self.con.commit()
        self.buffer = 0

    def insert(func):
        """
        Inserts a row into the database
        """

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
            except sqlite3.IntegrityError as error:
                print(e)
            else:
                self.buffer += 1
                if self.buffer >= self.max_buffer:
                    self.commit()

        return wrapper

    @insert
    def record_session_start(self, session_uuid, host, port, start):
        """Records the start time of a session

        Args:
            session_uuid (str): Active session UUID
            host (str): Active session host
            port (int): Active session port
            start: Active session start time
        """
        self.cur.execute((f'INSERT INTO {SESSION} '
                          f'({SESSION_UUID}, {HOST}, {PORT}, {START}) '
                          'VALUES (?,?,?,?)'),
                         (session_uuid, host, port, start))

    @insert
    def record_session_stop(self, session_uuid, stop):
        """Records the end time of a session

        Args:
            session_uuid (str): Active session UUID
            stop: Active session end time
        """
        self.cur.execute(
            f'UPDATE {SESSION} SET {STOP} = ? WHERE {SESSION_UUID} = ?',
            (stop, session_uuid))

    @insert
    def record_ident(self, ts, icao, callsign, tc, cat):
        """Records an identification message

        Args:
            ts (float): Message timestamp
            icao (str): Aircraft ICAO24 code
            callsign (str): Aircraft callsign
            tc (int): Aircraft typecode
            cat (int): Aircraft category
        """
        self.cur.execute(self.IDENT_INSERT, (ts, icao, callsign, tc, cat))

    # Order meant to match pyModeS return
    @insert
    def record_velocity(self, ts, icao, spd, angle, vs, spd_type, angle_src,
                        vs_src):
        """Records a velocity message

        Args:
            ts (float): Message timestamp
            icao (str): Aircraft ICAO24 code
            spd (int): Aircraft speed
            angle (float): Aircraft heading
            vs (int): Aircraft vertical speed
            spd_type (str): Type of speed recorded
            angle_src (str): Source of heading measurement
            vs_src (str): Source of vertical speed measurement
        """
        self.cur.execute(
            self.VELOCITY_INSERT,
            (ts, icao, spd, spd_type, vs, vs_src, angle, angle_src))

    @insert
    def record_position(self, ts, icao, lat, lon, alt, alt_src):
        """Records a position message
        
        Args:
            ts (float): Message timestamp
            icao (str): Aircraft ICAO24 code
            lat (float): Aircraft latitude
            lon (float): Aircraft longitude
            alt (int): Aircraft altitude
            alt_src (str): Source of altitude measurement
        """
        self.cur.execute(self.POSITION_INSERT,
                         (ts, icao, lat, lon, alt, alt_src))

    def replay_messages(self, start, stop):
        """Replays the message in a given time duration

        Args:
            start (float): Start time, seconds since UNIX epoch
            stop (float): Stop time, seconds since UNIX epoch
        
        Returns:
            list of tuples: The messages captured in the time duration
        """
        messages = []
        for table in (IDENT, VELOCITY, POSITION):
            self.cur.execute(
                f'SELECT * FROM {table} WHERE {TIMESTAMP} BETWEEN ? AND ?',
                (start, stop))
            for msg in self.cur.fetchall():
                messages.append((table,) + msg[1:])
        messages.sort(key=lambda x: x[1])
        return messages

    def list_sessions(self):
        """Lists the recording sessions of the receiver
        
        Returns:
            list of tuples: The sessions found in the database.
        """
        df = pd.read_sql_query(f'SELECT * FROM {SESSION}', self.con)
        df.drop(columns='id', inplace=True)
        string_col = [SESSION_UUID, HOST]
        df.loc[:, string_col] = df.loc[:, string_col].convert_dtypes()
        for col in START, STOP:
            df[col] = pd.to_datetime(df[col], unit='s')
        df[DURATION] = df[STOP] - df[START]
        return df

    def replay_session(self, session_uuid):
        """Replays the messages of a specified session.
        
        Args:
            session_uuid (str): Session ID
        
        Returns:
            list of tuples: the messages in the given session.
        """
        self.cur.execute(f'SELECT {SESSION_UUID} FROM {SESSION}')
        all_sessions = [x[0] for x in self.cur.fetchall()]
        if session_uuid in all_sessions:
            self.cur.execute((f'SELECT {START}, {STOP} FROM {SESSION} '
                              f'WHERE {SESSION_UUID} IS ?'), (session_uuid,))
            start, stop = self.cur.fetchone()
            return self.replay_messages(start, stop)
        raise ValueError('Session UUID not found')

    # def replay_session(self, session_uuid):
    #     pass

    def last_message(self):
        """Time of most recent message in database.
        
        Returns:
            float: The last message found in the database.
        """
        max_time = 0
        for table in (IDENT, VELOCITY, POSITION):
            self.cur.execute(
                f'SELECT {TIMESTAMP} FROM {table} ORDER BY {TIMESTAMP} DESC LIMIT 1'
            )
            table_max_time = self.cur.fetchall()[0][0]
            if table_max_time > max_time:
                max_time = table_max_time
        return max_time

    @abstractmethod
    def __init__(self, max_buffer):
        self.max_buffer = max_buffer
        self.buffer = 0


class DBSQLite(DBSQL):
    PRIMARY_KEY_COL = 'id INTEGER PRIMARY KEY AUTOINCREMENT'

    TEXT = 'TEXT'
    INTEGER = 'INTEGER'
    REAL = 'REAL'
    SQL_DATETIME = "datetime({}, 'unixepoch', 'localtime')"

    UNIVERSAL_COLUMNS = {TIMESTAMP: REAL, ICAO: TEXT}

    SESSION_TABLE = deepcopy(DBSQL.SESSION_TABLE)
    SESSION_TABLE[_COLUMNS] = {
        SESSION_UUID: TEXT,
        HOST: TEXT,
        PORT: INTEGER,
        START: REAL,
        STOP: [REAL, False],
    }
    SESSION_CREATE = _sql_create(SESSION_TABLE, PRIMARY_KEY_COL, None)

    IDENT_TABLE = deepcopy(DBSQL.IDENT_TABLE)
    IDENT_TABLE[_COLUMNS] = {
        CALLSIGN: TEXT,
        TYPECODE: INTEGER,
        CATEGORY: INTEGER,
    }
    IDENT_CREATE = _sql_create(IDENT_TABLE, PRIMARY_KEY_COL, UNIVERSAL_COLUMNS)
    IDENT_VIEW = _sql_view(IDENT_TABLE, SQL_DATETIME)

    VELOCITY_TABLE = deepcopy(DBSQL.VELOCITY_TABLE)
    VELOCITY_TABLE[_COLUMNS] = {
        SPEED: INTEGER,
        SPEED_TYPE: TEXT,
        VERTICAL_SPEED: INTEGER,
        VERTICAL_SPEED_SRC: TEXT,
        ANGLE: REAL,
        ANGLE_SRC: TEXT,
    }
    VELOCITY_CREATE = _sql_create(VELOCITY_TABLE, PRIMARY_KEY_COL,
                                  UNIVERSAL_COLUMNS)
    VELOCITY_VIEW = _sql_view(VELOCITY_TABLE, SQL_DATETIME)

    POSITION_TABLE = deepcopy(DBSQL.POSITION_TABLE)
    POSITION_TABLE[_COLUMNS][LATITUDE][0] = REAL
    POSITION_TABLE[_COLUMNS][LONGITUDE][0] = REAL
    POSITION_TABLE[_COLUMNS].update({
        ALTITUDE: INTEGER,
        ALTITUDE_SRC: TEXT,
    })
    POSITION_CREATE = _sql_create(POSITION_TABLE, PRIMARY_KEY_COL,
                                  UNIVERSAL_COLUMNS)
    POSITION_VIEW = _sql_view(POSITION_TABLE, SQL_DATETIME)

    def initialize(self):
        super().initialize(False)
        self.cur.execute(self.IDENT_VIEW)
        self.cur.execute(self.VELOCITY_VIEW)
        self.cur.execute(self.POSITION_VIEW)
        self.con.commit()

    def __init__(self, name, buffer=50):
        super().__init__(buffer)
        self.con = sqlite3.connect(name)
        self.cur = self.con.cursor()
        self.initialize()
