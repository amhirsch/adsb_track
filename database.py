from abc import ABC, abstractmethod
from copy import deepcopy
import sqlite3

from adsb_track.const import *


_NAME = 'name'
_COLUMNS = 'columns'

_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS'

_UNIVERSAL_COLUMNS = {
    TIMESTAMP: None,
    ICAO: None,
}


def _column_declaration(name, dtype, not_null=True):
    return f"{name} {dtype}{' NOT NULL' if not_null else ''}"


def _sql_create(table_def, pk_sql, universal_columns):
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


def _sql_insert(table_def):
    all_columns = (list(_UNIVERSAL_COLUMNS.keys())
                    + list(table_def[_COLUMNS].keys()))
    columns = ', '.join([x for x in all_columns])
    values = ', '.join(['?'] * len(all_columns))
    return f"INSERT INTO {table_def[_NAME]} ({columns}) VALUES ({values})"


class DatabaseSQL(ABC):
    IDENT_TABLE = {
        _NAME: IDENT,
        _COLUMNS: {
            CALLSIGN: None,
            TYPECODE: None,
            CATEGORY: None,
        }
    }
    IDENT_INSERT = _sql_insert(IDENT_TABLE)

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


    def initialize(self):
        self.cur.execute(self.IDENT_CREATE)
        self.cur.execute(self.VELOCITY_CREATE)
        self.cur.execute(self.POSITION_CREATE)
        self.con.commit()

    def commit(self):
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
    def insert_ident(self, ts, icao, callsign, tc, cat):
        self.cur.execute(IDENT_INSERT, (ts, icao, callsign, tc, cat))

    # Order meant to match pyModeS return
    @log
    def insert_velocity(self, ts, icao, spd, angle, vs, spd_type,
                        angle_src, vs_src):
        self.cur.execute(VELOCITY_INSERT, (ts, icao, spd, spd_type,
                                           vs, vs_src,angle, angle_src))

    @log
    def insert_position(self, ts, icao, lat, lon, alt, alt_src):
        self.cur.execute(POSITION_INSERT, (ts, icao, lat, lon, alt, alt_src))

    @abstractmethod
    def __init__(self, max_buffer):
        self.max_buffer = max_buffer
        self.buffer = 0


class DatabaseSQLite(DatabaseSQL):
    PRIMARY_KEY_COL = 'id INTEGER PRIMARY KEY AUTOINCREMENT'

    TEXT = 'TEXT'
    INTEGER = 'INTEGER'
    REAL = 'REAL'

    UNIVERSAL_COLUMNS = {TIMESTAMP: REAL, ICAO: TEXT}

    IDENT_TABLE = deepcopy(DatabaseSQL.IDENT_TABLE)
    IDENT_TABLE[_COLUMNS] = {
        CALLSIGN: TEXT,
        TYPECODE: INTEGER,
        CATEGORY: INTEGER,
    }
    IDENT_CREATE = _sql_create(IDENT_TABLE, PRIMARY_KEY_COL, UNIVERSAL_COLUMNS)

    VELOCITY_TABLE = deepcopy(DatabaseSQL.VELOCITY_TABLE)
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

    POSITION_TABLE = deepcopy(DatabaseSQL.POSITION_TABLE)
    POSITION_TABLE[_COLUMNS][LATITUDE][0] = REAL
    POSITION_TABLE[_COLUMNS][LONGITUDE][0] = REAL
    POSITION_TABLE[_COLUMNS] = {
        ALTITUDE: INTEGER,
        ALTITUDE_SRC: TEXT,
    }
    POSITION_CREATE = _sql_create(POSITION_TABLE, PRIMARY_KEY_COL,
                                  UNIVERSAL_COLUMNS)


    def __init__(self, name, buffer=50):
        super().__init__(buffer)
        self.con = sqlite3.connect(name)
        self.cur = self.con.cursor()
        self.initialize()

