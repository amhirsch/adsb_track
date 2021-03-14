from abc import ABC, abstractmethod
from copy import deepcopy
import sqlite3


_NAME = 'name'
_COLUMNS = 'columns'

_CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS'

_UNIVERSAL_COLUMNS = {
    'timestamp': None,
    'icao': None,
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
        _NAME: 'ident',
        _COLUMNS: {
            'callsign': None,
            'typecode': None,
            'category': None,
        }
    }
    IDENT_INSERT = _sql_insert(IDENT_TABLE)

    VELOCITY_TABLE = {
        _NAME: 'velocity',
        _COLUMNS: {
            'speed': None,
            'speed_type': None,
            'vert_speed': None,
            'vert_speed_src': None,
            'angle': None,
            'angle_src': None,
        }
    }
    VELOCITY_INSERT = _sql_insert(VELOCITY_TABLE)

    POSITION_TABLE = {
        _NAME: 'position',
        _COLUMNS: {
            'latitude': [None, False],
            'longitude': [None, False],
            'altitude': None,
            'altitude_src': None,
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
        self.cur.execute(VELOCITY_INSERT,
                    (ts, icao, spd, spd_type, vs, vs_src, angle, angle_src))

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

    UNIVERSAL_COLUMNS = deepcopy(_UNIVERSAL_COLUMNS)
    UNIVERSAL_COLUMNS['timestamp'] = REAL
    UNIVERSAL_COLUMNS['icao'] = TEXT

    IDENT_TABLE = deepcopy(DatabaseSQL.IDENT_TABLE)
    IDENT_TABLE[_COLUMNS]['callsign'] = TEXT
    IDENT_TABLE[_COLUMNS]['typecode'] = INTEGER
    IDENT_TABLE[_COLUMNS]['category'] = INTEGER
    IDENT_CREATE = _sql_create(IDENT_TABLE, PRIMARY_KEY_COL, UNIVERSAL_COLUMNS)

    VELOCITY_TABLE = deepcopy(DatabaseSQL.VELOCITY_TABLE)
    VELOCITY_TABLE[_COLUMNS]['speed'] = INTEGER
    VELOCITY_TABLE[_COLUMNS]['speed_type'] = TEXT
    VELOCITY_TABLE[_COLUMNS]['vert_speed'] = INTEGER
    VELOCITY_TABLE[_COLUMNS]['vert_speed_src'] = TEXT
    VELOCITY_TABLE[_COLUMNS]['angle'] = REAL
    VELOCITY_TABLE[_COLUMNS]['angle_src'] = TEXT
    VELOCITY_CREATE = _sql_create(VELOCITY_TABLE, PRIMARY_KEY_COL, UNIVERSAL_COLUMNS)

    POSITION_TABLE = deepcopy(DatabaseSQL.POSITION_TABLE)
    POSITION_TABLE[_COLUMNS]['latitude'][0] = REAL
    POSITION_TABLE[_COLUMNS]['longitude'][0] = REAL
    POSITION_TABLE[_COLUMNS]['altitude'] = INTEGER
    POSITION_TABLE[_COLUMNS]['altitude_src'] = TEXT
    POSITION_CREATE = _sql_create(POSITION_TABLE, PRIMARY_KEY_COL, UNIVERSAL_COLUMNS)


    def __init__(self, name, buffer=50):
        super().__init__(buffer)
        self.con = sqlite3.connect(name)
        self.cur = self.con.cursor()
        self.initialize()

