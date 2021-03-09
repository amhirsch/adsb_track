NAME = 'name'
COLUMNS = 'columns'

CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS'

UNIVERSAL_COLUMNS = (
    ('timestamp', 'REAL'),
    ('icao', 'TEXT'),
)
PRIMARY_KEYS = f"PRIMARY KEY ({','.join([x[0] for x in UNIVERSAL_COLUMNS])})"


def column_declaration(name, dtype):
    return f'{name} {dtype} NOT NULL'


def sql_create(table_def):
    columns = ','.join([column_declaration(x, y) for x, y
                        in UNIVERSAL_COLUMNS + table_def[COLUMNS]])
    return f'{CREATE_TABLE} {table_def[NAME]} ({columns})'


def sql_insert(table_def):
    all_columns = UNIVERSAL_COLUMNS + table_def[COLUMNS]
    columns = ','.join([x[0] for x in all_columns])
    values = ','.join(['?'] * len(all_columns))
    return f"INSERT INTO {table_def[NAME]} ({columns}) VALUES ({values})"

def sql_func(table_def):
    return sql_create(table_def), sql_insert(table_def)


IDENT_TABLE = {
    NAME: 'ident',
    COLUMNS: (
        ('callsign', 'TEXT'),
        ('typecode', 'INTEGER'),
        ('category', 'INTEGER'),
    )
}
IDENT_CREATE, IDENT_INSERT = sql_func(IDENT_TABLE)

VELOCITY_TABLE = {
    NAME: 'velocity',
    COLUMNS: (
        ('speed', 'INTEGER'),
        ('speed_type', 'TEXT'),
        ('vert_speed', 'INTEGER'),
        ('vert_speed_src', 'TEXT'),
        ('angle', 'REAL'),
        ('angle_src', 'TEXT'),
    )
}
VELOCITY_CREATE, VELOCITY_INSERT = sql_func(VELOCITY_TABLE)


def initialize(con, cur):
    cur.execute(IDENT_CREATE)
    cur.execute(VELOCITY_CREATE)
    con.commit()

def insert_ident(cur, ts, icao, callsign, tc, cat):
    cur.execute(IDENT_INSERT, (ts, icao, callsign, tc, cat))

# Order meant to match pyModeS return
def insert_velocity(cur, ts, icao, spd, angle, vs, spd_type,
                    angle_src, vs_src):
    cur.execute(VELOCITY_INSERT,
                (ts, icao, spd, spd_type, vs, vs_src, angle, angle_src))