import textwrap

# TODO: create function to normalize SQL creation

IDENT_CREATE = textwrap.dedent("""\
    CREATE TABLE IF NOT EXISTS ident (
        timestamp REAL NOT NULL,
        icao TEXT NOT NULL,
        callsign TEXT NOT NULL,
        typecode INTEGER NOT NULL,
        category INTEGER NOT NULL,
        PRIMARY KEY (timestamp, icao)
    )""")
IDENT_CAT_INSERT = textwrap.dedent("""\
    INSERT INTO ident
    (timestamp, icao, callsign, typecode, category)
    VALUES (?, ?, ?, ?, ?)""")


VELOCITY_CREATE = textwrap.dedent("""\
    CREATE TABLE IF NOT EXISTS velocity (
        timestamp REAL NOT NULL,
        icao TEXT NOT NULL,
        speed INTEGER NOT NULL,
        speed_type TEXT NOT NULL,
        vert_speed INTEGER NOT NULL,
        vert_speed_src TEXT NOT NULL,
        angle REAL NOT NULL,
        angle_src TEXT NOT NULL,
        PRIMARY KEY (timestamp, icao)
    )""")
VELOCITY_INSERT = textwrap.dedent("""
    INSERT INTO velocity
    (timestamp, icao, speed, speed_type,
     vert_speed, vert_speed_src, angle, angle_src)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""")

def initialize(con, cur):
    cur.execute(IDENT_CREATE)
    cur.execute(VELOCITY_CREATE)
    con.commit()

def insert_ident(cur, ts, icao, callsign, tc, cat):
    cur.execute(IDENT_CAT_INSERT, (ts, icao, callsign, tc, cat))

# Order meant to match pyModeS return
def insert_velocity(cur, ts, icao, spd, angle, vs, spd_type,
                    angle_src, vs_src):
    cur.execute(VELOCITY_INSERT, (ts, icao, spd, spd_type,
                                  vs, vs_src, angle, angle_src))