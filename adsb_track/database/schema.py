from sqlalchemy.orm import registry
from sqlalchemy import Column, Integer, String, DateTime, SmallInteger, Float

mapper_registry = registry()
Base = mapper_registry.generate_base()


class RecordingSession(Base):
    __tablename__ = 'session'

    id = Column(Integer, primary_key=True)
    session_hash = Column(String(40), nullable=False)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    start = Column(DateTime, nullable=False)
    stop = Column(DateTime)

    def __repr__(self):
        return (f'RecordingSession(id={self.id!r}, '
                f'session_hash={self.session_hash!r}, host={self.host!r}, '
                f'port={self.port!r}, start={self.start!r}, '
                f'stop={self.stop!r})')


class Ident(Base):
    __tablename__ = 'ident'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    icao = Column(String(6), nullable=False)
    callsign = Column(String(8), nullable=False)
    typecode = Column(SmallInteger, nullable=False)
    category = Column(SmallInteger, nullable=False)

    def __repr__(self):
        return (f'Ident(id={self.id!r}, timestamp={self.id!r}, '
                f'icao={self.icao!r}, callsign={self.callsign!r}, '
                f'typecode={self.typecode!r}, category={self.category!r})')


class Velocity(Base):
    __tablename__ = 'velocity'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    icao = Column(String(6), nullable=False)
    speed = Column(SmallInteger)
    speed_type = Column(String)  #TODO determine possible values
    vertical_speed = Column(SmallInteger)
    vertical_speed_src = Column(String)  #TODO make enum
    angle = Column(Float)
    angle_src = Column(String)  #TODO make enum

    def __repr__(self):
        return (f'Velocity(id={self.id!r}, timestamp={self.timestamp!r}, '
                f'icao={self.icao!r}, speed={self.speed!r}, '
                f'speed_type={self.speed_type!r}, '
                f'vertical_speed={self.vertical_speed!r}, '
                f'angle={self.angle!r}, '
                f'vertical_speed_src={self.vertical_speed_src!r}, '
                f'angle_src={self.angle_src!r})')


class Position(Base):
    __tablename__ = 'position'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    icao = Column(String(6), nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Integer)  #TODO determine if small int works
    altitude_src = Column(String)  #TODO make enum

    def __repr__(self):
        return (f'Position(id={self.id!r}, timestamp={self.timestamp!r}, '
                f'icao={self.icao!r}, latitude={self.latitude!r}, '
                f'longitude={self.longitude!r}, altitude={self.altitude!r}, '
                f'altitude_src={self.altitude_src!r})')
