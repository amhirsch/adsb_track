from datetime import datetime as dt
import pandas as pd
from adsb_track.const import *


class Aircraft:
    """
    A representation of an aircraft over time

    Parameters
    ----------
    icao : str
        The ICAO24 code of the aircraft, used as its unique identifier.
    """

    def __init__(self, icao):
        self.icao = icao

        self.callsign_update = None
        self.callsign = None
        self.callsign_history = []

        self.position_update = None
        self.latitude = None
        self.longitude = None
        self.altitude = None
        self.position_history = []

        self.velocity_update = None
        self.heading = None
        self.velocity = None
        self.vertical_speed = None
        self.velocity_history = []

    def __str__(self):

        def title():
            icao_lowercase = self.icao.lower()
            if self.callsign is None:
                return self.icao.lower()
            return f'{self.callsign} / {icao_lowercase}'

        return (
            f"------ {title()} ------\n"
            f'  ({self.latitude}, {self.longitude})  {self.altitude} ft\n'
            f'  {self.heading} degrees, {self.velocity} knots, {self.vertical_speed} ft/sec'
        )

    def last_update(self):
        """
        The last time the aircraft reported.

        Returns
        -------
        pandas.Timestamp
            The latest timestamp of either the callsign, position, or velocity.
        """
        update_canidates = [
            x for x in (self.callsign_update, self.position_update,
                        self.velocity_update) if x is not None
        ]
        if len(update_canidates) > 0:
            return max(update_canidates)

    def to_json(self):
        """
        Exports the aircraft data in JSON format.

        Returns
        -------
        dict
            The aircraft data in JSON format
        """
        return {
            ICAO: self.icao,
            LAST_UPDATE: self.last_update().timestamp(),
            CALLSIGN: self.callsign,
            LATITUDE: self.latitude,
            LONGITUDE: self.longitude,
            ALTITUDE: self.altitude,
            ANGLE: self.heading,
            SPEED: self.velocity,
            VERTICAL_SPEED: self.vertical_speed,
        }

    def process_timestamp(ts):
        if isinstance(ts, pd._libs.tslibs.timestamps.Timestamp):
            return ts
        elif isinstance(ts, float):
            return pd.to_datetime(ts, unit='s')
        elif isinstance(ts, dt):
            return pd.to_datetime(ts)

    def is_update(ts, comparison):
        return (comparison is None) or (ts > comparison)

    def get_callsign_history(self):
        if self.callsign_history:
            return pd.DataFrame(self.callsign_history,
                                columns=[TIMESTAMP, CALLSIGN]).convert_dtypes()

    def get_position_history(self):
        if self.position_history:
            return pd.DataFrame(
                self.position_history,
                columns=[TIMESTAMP, LATITUDE, LONGITUDE, ALTITUDE])

    def get_velocity_history(self):
        if self.velocity_history:
            return pd.DataFrame(
                self.velocity_history,
                columns=[TIMESTAMP, ANGLE, VELOCITY, VERTICAL_SPEED])

    def get_track(self):
        df = pd.concat(
            (self.get_callsign_history(), self.get_position_history(),
             self.get_velocity_history()))
        df.sort_values(TIMESTAMP, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df.convert_dtypes(convert_floating=False)

    def update_callsign(self, ts, callsign):
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.callsign_update):
            self.callsign_update = ts
            self.callsign = callsign
            self.callsign_history.append((ts, callsign))

    def update_position(self, ts, lat, lon, alt):
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.position_update):
            self.position_update = ts
            self.latitude = lat
            self.longitude = lon
            self.altitude = alt
            self.position_history.append((ts, lat, lon, alt))

    def update_velocity(self, ts, heading, velocity, vertical_speed):
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.velocity_update):
            self.velocity_update = ts
            self.heading = heading
            self.velocity = velocity
            self.vertical_speed = vertical_speed
            self.velocity_history.append(
                (ts, heading, velocity, vertical_speed))


class Airspace:

    def __init__(self):
        self.flights = {}

    def __len__(self):
        return len(self.flights)

    def to_json(self):
        return [x.to_json() for x in self.flights.values()]

    def aircraft_present(self):
        return self.flights.keys()

    def get_aircraft(self, icao):
        return self.flights.get(icao.upper())

    def check_aircraft(self, icao):
        icao_uppper = icao.upper()
        if icao_uppper not in self.flights:
            self.flights[icao_uppper] = Aircraft(icao_uppper)
        return self.flights[icao_uppper]

    def update_callsign(self, icao, ts, callsign):
        self.check_aircraft(icao).update_callsign(ts, callsign)

    def update_position(self, icao, ts, lat, lon, alt):
        self.check_aircraft(icao).update_position(ts, lat, lon, alt)

    def update_velocity(self, icao, ts, heading, velocity, vertical_speed):
        self.check_aircraft(icao).update_velocity(ts, heading, velocity,
                                                  vertical_speed)

    def __str__(self):
        return ('\n' * 2).join([str(x) for x in self.flights.values()])
