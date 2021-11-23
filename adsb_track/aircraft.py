from datetime import datetime as dt

import pandas as pd
import numpy as np

from adsb_track.const import *
import adsb_track.const as const


class SessionData:

    def __init__(self, df_ident, df_velocity, df_position):
        self.df_ident = df_ident
        self.df_velocity = df_velocity
        self.df_position = df_position
        self.unique_icao = np.unique(
            np.concatenate([
                x[const.ICAO].unique()
                for x in (df_ident, df_velocity, df_position)
            ]))

    def unique_icao(self):
        """Unique ICAO addresses in the message dataframes.

        Args:
            message_dataframes (Iterable[pandas.DataFrame]): The dataframes from
                the session output. The column with ICAO24 codes is expected to
                be 'icao'.

        Returns:
            Iterable[str]: The unique ICAO24 codes found across the input
                dataframes.
        """
        return self.unique_icao

    def isolate_icao(self, icao):
        """Isolates the messages of an aircraft from many dataframes.

        Args:
            icao (str): The ICAO24 code to isolate.

        Returns:
            Iterable[pandas.DataFrame]: A deepcopy of a subset of the input
                message_dataframes where the ICAO24 code matches the icao input
                parameter.
        """
        if icao not in self.unique_icao:
            return ValueError(f'The ICAO24 code {icao} could not be found in '
                              'any messages.')
        return tuple([
            x[x[const.ICAO] == icao].copy()
            for x in (self.df_ident, self.df_velocity, self.df_position)
        ])

    def build_track(self, icao):
        """Constructs the track of an aircraft from different types of messages.

        Args:
            icao (str): The ICAO24 code to isolate.

        Returns:
            pandas.DataFrame: A single dataframe constructed with the most
                recent information of each type.
        """
        if icao not in self.unique_icao:
            return ValueError(f'The ICAO24 code {icao} could not be found in '
                              'any messages.')
        df_ident, df_velocity, df_position = self.isolate_icao(icao)
        df_ident[const.MSG_TYPE] = const.IDENT
        df_velocity[const.MSG_TYPE] = const.VELOCITY
        df_position[const.MSG_TYPE] = const.POSITION
        df = pd.concat([df_ident, df_velocity, df_position])
        df.sort_values(const.TIMESTAMP, inplace=True)
        df.ffill(inplace=True)
        return df


class Aircraft:
    """A representation of an aircraft over time

    Args:
        icao (str): The ICAO24 code of the aircraft, used as its unique
            identifier.
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
        """The last time the aircraft reported.

        Returns:
            pandas.Timestamp: The latest timestamp of either the callsign,
                position, or velocity.
        """
        update_canidates = [
            x for x in (self.callsign_update, self.position_update,
                        self.velocity_update) if x is not None
        ]
        if len(update_canidates) > 0:
            return max(update_canidates)

    def to_json(self):
        """Exports the aircraft data in JSON format.

        Returns:
            dict: The aircraft data in JSON format
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
        """Produces a time series of recorded call sign.

        Returns:
            pandas.DataFrame: A dataframe with timestamps and callsigns.
        """
        if self.callsign_history:
            return pd.DataFrame(self.callsign_history,
                                columns=[TIMESTAMP, CALLSIGN]).convert_dtypes()

    def get_position_history(self):
        """Produces a time series of position data.

        Returns:
            pandas.DataFrame: A dataframe with timestamps, latitude, longitude,
                and altitude.
        """
        if self.position_history:
            return pd.DataFrame(
                self.position_history,
                columns=[TIMESTAMP, LATITUDE, LONGITUDE, ALTITUDE])

    def get_velocity_history(self):
        """Produces a time series of velocity.

        Returns:
            pandas.DataFrame: A dataframe with heading, speed, and vertical
                speed.
        """
        if self.velocity_history:
            return pd.DataFrame(
                self.velocity_history,
                columns=[TIMESTAMP, ANGLE, VELOCITY, VERTICAL_SPEED])

    def get_track(self):
        """Provides a single dataframe with all of callsign, position, and
            altitude history.

        Returns:
            pandas.DataFrame: A dataframe of callsign, position, and altitude
                concatenated.
        """
        df = pd.concat(
            (self.get_callsign_history(), self.get_position_history(),
             self.get_velocity_history()))
        df.sort_values(TIMESTAMP, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df.convert_dtypes(convert_floating=False)

    def update_callsign(self, ts, callsign):
        """Updates the aircraft callsign at a given time.

        Args:
            ts (Pandas Timestamp or float or Python datetime): The time at which
                the callsign record occurred. If the input is a float, that is
                the seconds since the UNIX epoch.
            callsign (str): The aircraft callsign
        """
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.callsign_update):
            self.callsign_update = ts
            self.callsign = callsign
            self.callsign_history.append((ts, callsign))

    def update_position(self, ts, lat, lon, alt):
        """Updates the aircraft velocity at a given time.

        Args:
            ts (Pandas Timestamp, float, Python datetime): The time at which the
                velocity was recorded.
            lat (float): Latitude
            lon (float): Longitude
            alt (int): Altitude
        """
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.position_update):
            self.position_update = ts
            self.latitude = lat
            self.longitude = lon
            self.altitude = alt
            self.position_history.append((ts, lat, lon, alt))

    def update_velocity(self, ts, heading, velocity, vertical_speed):
        """Updates the aircraft position at a given time.

        Args:
            ts (Pandas Timestamp, float, Python datetime): The time at which the
                velocity was recorded.
            heading (float): Heading
            velocity (int): Velocity
            vertical_speed (int): Vertical speed
        """
        ts = Aircraft.process_timestamp(ts)
        if Aircraft.is_update(ts, self.velocity_update):
            self.velocity_update = ts
            self.heading = heading
            self.velocity = velocity
            self.vertical_speed = vertical_speed
            self.velocity_history.append(
                (ts, heading, velocity, vertical_speed))


class Airspace:
    """A representation of the entire airspace."""

    def __init__(self):
        self.flights = {}

    def __len__(self):
        return len(self.flights)

    def to_json(self):
        """JSON representation of all aircraft in airspace.

        Returns:
            list of dict: A JSON list of all the aircraft included in airspace.
        """
        return [x.to_json() for x in self.flights.values()]

    def aircraft_present(self):
        """Lists all aircraft in the airspace.

        Returns:
            dict_keys: A set of ICAO24 codes
        """
        return self.flights.keys()

    def get_aircraft(self, icao):
        """Attempts to find the specified aircraft

        Args:
            icao (str): ICAO24 address

        Returns:
            adsb_track.Aircraft or None: The aircraft if found
        """
        return self.flights.get(icao.upper())

    def check_aircraft(self, icao):
        """Finds aircraft in airspace or creates a new one.

        Returns:
            adsb_track.Aircraft: The aircraft (possibly a new one) in the
                airspace with the corresponding ICAO24 code.
        """
        icao_uppper = icao.upper()
        if icao_uppper not in self.flights:
            self.flights[icao_uppper] = Aircraft(icao_uppper)
        return self.flights[icao_uppper]

    def update_callsign(self, icao, ts, callsign):
        """Updates the callsign information of the aircraft.

        Args:
            icao (str): ICAO24 code
            ts (Pandas Timestamp or float or Python datetime): The time at which
                the callsign record occurred. If the input is a float, that is
                the seconds since the UNIX epoch.
            callsign (str): The aircraft callsign
        """
        self.check_aircraft(icao).update_callsign(ts, callsign)

    def update_position(self, icao, ts, lat, lon, alt):
        """Updates the position information of an aircraft.

        Args:
            icao (str): ICAO24 code
            ts (Pandas Timestamp, float, Python datetime): The time at which the
                velocity was recorded.
            lat (float): Latitude
            lon (float): Longitude
            alt (int): Altitude
        """
        self.check_aircraft(icao).update_position(ts, lat, lon, alt)

    def update_velocity(self, icao, ts, heading, velocity, vertical_speed):
        """Updates the velocity information of an aircraft

        Args:
            icao (str): ICAO24 code
            ts (Pandas Timestamp, float, Python datetime): The time at which the
                velocity was recorded.
            heading (float): Heading
            velocity (int): Velocity
            vertical_speed (int): Vertical speed
        """
        self.check_aircraft(icao).update_velocity(ts, heading, velocity,
                                                  vertical_speed)

    def __str__(self):
        return ('\n' * 2).join([str(x) for x in self.flights.values()])
