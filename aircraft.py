from datetime import datetime as dt

class Aircraft:
    def __init__(self, icao):
        self.icao = icao

        self.callsign_update = None
        self.callsign = None

        self.position_update = None
        self.latitude = None
        self.longitude = None
        self.altitude = None

        self.velocity_update = None
        self.heading = None
        self.velocity = None
        self.vertical_speed = None

    def __str__(self):
        return (
            f'------ {self.icao} ------\n'
            f'  ({self.latitude}, {self.longitude})  {self.altitude} ft\n'
            f'  {self.heading} degrees, {self.velocity} knots, {self.vertical_speed} ft/sec'
        )


    def last_update(self):
        update_canidates = [x for x in
                            (self.callsign_update, self.position_update,
                             self.velocity_update) if x is not None]
        if len(update_canidates) > 0:
            return max(update_canidates)

    def process_timestamp(ts):
        if isinstance(ts, dt):
            return ts
        elif isinstance(ts, float):
            return dt.fromtimestamp(ts)

    def update_callsign(self, ts, callsign):
        ts = self.process_timestamp(ts)
        if ts > self.callsign_update:
            self.callsign_update = ts
            self.callsign = callsign

    def update_position(self, ts, lat, lon, alt):
        ts = self.process_timestamp(ts)
        if ts > self.position_update:
            self.position_update = ts
            self.latitude = lat
            self.longitude = lon
            self.altitude = alt

    def update_velocity(self, ts, heading, velocity, vertical_speed):
        ts = self.process_timestamp(ts)
        if ts > self.velocity_update:
            self.velocity_update = ts
            self.heading = heading
            self.velocity = velocity
            self.vertical_speed = vertical_speed