import argparse
from adsb_track.stream import FlightRecorder

supported_rawtypes = 'raw', 'beast', 'skysense'

parser = argparse.ArgumentParser(description='Capture and record ADS-B data.')
parser.add_argument('database',
                    help='The SQLite database file to record data.',
                    metavar='DATABASE')
parser.add_argument('--host',
                    default='localhost',
                    help='The host of the ADS-B source')
parser.add_argument('--port',
                    default=30002,
                    type=int,
                    help='The port of the ADS-B source')
parser.add_argument('--rawtype',
                    default='raw',
                    choices=supported_rawtypes,
                    help='The ADS-B output data format')
parser.add_argument('--latlon',
                    nargs=2,
                    type=float,
                    required=True,
                    help='Receiver latitude and longitude',
                    metavar=('LAT', 'LON'))

args = parser.parse_args()
if args.rawtype not in supported_rawtypes:
    raise ValueError(
        f"Rawtype {args.rawtype} not one of {', '.join(supported_rawtypes)}")
if abs(args.latlon[0]) > 90:
    raise ValueError(f"Provided latitude {args.latlon[0]} is outside -90 to 90")
if abs(args.latlon[1]) > 180:
    raise ValueError(
        f"Provided longitude {args.latlon[1]} is outside -180 to 180")

flights = FlightRecorder(args.host, args.database, args.latlon[0],
                         args.latlon[1], args.port, args.rawtype)

flights.record()
