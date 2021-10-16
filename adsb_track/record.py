import argparse
from adsb_track.stream import FlightRecorder

parser = argparse.ArgumentParser(description='Capture and record ADS-B data.')
parser.add_argument('database',
                    metavar='DATABASE',
                    help='The SQLite database file to record data.')
parser.add_argument('--host',
                    default='localhost',
                    help='The host of the ADS-B source')
parser.add_argument('--port',
                    default=30005,
                    type=int,
                    help='The port of the ADS-B source')
parser.add_argument('--rawtype',
                    default='beast',
                    choices=['raw', 'beast', 'skysense'],
                    help='The ADS-B output data format')
parser.add_argument('--lat',
                    type=float,
                    required=True,
                    help='Receiver latitude')
parser.add_argument('--lon',
                    type=float,
                    required=True,
                    help='Receiver longitude')

args = parser.parse_args()

flights = FlightRecorder(args.host, args.database, args.lat, args.lon)

flights.record()
