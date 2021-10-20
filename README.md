# ADS-B Track
Python logging utility for ADS-B receivers.

## Basic Installation
```
pip install adsb-track
```

## Developer Installation
1. Clone the repository `git clone https://github.com/xander-hirsch/adsb-track`.
2. Install `pipenv` to the user pip installation with `python3 -m pip install pipenv --user --upgrade`
3. Install the project pipenv developer environment.
   Navigate to the project directory root.
   Run the command `python3 -m pipenv install --dev`.
4. Install the `adsb-track` package to the pipenv environment with the command `python3 -m pipenv run pip install -e .`

## Usage
The database recording functionality is provided in `adsb_track/record.py`

```
$ python -m adsb_track.record --help
usage: record.py [-h] [--host HOST] [--port PORT]
                 [--rawtype {raw,beast,skysense}] --lat LAT --lon LON
                 DATABASE

Capture and record ADS-B data.

positional arguments:
  DATABASE              The SQLite database file to record data.

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           The host of the ADS-B source
  --port PORT           The port of the ADS-B source
  --rawtype {raw,beast,skysense}
                        The ADS-B output data format
  --lat LAT             Receiver latitude
  --lon LON             Receiver longitude

```

## Documentation
https://adsb-track.xanderhirsch.us