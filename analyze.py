from adsb_track.const import *
def outliers(series):
    q1, q3 = [series.quantile(x, 'linear') for x in [0.25, 0.75]]
    three_halves_iqr = 1.5 * (q3 - q1)
    return (max(0, q1-three_halves_iqr),
            min(q3+three_halves_iqr, series.max()))

def remove_outliers(series):
    low, high = outliers(series)
    return series[series]

def average_position(df_track):
    return tuple([df_track[x].mean() for x in (LATITUDE, LONGITUDE)])