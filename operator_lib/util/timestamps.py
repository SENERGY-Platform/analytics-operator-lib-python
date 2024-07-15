import pandas as pd

__all__ = ("todatetime", "timestamp_to_str")

def todatetime(timestamp):
    if str(timestamp).isdigit():
        if len(str(timestamp))==13:
            timestamp = pd.to_datetime(int(timestamp), unit='ms')
        elif len(str(timestamp))==19:
            timestamp = pd.to_datetime(int(timestamp), unit='ns')
    else:
        timestamp = pd.to_datetime(timestamp)

    try:
        timestamp = timestamp.tz_convert(tz='UTC')
    except TypeError:
        # TODO localize german time then convert?
        timestamp = timestamp.tz_localize(tz='UTC')

    return timestamp

def timestamp_to_str(timestamp):
    return timestamp.tz_localize(None).isoformat()+"Z"