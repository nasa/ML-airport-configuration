
import datetime
import pandas as pd


def sampling_times(
        delta_min,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime
)-> pd.DataFrame:

    time_range = pd.date_range(start=start_datetime,
                               end=end_datetime, freq=str(delta_min)+'min')

    time_df = pd.DataFrame({'timestamp': time_range})

    return time_df