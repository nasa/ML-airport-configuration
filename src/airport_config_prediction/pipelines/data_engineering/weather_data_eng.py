import os
import pandas as pd
import numpy as np

from typing import Dict, Any
import datetime
from .utils import sampling_times

#fill nans value by ffill and mean value for the mean value of previous 6 hours
def weather_process_nulls( 
    KCLT_weather_raw :pd.DataFrame,
    
) -> pd.DataFrame:
    KCLT_weather_raw['wind_gust'] = KCLT_weather_raw['wind_gust'] .fillna(KCLT_weather_raw['wind_gust']) #.replace('NG', 0).astype('int')
    KCLT_weather_raw["wind_direction"] = KCLT_weather_raw["wind_direction"].fillna(KCLT_weather_raw["wind_direction"].expanding(min_periods=6).mean())
    KCLT_weather_raw["wind_speed"] = KCLT_weather_raw["wind_speed"].fillna(KCLT_weather_raw["wind_speed"].expanding(min_periods=6).mean())
    KCLT_weather_raw["cloud_ceiling"] = KCLT_weather_raw["cloud_ceiling"].fillna(method = 'ffill')
    KCLT_weather_raw["precip"]= KCLT_weather_raw["precip"].fillna(method = 'ffill')
    KCLT_weather_raw["visibility"] = KCLT_weather_raw["visibility"].fillna(method = 'ffill')
    KCLT_weather_raw["cloud"]= KCLT_weather_raw["cloud"].fillna(method = 'ffill')
    KCLT_weather_raw["lightning_prob"]= KCLT_weather_raw["lightning_prob"].fillna(method = 'ffill')
    return KCLT_weather_raw

#conversion of units for speed and direction
def weather_units_conversion(weather_df):
    weather_df["wind_speed"] = 0.514444 * weather_df["wind_speed"] # speed from knots to m/s
    weather_df["wind_direction"] = 10 * weather_df["wind_direction"] #direction is recorded in tens of degrees
    weather_df["wind_direction_rad"] = np.deg2rad(weather_df["wind_direction"]) #conversion of degrees to radians
    return weather_df


def weather_to_wide_sampled(
        weather_data: pd.DataFrame,
        parameters: Dict[str, Any],
)-> pd.DataFrame:
    """
    This function formats the weather data from long to wide format, and samples the data using the prediction_delta
    parameter. For each row, the weather info is for the last known prediction in the past, with prediction_delta steps
    into the future up to lookahead. Columns are labeled as metric name + lookahead
    minutes, e.g. temperature_15, temperature_30
    """

    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    weather_data = weather_data.sort_values(by=['timestamp', 'forecast_timestamp'])

    # To speed up remove extra forecast_times
    weather_data['LAT'] = (weather_data['forecast_timestamp'] - weather_data['timestamp']) / np.timedelta64(1, 'h')
    weather_data = weather_data[weather_data['LAT'] <= (np.ceil(lookahead/60) + 1)].drop(columns=['LAT'])

    #######
    # weather_data is sampled per prediction_delta, both timestamp and forecast_timestamp are adjusted
    #######
    # Sample times for timestamp
    start_datetime = weather_data['timestamp'].min().ceil("H") + np.timedelta64(30, 'm')
    end_datetime = weather_data['timestamp'].max().floor("H") - np.timedelta64(30, 'm')
    time_df = sampling_times(parameters['prediction_delta'], start_datetime, end_datetime)
    time_df['timestamp_new'] = time_df['timestamp']
    #time_df['timestamp'] = time_df['timestamp'].dt.floor('H')

    weather_data_sampled = pd.merge(time_df, weather_data, on="timestamp")

    # Sampling times for forecast_timestamp
    start_datetime = weather_data['forecast_timestamp'].min().ceil("H")
    end_datetime = weather_data['forecast_timestamp'].max().floor("H")
    time_df = sampling_times(parameters['prediction_delta'], start_datetime, end_datetime)
    time_df['forecast_timestamp'] = time_df['timestamp'].dt.floor('H')
    time_df['forecast_timestamp_new'] = time_df['timestamp'] - np.timedelta64(1, 'h') + \
                                            np.timedelta64(parameters['prediction_delta'],'m')
    time_df.drop(columns=['timestamp'], inplace=True)

    weather_data_sampled2 = pd.merge(time_df, weather_data_sampled, on="forecast_timestamp")
    weather_data_sampled2.drop(columns=['forecast_timestamp', 'timestamp'], inplace=True)
    weather_data_sampled2 = weather_data_sampled2.sort_values(['timestamp_new', 'forecast_timestamp_new'])

    # Keep valid forecast entries
    weather_data_sampled2 = weather_data_sampled2[weather_data_sampled2['forecast_timestamp_new'] >
                                                  weather_data_sampled2['timestamp_new']]

    # Select rows per lookahead
    weather_data_sampled2 = weather_data_sampled2.assign(LAT_new=
                                                         (weather_data_sampled2['forecast_timestamp_new'] -
                                                          weather_data_sampled2['timestamp_new']) / np.timedelta64(1,
                                                                                                                   'm'))
    weather_data_sampled2 = weather_data_sampled2[weather_data_sampled2['LAT_new'] <= lookahead]

    # From long to wide data frame
    weather_data_sampled2.drop(columns='forecast_timestamp_new', inplace=True)
    weather_data_wide = weather_data_sampled2.pivot_table(index='timestamp_new', columns='LAT_new', aggfunc='first')
    weather_data_wide.columns = weather_data_wide.columns.map(lambda x: x[0] + '_' + str(int(x[1])))
    weather_data_wide.reset_index(inplace=True)
    weather_data_wide = weather_data_wide.rename(columns={'timestamp_new': 'timestamp'})

    return weather_data_wide



