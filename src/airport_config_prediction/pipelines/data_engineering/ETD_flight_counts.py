import os

import pandas as pd
from datetime import datetime, timedelta


from .utils import sampling_times

from typing import Dict, Any
def sampling_time_bins(parameters: Dict[str, Any]):
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    num_bins = lookahead/parameters['prediction_delta']
    bins_df = sampling_times(parameters['prediction_delta'], parameters['globals']['start_time'], parameters['globals']['end_time'])
    bins_df = pd.DataFrame(bins_df.values.repeat(num_bins, axis=0), columns=bins_df.columns)

    bins_df['forecast_timestamp'] = ((bins_df.index % num_bins) + 1) * timedelta(minutes=parameters['prediction_delta']) + bins_df.timestamp
    bins_df['bin'] = ((bins_df.index % num_bins) + 1) * parameters['prediction_delta']
    return bins_df

def ETD_sampling(df,
                 parameters: Dict[str, Any],
                 dep_df):
    dep_df= dep_df[['gufi','departure_runway_actual_time']]
    etd_sampled_df = df
    etd_sampled_df['truncated_etd'] = etd_sampled_df.departure_runway_estimated_time.dt.ceil(str(parameters['prediction_delta'])+'min')
    etd_sampled_df.rename(columns={'timestamp': 'etd_timestamp'}, inplace=True)
    etd_sampled_df
    etd_sampled_df = etd_sampled_df.merge(dep_df, on='gufi', how='left')

    # filter to get gufis that not departed before prediction time or departure time unknown
    etd_sampled_df = etd_sampled_df[(etd_sampled_df.etd_timestamp <= etd_sampled_df.departure_runway_actual_time) | (etd_sampled_df.departure_runway_actual_time.isnull())]

    # filter to get gufis that not departed before prediction time
    #etd_df = etd_df[(etd_df.etd_timestamp <= etd_df.departure_runway_actual_time)]

    etd_sampled_df = etd_sampled_df.sort_values(by=['etd_timestamp'], na_position='last')
    return etd_sampled_df


def dep_filtering_flights(bins_df, etd_df):
    return filtering_flights(bins_df, etd_df, 'truncated_etd', 'etd_timestamp')


def filtering_flights(bins_df, et_df, truncated_col, et_col):
    filter_flights = bins_df.merge(et_df, how='left', left_on='forecast_timestamp', right_on=truncated_col)

    # filter out records with prediction timestamp after NOW
    #dep_filter_flights = filter_flights[filter_flights.timestamp >= filter_flights[et_col]]
    # AA :
    filter_flights = filter_flights[filter_flights.timestamp >= filter_flights[et_col]]

    
    # filter out gufis estimated to land 6 hours after NOW      #No need as it's already taken care of in the merge
    #filter_flights = filter_flights[(filter_flights.timestamp + timedelta(hours=parameters['LOOKAHEAD_TIME'])) >= filter_flights.departure_runway_estimated_time]

    # filter out gufis estimated to land before NOW         #No need as it's already taken care of in the merge
    #filter_flights = filter_flights[filter_flights.timestamp <= filter_flights.departure_runway_estimated_time]


    return filter_flights.sort_values(by=['timestamp','gufi',et_col]).groupby(['timestamp', 'gufi'],sort = False).tail(1).sort_values('timestamp')


def arr_pivot_table(df,
                parameters: Dict[str, Any]):

    df = sampling_time_bins(parameters).merge(df, how='left')
    df = df.groupby(['timestamp', 'bin']).agg({"gufi": "nunique"}).reset_index().pivot(index='timestamp', columns='bin', values='gufi').reset_index()
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    df= df.rename(columns={o*1.0: 'arr_counts_'+str(o) for o in range(0, lookahead+1, parameters['prediction_delta'])})
    return df

def ETA_sampling(df,parameters: Dict[str, Any],
                 arr_df):
    arr_df= arr_df[['gufi','arrival_runway_actual_time']]
    eta_sampled_df = df
    eta_sampled_df['truncated_eta'] = eta_sampled_df.arrival_runway_best_time.dt.ceil(str(parameters['prediction_delta'])+'min')
    eta_sampled_df.rename(columns={'timestamp': 'eta_timestamp'}, inplace=True)
    eta_sampled_df
    eta_sampled_df = eta_sampled_df.merge(arr_df, on='gufi', how='left')

    # filter to get gufis that not departed before prediction time or departure time unknown
    eta_sampled_df = eta_sampled_df[(eta_sampled_df.eta_timestamp <= eta_sampled_df.arrival_runway_actual_time) | (eta_sampled_df.arrival_runway_actual_time.isnull())]

    # filter to get gufis that not departed before prediction time
    #etd_df = etd_df[(etd_df.etd_timestamp <= etd_df.departure_runway_actual_time)]

    eta_sampled_df = eta_sampled_df.sort_values(by=['eta_timestamp'], na_position='last')
    return eta_sampled_df


def arr_filtering_flights(bins_df, eta_df):
    return filtering_flights(bins_df, eta_df, 'truncated_eta', 'eta_timestamp')


def dep_pivot_table(df,
                    parameters: Dict[str, Any]):
    df= sampling_time_bins(parameters).merge(df, how='left')
    df=df.groupby(['timestamp', 'bin']).agg({"gufi": "nunique"}).reset_index().pivot(index='timestamp', columns='bin',
                                                                                     values='gufi').reset_index()
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    df=df.rename(columns={o * 1.0: 'dep_counts_' + str(o) for o in range(0, lookahead + 1, parameters['prediction_delta'])})
    print (df)
    return df
