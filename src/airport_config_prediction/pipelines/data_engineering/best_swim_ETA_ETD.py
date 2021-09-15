import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from .utils import sampling_times

from kedro.config import ConfigLoader


from typing import Dict, Any

# ETD and ETA for whole data set
def eta_process(
        eta_df :pd.DataFrame,
        parameters: Dict[str, Any]):

    # Filter out gufi that arrived before time of interest
    eta_df = eta_df[eta_df.timestamp < eta_df.arrival_runway_best_time]
    eta_df['truncated_timestamp'] = eta_df.set_index('timestamp').index.ceil(str(parameters['prediction_delta'])+'min')
    eta_df.sort_values(by=['timestamp'])
    return eta_df

def etd_process(

    etd_df :pd.DataFrame,
    parameters: Dict[str, Any]):
    # Filter out gufi that departed before time of interest

    etd_df= etd_df[etd_df.timestamp < etd_df.departure_runway_estimated_time]
    etd_df['truncated_timestamp']=etd_df.set_index('timestamp').index.ceil(str(parameters['prediction_delta'])+'min')
    etd_df.sort_values(by=['timestamp'])
    return etd_df

def count_between(df, start_time, col_name,
                  parameters: Dict[str, Any]):
    end_time = start_time+ timedelta(minutes= parameters['prediction_delta'])
    return len(df[df[col_name].between(start_time, end_time)].gufi.unique())

#counts on the predicted flights between certain bins
def counts(d,parameters: Dict[str, Any]):
    ts = pd.to_datetime(d.index)
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    count_departures = {o: count_between(d, ts+timedelta(minutes=o), 'departure_runway_estimated_time') for o in range(0, lookahead+1, parameters['prediction_delta'])}

    return count_departures

#counts on the predicted flights between certain bins
def arr_counts(a,parameters: Dict[str, Any]):
    ts = pd.to_datetime(a.index)
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    count_arrivals = {o: count_between(a, ts+timedelta(minutes=o), 'arrival_runway_best_time') for o in range(0, lookahead+1, parameters['prediction_delta'])}

    return count_arrivals
def counts_postprocess(etd_df :pd.DataFrame,
        eta_df :pd.DataFrame,
        parameters: Dict[str, Any]):
    start = time.time()
    lookahead = parameters['prediction_lookahead'] + parameters['model']['lookahead'] - parameters['prediction_delta']
    dep_counts = etd_df.set_index('truncated_timestamp').groupby('truncated_timestamp').apply(counts, parameters)
    dep_count_df = dep_counts.to_frame()
    dep_count_df.rename(columns={0: 'counts'}, inplace=True)
    dep_count_df.reset_index(level=0, inplace=True)
    dep_count_df = dep_count_df.join(pd.json_normalize(dep_count_df.counts)).set_index('truncated_timestamp')
    #dep_count_df = sampling_times("15min", "2019-08-15 08:15:00", "2019-09-16 08:00:00").set_index('timestamp').join(dep_count_df).drop(columns=['counts']).fillna(0)
    dep_count_df = sampling_times(parameters['prediction_delta'], parameters['start_datetime'], parameters['end_datetime']).set_index('timestamp').join(dep_count_df).drop(columns=['counts']).fillna(0)
    dep_count_df = dep_count_df.rename(columns={o: "count_dep_"+str(o) for o in range(0, lookahead+1, parameters['prediction_delta'])})

    #arrivals
    arr_counts_df = eta_df.set_index('truncated_timestamp').groupby('truncated_timestamp').apply(arr_counts, parameters)
    arr_count_df = arr_counts_df.to_frame()
    arr_count_df.rename(columns={0: 'arr_counts'}, inplace=True)
    arr_count_df.reset_index(level=0, inplace=True)
    arr_count_df = arr_count_df.join(pd.json_normalize(arr_count_df.arr_counts)).set_index('truncated_timestamp')
    #arr_count_df = sampling_times(15, "2019-08-15 08:15:00", "2019-09-16 08:00:00").set_index('timestamp').join(arr_count_df).drop(columns=['arr_counts']).fillna(0)
    arr_count_df = sampling_times(parameters['prediction_delta'], parameters['start_datetime'], parameters['end_datetime']).set_index('timestamp').join(arr_count_df).drop(columns=['arr_counts']).fillna(0)
    arr_count_df = arr_count_df.rename(columns={o: "count_arr_"+str(o) for o in range(0, parameters['']*60+1, parameters['prediction_delta'])})
    end = time.time()

    # total time taken
    print(f"Runtime of the program is {end - start}")
    return dep_count_df, arr_count_df
