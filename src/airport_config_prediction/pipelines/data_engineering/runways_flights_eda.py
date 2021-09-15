import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Merging runways and flights with the aircrafts mappings
def Merge_run_flight (KCLT_MFS,KCLT_runways,aircraft_mapping): 
    runway_flights = pd.merge(KCLT_MFS[['gufi', 'aircraft_engine_class', 'aircraft_type','arrival_aerodrome_icao_name', 'major_carrier','flight_type','isarrival','isdeparture','arrival_stand_actual','arrival_stand_actual_time','departure_stand_actual','departure_stand_actual_time']] ,KCLT_runways, on = ['gufi'])
    merged_runway_flights= pd.merge(runway_flights,aircraft_mapping[['faa_weight_class','engine_type','type']], how ='left', left_on= ['aircraft_type'], right_on= ['type'])

    return merged_runway_flights

#runway_flights = pd.merge (runway_flights, KCLT_weather_raw, left_on = ['timestamp'], right_on = ['timestamp'])
#arr_runway_config_flights = pd.merge(runway_flights,KCLT_configs, left_on = ['arrival_runway_actual_time'], right_on = ['timestamp'])
# A function that returns arrivals counts, departure counts and flights counts on the surface every 15 mins time interval
# A function that returns arrivals counts, departure counts and flights counts on the surface every 15 mins time interval
def arr_dep_runway_counts(runway_flights):
    # arrivals
    arr_df=runway_flights.loc[runway_flights['isarrival'] == True].drop( ['departure_stand_actual_time', 'departure_stand_actual', 'departure_runway_actual_time',
         'departure_runway_actual'], axis=1)
    arr_df.dropna(subset=['arrival_runway_actual_time', 'arrival_runway_actual'], inplace=True)
    arr_df=arr_df.sort_values(by=['arrival_runway_actual_time'], ascending=[1])
    #arr_count=arr_df.set_index('arrival_runway_actual_time').resample(timedelta(minutes=15)).count()

    #arr_count=arr_count.rename_axis('date').reset_index()
   # arr_count=arr_count[['date', 'gufi']].rename({'gufi': 'arr_count'}, axis=1)
    # arr_count = arr_count.dropna()

    # departures
    dep_df=runway_flights.loc[runway_flights['isdeparture'] == True].drop(['arrival_stand_actual_time', 'arrival_stand_actual', 'arrival_runway_actual_time', 'arrival_runway_actual'],
        axis=1)
    dep_df.dropna(subset=['departure_runway_actual_time', 'departure_runway_actual'], inplace=True)
    print ('len df',len(dep_df))
    dep_df=dep_df.sort_values(by=['departure_runway_actual_time'], ascending=[1])
    #dep_count=dep_df.set_index('departure_runway_actual_time').resample(timedelta(minutes=15)).count()
    #dep_count=dep_count.rename_axis('date').reset_index()
    #dep_count=dep_count[['date', 'gufi']].rename({'gufi': 'dep_count'}, axis=1)
    #dep_count=dep_count.dropna(inplace=True)

    # flight_counts = pd.merge(arr_count,dep_count, on = ['date'])

    return arr_df, dep_df


def merge_weather_flights_data(
        weather_info: pd.DataFrame,
        flight_counts: pd.DataFrame,
):
    weather_flight_features = pd.merge(flight_counts, weather_info, on='timestamp', how='inner')

    return weather_flight_features


def faa_class_counts(c, col_name, prefix, arr_runway_by_aircraft):
    # print(arr_runway_by_aircraft['faa_weight_class']==c)
    print(arr_runway_by_aircraft)
    arr_runway_by_aircraft=arr_runway_by_aircraft.loc[arr_runway_by_aircraft['faa_weight_class'] == c]
    # arr_runway_by_aircraft.dropna (inplace = True)
    # print(arr_runway_by_aircraft)
    arr_runway_by_aircraft=arr_runway_by_aircraft.sort_values(by=[prefix + '_runway_actual_time'], ascending=[1])
    c_count=arr_runway_by_aircraft.set_index(prefix + '_runway_actual_time').resample(timedelta(minutes=15)).count()

    c_count=c_count.rename_axis('date').reset_index()
    c_count=c_count[['date', 'gufi']].rename({'gufi': col_name}, axis=1)
    c_count=c_count.dropna()
    # c_count['truncated_date'] = c_count['date'].apply(lambda t: t.replace(minute=0, second=0))
    return c_count


def merge_df(df, nf):
    return nf.merge(df, how='left', left_on='truncated_timestamp', right_on='date')

def combined_features(aircraft_mapping, runway_flights, KCLT_configs):
    #print(aircraft_mapping)
    aircraft_classes = aircraft_mapping['faa_weight_class'].unique()
    arr_df, dep_df, _, _, _ = arr_dep_runway_counts(runway_flights)
    #print(arr_df)
    KCLT_configs= KCLT_configs.sort_values('timestamp', ascending=True)
    features = KCLT_configs.copy()
    features['truncated_timestamp'] = features['timestamp'].dt.floor('15min')
    new_features = features.copy()
    for c in aircraft_classes:
        f = faa_class_counts(c, c+'_arr_count', 'arrival', arr_df)
        new_features = merge_df(f, new_features)
        new_features.sort_values('truncated_timestamp', ascending=True)
        
        f = faa_class_counts(c, c+'_dep_count', 'departure', dep_df)
        new_features = merge_df(f, new_features)
        new_features = new_features.fillna(0)
    return new_features
