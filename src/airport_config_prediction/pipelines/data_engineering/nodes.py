
import pandas as pd
from typing import Dict, Any
import numpy as np
from .utils import sampling_times
import logging
from kedro.extras.datasets.pickle import PickleDataSet
import os
import pickle
from datetime import date


def merge_target_airport_configs(
        weather_flight_features: pd.DataFrame,
        configs: pd.DataFrame,
        parameters: Dict[str, Any],
)-> pd.DataFrame:
    """
    This function merges actual airport configuration values to the main data frame. Multiple future configuration values
    are added as defined by the prediction_lookahead and  prediction_delta (prediction_lookahead/prediction_delta columns).
    The current configuration is also added to the input data

    """
    configs= configs.rename(columns={'start_time': 'timestamp'})
    configs = configs[['timestamp', 'airport_configuration_name']]
    configs = configs.assign(timestamp_config=configs['timestamp'])
    configs = configs.sort_values('timestamp_config', ascending=True)

    # Sample configuration data
    start_datetime = configs['timestamp_config'].min().ceil("H")
    end_datetime = configs['timestamp_config'].max().floor("H")
    time_df = sampling_times(parameters['prediction_delta'], start_datetime, end_datetime)
    configs_sampled = pd.merge_asof(time_df, configs, on="timestamp", direction="backward")

    #  TODO: for CLT 93.6% of the data is kept after removing stale configs. For other airports the current logic
    #  could lead to removing too many rows. Need to keep an eye on the logged value below to see if additional logic
    #  is needed
    # Remove stale configs data
    is_stale = (configs_sampled['timestamp'] - configs_sampled['timestamp_config']) \
                                    / np.timedelta64(1,'h') > parameters['stale_airport_config_th']

    # Log rows with stale configs
    log = logging.getLogger(__name__)
    log.info('Kept {:.1f}% of rows when removing stale airport configuration'.format(
        100 * (1-(sum(is_stale) / configs_sampled.shape[0])) ))
    configs_sampled.loc[is_stale, 'airport_configuration_name'] = None
    configs_sampled.drop(columns=['timestamp_config'], inplace=True)

    # Restructure data, add future values
    configs_wide = future_values_reshape(configs_sampled,
                                                   parameters['prediction_lookahead'],
                                                   parameters['prediction_delta'],
                                                   'timestamp')

    # Add current configuration
    configs_wide = pd.merge(configs_wide, configs_sampled, on='timestamp')

    # Remove NAs, only removing NAs in current config and first future config
    fields_remove_na = ['airport_configuration_name', 'airport_configuration_name' + '_' + str(parameters['prediction_delta'])]
    is_na = configs_wide[fields_remove_na].isna().any(axis=1)
    configs_wide = configs_wide[is_na == False]

    # All future airport configuration columns are stored in a single columns
    configs_wide = lookahead_cols_to_single_col(configs_wide, 'airport_configuration_name_')
    configs_wide = configs_wide.rename(columns={'airport_configuration_name': 'airport_configuration_name_current'})

    # Merge target configuration data
    data = pd.merge(weather_flight_features, configs_wide, on='timestamp', how='inner')

    return data


def lookahead_cols_to_single_col(
        data: pd.DataFrame,
        col_name: str,
        remove_lookahead_cols: bool = True,
)-> pd.DataFrame:
    """
    This function creates a single column with all future columns for the selected col_name. The info is stored in each
    cell as a dictionary: {'value': [], 'lookahead': []}, with the list of future values and associated lookaheads
    for each row
    """

    cols_valid = np.array([v for v in data.columns if col_name in v])
    lookaheads = np.array([int(v.split(col_name)[-1]) for v in cols_valid])

    idxs_sorted = np.argsort(lookaheads)
    cols_valid_sorted = cols_valid[idxs_sorted]
    lookaheads_sorted = lookaheads[idxs_sorted]

    new_field = []
    for index, row in data[cols_valid_sorted].iterrows():
        new_field.append({'value': row.values, 'lookahead': lookaheads_sorted})

    data[col_name + 'future'] = new_field

    if remove_lookahead_cols == True:
        data = data.drop(columns=cols_valid)  # comment to keep individual future values columns

    return data


def future_values_reshape(
        data: pd.DataFrame,
        lookahead_time: int,
        prediction_delta: int,
        time_field: str,
)-> pd.DataFrame:
    """
    This function reshapes data, the output contains the time_field and n_future_steps columns
    with future values (e.g. airport_configuration_name_60, 60 denotes lookahead minutes)
    """
    n_future_steps = int(np.floor(lookahead_time / prediction_delta))
    data = data.sort_values(by=time_field, ascending=True)
    l_data = []
    for i in range(1, n_future_steps + 1):
        data_lat = data.copy()
        data_lat[time_field] = data_lat[time_field].shift(i)
        data_lat['LAT'] = i * prediction_delta
        l_data.append(data_lat)

    df = pd.concat(l_data).sort_values(by=[time_field, 'LAT'])

    df = df.pivot_table(index=time_field, columns='LAT', aggfunc='first')
    df.columns = df.columns.map(lambda x: x[0] + '_' + str(int(x[1])))
    df.reset_index(inplace=True)

    return df


def add_train_test_group_random(
    data: pd.DataFrame,
    test_size: float,
    random_seed: int,
) -> pd.DataFrame:
    # Set random seed
    np.random.seed(random_seed)
    # Apply group
    data['group'] = data.apply(
        lambda row: 'test' if np.random.uniform() < test_size else 'train',
        axis=1,
    )

def get_etd(
        data: pd.DataFrame,

) -> pd.DataFrame:

    return data

def merge_weather_flights_data(
        weather_data: pd.DataFrame,
        dep_counts: pd.DataFrame,
        arr_counts: pd.DataFrame,
):
    flight_counts = pd.merge(dep_counts, arr_counts, on='timestamp', how='inner')
    weather_flight_features = pd.merge(flight_counts, weather_data, on='timestamp', how='inner')

    return weather_flight_features.sort_values('timestamp')


def de_save(
    data: pd.DataFrame,
    params_globals: str,
    data_folder: str ='./data/05_model_input/'
):
    wind_gust_col_names = data.columns.str.startswith("wind_gust")
    data.loc[:, wind_gust_col_names] = data.loc[:, wind_gust_col_names].fillna('0').replace('NG', '0').astype(int)
    if 'batch_mode' in params_globals:

        # Delete previous runs batch files for airport_icao
        if params_globals['start_time'] == params_globals['batch_mode']['run_start_time']:
            files = os.listdir(data_folder)
            files = [f for f in files if
                     f[0:len(params_globals['airport_icao']) + 1] == params_globals['airport_icao'] + '_']
            for f in files:
                os.remove(data_folder + f)

        # Save current batch
        data_set = PickleDataSet(
            filepath=data_folder + params_globals['airport_icao'] + '_' + str(params_globals['start_time']) \
                     + '_' + str(params_globals['end_time']) + ".de_data_set.pkl", backend="pickle")
        data_set.save(data)

        # Concatenate all data in single file in last iteration, ds pipeline expecting single file
        if params_globals['end_time'] >= params_globals['batch_mode']['run_end_time']:

            files = os.listdir(data_folder)
            files = [f for f in files if f[0:len(params_globals['airport_icao']) + 1] == params_globals['airport_icao'] + '_']

            file_start_dates = [date.fromisoformat(f.split('_')[1]) for f in files]
            idx_sorted = np.argsort(file_start_dates)

            # Load data ordered
            de_data = []
            for idx in idx_sorted:
                with open(data_folder + files[idx], "rb") as f:
                    de_data.append(pickle.load(f))

            # Concatenate all data, keep order and remove duplicates
            de_data = pd.concat(de_data, sort=False)
            # For duplicates, keep "last", the "first" duplicates from previous batch may not include all data due to
            # the end of batch
            de_data = de_data[de_data['timestamp'].duplicated(keep='last') == False]
            de_data = de_data.sort_values(by='timestamp')

            # Save data
            data_set = PickleDataSet(
                filepath=data_folder + params_globals['airport_icao'] + ".de_data_set.pkl", backend="pickle")
            data_set.save(de_data)
    else:

        data_set = PickleDataSet(
            filepath=data_folder + params_globals['airport_icao']+ ".de_data_set.pkl", backend="pickle")
        data_set.save(data)
