from sklearn.pipeline import Pipeline as sklearn_Pipeline
from mlflow import sklearn as mlf_sklearn
from data_services.mlflow_utils import add_environment_specs_to_conda_file
from data_services.FilterPipeline import FilterPipeline
from typing import Any, Dict
from .utils import *
from .baseline import PropagateCurrentConfigModel
from data_services.recursive_multistep_pipeline import RecursiveMultiStepPipeline
from data_services.recursive_multistep_pipeline import get_features_df
from data_services.airport_configuration_pipeline import airport_configuration_pipeline
from data_services.filter_pipeline_utils import create_exclude_func_min, create_exclude_func_max,\
create_exclude_func_not_numeric,create_exclude_func_not_bool, create_exclude_func_not_categorical,\
    create_exclude_func_not_datetime
import random
from datetime import datetime, date
import time
import numbers

def train_model(
    active_run: str,
    data: pd.DataFrame,
    parameters: Dict[str, Any]
) -> sklearn_Pipeline:

    recursive_pipeline = RecursiveMultiStepPipeline(parameters['prediction_lookahead'],
                                                    parameters['prediction_delta'],
                                                    airport_configuration_pipeline,
                                                    parameters['model'],
                                                    parameters['inputs'],
                                                    parameters['target'],)


    # Add wrapper to skip model and return None for invalid core features
    default_response = data['airport_configuration_name_current'].mode()[0]
    lookaheads = list(range(parameters['prediction_delta'],
                            parameters['prediction_delta']+parameters['prediction_lookahead'],
                            parameters['prediction_delta']))
    n_lookaheads = len(lookaheads)
    default_response = {'value':np.array([default_response]*n_lookaheads),
                        'lookahead':np.array(lookaheads)}

    # Create a default response function 
    def func_creator(lookahead_list, default_val) :
        npts = len(lookahead_list)
        def func_default_response(X):
            current_conf = X['airport_configuration_name_current'].copy()
            prediction = current_conf.apply(
                lambda x : {'value':np.array([x]*npts),
                            'lookahead':np.array(lookahead_list)})
            na_conf = current_conf.isna()
            prediction.loc[na_conf] = [default_val]*(na_conf.sum()) # use the most common if not defined
            prediction = prediction.values # to return an array
            # Return a single value if only 1 element array to match a singular default value
            if (len(prediction) == 1) :
                return prediction[0] # .iloc[0] if returning a pd.Series
            else :
                return prediction
        return func_default_response
    
    filt_pipeline = FilterPipeline(recursive_pipeline, func_creator(lookaheads, default_response))
    
    features = get_features_df(data.columns, parameters['inputs'])
    missing_values = [np.nan, None, '']
    for i in range(len(features['name'])):
        if features['is_core'][i] == True:
            feature_name = features['name'][i]
            if features['type'][i] == 'categorical':
                feature_values = list(data[feature_name].unique())
                # Rules flagging unknown values (ignoring missing values)
                filt_pipeline.add_include_rule(feature_name, feature_values + missing_values, 'Unknown ' + feature_name)
                # Rules flagging bad data type
                filt_pipeline.add_exclude_rule(feature_name,
                                               create_exclude_func_not_categorical(),
                                               'Bad type for ' + feature_name)

            elif features['type'][i] == 'bool':
                feature_values = list(data[feature_name].unique())
                # Rules flagging unknown values (ignoring missing values)
                filt_pipeline.add_include_rule(feature_name, feature_values + missing_values, 'Unknown ' + feature_name)
                # Rules flagging bad data type
                filt_pipeline.add_exclude_rule(feature_name,
                                               create_exclude_func_not_bool(),
                                               'Bad type for ' + feature_name)

            elif features['type'][i] == 'datetime':
                # Rules flagging bad data type
                filt_pipeline.add_exclude_rule(feature_name,
                                               create_exclude_func_not_datetime(),
                                               'Bad type for ' + feature_name)

            elif features['type'][i] == 'numeric':
                if 'min' in features['constraints'][i]:
                    min_val = features['constraints'][i]['min']
                    filt_pipeline.add_exclude_rule(feature_name, create_exclude_func_min(min_val), 'Value less than accepted min ' + feature_name)

                if 'max' in features['constraints'][i]:
                    max_val = features['constraints'][i]['max']
                    filt_pipeline.add_exclude_rule(feature_name, create_exclude_func_max(max_val), 'Value greater than accepted max ' + feature_name)

                # Rules flagging bad data type
                filt_pipeline.add_exclude_rule(feature_name,
                                               create_exclude_func_not_numeric(),
                                               'Bad type for ' + feature_name)
            # Rules flagging missing values
            filt_pipeline.add_exclude_rule(feature_name, missing_values, 'Missing ' + feature_name)

    # Train pipeline
    tic = time.time()

    filt_pipeline.fit(
        data.loc[
            data.group == 'train',
            features['name']
        ],
        data.loc[
            data.group == 'train',
            parameters['target']
        ]
    )
    toc = time.time()
    log = logging.getLogger(__name__)
    log.info('training airport configuration model took {:.1f} minutes'.format(
        (toc-tic)/60)
    )

    # Log trained model
    mlf_sklearn.log_model(
        sk_model=filt_pipeline,
        artifact_path='model',
        conda_env=add_environment_specs_to_conda_file()
    )

    return filt_pipeline


def train_baseline(
    active_run: str,
    parameters: Dict[str, Any],
) -> None:

    if ('baseline' in parameters) and (parameters['baseline'] == 'PropagateCurrentConfigModel'):

        LATs = list(range(  parameters['prediction_delta'],
                            parameters['prediction_lookahead']+parameters['prediction_delta'],
                            parameters['prediction_delta']))

        baseline_model = FilterPipeline(PropagateCurrentConfigModel(LATs), np.nan)
        # Log trained model
        mlf_sklearn.log_model(
            sk_model=baseline_model,
            artifact_path='baseline',
            conda_env=add_environment_specs_to_conda_file()
            )

    else:
        baseline_model = []

    return baseline_model


def predict_baseline(
    pipeline: sklearn_Pipeline,
    data: pd.DataFrame,
    parameters: Dict[str, Any]
) -> pd.DataFrame:

    if 'baseline' in parameters:
        data['predicted_baseline'] = pipeline.predict(data)

    return data


def predict(
    pipeline: sklearn_Pipeline,
    data: pd.DataFrame,
    model_params: Dict[str, Any],
) -> pd.DataFrame:
    # Run model
    tic = time.time()
    predictions = pipeline.predict(data)
    toc = time.time()

    log = logging.getLogger(__name__)
    log.info('predicting took {:.1f} minutes'.format(
        (toc-tic)/60)
    )

    # Add predictions to dataframe
    data['predicted_{}'.format(model_params['name'])] = predictions
    # Flag predictions with missing core features
    data['missing_core_features'] = pipeline._filter(data) == False

    return data


def report_performance_metrics(
    data: pd.DataFrame,
    parameters: Dict[str, Any]
) -> pd.DataFrame:
    """Node for reporting performance metrics.
    """

    # Predictive model
    performance_metrics={}
    performance_metrics['model'] =calculate_model_metrics(data,parameters,'predicted_{}'.format(parameters['model']['name']))

    # Baseline
    if 'predicted_baseline' in data.columns:
        performance_metrics['baseline'] = calculate_model_metrics(data, parameters, 'predicted_baseline','baseline_',['train', 'test'])

    if 'classification_report_by_lookahead_hour' in parameters['metrics']:
        archive_accuracy_plot(performance_metrics,group='test')
        archive_accuracy_plot(performance_metrics, group='train')

    if 'misclassified_pseudo_levenshtein_by_lookahead_hour' in parameters['metrics']:
        archive_pseudo_levenshtein_plot(performance_metrics,group='test')
        archive_pseudo_levenshtein_plot(performance_metrics, group='train')


def calculate_ATM_datetime(
    utc_timestamp: datetime,  # tz naive
    local_tz_name: str,
    ATM_date_start_hour_local: int = 4,
    type: str = 'date'

) -> date:
    """Take in a tz naive utc_timetamp and find its "ATM date" based on a
    timezone and local hour of day at which "ATM datetime" starts
    (usually 4am local).
    """
    if utc_timestamp.tz is not None:
        raise(ValueError, 'utc_timestamp is not tz naive')

    local_datetime = utc_timestamp\
        .tz_localize('UTC')\
        .tz_convert(local_tz_name)

    atm_datetime = local_datetime - pd.to_timedelta(ATM_date_start_hour_local, unit='h')

    return atm_datetime


def add_train_test_group_disjoint(
    data: pd.DataFrame,
    parameters: Dict[str, Any],
) -> pd.DataFrame:

    data = data.sort_values(by='timestamp', ascending=True)
    n_samples_train = int(len(data.index) * (1-parameters['test_size']))
    data['group'] = ['train' if i < n_samples_train else 'test' for i in range(len(data.index))]

    return data


def add_train_test_random_days(
            data: pd.DataFrame,
            parameters: Dict[str, Any],
            ATM_date_start_hour_local: int = 4,
    ) -> pd.DataFrame:

    # Get the ATM dates in the data
    days_local_ATM = data.apply(
        lambda row: calculate_ATM_datetime(
            row['timestamp'],
            parameters['globals']['tz_name'],
            ATM_date_start_hour_local,
        ),
        axis=1,
    ).dt.date

    days_local_ATM_unique = set(np.unique(days_local_ATM))
    random.seed(parameters['random_seed'])
    dates_test = random.sample(days_local_ATM_unique, int(len(days_local_ATM_unique) * parameters['test_size']))

    data['group']=['test' if v in dates_test else 'train' for v in days_local_ATM]

    # N = int(parameters['prediction_lookahead'] / parameters['prediction_delta'])
    # data = data.sort_values('timestamp', ascending = True)
    # idx = (data['group'].shift(N) == 'test') & (data['group'] == 'train')
    # data = data[idx==False]

    return data


def filter_configurations(
            data: pd.DataFrame,
            parameters: Dict[str, Any]
    ) -> pd.DataFrame:
    config_percent = data[data.group == 'train']['airport_configuration_name_current'].value_counts(normalize=True)
    keep_configs = config_percent[config_percent >= (parameters['min_airport_config_percent']/100)].index
    n_input_rows = len(data.index)
    n_input_configs = data.airport_configuration_name_current.nunique()
    data = data[data.airport_configuration_name_current.isin(keep_configs)]

    log = logging.getLogger(__name__)
    log.info('Kept {:.2f}% of rows when removing configurations active less than {} percent'.format(
        len(data.index) / n_input_rows * 100.0, parameters['min_airport_config_percent']))
    log.info('Kept {} configurations from {}'.format(len(keep_configs),n_input_configs))

    for i in range(data.shape[0]):
        idx = np.isin(data.airport_configuration_name_future.iloc[i]['value'],keep_configs) == False
        if sum(idx)>0:
            data.airport_configuration_name_future.iloc[i]['value'][idx] = np.nan

    return data


def add_train_test_random_weeks(
            data: pd.DataFrame,
            parameters: Dict[str, Any],
            ATM_date_start_hour_local: int = 4,
    ) -> pd.DataFrame:

    # Get the ATM dates in the data
    data['ATM_timestamp'] = data.apply(
        lambda row: calculate_ATM_datetime(
            row['timestamp'],
            parameters['globals']['tz_name'],
            ATM_date_start_hour_local,
        ),
        axis=1,
    )
    weeks_local_ATM=data['ATM_timestamp'].dt.week

    weeks_local_ATM_unique = set(np.unique(weeks_local_ATM))
    random.seed(parameters['random_seed'])
    weeks_test = random.sample(weeks_local_ATM_unique, int(len(weeks_local_ATM_unique) * parameters['test_size']))

    data['group']=['test' if v in weeks_test else 'train' for v in weeks_local_ATM]

    data=data[data.apply(lambda row: drop_overlapping_row(row['ATM_timestamp'], weeks_test), axis=1)]

    data.drop(columns=['ATM_timestamp'], inplace=True)

    return data


""" 
Check if there is a change to a  training/testing from a previous week , and if exists, drops rows for the first 6 hours from the beginning of the week.
"""
def drop_overlapping_row(ts, weeks_test):
    prev_week = ts.week - 1 if ts.week > 1 else 52
    week_grp = ts.week in weeks_test
    prev_week_grp = prev_week in weeks_test
    return not((ts.dayofweek == 0) & (ts.hour < 6) & (week_grp != prev_week_grp))

def add_train_test(
        data: pd.DataFrame,
        parameters: Dict[str, Any]
) -> pd.DataFrame:

    data = globals()[parameters['test_split_fcn']](data, parameters)

    return data



