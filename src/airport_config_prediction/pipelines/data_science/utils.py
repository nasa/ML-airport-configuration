from sklearn import metrics
import pandas as pd
import numpy as np
from typing import Any, Dict
import logging
import mlflow
import os
from sklearn.base import BaseEstimator, TransformerMixin
import matplotlib.pyplot as plt

def calculate_model_metrics(
        data: pd.DataFrame,
        parameters: Dict[str, Any],
        y_pred: str,
        name_prefix: str = '',
        group_values: list = ['train', 'test'],
) -> None:
    """Node for reporting the performance metrics of the predictions performed
    by the previous node. Notice that this function has no outputs, except
    logging.
    """

    metrics_dict = {
        metric_name: METRIC_NAME_TO_FUNCTION_DICT[metric_name]
        for metric_name in parameters['metrics']
    }

    evaluation_df = evaluate_predictions(
        data[ data.group.isin(group_values) &
              (data.missing_core_features == False)],
        y_true=parameters['target'],
        y_pred=y_pred,
        metrics_dict=metrics_dict,
    )

    # Log the accuracy of the model
    log = logging.getLogger(__name__)

    for metric_name in metrics_dict.keys():
        log.info("metric {}:".format(name_prefix + metric_name))
        for group in [v for v in data.group.unique() if v in group_values]:

            log.info("{} group: {}".format(
                group,
                evaluation_df.loc[group, metric_name]
            ))

            if 'classification_report' in metric_name:
                for creport in evaluation_df.loc[group, metric_name]:
                    # Log reports as html artifacts in mlflow. For reports by lookahead storing only hourly reports
                    if (type(creport[0]) == str) or (creport[0] % 60 == 0):
                        report_df = pd.DataFrame(creport[1]).transpose()
                        file_path_name = './' + name_prefix+ 'classification_report_' + group +'_' +str(creport[0]) + '.html'
                        report_df.to_html(file_path_name)
                        mlflow.log_artifact(file_path_name)
                        os.remove(file_path_name)
            else:
                if type(evaluation_df.loc[group, metric_name]) != list:
                    mlflow.log_metric(
                        name_prefix + metric_name + '_' + group,
                        evaluation_df.loc[group, metric_name]
                    )

    return evaluation_df


def evaluate_predictions(
        df,
        y_true,
        y_pred,
        metrics_dict,
):
    evaluation_df = pd.DataFrame(
        index=df.group.unique(),
    )

    for metric_name, metric_func in metrics_dict.items():

        evaluation_df[metric_name] = None

        for group in df.group.unique():
            evaluation_df.loc[group, metric_name] = \
                metric_func(
                    df.loc[df.group == group, y_true],
                    df.loc[df.group == group, y_pred],
                )

    return evaluation_df


def classification_report_by_lookahead_hour(
        y_true: list,
        y_pred: list,
) -> Dict:

    lookaheads = y_true.iloc[0]['lookahead']

    classfication_reports = []
    for lookahead in lookaheads:
        classfication_reports.append( classification_report(y_true, y_pred, lookahead=lookahead))

    classfication_reports.append( classification_report(y_true, y_pred, lookahead='all'))

    return classfication_reports


def classification_report(
        y_true: list,
        y_pred: list,
        ignore_y_true_nas: bool = True,
        lookahead='all'
) -> Dict:
    if lookahead != 'all':
        y_true = [v['value'][np.where(v['lookahead'] == lookahead)[0][0]] for v in y_true]
        y_pred = [v['value'][np.where(v['lookahead'] == lookahead)[0][0]] for v in y_pred]
    else:
        y_true = [item for v in y_true for item in v['value'][np.argsort(v['lookahead'])]]
        y_pred = [item for v in y_pred for item in v['value'][np.argsort(v['lookahead'])]]

    if ignore_y_true_nas == True:
        idx = (pd.isnull(y_true) == False)
        y_true = np.array(y_true)[idx]
        y_pred = np.array(y_pred)[idx]

    return (lookahead,metrics.classification_report(y_true, y_pred, output_dict=True))


def misclassified_pseudo_levenshtein_by_lookahead_hour(
        y_true: list,
        y_pred: list,
) -> Dict:
    """
    For incorrect predictions the metric obtains the distance between the true and predicted config values,
    by counting the number of runways that need to be added or removed from the predicted config to get to
    the actual config. This approach resembles the levenshtein distance between strings.

    """
    lookaheads = y_true.iloc[0]['lookahead']

    metric = []
    for lookahead in lookaheads:
        metric.append((lookahead, misclassified_pseudo_levenshtein(y_true,y_pred,lookahead)))

    return metric


def  misclassified_pseudo_levenshtein(
        y_true: list,
        y_pred: list,
        lookahead: np.int,
        ignore_y_true_nas: bool = True,
) -> Dict:

    y_true = [v['value'][np.where(v['lookahead'] == lookahead)[0][0]] for v in y_true]
    y_pred = [v['value'][np.where(v['lookahead'] == lookahead)[0][0]] for v in y_pred]

    if ignore_y_true_nas:
        idx = (pd.isnull(y_true) == False)
        y_true = np.array(y_true)[idx]
        y_pred = np.array(y_pred)[idx]

    idx = y_true != y_pred
    y_true = y_true[idx]
    y_pred = y_pred[idx]

    return np.mean([pseudo_levenshtein_distance(y_true[i],y_pred[i])  for i in range(len(y_pred))])


def pseudo_levenshtein_distance(config_true, config_pred):
    arr_true = config_true.split('A_')[1].split('_D')[0].split('_')
    arr_pred = config_pred.split('A_')[1].split('_D')[0].split('_')
    dep_true = config_true.split('D_')[1].split('_A')[0].split('_')
    dep_pred = config_pred.split('D_')[1].split('_A')[0].split('_')

    matches_pred = np.array([v in dep_true for v in dep_pred] + [v in arr_true for v in arr_pred])
    matches_true = np.array([v in dep_pred for v in dep_true] + [v in arr_pred for v in arr_true])
    distance = sum(matches_pred == False) + sum(matches_true == False)

    return distance

METRIC_NAME_TO_FUNCTION_DICT = {
    'classification_report': classification_report,
    'classification_report_by_lookahead_hour':classification_report_by_lookahead_hour,
    'misclassified_pseudo_levenshtein_by_lookahead_hour':misclassified_pseudo_levenshtein_by_lookahead_hour
}


def archive_pseudo_levenshtein_plot(
        performance_metrics: pd.DataFrame,
        colors: Dict[str, Any] = {'baseline': 'b', 'model': 'r'},
        group: str ='test'
) -> pd.DataFrame:
    file_path_name = './model_vs_baseline_misclassified_pseudo_levenshtein_error_'+group+'.png'
    plt.rc('font', size=20)
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot()

    for model in performance_metrics:
        x = [];
        y = [];
        for i in range(len(performance_metrics[model].loc[group].loc['misclassified_pseudo_levenshtein_by_lookahead_hour'])):
            v = performance_metrics[model].loc[group]['misclassified_pseudo_levenshtein_by_lookahead_hour'][i]
            if type(v[0]) != str:
                x.append(v[0])
                y.append(v[1])

        ax.plot(x,  np.array(y), c=colors[model], marker=(8, 2, 0), ls='--', label=model)
        plt.xlabel('Planning Horizon Forecast (min)')
        plt.ylabel('Pseudo-levenshtein runway error')
        plt.legend(loc=1)

    fig.savefig(file_path_name)
    mlflow.log_artifact(file_path_name)
    os.remove(file_path_name)


def archive_accuracy_plot(
        performance_metrics: pd.DataFrame,
        colors: Dict[str, Any] = {'baseline': 'b', 'model': 'r'},
        group: str ='test'
) -> pd.DataFrame:
    file_path_name = './model_vs_baseline_accuracy_'+group+'.png'
    plt.rc('font', size=20)
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot()

    for model in performance_metrics:
        x = [];
        y = [];
        for i in range(len(performance_metrics[model].loc[group].loc['classification_report_by_lookahead_hour'])):
            v = performance_metrics[model].loc[group]['classification_report_by_lookahead_hour'][i]
            if type(v[0]) != str:
                x.append(v[0])
                y.append(v[1]['accuracy'])

        ax.plot(x, 100 * np.array(y), c=colors[model], marker=(8, 2, 0), ls='--', label=model)
        plt.xlabel('Planning Horizon Forecast (min)')
        plt.ylabel('Prediction accuracy (%)')
        plt.legend(loc=1)

    fig.savefig(file_path_name)
    mlflow.log_artifact(file_path_name)
    os.remove(file_path_name)