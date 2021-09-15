"""Pipeline for airport configuration prediction
"""
from typing import Dict

from kedro.pipeline import Pipeline, node
from .nodes import *
from .mlflow_utils import init_mlflow


def create_pipelines(**kwargs):
    airport_config_pipeline = Pipeline(
        [
            node(
                name="init_mlflow",
                func=init_mlflow,
                inputs=[
                    "parameters"
                ],
                outputs="active_run",
            ),
            node(
                name='add_train_test',
                func=add_train_test,
                inputs=["de_data_set@PKL", "parameters"],
                outputs="data1"
            ),
            node(
                name='filter_configurations',
                func=filter_configurations,
                inputs=["data1", "parameters"],
                outputs="data2"
            ),
            node(
                name='train_model',
                func=train_model,
                inputs=["active_run",
                        "data2",
                        "parameters"
                        ],
                outputs="model_pipeline",
            ),
            node(
                name = 'train_baseline',
                func=train_baseline,
                inputs=["active_run",
                        "parameters"
                ],
                outputs="baseline_pipeline",
            ),
            node(
                name = 'predict',
                func=predict,
                inputs=[
                    "model_pipeline",
                    "data2",
                    "params:model"
                ],
                outputs="data_predicted_m",
            ),
            node(
                name = 'predict_baseline',
                func=predict_baseline,
                inputs=[
                    "baseline_pipeline",
                    "data_predicted_m",
                    "parameters"
                ],
                outputs="data_predicted",
            ),
            node(
                name = 'report_performance_metrics',
                func=report_performance_metrics,
                inputs=[
                    "data_predicted",
                    "parameters"
                ],
                outputs=None,
            )
        ]
    )

    return {
        'airport_config': airport_config_pipeline,
    }


