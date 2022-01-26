"""Pipeline for overall airport_config data query and save
"""

import os
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import *


def create_pipelines(**kwargs):

    return Pipeline(
        [
            node(
                func=query_save_version_LAMP,
                inputs= [
                    "lamp_weather_data_set@DB",
                    "params:globals",
                ],

                outputs=None,
            ),
            node(
                func=query_save_version_configs,
                inputs=[
                    "configs_data_set@DB",
                    "params:globals",
                ],
                outputs=None,
            ),
            node(
                func=query_save_version_MFS,
                inputs=[
                    "MFS_data_set@DB",
                    "params:globals",
                ],
                outputs=None,
            ),
            node(
                func=query_save_version_runways,
                inputs=[
                    "runways_data_set@DB",
                    "params:globals",
                ],
                outputs=None,
            ),
            node(
                func=query_save_version_first_position,
                inputs=[
                    "first_position_data_set@DB",
                    "params:globals",
                ],
                outputs=None,

            ),
            node(
                func=query_save_version_TFM_track,
                inputs=[
                    "TFM_track_data_set@DB",
                    "params:globals",
                ],
                outputs=None,

            ),
            node(
                func=query_save_version_TBFM,
                inputs=[
                    "TBFM_data_set@DB",
                    "params:globals",
                ],
                outputs=None,

            ),
            node(
                func=query_save_version_ETD,
                inputs=[
                    "ETD_data_set@DB",
                    "params:globals",
                ],
                outputs=None,

            ),


        ],
    )
