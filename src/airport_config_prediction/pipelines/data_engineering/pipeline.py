from .weather_data_eng import *
from .runways_flights_eda import *
from .best_swim_ETA_ETD import *
from kedro.pipeline import Pipeline, node
from .nodes import *
from kedro.config import ConfigLoader
from .nodes import *
from .ETD_flight_counts import *
from kedro.config import ConfigLoader
from .remove_first_position_dup import *


from data_services.swim_based_eta import build_swim_eta





def create_airport_config_de_pipeline(**kwargs):
    
    config_de_pipeline = Pipeline(
        [
            node(
                func=weather_process_nulls,
                inputs=["lamp_weather_data_set@CSV"],
                outputs="weather_data",
            ),
            node(
                func=weather_to_wide_sampled,
                inputs=["weather_data","parameters"],
                outputs="weather_data_wide_sampled"
            ),
            node(
                func=Merge_run_flight,
                inputs=["MFS_data_set@CSV", "runways_data_set@CSV", "aircraft_mapping@CSV"],
                outputs="runway_flights"
            ),
            node(
                func=arr_dep_runway_counts,
                inputs= "runway_flights",
                outputs= ["arr_df", "dep_df"]
            ),
            node(
                func=remove_first_pos_dup,
                inputs="first_position_data_set@CSV",
                outputs="filtered_first_pos"
            ),

            node(
                func=build_swim_eta,
                inputs=[
                    "TFM_track_data_set@CSV",
                    "TBFM_data_set@CSV",
                    "filtered_first_pos"
                    ,
                ],
                outputs="best_swim_eta",
                name='build_swim_eta',
            ),
            node(
                func=get_etd,
                inputs=[
                    "ETD_data_set@CSV"
                ],
                outputs="ETD",
                name="ETD",
            ),
            node(
                func=sampling_time_bins,
                inputs=[
                    "parameters",
                       ],
                outputs="sampled_bins",
                name="sampled_bins",
            ),
            node(
                func=ETD_sampling,
                inputs=[
                    "ETD",
                    "parameters",
                    "dep_df"
                ],
                outputs="etd_sampled",
                name="etd_sampled",
            ),
            node(
                func=dep_filtering_flights,
                inputs=[
                    "sampled_bins",
                    "etd_sampled",
                ],

                outputs="dep_filtered",
                name="dep_filtered",
            ),
            node(
                func=dep_pivot_table,
                inputs=[
                    "dep_filtered",
                    "parameters",

                ],

                outputs="dep_counts",
                name="dep_counts",
            ),

            node(
                func=ETA_sampling,
                inputs=[
                    "best_swim_eta",
                    "parameters",
                    "arr_df"
                ],
                outputs="eta_sampled",
                name="eta_sampled",
            ),
            node(
                func=arr_filtering_flights,
                inputs=[
                    "sampled_bins",
                    "eta_sampled",
                ],

                outputs="arr_filtered",
                name="arr_filtered",
            ),
            node(
                func=arr_pivot_table,
                inputs=[
                    "arr_filtered",
                    "parameters",

                ],

                outputs="arr_counts",
                name="arr_counts",
            ),
            node(
                func=merge_weather_flights_data,
                inputs=["weather_data_wide_sampled", "dep_counts","arr_counts"],
                outputs="weather_flight_features"
            ),
            node(
                func=merge_target_airport_configs,
                inputs=["weather_flight_features","configs_data_set@CSV","parameters"],
                outputs="weather_flight_target_features"
            ),
            node(
                func=de_save,
                inputs=["weather_flight_target_features","params:globals"],
                outputs=None
            )
        ]
    )
    return config_de_pipeline
