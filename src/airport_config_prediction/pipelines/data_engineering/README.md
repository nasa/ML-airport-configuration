# Data Engineering pipeline

> *Note:* This `README.md` was generated using `Kedro 0.15.8` for illustration purposes. Please modify it according to your pipeline structure and contents.

## Overview

This pipeline process the incoming data from the data query and save pipeline and sets it in a format ready  for the data science pipeline.
The pipeline consists of many nodes,each of which call a specific function

## Pipeline nodes


### `weather_process_nulls`

|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
|Inputs | `lamp_weather_data_set@CSV`|,
| Description | DataFrame containing the processed weather data. This node process the weather data by filling nans value by ffill and mean value for the mean value of previous 6 hours  |

### `weather_to_wide_sampled`

|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
|Inputs | `weather data`,`lookahead parameter`|,
| Description | This node formats the weather data from long to wide format, and samples the data using the prediction_delta parameter. For each row, the weather info is for the last known prediction in the past, with prediction_delta steps into the future up to lookahead. Columns are labeled as metric name + lookahead  minutes, e.g. temperature_15, temperature_30 |

### `Merge_run_flight`

|      |                    |
| ---- | ------------------ |
|Type | `pd.DataFrame` |
|Inputs | `MFS_data_set@CSV`, `runways_data_set@CSV`, `aircraft_mapping@CSV`|,
| Description | This node merges the runways and flights with the aircrafts mappings|


### `arr_dep_runway_count`

|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `runway_flights` |
| Description | This node returns arrivals counts, departure counts  on the surface every 15 mins time interval |

### `remove_first_pos_dup`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `first_position_data_set@CSV` |
| Description | This node removes the duplicates from the first position data for the flights |

### `build_swim_eta`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `TFM_track_data_set@CSV"`,`TBFM_data_set@CSV`,`filtered_first_pos` |
| Description | This node computes the best available ETA based only on the SWIM data provided for arrivals to one airport. It is expected that input data are only provided at various instants when messages were received from various SWIM systems. This will require users to use an approximate merge (e.g., pandas.merge_asof() to determine which best available ETA applied at any instant in a different (e.g., model training) dataset. |

### `get_etd`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs |`ETD_data_set@CSV`|
| Description | This node gets the ETD(expected time of departures) for flights |


### `sampling_time_bin`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | parameters `prediction_lookahead`,`prediction_delta`|
| Description | This node samples the input timestamp into bins based on the prediction lookahead and the prediction delta parameters"|


### `ETD_sampling`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `prediction_delta`, `departure_runway_actual_time`|
|Description | This node uses the prediction_delta parameter, along with the gufi,departure_runway_actual_time to filter and get gufis that did not depart before prediction time |

### `dep_pivot_table`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `prediction_delta`, `departure_runway_actual_time`|
|Description | This node pivot the filtered departure counts based on the prediction delta |

### `ETA_sampling`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `prediction_delta`, `arrival_runway_actual_time`|
|Description | This node uses the prediction_delta parameter, best calculated swim_ETA, along with the gufi,arrival_runway_actual_time to filter and get gufis that did not arrive before prediction time |

### `arr_pivot_table`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `prediction_delta`, `arrival_runway_actual_time`|
|Description | This node pivot the filtered arrival counts based on the prediction delta |

### `merge_weather_flights_data`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `weather_data`, `flights_data`|
|Description | This node merge the processed weather data with the flights data|

### `merge_target_airport_configs`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
| Inputs | `weather_flight_features`,`configs_data_set@CSV`,`parameters`|
|Description |This node merges actual airport configuration values to the main data frame. Multiple future configuration values are added as defined by the prediction_lookahead and  prediction_delta (prediction_lookahead/prediction_delta columns).The current configuration is also added to the input data|

### `de_save`
|      |                    |
| ---- | ------------------ |
| Type | `pd.DataFrame` |
|Description |This node is the last step in the DE pipeline. it Concatenates all processed batch data in single file, keep order and remove duplicates in last iteration as ds pipeline expects single file. |