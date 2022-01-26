# Pipeline data_query_and_save

## Overview

This pipeline issue SQL queries to the databases and save the returned tables into CSV files using kedro transcoding
capability.

## Pipeline inputs
</br>
<!---
The list of pipeline inputs.
-->

### `lamp_weather_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type  |  data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileDataSet|
| Description | Input data from lamp_weather_data : timestamp,forecast_timestamp,temperature,wind_direction,wind_speed,wind_gust,cloud_ceiling,visibility,cloud,lightning_prob and precip|
</br>


### `MFS_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type  |  data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
| Description | Input data from matm_flight_summary : arrival, departure runways for params:airport_icao|
</br>

### `configs_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type  |  data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
| Description | Input data from datis_parser_message : arrival, departure configurations, and datis-time for params:airport_icao|
</br>




### `runways_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type  |  data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
| Description | Input data from runway_actuals :actual arrival, departure runways and arrival , departure actual times for params:airport_icao|
</br>



### `TBFM_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type  |  data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
| Description | Input data from tbfm_extension_all: gufi, timestamp and arrival_runway_sta. The output is then aggregated to calculate the best ETA time|

</br> 

### `TFM_track_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type | data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
|Description | Input data from matm_flight : timestamp, arrival_runway_estimated_time.The output is then aggregated to calculate the best ETA time|
</br> 

### `first_position_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type | data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
|Description | Input data from matm_flight : gufi, the time first tracked .The output is then aggregated to calculate the best ETA time|
</br> 


### `ETD_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type | data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
|Description | Input data from matm_flight : gufi, departure_runway_estimated_time|
</br> 

## Pipeline outputs
</br>
<!---
The list of pipeline outputs.
-->


### `lamp_weather_data_set@CSV`
|      |                    |
| ---- | ------------------ |
| Type | pandas.CSVDataSet
|Description |  Output data from lamp_weather_data : timestamp,forecast_timestamp,temperature,wind_direction,wind_speed,wind_gust,cloud_ceiling,visibility,cloud,lightning_prob and precip|
</br> 

### `MFS_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type  |  pandas.CSVDataSet
| Description | output data from matm_flight_summary : arrival, departure runways for params:airport_icao|
</br>

### `configs_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type  |  pandas.CSVDataSet|
| Description | output data from datis_parser_message : arrival, departure configurations, and datis-time for params:airport_icao|
</br>




### `runways_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type  |  pandas.CSVDataSet
| Description | output data from runway_actuals :actual arrival, departure runways and arrival , departure actual times for params:airport_icao|
</br>



### `TBFM_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type  |  pandas.CSVDataSet|
| Description | output data from tbfm_extension_all: gufi, timestamp and arrival_runway_sta. The output is then aggregated to calculate the best ETA time|

</br> 

### `TFM_track_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type | data_services.kedro_extensions.io.sqlfile_dataset.SQLQueryFileChunkedDataSet|
|Description | Input data from matm_flight : timestamp, arrival_runway_estimated_time.The output is then aggregated to calculate the best ETA time|
</br> 

### `first_position_data_set@CSV`
|      |                    |
| ---- | ------------------ |
Type | pandas.CSVDataSet
|Description | output data from matm_flight : gufi, the time first tracked .The output is then aggregated to calculate the best ETA time|
</br> 


### `ETD_data_set@DB`
|      |                    |
| ---- | ------------------ |
Type | pandas.CSVDataSet
|Description | output data from matm_flight : gufi, departure_runway_estimated_time|
</br> 
