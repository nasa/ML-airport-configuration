select distinct on (airport_id, datis_time)
 *,
 'D_' || replace(departure_runways, ', ', '_') || '_A_' || replace(arrival_runways, ', ', '_') as airport_configuration_name
from datis_parser_message
where datis_time between :start_time and :end_time
and airport_id = :airport_icao

order by airport_id, datis_time, timestamp_source_received desc