select
	 gufi,
	 aircraft_engine_class,
	 aircraft_type,
	 arrival_aerodrome_icao_name,
	 major_carrier,
     flight_type,
     case when (arrival_aerodrome_icao_name = :airport_icao) then True
        else False
	 end as isArrival,
	 case when (departure_aerodrome_icao_name = :airport_icao) then True
		 else False
	 end as isDeparture,
	 COALESCE(arrival_stand_actual,
	 arrival_stand_user,
	 arrival_stand_airline) as arrival_stand_actual,
	 COALESCE(arrival_stand_actual_time,
	 arrival_stand_airline_time) as arrival_stand_actual_time,
	 COALESCE(arrival_runway_actual,
	 arrival_runway_user,
	 arrival_runway_assigned,
	 arrival_runway_airline) as arrival_runway_actual,
	 arrival_runway_actual_time,
	 COALESCE(departure_stand_actual,
	 departure_stand_user,
	 departure_stand_airline) as departure_stand_actual,
	 COALESCE(departure_stand_actual_time,
	 departure_stand_airline_time) as departure_stand_actual_time,
	 COALESCE(departure_runway_actual,
	 departure_runway_user,
	 departure_runway_assigned,
	 departure_runway_airline) as departure_runway_actual,
	 departure_runway_actual_time
	from 
	 matm_flight_summary
	where 
	(departure_aerodrome_icao_name = :airport_icao and departure_runway_actual_time between :start_time and :end_time) or
	(arrival_aerodrome_icao_name = :airport_icao and arrival_runway_actual_time between :start_time and :end_time)

