select

    gufi,
    min("timestamp") as time_first_tracked
    from matm_flight
    where "timestamp" between :start_time and :end_time
    and arrival_aerodrome_icao_name = :airport_icao
    and position_latitude is not null
    and last_update_source in ('TFM', 'TMA')
group by gufi