select
   tea.gufi,
   tea."timestamp",
   tea.arrival_runway_sta
   from tbfm_extension_all tea
   left join matm_flight_summary mfs on tea.gufi = mfs.gufi
   where tea."timestamp" between :start_time and :end_time
   and mfs.arrival_aerodrome_icao_name = :airport_icao
