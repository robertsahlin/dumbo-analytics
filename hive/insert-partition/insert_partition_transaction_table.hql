set hive.exec.dynamic.partition.mode=nonstrict

FROM rows_table
INSERT OVERWRITE TABLE travel_table PARTITION(date)
SELECT 
timestamp,
visitor_id,
session_id,
time,
minute_of_hour,
date_time,
day_of_month,
month_of_year,
year_week,
year,
week_of_year,
hour_of_day,
day_of_week,
day_of_year,
domain,
travel_action,
travel_adults,
travel_ages,
travel_alternative_result,
travel_booking_number,
travel_departure_code,
travel_departure_date,
travel_departure_month,
travel_departure_lead_time,
travel_destination_code,
travel_duration,
travel_hits,
travel_hotel_code,
travel_pax,
travel_price,
travel_resort_code,
travel_room_type,
travel_sale_type,
travel_same_room,
travel_search_page,
travel_type,
date
WHERE travel_action = 'Booking';