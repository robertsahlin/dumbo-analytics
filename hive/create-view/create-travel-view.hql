CREATE VIEW travel_view AS
SELECT 
visitor_id,
session_id,
date_time,
date,
time,
year,
month_of_year,
day_of_month,
hour_of_day,
minute_of_hour,
year_week,
week_of_year,
day_of_year,
day_of_week,
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
travel_type
FROM rows_table
WHERE travel_action is not null;