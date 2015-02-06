CREATE TABLE transaction_table 
(
timestamp string,
visitor_id string,
session_id string,
time string,
minute_of_hour string,
date_time string,
day_of_month string,
month_of_year string,
year_week string,
year string,
week_of_year string,
hour_of_day string,
day_of_week string,
day_of_year string,
domain string,
travel_adults int,
travel_ages string,
travel_booking_number string,
travel_children int,
travel_departure_code string,
travel_departure_date string,
travel_departure_month string,
travel_departure_lead_time int,
travel_destination_code string,
travel_duration int,
travel_hotel_code string,
travel_pax int,
travel_price int,
travel_resort_code string,
travel_room_type string,
travel_sale_type string,
travel_type string
)
COMMENT 'This is the transaction table'
PARTITIONED BY (date string) 
CLUSTERED BY(session_id) INTO 32 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002' 
MAP KEYS TERMINATED BY '\003' 
STORED AS ORC;