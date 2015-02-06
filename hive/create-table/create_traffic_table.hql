CREATE TABLE traffic_table( 
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
status_code string,
user_agent string,
path string,
next_path string,
bounce int,
page_view_number int,
search_matchtype string,
referrer string,
referrer_path string,
domain string,
page_type string,
screen_size string,
medium string,
source string,
campaign string,
search_phrase string
) 
COMMENT 'This is the traffic table'
PARTITIONED BY (date string) 
CLUSTERED BY(session_id) INTO 32 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002' 
MAP KEYS TERMINATED BY '\003' 
STORED AS ORC;