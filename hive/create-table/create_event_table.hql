CREATE TABLE event_table 
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
status_code string,
user_agent string,
host string,
path string,
domain string,
event_action string,
event_category string,
event_label string,
event_value float
)
COMMENT 'This is the event table'
PARTITIONED BY (date string) 
CLUSTERED BY(session_id) INTO 32 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002' 
MAP KEYS TERMINATED BY '\003' 
STORED AS ORC;