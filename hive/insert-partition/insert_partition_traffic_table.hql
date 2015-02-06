set hive.exec.dynamic.partition.mode=nonstrict

FROM rows_table
INSERT OVERWRITE TABLE page_table PARTITION(date)
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
status_code,
user_agent,
path,
next_path,
bounce,
page_view_number,
search_matchtype,
referrer,
referrer_path,
domain,
page_type,
screen_size,
medium,
source,
campaign,
search_phrase,
date
WHERE medium is not NULL;