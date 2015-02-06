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
host,
path,
domain,
event_action,
event_category,
event_label,
event_value,
date 
WHERE hit_type = 'event';