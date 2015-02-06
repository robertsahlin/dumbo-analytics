CREATE VIEW event_view AS
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
user_agent,
path,
domain,
event_action,
event_category,
event_label,
event_value 
FROM rows_table
WHERE hit_type = 'event';