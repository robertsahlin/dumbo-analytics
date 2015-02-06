set hive.exec.dynamic.partition.mode=nonstrict;

FROM master_table
INSERT OVERWRITE TABLE rows_table PARTITION(date)
SELECT 
dt as timestamp,
visitor_id,
session_id,
concat(time['date'],' ',time['time']) as local_timestamp,
time['time'] as time,
time['minuteOfHour'] as minute_of_hour,
time['dateTime'] as date_time,
time['dayOfMonth'] as day_of_month,
time['monthOfYear'] as month_of_year,
time['yearWeek'] as year_week,
time['year'] as year,
time['weekOfYear'] as week_of_year,
time['hourOfDay'] as hour_of_day,
time['dayOfWeek'] as day_of_week,
time['dayOfYear'] as day_of_year,
tech['statusCode'] as status_code,
tech['userAgent'] as user_agent,
tech['ipAddress'] as ip_address,
page['host'] as host,
page['path'] as path,
page['prev_path'] as previous_path,
page['next_path'] as next_path,
page['page_number'] as page_number,
page['matchtype'] as search_matchtype,
referrer['host'] as referrer,
referrer['path'] as referrer_path,
meta['customerId'] as customer_id,
meta['domain'] as domain,
meta['hitType'] as hit_type,
meta['membershipId'] as membership_id,
meta['eventAction'] as event_action,
meta['eventCategory'] as event_category,
meta['eventLabel'] as event_label,
meta['eventValue'] as event_value,
meta['experiment'] as experiment,
meta['pageType'] as page_type,
meta['screenSize'] as screen_size,
meta['title'] as title,
meta['travelAction'] as travel_action,
meta['travelAdults'] as travel_adults,
meta['travelAges'] as travel_ages,
meta['travelAlternativeResult'] as travel_alternative_result,
meta['travelBno'] as travel_booking_number,
meta['travelChildren'] as travel_children,
meta['travelDepartureCode'] as travel_departure_code,
meta['travelDepartureDate'] as travel_departure_date,
substr(meta['travelDepartureDate'],0,7) as travel_departure_month,
DATEDIFF(meta['travelDepartureDate'],time['date']) as travel_departure_lead_time,
meta['travelDestinationCode'] as travel_destination_code,
meta['travelDuration'] as travel_duration,
meta['travelHits'] as travel_hits,
meta['travelHotelCode'] as travel_hotel_code,
meta['travelPax'] as travel_pax,
meta['travelPrice'] as travel_price,
meta['travelResortCode'] as travel_resort_code,
meta['travelRoomType'] as travel_room_type,
meta['travelSaleType'] as travel_sale_type,
meta['travelSameRoom'] as travel_same_room,
meta['travelSearchPage'] as travel_search_page,
meta['travelType'] as travel_type,
CASE
	WHEN flags['is_search_engine'] = 1 AND flags['is_campaign'] = 1 AND array_contains(map_keys(page),'gclid') THEN 'cpc'
	WHEN flags['is_search_engine'] = 1 AND flags['is_campaign'] = 1 THEN page['utm_medium']
	WHEN flags['is_campaign'] = 1 THEN page['utm_medium']
	WHEN flags['is_search_engine'] = 1 THEN 'search organic'
	WHEN flags['is_social_network'] = 1 THEN 'social'
	WHEN flags['is_referral'] = 1 THEN 'referral'
	WHEN flags['is_direct'] = 1 THEN 'direct'
	ELSE NULL
END AS medium, 
CASE
	WHEN flags['is_search_engine'] = 1 AND flags['is_campaign'] = 1 THEN referrer['host']
	WHEN flags['is_campaign'] = 1 THEN page['utm_source']
	WHEN flags['is_search_engine'] = 1 THEN referrer['host']
	WHEN flags['is_social_network'] = 1 THEN referrer['host']
	WHEN flags['is_referral'] = 1 THEN referrer['host']
	WHEN flags['is_direct'] = 1 THEN 'direct'
	ELSE NULL
END AS source,
CASE
	WHEN flags['is_search_engine'] = 1 AND flags['is_campaign'] = 1 AND array_contains(map_keys(page),'gclid') THEN 'gclid'
	WHEN flags['is_search_engine'] = 1 AND flags['is_campaign'] = 1 THEN page['utm_campaign']
	WHEN flags['is_campaign'] = 1 THEN page['utm_campaign']
	WHEN flags['is_search_engine'] = 1 THEN '(not set)'
	WHEN flags['is_social_network'] = 1 THEN '(not set)'
	WHEN flags['is_referral'] = 1 THEN '(not set)'
	WHEN flags['is_direct'] = 1 THEN '(not set)'
	ELSE NULL
END AS campaign,
page['utm_content'] as content,
CASE
	WHEN flags['is_search_engine'] = 1 AND referrer IS NOT NULL AND array_contains(map_keys(referrer),'q') THEN referrer['q']
	WHEN flags['is_search_engine'] = 1 AND referrer IS NOT NULL AND array_contains(map_keys(referrer),'search_word') THEN referrer['search_word']
	WHEN flags['is_search_engine'] = 1 AND referrer IS NOT NULL AND array_contains(map_keys(referrer),'query') THEN referrer['query']
	WHEN flags['is_search_engine'] = 1 AND referrer IS NOT NULL AND array_contains(map_keys(referrer),'p') THEN referrer['p']
	ELSE NULL
END AS search_phrase,
IF(page['next_path'] is NULL AND page['page_number'] = 1, 1, 0) as bounce,
IF(page['next_path'] is NULL AND meta['hitType'] = 'pageview', 1, 0) as exit,
IF(page['previous_path'] is NULL AND page['page_number'] = 1, 1, 0) as entrance,
IF(meta['travelAction'] = 'Booking', 1, 0) as transaction,
IF(meta['travelAction'] = 'Booking', meta['travelPrice'], 0) as revenue,
IF(meta['travelAction'] = 'Booking', meta['travelPax'],0) as quantity,
IF(meta['travelAction'] = 'Booking', 1,0) as booking,
IF(meta['travelAction'] = 'Quote', 1,0) as quote,
IF(meta['travelAction'] = 'Search', 1,0) as search,
IF(page['mode'] = 'true' AND page['path'] = '/Member/SubscriptionThanks.owpx/Static', 1, 0) as newsletter_signup,
IF(page['path'] = '/pbc-payment-accepted' OR page['path'] = '/payment-accepted', 1, 0) as payment_accepted,
date;