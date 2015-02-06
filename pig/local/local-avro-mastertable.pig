--%PIG_HOME%/bin/pig -f asv://pigscripts@tcneprod.blob.core.windows.net/azure-avro-mastertable.pig -param LOAD_PATH"="2013/12/01

--REGISTER asv://jars@tcneprod.blob.core.windows.net/*.jar;

REGISTER dumbo-*.jar;
REGISTER piggybank.jar;
REGISTER commons-lang3-3.1.jar;
REGISTER datafu-*.jar;
REGISTER jackson-core-asl-*.jar
REGISTER jackson-mapper-asl-*.jar
REGISTER snappy-java-*.jar
REGISTER json_simple-*.jar;
REGISTER avro-*.jar;


-- Set session timeout
%declare TIME_WINDOW  30m
%declare IGNORE_REFERRER  '.*ving.se|.*ving.no|.*spies.dk|.*tjareborg.fi|.*globetrotter.se'
%declare INTERNAL_SEARCH_PARAMETER '.*q=.*'
%declare CAMPAIGN_PARAMETERS '.*utm_source.*|.*utm_campaign.*|.*utm_medium.*|.*utm_medium.*|.*ad_id.*|.*gclid.*|.*gclsrc.*|.*keyword.*|.*matchtype.*'
%declare SEARCH_ENGINE '.*google.*|.*yahoo.*|.*bing.*|.*eniro.*|.*kvasir.*'
%declare SEARCH_PARAMETER '.*q=.*|.*search_word=.*|.*query=.*|.*p=.*'
%declare SOCIAL_NETWORK '.*facebook.com|.*plus.google.com|t.co|twitter.com.*|youtube.com.*'
%declare EXCLUDE_IP '^62.119.80.89.*|^213.106.162.85.*|^88.188.161.220.*|^192.168.75.*'
%declare PAGEVIEW_HIT_TYPE  'pageview'

%declare LOAD_BLOB  'asv://avro@tcneprod.blob.core.windows.net/tier0';
--%declare LOAD_PATH  $path;
--%declare LOAD_PATH  '2013/12/01/part-m-00000.avro';

--define Sessionize dumbo.pig.util.Sessionize('$TIME_WINDOW');
define Sessionize datafu.pig.sessions.Sessionize('$TIME_WINDOW');
define Enumerate datafu.pig.bags.Enumerate('1');
DEFINE PreviousNext dumbo.pig.util.PreviousNext('1');
DEFINE UnescapeHtml dumbo.pig.util.UnescapeHtml();
DEFINE UrlQueryParser dumbo.pig.parsers.UrlQueryParser();
DEFINE UrlQueryParameterParser dumbo.pig.parsers.UrlQueryParameterParser();
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtractAll();
DEFINE Size org.apache.pig.builtin.SIZE();
DEFINE ISOToDateTimeMap dumbo.pig.evaluation.datetime.truncate.ISOToDateTimeMap();
DEFINE AppendToMap dumbo.pig.maps.AppendToMap();

LOGS_BASE = LOAD '$LOAD_BLOB/$LOAD_PATH' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
--describe LOGS_BASE;

--Remove internal traffic (IP)
EXCLUDE_IP = filter LOGS_BASE BY not requester_ip_address matches '$EXCLUDE_IP';

-- Build filter to remove robots

-- Extract logfields and create maps with query parameters as relevant fields
LOGS_READY = foreach EXCLUDE_IP generate request_start_time,TOMAP('statusCode',http_status_code, 'ipAddress', requester_ip_address, 'userAgent',user_agent_header) as tech, UrlQueryParameterParser(request_url) as meta, UrlQueryParser(referrer_header) as page;
--rows10 = limit LOGS_READY 10;
--dump rows10;


--Build a generic UDF-function that maps top domain to timezone alt. use a lookup-file to map domains with timezones
LOGS_SET = foreach LOGS_READY generate request_start_time AS visit_date, (chararray) meta#'visitorId' as member_id, (chararray) meta#'page' as url, (meta#'domain' matches '.*tjareborg.fi'? ISOToDateTimeMap(SUBSTRING(REPLACE(request_start_time, 'T', ' '),0,19),'UTC','Europe/Helsinki','yyyy-MM-dd HH:mm:ss') : ISOToDateTimeMap(SUBSTRING(REPLACE(request_start_time, 'T', ' '),0,19),'UTC','Europe/Stockholm','yyyy-MM-dd HH:mm:ss')) AS time, tech, meta, page, ((meta#'referrer' is null OR meta#'referrer' matches '^$') ?  TOMAP('dummy','dummy') : UrlQueryParser(meta#'referrer')) as referrer;

--READY_FOR_SESSIONIZE = filter LOGS_SET BY visit_date is not null AND page is not null;
READY_FOR_SESSIONIZE = filter LOGS_SET BY visit_date is not null AND member_id is not null AND page is not null;
--describe READY_FOR_SESSIONIZE;
--rows10 = limit READY_FOR_SESSIONIZE 10;
--dump rows10;


-- Sessionize
views = GROUP READY_FOR_SESSIONIZE BY member_id;
sessions = FOREACH views {
    visits = ORDER READY_FOR_SESSIONIZE BY visit_date;
    GENERATE FLATTEN(Sessionize(visits)) AS (visit_date, member_id, url, time, tech, meta, page, referrer, session_id);  
};

--rows10 = limit sessions 10;
--dump rows10;

-- Split page views from other hit types
split sessions into hit_type_page_views_1 if meta#'hitType' == '$PAGEVIEW_HIT_TYPE', hit_type_other if meta#'hitType' != '$PAGEVIEW_HIT_TYPE';
hit_type_page_views = foreach hit_type_page_views_1 generate url, visit_date, member_id, time, tech, meta, page, referrer, session_id;

-- Enumerate page view hits and add previous and next path (solve previous and next with window functions in pig 0.12)
s_group = GROUP hit_type_page_views BY session_id;
ENUMERATE_PAGEVIEWS = FOREACH s_group {
    page_view = ORDER hit_type_page_views BY visit_date;
    --GENERATE FLATTEN(Enumerate(page_view)) AS (visit_date,member_id,url, time, tech, meta, page, referrer, session_id, page_view_id);  
    GENERATE FLATTEN(PreviousNext(page_view)) AS (url, visit_date,member_id, time, tech, meta, page, referrer, session_id, page_view_id, previous_url, next_url);  
};
APPEND_PREV_NEXT_PATH_TO_PAGE_MAP = foreach ENUMERATE_PAGEVIEWS generate visit_date, member_id, session_id, time, tech, meta, AppendToMap(page,'page_number',page_view_id,'prev_path', previous_url,'next_path', next_url) as page, referrer;

--rows10 = limit APPEND_PREV_NEXT_PATH_TO_PAGE_MAP 10;
--dump rows10;


-- Flag traffic sources
SET_TRAFFIC_FLAGS = foreach APPEND_PREV_NEXT_PATH_TO_PAGE_MAP generate (int) (((page#'url' matches '$CAMPAIGN_PARAMETERS') AND (meta#'hitType' == '$PAGEVIEW_HIT_TYPE')) ? 1 : 0) as campaign_flag, (int) (((meta#'hitType' == '$PAGEVIEW_HIT_TYPE') AND (NOT(referrer#'host' matches '$IGNORE_REFERRER'))) AND (referrer#'host' is not null)  ? 1 : 0) as referral_flag, (int) (((meta#'hitType' == '$PAGEVIEW_HIT_TYPE') AND (referrer#'host' matches '$SEARCH_ENGINE') AND (referrer#'url' matches '$SEARCH_PARAMETER') AND (referrer#'host' is not null)) ? 1 : 0) as search_engine_flag,  (int) (meta#'hitType' == '$PAGEVIEW_HIT_TYPE' AND referrer#'host' matches '$SOCIAL_NETWORK'  AND referrer#'host' is not null ? 1 : 0) as social_network_flag, visit_date, member_id,session_id, time, tech, meta, page, referrer;
SET_DIRECT_FLAG = foreach SET_TRAFFIC_FLAGS generate (int) ((referral_flag == 0 AND campaign_flag == 0 AND search_engine_flag == 0 AND social_network_flag == 0 AND page#'page_number' == 1) ? 1 : 0) as direct_flag, campaign_flag,  referral_flag, search_engine_flag,  social_network_flag, visit_date, member_id, session_id, time, tech, meta, page, referrer;
ADD_FLAGS_TO_MAP = foreach SET_DIRECT_FLAG generate visit_date, member_id, session_id, time, tech, meta, page, referrer, TOMAP('is_campaign',campaign_flag, 'is_referral', referral_flag, 'is_search_engine', search_engine_flag, 'is_social_network', social_network_flag, 'is_direct', direct_flag) as flags;

--rows10 = limit ADD_FLAGS_TO_MAP 10;
--dump rows10;


-- Join page views and other hits (not enumerated)
PAGEVIEWS = foreach ADD_FLAGS_TO_MAP generate visit_date, member_id, session_id, time, tech, meta, page, referrer, flags;
EVENTS = foreach hit_type_other generate visit_date, member_id, session_id, time, tech, meta, page, referrer, TOMAP('is_event',1) as flags;
UNION_PAGEVIEWS_EVENTS = UNION onschema EVENTS,PAGEVIEWS;
ROWS_FOR_STORAGE = foreach UNION_PAGEVIEWS_EVENTS generate visit_date as dt, member_id as visitor_id, session_id, time, tech, meta, page, referrer, flags, (chararray) time#'date' as date;
--describe UNION_PAGEVIEWS_EVENTS;
--describe ROWS_FOR_STORAGE;
--rows10 = limit ROWS_FOR_STORAGE 10;
--dump rows10;


store ROWS_FOR_STORAGE into 'master_table' using org.apache.hcatalog.pig.HCatStorer();
--EXPLAIN ROWS_FOR_STORAGE;

