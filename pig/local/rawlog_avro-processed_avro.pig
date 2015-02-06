-- Configure that jobs doesn't store logs together with job outputs
--SET hadoop.job.history.user.location none;

-- Register jar files needed
register piggybank.jar
register commons-validator-1.4.0.jar
register commons-lang3-3.1.jar
register datafu-1.1.0.jar
register dumbo-0.7.jar
register json-simple-1.1.1.jar
register avro-1.7.4.jar

-- Set session timeout
%declare TIME_WINDOW  30m
-- Set domains to track
%declare IGNORE_REFERRER  '.*ving.se|.*ving.no|.*spies.dk|.*tjareborg.fi|.*globetrotter.se'
-- Set internal search parameters
%declare INTERNAL_SEARCH_PARAMETER '.*q=.*'
-- Set campaigns parameters
%declare CAMPAIGN_PARAMETERS '.*utm_source.*|.*utm_campaign.*|.*utm_medium.*|.*utm_medium.*|.*ad_id.*|.*gclid.*|.*gclsrc.*|.*keyword.*|.*matchtype.*'
-- Set search engines
%declare SEARCH_ENGINE '.*google.*|.*yahoo.*|.*bing.*|.*eniro.*|.*kvasir.*'
-- Set search engine parameters
%declare SEARCH_PARAMETER '.*q=.*|.*search_word=.*|.*query=.*|.*p=.*'
-- Set social networks
%declare SOCIAL_NETWORK '.*facebook.com|.*plus.google.com|t.co|twitter.com.*|youtube.com.*'
-- Set IP:s to exclude from traffic
%declare EXCLUDE_IP '^62.119.80.89.*|^213.106.162.85.*|^88.188.161.220.*|^192.168.75.*'
-- Set hit type for pageviews
%declare PAGEVIEW_HIT_TYPE  'pageview'

-- Set start and stop date for storage filter
%declare STORE_START_DATE '2013-11-24'
%declare STORE_STOP_DATE '2013-11-24'

-- Define UDF:s to user in script
define Sessionize datafu.pig.sessions.Sessionize('$TIME_WINDOW');
define Enumerate datafu.pig.bags.Enumerate('1');
define PreviousNext dumbo.pig.util.PreviousNext('1');
define UrlQueryParser dumbo.pig.parsers.UrlQueryParser();
define UrlQueryParameterParser dumbo.pig.parsers.UrlQueryParameterParser();
define ISOToDateTimeMap dumbo.pig.evaluation.datetime.truncate.ISOToDateTimeMap();
define AppendToMap dumbo.pig.maps.AppendToMap();

-- Load files
LOGS_BASE = LOAD 'avro/part-m-00000.avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

-- Remove traffic based on IP
EXCLUDE_IP = filter LOGS_BASE BY not requester_ip_address matches '$EXCLUDE_IP';

-- TODO: Build filter to remove robots

-- Extract logfields and create maps with query parameters as relevant fields
LOGS_READY = foreach EXCLUDE_IP generate request_start_time,TOMAP('statusCode',http_status_code, 'ipAddress', requester_ip_address, 'userAgent',user_agent_header) as tech, UrlQueryParser(request_url) as meta, UrlQueryParser(referrer_header) as page;

-- Create time map with local time
LOGS_SET = foreach LOGS_READY generate request_start_time AS visit_date, (chararray) meta#'visitorId' as member_id, (chararray) meta#'page' as url, (meta#'domain' matches '.*tjareborg.fi'? ISOToDateTimeMap(SUBSTRING(REPLACE(request_start_time, 'T', ' '),0,19),'UTC','Europe/Helsinki','yyyy-MM-dd HH:mm:ss') : ISOToDateTimeMap(SUBSTRING(REPLACE(request_start_time, 'T', ' '),0,19),'UTC','Europe/Stockholm','yyyy-MM-dd HH:mm:ss')) AS time, tech, meta, page, ((meta#'referrer' is null OR meta#'referrer' matches '^$') ?  TOMAP('dummy','dummy') : UrlQueryParser(meta#'referrer')) as referrer;

-- Keep rows with timestamp, visitor ID, requesting url and date between declared start and stop dates for storage
--READY_FOR_SESSIONIZE = filter LOGS_SET BY visit_date is not null AND member_id is not null AND page is not null;
READY_FOR_SESSIONIZE = filter LOGS_SET BY visit_date is not null AND member_id is not null AND page is not null AND time#'date' >= '$STORE_START_DATE' AND time#'date' <= '$STORE_STOP_DATE';

-- Sessionize rows
views = GROUP READY_FOR_SESSIONIZE BY member_id;
sessions = FOREACH views {
    visits = ORDER READY_FOR_SESSIONIZE BY visit_date;
    GENERATE FLATTEN(Sessionize(visits)) AS (visit_date, member_id, url, time, tech, meta, page, referrer, session_id);  
};

-- Split page views from other hit types in order to enumerate pageviews and flag different traffic sources
split sessions into hit_type_page_views_1 if meta#'hitType' == '$PAGEVIEW_HIT_TYPE', hit_type_other if meta#'hitType' != '$PAGEVIEW_HIT_TYPE';
hit_type_page_views = foreach hit_type_page_views_1 generate url, visit_date, member_id, time, tech, meta, page, referrer, session_id;

-- Enumerate page view hits and append to page map
s_group = GROUP hit_type_page_views BY session_id;
ENUMERATE_PAGEVIEWS = FOREACH s_group {
    page_view = ORDER hit_type_page_views BY visit_date;
    GENERATE FLATTEN(Enumerate(page_view)) AS (visit_date,member_id,url, time, tech, meta, page, referrer, session_id, page_view_id);  
};
APPEND_PAGE_NUMBER_TO_PAGE_MAP = foreach ENUMERATE_PAGEVIEWS generate visit_date, member_id, session_id, time, tech, meta, AppendToMap(page,'page_number',page_view_id) as page, referrer;

-- Flag traffic sources as campaign, search, social or direct
SET_TRAFFIC_FLAGS = foreach APPEND_PAGE_NUMBER_TO_PAGE_MAP generate (chararray) (((page#'url' matches '$CAMPAIGN_PARAMETERS') AND (meta#'hitType' == '$PAGEVIEW_HIT_TYPE')) ? 'Y' : 'N') as campaign_flag, (chararray) (((meta#'hitType' == '$PAGEVIEW_HIT_TYPE') AND (NOT(referrer#'host' matches '$IGNORE_REFERRER'))) AND (referrer#'host' is not null)  ? 'Y' : 'N') as referral_flag, (chararray) (((meta#'hitType' == '$PAGEVIEW_HIT_TYPE') AND (referrer#'host' matches '$SEARCH_ENGINE') AND (referrer#'url' matches '$SEARCH_PARAMETER') AND (referrer#'host' is not null)) ? 'Y' : 'N') as search_engine_flag,  (chararray) (meta#'hitType' == '$PAGEVIEW_HIT_TYPE' AND referrer#'host' matches '$SOCIAL_NETWORK'  AND referrer#'host' is not null ? 'Y' : 'N') as social_network_flag, visit_date, member_id,session_id, time, tech, meta, page, referrer;
SET_DIRECT_FLAG = foreach SET_TRAFFIC_FLAGS generate (chararray) ((referral_flag == 'N' AND campaign_flag == 'N' AND search_engine_flag == 'N' AND social_network_flag == 'N' AND page#'page_number' == 1) ? 'Y' : 'N') as direct_flag, campaign_flag,  referral_flag, search_engine_flag,  social_network_flag, visit_date, member_id, session_id, time, tech, meta, page, referrer;
ADD_FLAGS_TO_MAP = foreach SET_DIRECT_FLAG generate visit_date, member_id, session_id, time, tech, meta, page, referrer, TOMAP('is_campaign',campaign_flag, 'is_referral', referral_flag, 'is_search_engine', search_engine_flag, 'is_social_network', social_network_flag, 'is_direct', direct_flag) as flags;

-- Project same fields for pageviews and events and then union relations
PAGEVIEWS = foreach ADD_FLAGS_TO_MAP generate visit_date, member_id, session_id, (map[chararray]) time, (map[chararray]) tech, (map[chararray]) meta, (map[chararray]) page, (map[chararray]) referrer, (map[chararray]) flags;
EVENTS = foreach hit_type_other generate visit_date, member_id, session_id, (map[chararray]) time,(map[chararray]) tech, (map[chararray]) meta, (map[chararray]) page, (map[chararray]) referrer, TOMAP('is_event','Y') as flags;
UNION_PAGEVIEWS_EVENTS = UNION onschema EVENTS,PAGEVIEWS;

-- Store as avro file
STORE UNION_PAGEVIEWS_EVENTS INTO 'avro4/tier2/2013/11/24'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage('{
            "schema": {
                "type": "record",
                "name": "processedlogs",
                "namespace": "se.thomascook.avro",
                "fields": [
                    {"name": "dt","type": ["null","string"]},
                    {"name": "visitor_id","type": ["null","string"]},
                    {"name": "session_id","type": ["null","string"]},
                    {"name" : "time","type":{"type":"map","values" : "string"}},
                    {"name" : "tech","type":{"type":"map","values" : "string"}},
                    {"name" : "meta","type":{"type":"map","values" : "string"}},
                    {"name" : "page","type":{"type":"map","values" : "string"}},
                    {"name" : "referrer","type":{"type":"map","values" : "string"}},
                    {"name" : "flags","type":{"type":"map","values" : "string"}}
                ]
            }
         }');