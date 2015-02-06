--REGISTER asv://jars@tcneprod.blob.core.windows.net/*.jar;
REGISTER piggybank.jar;
--REGISTER jackson-core-asl-*.jar
--REGISTER jackson-mapper-asl-*.jar
--REGISTER snappy-java-*.jar
REGISTER json_simple-*.jar;
REGISTER avro-*.jar;

--SET dfs.block.size 268435456;
--SET pig.maxCombinedSplitSize 268435456;
--SET mapred.compress.map.output true;
--SET mapred.output.compress true;
--SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec
--SET avro.output.codec snappy;



--%declare LOAD_BLOB  'asv://\$logs@tcneprod.blob.core.windows.net/blob';
--%declare LOAD_PATH  '2013/11/29/*';
--%declare STORE_BLOB  'asv://avro@tcneprod.blob.core.windows.net/tier0';
--%declare STORE_PATH  '2013-11-29/';

%declare LOAD_BLOB  'adobeinsight';
%declare LOAD_PATH  'Event.txt';
%declare STORE_BLOB  'pig';
%declare STORE_PATH  'event';


DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtractAll();

-- load tab separated event logs, format <etimestamp>	<requester ip address>	<v1st>	<Session Number>	<www.Brand Label>	<Event Category>	<Event Page>	<Event Action>	<Event Label>	<Event Value>
RAW_LOGS  = LOAD '$LOAD_BLOB/$LOAD_PATH' USING PigStorage('\t') AS (
	request_start_time:    chararray,
    requester_ip_address:    chararray,
    visitor_id:    chararray,
    session_number: chararray,
    domain: chararray,
    user_agent: chararray,
    event_category: chararray,
    event_page: chararray,
    event_action: chararray,
    event_label: chararray,
    event_value: chararray
);

LOGS_AVRO = FOREACH RAW_LOGS GENERATE request_start_time, requester_ip_address, visitor_id, CONCAT(visitor_id, session_number) as session_id, domain, user_agent, event_category, event_page, event_action, event_label, event_value;

rows10 = limit LOGS_AVRO 10;
dump rows10;


/*

LOGS_AVRO =  filter LOGS_BASE BY request_start_time is not null;
  

STORE LOGS_AVRO INTO '$STORE_BLOB/$STORE_PATH'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage('{
            "schema": {
                "type": "record",
                "name": "blobstoragelogrow",
                "namespace": "se.thomascook.avro",
                "fields": [
                    {"name": "version_number","type": ["null","string"]},
					{"name": "request_start_time","type": ["null","string"]},
					{"name": "operation_type","type": ["null","string"]},
					{"name": "request_status","type": ["null","string"]},
					{"name": "http_status_code","type": ["null","string"]},
					{"name": "end_to_end_latency_in_ms","type": ["null","string"]},
					{"name": "server_latency_in_ms","type": ["null","string"]},
					{"name": "authentication_type","type": ["null","string"]},
					{"name": "requester_account_name","type": ["null","string"]},
					{"name": "owner_account_name","type": ["null","string"]},
					{"name": "service_type","type": ["null","string"]},
					{"name": "request_url","type": ["null","string"]},
					{"name": "requested_object_key","type": ["null","string"]},
					{"name": "request_id_header","type": ["null","string"]},
					{"name": "operation_count","type": ["null","string"]},
					{"name": "requester_ip_address","type": ["null","string"]},
					{"name": "request_version_header","type": ["null","string"]},
					{"name": "request_header_size","type": ["null","string"]},
					{"name": "request_packet_size","type": ["null","string"]},
					{"name": "response_header_size","type": ["null","string"]},
					{"name": "response_packet_size","type": ["null","string"]},
					{"name": "request_content_length","type": ["null","string"]},
					{"name": "request_md5","type": ["null","string"]},
					{"name": "server_md5","type": ["null","string"]},
					{"name": "etag_identifier","type": ["null","string"]},
					{"name": "last_modified_time","type": ["null","string"]},
					{"name": "conditions_used","type": ["null","string"]},
					{"name": "user_agent_header","type": ["null","string"]},
					{"name": "referrer_header","type": ["null","string"]},
					{"name": "client_request_id","type": ["null","string"]}
                ]
            }
         }');
         */