--%PIG_HOME%/bin/pig -f asv://pigscripts@tcneprod.blob.core.windows.net/avrostore-4.pig -param loadpath"="2014/01/28 
--%PIG_HOME%/bin/pig -f asv://pigscripts@tcneprod.blob.core.windows.net/avrostore-4.pig -param loadpath"="2013/11/28 -param storepath"="2013-11-28

REGISTER asv://jars@tcneprod.blob.core.windows.net/*.jar;

SET dfs.block.size 268435456;
SET pig.maxCombinedSplitSize 268435456;
SET mapred.compress.map.output true;
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec
SET avro.output.codec snappy;



%declare LOAD_BLOB  'asv://\$logs@tcneprod.blob.core.windows.net/blob';
--%declare LOAD_PATH  '2013/11/29/*';
%declare STORE_BLOB  'asv://avro@tcneprod.blob.core.windows.net/tier0';
%declare STORE_PATH  '2013-11-29/';

DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtractAll();

--RAW_LOGS  = LOAD 'azurelogs2/000002.log' USING TextLoader as (line:chararray);
RAW_LOGS  = LOAD '$LOAD_BLOB/$loadpath' USING TextLoader as (line:chararray);

-- load azure logs, format <version-number>;<request-start-time>;<operation-type>;<request-status>;<http-status-code>;<end-to-end-latency-in-ms>;<server-latency-in-ms>;<authentication-type>;<requester-account-name>;<owner-account-name>;<service-type>;<request-url>;<requested-object-key>;<request-id-header>;<operation-count>;<requester-ip-address>;<request-version-header>;<request-header-size>;<request-packet-size>;<response-header-size>;<response-packet-size>;<request-content-length>;<request-md5>;<server-md5>;<etag-identifier>;<last-modified-time>;<conditions-used>;<user-agent-header>;<referrer-header>;<client-request-id>
LOGS_BASE = foreach RAW_LOGS generate 
    FLATTEN (
    EXTRACT (line, '(.+?)\\u003B(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{7}Z?)\\u003B(.+?)\\u003B(.+?)\\u003B(\\d{3})\\u003B(\\d+)\\u003B(\\d+)\\u003B(.+?)\\u003B(.*)\\u003B(.+?)\\u003B(\\w+)\\u003B\\u0022([^\\u0022]+)\\u0022\\u003B\\u0022([^\\u0022]+)\\u0022\\u003B(.{8}-.{4}-.{4}-.{4}-.{12})\\u003B(\\d+)\\u003B(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d{1,6})\\u003B(\\d{4}-\\d{2}-\\d{2})\\u003B(\\d+)\\u003B(\\d+)\\u003B(\\d+)\\u003B(\\d+)\\u003B(\\d+)\\u003B(.*)\\u003B(.*)\\u003B\\u0022([^\\u0022]+)\\u0022\\u003B(.+?)\\u003B(.*)\\u003B\\u0022([^\\u0022]+)\\u0022\\u003B\\u0022([^\\u0022]+)\\u0022\\u003B(.*)')
    )
    as (
      version_number:    chararray,
      request_start_time:    chararray, 
      operation_type:    chararray, 
      request_status:    chararray, 
      http_status_code:    chararray, 
      end_to_end_latency_in_ms:    chararray, 
      server_latency_in_ms:    chararray, 
      authentication_type:    chararray, 
      requester_account_name:    chararray, 
      owner_account_name:    chararray, 
      service_type:    chararray,
      request_url:    chararray,
      requested_object_key:    chararray, 
      request_id_header:    chararray, 
      operation_count:    chararray, 
      requester_ip_address:    chararray, 
      request_version_header:    chararray, 
      request_header_size:    chararray, 
      request_packet_size:    chararray, 
      response_header_size:    chararray, 
      response_packet_size:    chararray, 
      request_content_length:    chararray,
      request_md5:    chararray,
      server_md5:    chararray, 
      etag_identifier:    chararray, 
      last_modified_time:    chararray, 
      conditions_used:    chararray, 
      user_agent_header:    chararray, 
      referrer_header:    chararray, 
      client_request_id:    chararray
  );

LOGS_AVRO =  filter LOGS_BASE BY request_start_time is not null;

STORE B INTO '$STORE_BLOB/$loadpath/'
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