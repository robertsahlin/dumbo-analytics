--REGISTER asv://jars@tcneprod.blob.core.windows.net/*.jar;
REGISTER piggybank.jar;
--REGISTER jackson-core-asl-*.jar
--REGISTER jackson-mapper-asl-*.jar
REGISTER snappy-java-*.jar
REGISTER json_simple-*.jar;
REGISTER avro-*.jar;

SET dfs.block.size 268435456;
SET pig.maxCombinedSplitSize 268435456;
SET mapred.compress.map.output true;
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec
SET avro.output.codec snappy;


--%declare LOAD_BLOB  'asv://\$logs@tcneprod.blob.core.windows.net/blob';
--%declare LOAD_PATH  '2013/11/29/*';
--%declare STORE_BLOB  'asv://avro@tcneprod.blob.core.windows.net/tier0';
--%declare STORE_PATH  '2013-11-29/';

%declare LOAD_BLOB  'adobeinsight';
%declare LOAD_PATH  'Travel.txt';
%declare STORE_BLOB  'pig';
%declare STORE_PATH  'aitravel';



RAW_LOGS  = LOAD '$LOAD_BLOB/$LOAD_PATH' USING PigStorage('\t') as (
	timestamp:    chararray,
    ip_address:    chararray, 
      visitor_id:    chararray, 
      session_number:    chararray, 
      domain:    chararray, 
      travel_action:    chararray, 
      travel_type:    chararray, 
      travel_departure_code:    chararray, 
      travel_destination_code:    chararray, 
      travel_departure_time:    chararray, 
      travel_duration:    chararray,
      travel_passengers:    chararray,
      travel_adults:    chararray, 
      travel_children:    chararray, 
      travel_resort_code:    chararray, 
      travel_hotel_code:    chararray, 
      travel_search_page:    chararray, 
      travel_hits:    chararray, 
      travel_category_info:    chararray, 
      travel_resort_info:    chararray, 
      travel_hotel_info:    chararray, 
      travel_same_room:    chararray,
      travel_country_code:    chararray,
      travel_booking_number:    chararray, 
      travel_price:    chararray
);

--RL10 = limit RAW_LOGS 10;
--DUMP RL10;

LOGS_AVRO =  filter RAW_LOGS BY timestamp is not null;
  

STORE LOGS_AVRO INTO '$STORE_BLOB/$STORE_PATH'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage('{
            "schema": {
                "type": "record",
                "name": "aitravel",
                "namespace": "se.thomascook.ai.travel.avro",
                "fields": [
                    {"name": "timestamp","type": ["null","string"]},
					{"name": "ip_address","type": ["null","string"]},
					{"name": "visitor_id","type": ["null","string"]},
					{"name": "session_number","type": ["null","string"]},
					{"name": "domain","type": ["null","string"]},
					{"name": "travel_action","type": ["null","string"]},
					{"name": "travel_type","type": ["null","string"]},
					{"name": "travel_departure_code","type": ["null","string"]},
					{"name": "travel_destination_code","type": ["null","string"]},
					{"name": "travel_departure_time","type": ["null","string"]},
					{"name": "travel_duration","type": ["null","string"]},
					{"name": "travel_passengers","type": ["null","string"]},
					{"name": "travel_adults","type": ["null","string"]},
					{"name": "travel_children","type": ["null","string"]},
					{"name": "travel_resort_code","type": ["null","string"]},
					{"name": "travel_hotel_code","type": ["null","string"]},
					{"name": "travel_search_page","type": ["null","string"]},
					{"name": "travel_hits","type": ["null","string"]},
					{"name": "travel_category_info","type": ["null","string"]},
					{"name": "travel_resort_info","type": ["null","string"]},
					{"name": "travel_hotel_info","type": ["null","string"]},
					{"name": "travel_same_room","type": ["null","string"]},
					{"name": "travel_country_code","type": ["null","string"]},
					{"name": "travel_booking_number","type": ["null","string"]},
					{"name": "travel_price","type": ["null","string"]}
                ]
            }
         }');
         