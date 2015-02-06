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
%declare LOAD_PATH  'Traffic.txt';
%declare STORE_BLOB  'pig';
%declare STORE_PATH  'aitraffic';



RAW_LOGS  = LOAD '$LOAD_BLOB/$LOAD_PATH' USING PigStorage('\t') as (
	timestamp:    chararray,
    ip_address:    chararray, 
      visitor_id:    chararray, 
      session_number:    chararray, 
      domain:    chararray, 
      source:    chararray, 
      medium:    chararray, 
      campaign:    chararray, 
      content:    chararray, 
      referrer:    chararray, 
      search_phrase:    chararray
);

--RL10 = limit RAW_LOGS 10;
--DUMP RL10;

LOGS_AVRO =  filter RAW_LOGS BY timestamp is not null;
  

STORE LOGS_AVRO INTO '$STORE_BLOB/$STORE_PATH'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage('{
            "schema": {
                "type": "record",
                "name": "aitraffic",
                "namespace": "se.thomascook.ai.travel.avro",
                "fields": [
                    {"name": "timestamp","type": ["null","string"]},
					{"name": "ip_address","type": ["null","string"]},
					{"name": "visitor_id","type": ["null","string"]},
					{"name": "session_number","type": ["null","string"]},
					{"name": "domain","type": ["null","string"]},
					{"name": "source","type": ["null","string"]},
					{"name": "medium","type": ["null","string"]},
					{"name": "campaign","type": ["null","string"]},
					{"name": "content","type": ["null","string"]},
					{"name": "referrer","type": ["null","string"]},
					{"name": "search_phrase","type": ["null","string"]}
                ]
            }
         }');
         