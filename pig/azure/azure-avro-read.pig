--%PIG_HOME%/bin/pig -f asv://pigscripts@tcneprod.blob.core.windows.net/avroread-2.pig
REGISTER asv://jars@tcneprod.blob.core.windows.net/*.jar;

%declare LOAD_BLOB  'asv://avro@tcneprod.blob.core.windows.net/tier0';
%declare LOAD_PATH  '2013-11-29/*';

LOGS_AVRO = LOAD '$LOAD_BLOB/$LOAD_PATH' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

rows10 = limit LOGS_AVRO 10;
dump rows10;
Describe LOGS_AVRO;