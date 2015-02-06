CREATE EXTERNAL TABLE master_table 
COMMENT "just drop the schema right into the HQL"  
PARTITIONED BY (ds string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
LOCATION 'wasb://avro@tcneprod.blob.core.windows.net/tier1/' 
TBLPROPERTIES ('avro.schema.literal'='{
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
}');