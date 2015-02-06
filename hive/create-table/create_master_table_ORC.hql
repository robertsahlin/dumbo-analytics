--%HIVE_HOME%/bin/hive -f asv://hivescripts@tcneprod.blob.core.windows.net/create_master_table.hql

CREATE TABLE master_table(
	 dt string,
	 visitor_id string,
	 session_id string,
	 time map<string,string>,
	 tech map<string,string>,
	 meta map<string,string>,
	 page map<string,string>,
	 referrer map<string,string>,
	 flags map<string,string>    
)
PARTITIONED BY (date string) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002' 
MAP KEYS TERMINATED BY '\003' 
STORED AS ORC tblproperties ("orc.compress"="SNAPPY");