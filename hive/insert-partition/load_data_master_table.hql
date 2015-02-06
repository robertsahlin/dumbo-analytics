load data inpath '$inpath' into table master_table partition (ds='$partition');

load data inpath 'wasb://avro@tcneprod.blob.core.windows.net/tier1/2013/12/08' into table master_table partition (ds='2013-12-08');