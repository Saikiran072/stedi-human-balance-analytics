CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-data-lake-project/trusted/accelerometer/'
TBLPROPERTIES (
  'CreatedByJob'='accelerometer_landing_to_trusted', 
  'CreatedByJobRun'='jr_e9e7d30e0a297bebf62187931ccd63d5e3eaded7da2098da671467a0bedba036', 
  'classification'='json')