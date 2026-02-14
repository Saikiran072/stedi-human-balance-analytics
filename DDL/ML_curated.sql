CREATE EXTERNAL TABLE `ml_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
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
  's3://stedi-data-lake-project/curated/ml_curated/'
TBLPROPERTIES (
  'CreatedByJob'='step_trainer_to_trusted-copy', 
  'CreatedByJobRun'='jr_0e1ce7104db119bf8f060d482683393860b28e8cea1d8fb7084d0a43a6ae6f01', 
  'classification'='json')