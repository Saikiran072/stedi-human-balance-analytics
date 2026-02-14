CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-data-lake-project/trusted/step_trainer/'
TBLPROPERTIES (
  'CreatedByJob'='step_trainer_to_trusted', 
  'CreatedByJobRun'='jr_6d616d383616b6912d8a2384f1135696e2b5bc0b317e8b4655f1a1cf37e32f6e', 
  'classification'='json')