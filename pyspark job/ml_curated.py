import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1769073412431 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1769073412431")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1769073417483 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1769073417483")

# Script generated for node SQL Query
SqlQuery4496 = '''
select st.sensorreadingtime, st.distancefromobject, a.x, a.y, a.z
from step_trainer_trusted st
inner join accelerometer_trusted a
on st.sensorreadingtime = a.timestamp
'''
SQLQuery_node1769073425291 = sparkSqlQuery(glueContext, query = SqlQuery4496, mapping = {"accelerometer_trusted":accelerometer_trusted_node1769073412431, "step_trainer_trusted":step_trainer_trusted_node1769073417483}, transformation_ctx = "SQLQuery_node1769073425291")

# Script generated for node ml_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769073425291, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769073034953", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ml_curated_node1769074472439 = glueContext.getSink(path="s3://stedi-data-lake-project/curated/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ml_curated_node1769074472439")
ml_curated_node1769074472439.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="ml_curated")
ml_curated_node1769074472439.setFormat("json")
ml_curated_node1769074472439.writeFrame(SQLQuery_node1769073425291)
job.commit()