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

# Script generated for node Customer trusted
Customertrusted_node1769073412431 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="Customertrusted_node1769073412431")

# Script generated for node accelerometer_landing
accelerometer_landing_node1769073417483 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="landing_accelerometer_zone", transformation_ctx="accelerometer_landing_node1769073417483")

# Script generated for node SQL Query
SqlQuery4685 = '''
select a.* from accelerometer_landing a
inner join customer_trusted c
on a.user = c.email
'''
SQLQuery_node1769073425291 = sparkSqlQuery(glueContext, query = SqlQuery4685, mapping = {"accelerometer_landing":accelerometer_landing_node1769073417483, "customer_trusted":Customertrusted_node1769073412431}, transformation_ctx = "SQLQuery_node1769073425291")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769073425291, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769073034953", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1769074472439 = glueContext.getSink(path="s3://stedi-data-lake-project/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1769074472439")
accelerometer_trusted_node1769074472439.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1769074472439.setFormat("json")
accelerometer_trusted_node1769074472439.writeFrame(SQLQuery_node1769073425291)
job.commit()