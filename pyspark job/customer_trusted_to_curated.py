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

# Script generated for node customer_trusted
customer_trusted_node1769073417483 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1769073417483")

# Script generated for node SQL Query
SqlQuery4913 = '''
select distinct c.* from customer_trusted c
inner join accelerometer_trusted a
on c.email = a.user
'''
SQLQuery_node1769073425291 = sparkSqlQuery(glueContext, query = SqlQuery4913, mapping = {"accelerometer_trusted":accelerometer_trusted_node1769073412431, "customer_trusted":customer_trusted_node1769073417483}, transformation_ctx = "SQLQuery_node1769073425291")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769073425291, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769073034953", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1769074472439 = glueContext.getSink(path="s3://stedi-data-lake-project/curated/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1769074472439")
customer_curated_node1769074472439.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
customer_curated_node1769074472439.setFormat("json")
customer_curated_node1769074472439.writeFrame(SQLQuery_node1769073425291)
job.commit()