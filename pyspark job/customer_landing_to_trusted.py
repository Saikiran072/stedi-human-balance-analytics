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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1769062479991 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1769062479991")

# Script generated for node SQL Query
SqlQuery4894 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1769062506368 = sparkSqlQuery(glueContext, query = SqlQuery4894, mapping = {"myDataSource":AWSGlueDataCatalog_node1769062479991}, transformation_ctx = "SQLQuery_node1769062506368")

# Script generated for node Customer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769062506368, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769062204604", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customertrusted_node1769062526824 = glueContext.getSink(path="s3://stedi-data-lake-project/trusted/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusted_node1769062526824")
Customertrusted_node1769062526824.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
Customertrusted_node1769062526824.setFormat("json")
Customertrusted_node1769062526824.writeFrame(SQLQuery_node1769062506368)
job.commit()