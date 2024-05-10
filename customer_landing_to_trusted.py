import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Customer Landing
CustomerLanding_node1715029950291 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/customer/landing/customer-1691348231425.json"], "recurse": True}, transformation_ctx="CustomerLanding_node1715029950291")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where sharewithresearchasofdate is not null;

'''
SQLQuery_node1715193884368 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1715029950291}, transformation_ctx = "SQLQuery_node1715193884368")

# Script generated for node Customer Trusted
CustomerTrusted_node1715030217548 = glueContext.getSink(path="s3://philbucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1715030217548")
CustomerTrusted_node1715030217548.setCatalogInfo(catalogDatabase="stedi-phil",catalogTableName="customer_trusted")
CustomerTrusted_node1715030217548.setFormat("json")
CustomerTrusted_node1715030217548.writeFrame(SQLQuery_node1715193884368)
job.commit()
