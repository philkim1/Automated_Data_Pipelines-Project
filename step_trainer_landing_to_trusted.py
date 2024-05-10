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

# Script generated for node step trainer landing
steptrainerlanding_node1715363827319 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/step trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1715363827319")

# Script generated for node accelerometer trusted
accelerometertrusted_node1715363441069 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1715363441069")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct source2.serialnumber, source.sensorreadingtime, source.distancefromobject
from source2
join source on Source.serialnumber = source2.serialnumber;
'''
SQLQuery_node1715363921013 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"source":steptrainerlanding_node1715363827319, "source2":accelerometertrusted_node1715363441069}, transformation_ctx = "SQLQuery_node1715363921013")

# Script generated for node step trainer trusted
steptrainertrusted_node1715364059876 = glueContext.getSink(path="s3://philbucket/step trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1715364059876")
steptrainertrusted_node1715364059876.setCatalogInfo(catalogDatabase="stedi-phil",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1715364059876.setFormat("json")
steptrainertrusted_node1715364059876.writeFrame(SQLQuery_node1715363921013)
job.commit()
