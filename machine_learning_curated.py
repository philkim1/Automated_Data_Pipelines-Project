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

# Script generated for node step trainer trusted
steptrainertrusted_node1715369464020 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/step trainer/trusted/"], "recurse": True}, transformation_ctx="steptrainertrusted_node1715369464020")

# Script generated for node acclerometer trusted
acclerometertrusted_node1715369468347 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="acclerometertrusted_node1715369468347")

# Script generated for node SQL Query
SqlQuery0 = '''
select source2.user,source2.x,source2.y,source2.z,source2.timestamp, source.distancefromobject, source.serialnumber 
from source
join source2 on source2.timestamp = source.sensorreadingtime;
'''
SQLQuery_node1715370033779 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"source":steptrainertrusted_node1715369464020, "source2":acclerometertrusted_node1715369468347}, transformation_ctx = "SQLQuery_node1715370033779")

# Script generated for node machine learning curated
machinelearningcurated_node1715370305661 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1715370033779, connection_type="s3", format="json", connection_options={"path": "s3://philbucket/step trainer/curated/", "partitionKeys": []}, transformation_ctx="machinelearningcurated_node1715370305661")

job.commit()
