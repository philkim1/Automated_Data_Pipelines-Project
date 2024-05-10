import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1715201802798 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1715201802798")

# Script generated for node Customer Trusted
CustomerTrusted_node1715201604883 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1715201604883")

# Script generated for node Join Customer
JoinCustomer_node1715202302135 = Join.apply(frame1=CustomerTrusted_node1715201604883, frame2=AccelerometerLanding_node1715201802798, keys1=["email"], keys2=["user"], transformation_ctx="JoinCustomer_node1715202302135")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1715202422269 = glueContext.write_dynamic_frame.from_options(frame=JoinCustomer_node1715202302135, connection_type="s3", format="json", connection_options={"path": "s3://philbucket/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerTrusted_node1715202422269")

job.commit()
