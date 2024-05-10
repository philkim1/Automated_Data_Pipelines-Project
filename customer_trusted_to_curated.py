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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1715201802798 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometertrusted_node1715201802798")

# Script generated for node Customer Trusted
CustomerTrusted_node1715201604883 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://philbucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1715201604883")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct s1.customername, s1.email, s1.phone, s1.birthday, s1.serialnumber, s1.registrationdate, s1.lastupdatedate, s1.sharewithpublicasofdate, s1.sharewithresearchasofdate, s1.sharewithfriendsasofdate
from s1
join s2 on s1.email = s2.user;
'''
SQLQuery_node1715368220821 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s1":CustomerTrusted_node1715201604883, "s2":Accelerometertrusted_node1715201802798}, transformation_ctx = "SQLQuery_node1715368220821")

# Script generated for node Customer Curated2
CustomerCurated2_node1715202422269 = glueContext.getSink(path="s3://philbucket/customer/curated2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated2_node1715202422269")
CustomerCurated2_node1715202422269.setCatalogInfo(catalogDatabase="stedi-phil",catalogTableName="customer_curated2")
CustomerCurated2_node1715202422269.setFormat("json")
CustomerCurated2_node1715202422269.writeFrame(SQLQuery_node1715368220821)
job.commit()
