from doordahost_glue.connection import GlueETL
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# # @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'USERNAME', 'PASSWORD', 'CATALOG', 'SCHEMA', 'TABLENAME',
                                     'FILE_OUTPUT_DIRECTORY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

cls = GlueETL(username=args["USERNAME"],  # Modify username
              password=args["PASSWORD"],  # Modify Password
              catalog=args["CATALOG"],  # Modify Catalog Name
              schema=args["SCHEMA"],  # Modify Schema Name
              spark_session=spark)

cursor = cls.get_connection()

# To bulk extract all tables in a schema into parquet:
# >>> tables = cursor.show_tables()
# >>> for table in tables:
# >>>   spark_dataframe = cls.query("select * from {tn}".format(tn=table[0]))


spark_dataframe = cls.query("SELECT * FROM %s" % args["TABLENAME"])  # Modify Table Name

dynamic_df = DynamicFrame.fromDF(spark_dataframe, glueContext, 'sample_job')

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_df,
    connection_type="s3",
    connection_options={"path": args["FILE_OUTPUT_DIRECTORY"]},
    format="parquet",
    transformation_ctx="")

job.commit()
