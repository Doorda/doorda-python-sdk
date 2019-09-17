from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import from_json
from pyspark.sql.types import *


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'USERNAME', 'PASSWORD', 'CATALOG', 'SCHEMA', 'FILE_OUTPUT_DIRECTORY'])


def create_select_statement(columns, table_name):
    """
    Casts Complex types as String as Pyspark version used in AWS Glue does not support certain data types from JDBC
    https://jira.apache.org/jira/browse/SPARK-24391

    Returned select statement format:
    `(SELECT {column_names} FROM tablename) table`

    Returned column name to complex type dictionary format:
    `{column name: data type}`

    :param columns:
    :param table_name:
    :return: select_statement, column_name_to_complex_type
    """
    column_name_list = []
    column_name_to_complex_type = {}
    for name in columns:
        if columns[name].startswith(('array', 'map')):
            column_name_list.append("".join(('JSON_FORMAT(CAST("', name, '" as JSON)) as "', name, '"')))
            column_name_to_complex_type[name] = columns[name]
        else:
            column_name_list.append(name)
    select_statement = "(SELECT " + ", ".join(column_name_list) + " FROM " + table_name + ") data"
    return select_statement, column_name_to_complex_type


complex_value_map = {'array(map(varchar, varchar))': ArrayType(MapType(StringType(), StringType())),
                     'array(varchar)': ArrayType(StringType()),
                     'map(varchar, varchar)': MapType(StringType(), StringType())}

# Initialise Glue Spark Session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Driver Class
jdbc_driver = "io.prestosql.jdbc.PrestoDriver"

# JDBC Path Format: //host.doorda.com:443/{catalog name}/{schema name}
jdbc_path = "//host.doorda.com:443/" + args['CATALOG'] + "/" + args["SCHEMA"]

jdbc_url = "jdbc:doordahost:" + jdbc_path


# JDBC Connection
jdbc_conn = spark.read.format("jdbc"). \
    option("url", jdbc_url). \
    option("driver", jdbc_driver). \
    option('user', args['USERNAME']). \
    option('password', args['PASSWORD']). \
    option("SSL", 'true')

# Get List of Table Names in Catalog
table_list_df = jdbc_conn.option("dbtable", f"(SELECT table_name FROM system.jdbc.tables "
                                            f"WHERE table_cat = '{args['CATALOG']}' AND "
                                            f"table_schem = '{args['SCHEMA']}') table_list").load()


for row in table_list_df.rdd.collect():
    if row.table_name != "register_company_charge_ledger":
        continue
    print(row.table_name)

    # Get Table Schema
    table_schema_df = jdbc_conn.option("dbtable",
                                       f"(SELECT column_name, type_name FROM system.jdbc.columns "
                                       f"WHERE table_name = '{row.table_name}') table_schema").load()

    # Create dictionary of column names to column types
    column_type = {val.column_name: val.type_name for val in table_schema_df.rdd.collect()}

    # Creates SELECT statement
    select_statement, columns_w_complex_types = create_select_statement(column_type, row.table_name)

    # Query Table
    spark_df = jdbc_conn.option("dbtable", select_statement).load()

    # Convert Complex Types (Array/Map etc) cast as String previously back to Complex Types
    for col in columns_w_complex_types:
        spark_dataframe = spark_df.withColumn(col, from_json(col, complex_value_map[columns_w_complex_types[col]]))

    # Load pyspark df to dynamic frame
    dynamic_df = DynamicFrame.fromDF(spark_df, glueContext, 'sample_job')

    # Write df to s3 as Parquet
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df,
        connection_type="s3",
        connection_options={"path": args['FILE_OUTPUT_DIRECTORY'] + row.table_name},
        format="parquet",
        transformation_ctx="")

job.commit()
