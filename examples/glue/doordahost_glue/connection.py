from doorda_sdk.host import client
from schema_conversion import pyspark_conversion, python_conversion
from pyspark.sql.types import StructType, StructField


class GlueETL:
    """
    Glue ETL script to convert doordahost schema into pyspark schema and
    export table(s) in doordahost into spark dataframes.

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName('test_job').getOrCreate()
    >>> cls = GlueETL(username="username",  # Modify username
    >>>               password="password",  # Modify Password
    >>>               catalog="catalog_name",  # Modify Catalog Name
    >>>               schema="schema_name",  # Modify Schema Name
    >>>               spark_session=spark)
    >>> cursor = cls.get_connection()
    >>> spark_dataframe = cls.query("SELECT * FROM table_name")

    """

    def __init__(self, username, password, catalog, schema, spark_session):
        self.username = username
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        self.cursor = None
        self.describe_query_string = "DESCRIBE {catalog_name}.{schema_name}.{table_name}"
        self.spark = spark_session

    def get_connection(self):
        """
        Create Connection to DoordaHost

        :return:
        """
        self.connection = client.connect(username=self.username,
                                         password=self.password,
                                         catalog=self.catalog,
                                         schema=self.schema)
        self.cursor = self.connection.cursor()
        return self.cursor

    def query(self, query_string):
        """
        Function to convert doordahost query results to pyspark dataframe

        :param query_string:
        :return:
        :rtype:
        """

        # Execute Query
        self.cursor.execute(query_string)

        # Map Column Names to Column Types
        schema_results_dict = self.cursor.col_types
        schema_results_list = self.cursor.col_names

        # Create PySpark Schema
        struct_fields = [StructField(col,
                                     pyspark_conversion[schema_results_dict[col]],
                                     True)
                         for col in schema_results_list]
        pyspark_schema = StructType(struct_fields)

        # Create Spark Dataframe
        spark_df = self.spark.createDataFrame([self.__convert_schema(row, schema_results_dict, schema_results_list)
                                               for row in self.cursor.fetchall()],
                                              schema=pyspark_schema)
        return spark_df

    @staticmethod
    def __convert_schema(row_results, col_types, col_names):
        """
        Convert row results into respective types based on schema return.


        :param row_results:
        :param col_types:
        :param col_names:
        :return:
        :rtype: tuple
        """
        converted_rows = tuple([python_conversion[col_types[col_names[item]]](row_results[item])
                                if row_results[item]
                                else None
                                for item in range(0, len(row_results))])
        return converted_rows
