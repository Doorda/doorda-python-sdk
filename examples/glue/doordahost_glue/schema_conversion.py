from pyspark.sql.types import *
from datetime import datetime


pyspark_conversion = {"tinyint": ShortType(),
                      "smallint": ShortType(),
                      "integer": IntegerType(),
                      "bigint": LongType(),
                      "real": FloatType(),
                      "double": DoubleType(),
                      "decimal": DecimalType(),
                      "varchar": StringType(),
                      "char": StringType(),
                      "json": StringType(),
                      "varbinary": BinaryType(),
                      "date": DateType(),
                      "boolean": BooleanType(),
                      "time": TimestampType(),
                      "timestamp": TimestampType(),
                      "array(varchar)": ArrayType(StringType()),
                      "map(varchar,varchar)":  MapType(StringType(), StringType()),
                      "array(map(varchar,varchar))":  ArrayType(MapType(StringType(),StringType()))}


def date_conversion(value):
    date = "%Y-%m-%d"
    return datetime.strptime(value, date).date()


def time_conversion(value):
    time = "%H:%M:%S"
    return datetime.strptime(value, time).time()


def datetime_conversion(value):
    date_time = "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(value.replace(".000", ""), date_time)


def __dummy(value):
    """
    Dummy function used when no type conversion is needed

    :param value:
    :return:
    """
    return value


python_conversion = {"tinyint": int,
                     "smallint": int,
                     "integer": int,
                     "bigint": int,
                     "real": float,
                     "double": float,
                     "decimal": float,
                     "varchar": str,
                     "char": str,
                     "json": str,
                     "varbinary": bytearray,
                     "date": date_conversion,
                     "boolean": bool,
                     "time": time_conversion,
                     "timestamp": datetime_conversion,
                     "array(varchar)": __dummy,
                     "map(varchar,varchar)": __dummy,
                     "array(map(varchar,varchar))": __dummy}

