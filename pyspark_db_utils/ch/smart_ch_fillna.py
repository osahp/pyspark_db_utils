from functools import reduce

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType, BinaryType, BooleanType, DateType,
    TimestampType, DecimalType, DoubleType, FloatType, ByteType, IntegerType,
    LongType, ShortType)


# Default value (if null) for Spark types
# IMPORTANT! No default values for Date and TimeStamp types yet!
SparkType2Default = {
    StringType: '',
    BinaryType: '',
    BooleanType: 0,
    # DateType: '0001-01-03',  # https://issues.apache.org/jira/browse/SPARK-22182
    # TimestampType: '0001-01-03 00:29:43',  # https://issues.apache.org/jira/browse/SPARK-22182
    # DateType: '0001-01-01',
    # TimestampType: '0001-01-01 00:00:00',
    DoubleType: 0.0,
    FloatType: 0.0,
    ByteType: 0,
    IntegerType: 0,
    LongType: 0,
    ShortType: 0,
    DecimalType: 0.0,
}


def check_date_columns_for_nulls(df: DataFrame) -> bool:
    """ returns True if any Date or Timestamp column consist NULL value """
    expr_list = []
    for field in df.schema.fields:
        name = field.name
        spark_type = type(field.dataType)
        if spark_type in [DateType, TimestampType]:
            expr_list.append(F.isnull(F.col(name)))
    if not expr_list:
        return False
    expr = reduce(lambda x, y: x | y, expr_list)
    df_nulls = df.filter(expr)
    return not df_nulls.rdd.isEmpty()


def smart_ch_fillna(df: DataFrame) -> DataFrame:
    """ change null-value to default values """
    mapping = {}
    if check_date_columns_for_nulls(df):
        raise Exception('Date and Timestamp columns mustn\'t be null!')
    for field in df.schema.fields:
        name = field.name
        spark_type = type(field.dataType)
        if spark_type in [DateType, TimestampType]:
            continue
        default_value = SparkType2Default[spark_type]
        mapping[name] = default_value
    df = df.fillna(mapping)
    return df
