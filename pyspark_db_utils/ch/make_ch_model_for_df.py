from typing import Optional, List, Set, Tuple
import types
from pyspark.sql.types import (
    StringType, BinaryType, BooleanType, DateType,
    TimestampType, DecimalType, DoubleType, FloatType, ByteType, IntegerType,
    LongType, ShortType)
from infi.clickhouse_orm import models, engines
from infi.clickhouse_orm.fields import (
    StringField, FixedStringField, DateField, DateTimeField,
    UInt8Field, UInt16Field, UInt32Field, UInt64Field,
    Int8Field, Int16Field, Int32Field, Int64Field,
    Float32Field, Float64Field, Enum8Field, Enum16Field, NullableField, Field as CHField)
from pyspark.sql.types import StructField as SparkField


# mapping from spark type to ClickHouse type
SparkType2CHField = {
    StringType: StringField,
    BinaryType: StringField,
    BooleanType: UInt8Field,  # There are no bool type in ClickHouse
    DateType: DateField,
    TimestampType: DateTimeField,
    DoubleType: Float64Field,
    FloatType: Float32Field,
    ByteType: UInt8Field,
    IntegerType: Int32Field,
    LongType: Int64Field,
    ShortType: Int16Field,
    DecimalType: Float64Field,
}


def spark_field2clickhouse_field(spark_field: SparkField) -> CHField:
    """ spark field to clickhouse field """
    spark_type = type(spark_field.dataType)
    clickhouse_field_class = SparkType2CHField[spark_type]
    clickhouse_field = clickhouse_field_class()
    # if spark_field.nullable:
    #     logger.warning('spark_field {} is nullable, it is not good for ClickHouse'.format(spark_field))
    #     # IDEA
    #     # clickhouse_field = NullableField(clickhouse_field)
    return clickhouse_field


def make_ch_model_for_df(df, date_field_name, table_name, pk_columns=None):
    """
        creates ORM Model for DataFrame
        models.Model is meta class so it is a bit tricky to dynamically create child-class with given attrivutes
        ToDo: Add support for engine Memory and Log
    :param df: PySpark DataFrame
    :param date_field_name: Date-typed field for partitioning
    :param pk_columns: primary key columns
    :param table_name: table name in DB
    :return: ORM Model class
    """
    assert date_field_name in df.schema.names
    assert 'engine' not in df.schema.names
    if pk_columns is None:
        pk_columns = df.schema.names
    attrs = {'engine': engines.MergeTree(date_field_name, pk_columns)}
    for field in df.schema.fields:
        clickhouse_field = spark_field2clickhouse_field(field)
        attrs[field.name] = clickhouse_field
    Model = type('MyModel', (models.Model,), attrs)
    Model.table_name = staticmethod(types.MethodType(lambda cls: table_name, Model))
    return Model
