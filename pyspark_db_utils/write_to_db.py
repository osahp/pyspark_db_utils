from pyspark_db_utils.utils.ensure_columns_in_table import ensure_columns_in_table
from pyspark_db_utils.utils.spark.get_spark_con_params import get_spark_con_params


def write_to_db(con_info, df, table, mode='error'):
    """
    Note: With ClickHouse only mode='append' is supported

    mode: specifies the behavior of the save operation when data already exists.
    * append: Append contents of this :class:`DataFrame` to existing data.
    * overwrite: Overwrite existing data.
    * ignore: Silently ignore this operation if data already exists.
    * error: Throw an exception if data already exists.
    """
    df = ensure_columns_in_table(df=df, con_info=con_info, table=table)
    df.write.jdbc(table=table, mode=mode, **get_spark_con_params(con_info))


def write_to_pg(config, **kwargs):
    return write_to_db(config['postgresql'], **kwargs)


def write_to_ch(config, **kwargs):
    return write_to_db(config['clickhouse'], **kwargs)
