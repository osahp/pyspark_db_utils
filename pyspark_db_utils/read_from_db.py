from pyspark_db_utils.utils.spark.get_spark_con_params import get_spark_con_params
from pyspark_db_utils.utils.spark.get_or_create_spark_session import get_or_create_spark_session


def read_from_db(con_info, table, spark_session=None, **kwargs):
    """
    kwargs: kw arguments of pyspark.sql.readwriter.DataFrameReader.jdbc()
    """
    if spark_session is None:
        spark_session = get_or_create_spark_session()

    return spark_session.read.jdbc(table=table,
                                   **get_spark_con_params(con_info),
                                   **kwargs)


def read_from_ch(config, **kwargs):
    """
    read DataFrame from ClickHouse
    """
    return read_from_db(config['clickhouse'], **kwargs)


def read_from_pg(config, **kwargs):
    """
    read DataFrame from PostgreSQL
    """
    return read_from_db(config['postgresql'], **kwargs)
