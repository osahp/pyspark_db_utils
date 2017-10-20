from pyspark import SparkContext
from pyspark.sql import DataFrame
from .coalesce_sql_context import coalesce_sql_context


def read_from_pg(sql, settings, spark_context=None):
    """
    Read dataframe from postgres
    :param settings: settings for connect
    :type settings: dict
    :param sql:
    :type sql: str
    :param spark_context: specific current spark_context or None
    :type spark_context: SparkContext
    :return: selected DF
    :rtype: DataFrame
    """
    sc = coalesce_sql_context(spark_context)
    df = sс.read.format("jdbc").options(
        url=settings['PG_URL'],
        dbtable=sql,
        **settings['PG_PROPERTIES']
    ).load().cache()
    return df
