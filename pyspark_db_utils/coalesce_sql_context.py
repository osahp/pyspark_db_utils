from pyspark.sql import SQLContext
from pyspark import SparkContext


def coalesce_sql_context(spark_context=None):
    """ get sql context from spark_context if it is not None
        or exists
        or create new one
    :type spark_context: SparkContext
    :rtype: SQLContext
    """
    if spark_context is not None:
        sc = SQLContext(spark_context)
    else:
        sс = SQLContext.getOrCreate()
    return sc
