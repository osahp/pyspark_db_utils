from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    """ get or create spark session """
    return SparkSession.builder.config().getOrCreate()
