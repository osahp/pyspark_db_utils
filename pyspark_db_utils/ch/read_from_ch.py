from pyspark.sql import SparkSession


def read_from_ch(config, sql):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("jdbc").options(
        url=config['CH_JDBC_URL'],
        dbtable=sql,
        **config['CH_JDBC_PROPERTIES']
    ).load().cache()
    return df
