""" It's just simple example of using lib
    It asks you about DB connection parameters, makes DF, writes to DB, loads it back and shows.
"""

import os

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as F

from pyspark_db_utils.pg import write_to_pg, read_from_pg


SPARK_CONFIG = {
    "MASTER": "local[*]",
    "settings": {
      "spark.executor.cores": "1",
      "spark.executor.memory": "1g",
      "spark.driver.cores": "1",
      "spark.driver.memory": "1g",
      "spark.cores.max": "1"
    }
}


def get_pg_config() -> dict:
    """ Ask DB connections params"""
    host = input('host: ')
    db = input('db: ')
    user = input('user: ')
    password = input('password: ')

    return {
      "PG_PROPERTIES": {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
      },
      "PG_DRIVER_PATH": "jars/postgresql-42.1.4.jar",
      "PG_URL": "jdbc:postgresql://{host}/{db}".format(host=host, db=db),
    }


def init_spark_context(appname: str) -> SparkContext:
    """ init spark context """
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars jars/postgresql-42.1.4.jar pyspark-shell'
    conf = SparkConf()
    conf.setMaster(SPARK_CONFIG['MASTER'])
    conf.setAppName(appname)

    for setting, value in SPARK_CONFIG['settings'].items():
        conf.set(setting, value)

    sc = SparkContext(conf=conf)

    return sc


def main(spark) -> None:
    """ run example """
    PG_CONFIG = get_pg_config()

    print('TRY: create df')
    df = spark.range(1, 20, 1, 4).withColumn('mono_id', F.monotonically_increasing_id())
    print('OK: create df')
    df.show()

    print('')

    print('TRY: write_to_pg')
    write_to_pg(df=df, config=PG_CONFIG, table='test_table')
    print('OK: write_to_pg')

    print('')

    print('TRY: read_from_pg')
    df_loaded = read_from_pg(config=PG_CONFIG, sql='test_table', sc=sc)
    print('OK: read_from_pg')
    df_loaded.show()


if __name__ == '__main__':
    sc = init_spark_context('app')
    spark = SparkSession(sc)
    main(spark)
    spark.stop()
