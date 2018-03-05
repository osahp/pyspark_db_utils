import json
import sys
import sys
import json
import logging
from pyspark.sql import SparkSession
import os.path
import os
from pyspark import SparkContext, SparkConf
import shutil
import json
import shutil
import pyspark.sql.functions as F


SPARK_CONFIG = {
    "MASTER": "local[*]",
    "settings":{
      "spark.executor.cores": "1",
      "spark.executor.memory": "1g",
      "spark.driver.cores": "1",
      "spark.driver.memory": "1g",
      "spark.cores.max": "1"
    }
}

with open('config.json') as f:
    config = json.load(f)


def init_spark_context(appname, config):
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars jars/postgresql-42.1.4.jar pyspark-shell'

    conf = SparkConf()
    conf.setMaster(SPARK_CONFIG['MASTER'])
    conf.setAppName(appname)

    for setting, value in SPARK_CONFIG['settings'].items():
        conf.set(setting, value)

    sc = SparkContext(conf=conf)

    return sc


def main(spark):
    df = spark.range(1, 20, 1, 4).withColumn('mono_id', F.monotonically_increasing_id())
    df.show()

if __name__ == '__main__':
    sc = init_spark_context('app', config)
    spark = SparkSession(sc)
    main(spark)
    spark.stop()
