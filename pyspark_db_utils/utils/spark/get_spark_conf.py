import os
import json

import pyspark

from pyspark_db_utils.get_jars import get_jars
from pyspark_db_utils.utils.execute_sql import execute_sql
from pyspark_db_utils.mogrify import mogrify

CONFIGS_TABLE = 'analytics.configs'


def load_config_from_table(all_con_info,
                           key=None,
                           config_id=None,
                           table_name=CONFIGS_TABLE) -> dict:
    """ load spark settings from DB """
    assert bool(key) ^ bool(config_id)

    if config_id is None:
        config_id = execute_sql(all_con_info['postgresql'], '''
            SELECT MAX(id)
            FROM {table}
            WHERE key = {key}
        '''.format(table=table_name,
                   key=mogrify(key)))[0][0]
    value = execute_sql(all_con_info['postgresql'], '''
        SELECT value
        FROM {table}
        WHERE id = {id}
        LIMIT 1
    '''.format(table=table_name,
               id=mogrify(config_id)))
    if not value:
        raise ValueError(
            'No value in {table} for key={key!r} and config_id={id}'.format(
                table=table_name,
                key=key,
                id=config_id))
    value = json.loads(str(value[0][0]))
    return value


def get_spark_conf(spark_config=None,
                   con_info=None,
                   config_key=None,
                   config_id=None,
                   configs_table=CONFIGS_TABLE,
                   app_name=None,
                   master=None):
    assert bool(spark_config) ^ bool(con_info), 'spark_config or con_info must be set'

    if spark_config:
        master = spark_config['MASTER']
        values = spark_config['settings']
        jars = get_jars()
    elif con_info:
        values = {}
        if con_info and (config_key or config_id):
            values = load_config_from_table(con_info, config_key, config_id,
                                            configs_table)
            values = dict(values)
        config_jars = {os.path.abspath(jar.replace('file://', ''))
                       for jar in values.get('spark.jars', '').split(',')
                       if jar}
        jars = config_jars.union(set(get_jars()))
    else:
        raise ValueError('spark_config or con_info must be set')

    values['spark.jars'] = ','.join(jars)
    values['spark.sql.execution.arrow.enabled'] = True
    conf = pyspark.SparkConf()
    conf.setAll(list(values.items()))
    if app_name is not None:
        conf = conf.setAppName(app_name)
    if master is not None:
        conf = conf.setMaster(master)
    return conf
