from contextlib import contextmanager
import jaydebeapi
import datetime
import string
import functools
from pyspark.sql import SQLContext
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


def write_to_pg(df, settings, table_name, mode=None):
    """
    Write dataframe to postgres
    :type df: DataFrame
    :type settings: dict
    :type table_name: str
    :type mode: Optional[str]
    :return:
    """
    df.write.jdbc(url=settings['PG_URL'],
                  table=table_name,
                  mode=mode,
                  properties=settings['PG_PROPERTIES'])


@contextmanager
def jdbc_connect(config, autocommit=False):
    """ context manager, opens and closes connection correctly """
    conn = jaydebeapi.connect(config["PG_PROPERTIES"]['driver'],
                              config["PG_URL"],
                              {'user': config["PG_PROPERTIES"]['user'],
                               'password': config["PG_PROPERTIES"]['password']},
                                config["PG_DRIVER_PATH"]
                              )
    if not autocommit:
        conn.jconn.setAutoCommit(False)
    curs = conn.cursor()
    yield conn, curs
    curs.close()
    conn.close()


def mogrify(val):
    """ cast python values to raw-sql correctly and escape if necessary """
    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return "'{}'".format(escaped)
    elif isinstance(val, (int, float)):
        return str(val)
    elif isinstance(val, datetime.datetime):
        return "'{}'::TIMESTAMP".format(val)
    elif isinstance(val, datetime.date):
        return "'{}'::DATE".format(val)
    else:
        raise TypeError('unknown type {} for mogrify'.format(type(val)))


class MogrifyFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        row = args[0]
        return mogrify(row[key])


def batcher(iterable, batch_size):
    batch = []
    for obj in iterable:
        batch.append(obj)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


mogrifier = MogrifyFormatter()


def execute_batch_partition(partition, sql_temp, config, batch_size):
    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            sql = ';'.join(
                mogrifier.format(sql_temp, row)
                for row in batch
            )
            # if config.get('DEBUG_SQL'):
            print('\n\nsql: {}\n\n'.format(sql[:500]))
            curs.execute(sql)
        conn.commit()


def execute_batch(df, sql_temp, config, batch_size=1000):
    """
    Attention!
    It's expecting that sql_temp string using {} like formatting (because it's easy to overload it by custom formatter.
    So, if you want to make some formatting (such as table_name or constant values) you should use %()s formatting.
        i.e.
        execute_batch(df, config=config,
                      sql_temp='update %(table_name)s set out_date=%(filename_date)s where id={id}'
                                    % {'table_name': table_name, 'filename_date': filename_date})
    :param df:
    :param sql_temp:
    :param config:
    :param batch_size:
    :return:
    """
    df.foreachPartition(
        functools.partial(execute_batch_partition, sql_temp=sql_temp, config=config, batch_size=batch_size))


def update_many_partition(partition, table_name, set_to, config, batch_size):
    field_stmt_list = []
    for field_name, new_value in set_to.items():
        field_stmt_list.append('{}={}'.format(field_name, mogrify(new_value)))
    fields_stmt = ', '.join(field_stmt_list)
    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            ids = [row['id'] for row in batch]
            if not ids:
                break
            ids_str = ', '.join(str(id_) for id_ in ids)
            sql = 'UPDATE {} SET {} WHERE id IN ({})'.format(table_name, fields_stmt, ids_str)
            print(sql)
            curs.execute(sql)
        conn.commit()


def update_many(df, table_name, set_to, config, batch_size=1000):
    df.foreachPartition(
        functools.partial(update_many_partition, table_name=table_name, set_to=set_to, config=config, batch_size=batch_size))


def insert_values_partition(partition, sql_temp, values_temp, config, batch_size, fields_stmt=None, table_name=None):
    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            values = ','.join(
                mogrifier.format(values_temp, row)
                for row in batch
            )
            sql = sql_temp.format(values=values, fields=fields_stmt, table_name=table_name)
            if config.get('DEBUG_SQL'):
                print('\n\nsql: {}\n\n'.format(sql[:500]))
            curs.execute(sql)
        conn.commit()


def insert_values(df, config, batch_size=1000, fields=None, values_temp=None, sql_temp=None,
                  table_name=None, on_conflict_do_nothing=False, on_conflict_do_update=None):
    # TODO: add mogrify values, not table_name, fields, etc
    assert table_name or sql_temp
    if sql_temp is None:
        sql_temp = 'INSERT INTO {table_name}({fields}) VALUES {values}'

    if on_conflict_do_nothing:
        sql_temp += ' ON CONFLICT DO NOTHING'

    if on_conflict_do_update:
        sql_temp += on_conflict_do_update

    if fields is None:
        fields = df.columns
    fields_stmt = ','.join(fields)

    if values_temp is None:
        values_temp = ','.join('{'+field+'}' for field in fields)
        values_temp = "({})".format(values_temp)

    df.foreachPartition(
        functools.partial(insert_values_partition,
                          sql_temp=sql_temp, values_temp=values_temp, fields_stmt=fields_stmt, table_name=table_name,
                          config=config, batch_size=batch_size))
