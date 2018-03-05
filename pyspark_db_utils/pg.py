from typing import Dict, List, Set, Optional, Iterator, Iterable
from contextlib import contextmanager
from itertools import chain
from logging import Logger

import jaydebeapi
import datetime
import string
import functools
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import DataFrame

from pyspark_db_utils.utils.drop_columns import drop_other_columns


def read_from_pg(config: Dict, sql: str, sc: SparkContext, logger: Optional[Logger]=None) -> DataFrame:
    """
    Read dataframe from postgres
    :param settings: settings for connect
    :param sql: sql
    :param sc: specific current spark_context or None
    :param logger: logger
    :return: selected DF
    """
    if logger:
        logger.info('read_from_pg:\n{}'.format(sql))
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("jdbc").options(
        url=config['PG_URL'],
        dbtable=sql,
        **config['PG_PROPERTIES']
    ).load().cache()
    return df


def write_to_pg(df: DataFrame, config: dict, table: str, mode: str='append', logger: Optional[Logger]=None) -> None:
    """
    Write dataframe to postgres
    Args:
        df: DataFrame to write
        config: config dict
        table: table_name
        logger: logger
        mode: mode, one of these
            append - create table if not exists (with all columns of DataFrame)
                     and write records to table (using fields only in table columns)
            overwrite - truncate table (if exists) and write records (using fields only in table columns)
            overwrite_full - drop table and create new one with all columns and DataFrame and append records to it
            fail - fail if table is not exists, otherwise append records to it
    """
    field_names = get_field_names(table, config)
    table_exists = bool(field_names)

    if mode == 'fail':
        if not table_exists:
            raise Exception('table {} does not exist'.format(table))
        else:
            mode = 'append'  # if table exists just append records to it

    if mode == 'append':
        if table_exists:
            df = drop_other_columns(df, field_names)
    elif mode == 'overwrite_full':
        if table_exists:
            run_sql('drop table {}'.format(table), config, logger=logger)
    elif mode == 'overwrite':
        if table_exists:
            df = drop_other_columns(df, field_names)
            run_sql('truncate {}'.format(table), config, logger=logger)
    df.write.jdbc(url=config['PG_URL'],
                  table=table,
                  mode='append',  # always just append because all logic already done
                  properties=config['PG_PROPERTIES'])


def run_sql(sql: str, config: Dict, logger: Optional[Logger]=None) -> None:
    """ just run sql """
    if logger:
        logger.info('run_sql: {}'.format(sql))
    with jdbc_connect(config, autocommit=True) as (conn, curs):
        curs.execute(sql)


def get_field_names(table_name: str, config: Dict) -> Set[str]:
    """ get field names of table """
    if len(table_name.split('.')) > 1:
        table_name = table_name.split('.')[-1]
    with jdbc_connect(config) as (conn, cur):
        sql = "SELECT column_name FROM information_schema.columns WHERE table_name='{}'".format(table_name)
        cur.execute(sql)
        res = cur.fetchall()
    field_names = list(chain(*res))
    return set(field_names)


def get_field_names_stub(df: DataFrame, config: Dict, table_name: str, sc: SparkContext) -> Set[str]:
    """ get field names of table
        WTF? Why not just use get_field_names
    """
    sql = '(select * from {} limit 1) as smth'.format(table_name)
    df_tmp = read_from_pg(config, sql, sc)
    columns_in_db = set(df_tmp.columns)
    columns_in_df = set(df.columns)
    field_names = columns_in_db.intersection(columns_in_df)
    return set(field_names)


@contextmanager
def jdbc_connect(config: Dict, autocommit: bool=False):
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


def mogrify(val) -> str:
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
    elif val is None:
        return 'null'
    else:
        raise TypeError('unknown type {} for mogrify'.format(type(val)))


class MogrifyFormatter(string.Formatter):
    def get_value(self, key, args, kwargs) -> str:
        row = args[0]
        return mogrify(row[key])


def batcher(iterable: Iterable, batch_size: int):
    """ yields batches of iterable """
    batch = []
    for obj in iterable:
        batch.append(obj)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


mogrifier = MogrifyFormatter()


def execute_batch_partition(partition: Iterator, sql_temp: str, config: Dict, batch_size: int) -> None:
    """ execute sql_temp for rows in partition in batch """

    # For debugging RAM
    # def get_ram():
    #     import os
    #     ram = os.popen('free -m').read()
    #     return ram

    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            sql = ';'.join(
                mogrifier.format(sql_temp, row)
                for row in batch
            )
            # if config.get('DEBUG_SQL'):
            print('\n\nsql: {}\n\n'.format(sql[:500]))
            curs.execute(sql)
            # print('\nFREE RAM: %s\n' % get_ram())
        conn.commit()


def execute_batch(df: DataFrame, sql_temp: str, config: Dict, batch_size: int=1000) -> None:
    """
    Very useful function to run custom SQL on each rows in DataFrame by batches.
    For example UPDATE / DELETE / etc

    Attention!
    It's expecting that sql_temp string using {} like formatting (because it's easy to overload it by custom formatter.
    execute_batch replace {field} by field value for each row in DataFrame.
    So, if you want to make some formatting (such as table_name or constant values) you should use %()s formatting.
        i.e.
        execute_batch(df, config=config,
                      sql_temp='update %(table_name)s set out_date=%(filename_date)s where id={id}'
                                    % {'table_name': table_name, 'filename_date': filename_date})
    """
    df.foreachPartition(
        functools.partial(execute_batch_partition, sql_temp=sql_temp, config=config, batch_size=batch_size))


def update_many_partition(partition: Iterator,
                          table_name: str,
                          set_to: Dict,
                          config: Dict,
                          batch_size: int,
                          id_field: str='id'
                          ) -> None:
    """ Update rows in partition. Set some fields to the new same-values.

    Args:
        partition: DataFrame partition
        table_name: table name
        set_to: dict such as {'field_name1': new_value1, 'field_name2': new_value2}
        config: config
        batch_size: batch size
        id_field: id field
    """
    field_stmt_list = []
    for field_name, new_value in set_to.items():
        field_stmt_list.append('{}={}'.format(field_name, mogrify(new_value)))
    fields_stmt = ', '.join(field_stmt_list)
    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            ids = [row[id_field] for row in batch]
            if not ids:
                break
            ids_str = ', '.join(str(id_) for id_ in ids)
            sql = 'UPDATE {} SET {} WHERE id IN ({})'.format(table_name, fields_stmt, ids_str)
            curs.execute(sql)
        conn.commit()


def update_many(df: DataFrame,
                table_name: str,
                set_to: Dict,
                config: Dict,
                batch_size: int=1000,
                id_field: str='id'
                ) -> None:
    """ Update rows in DataFrame. Set some fields to the new same-values.

    Args:
        df: DataFrame
        table_name: table name
        set_to: dict such as {'field_name1': new_value1, 'field_name2': new_value2}
        config: config
        batch_size: batch size
        id_field: id field
    """
    df.foreachPartition(
        functools.partial(update_many_partition, table_name=table_name, set_to=set_to, config=config,
                          batch_size=batch_size, id_field=id_field))


def insert_values_partition(partition: Iterator,
                            sql_temp: str,
                            values_temp: str,
                            config: Dict,
                            batch_size: int,
                            fields_stmt: Optional[str]=None,
                            table_name: Optional[str]=None,
                            logger: Optional[Logger]=None):
    """ Insert rows from partition.

    Args:
        partition: DataFrame partition
        sql_temp: sql template (may consist values, fields, table_name formatting-arguments)
        values_temp: string template for values
        config: config
        batch_size: batch size
        fields_stmt: string template for fields
        table_name: table name argument for string-formatting
    """

    with jdbc_connect(config) as (conn, curs):
        for batch in batcher(partition, batch_size):
            values = ','.join(
                mogrifier.format(values_temp, row)
                for row in batch
            )
            sql = sql_temp.format(values=values, fields=fields_stmt, table_name=table_name)
            if logger:
                max_len = 1024
                logger.info('insert_values_partition sql[:{}]: {}'.format(max_len, sql[:max_len]))
            curs.execute(sql)
        conn.commit()


def insert_values(df: DataFrame,
                  config: Dict,
                  batch_size: int=1000,
                  fields: Optional[List[str]]=None,
                  values_temp: Optional[str]=None,
                  sql_temp: Optional[str]=None,
                  table_name: Optional[str]=None,
                  on_conflict_do_nothing: bool=False,
                  on_conflict_do_update: bool=False,
                  drop_duplicates: bool=False,
                  exclude_null_field: Optional[str]=None,
                  logger: Optional[Logger]=None,
                  sc: SparkContext=None
                  ) -> None:
    """ Insert rows from DataFrame.

    Args:
        df: DataFrame
        sql_temp: sql template (may consist values, fields, table_name formatting-arguments)
        values_temp: string template for values
        config: config
        fields: list of columns for insert (if None, all olumns will be used)
        batch_size: batch size
        table_name: table name argument for string-formatting
        on_conflict_do_nothing: add ON CONFLICT DO NOTHING statement to each INSERT
        on_conflict_do_update: add ON CONFLICT DO UPDATE statement to each INSERT
        drop_duplicates: drop duplicates if set to True
        exclude_null_field: exclude rows where field=exclude_null_field is null
        logger: logger
        sc: Spark context
    """

    # prevent changing original dataframe
    cleaned_df = df.select(*df.columns)

    # select columns to write
    if table_name:
        field_names = get_field_names_stub(df, config, table_name, sc)
        cleaned_df = df.select(*field_names)

    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates(drop_duplicates)

    if exclude_null_field:
        spark = SQLContext(sc)
        cleaned_df.createOrReplaceTempView('cleaned_df')
        cleaned_df = spark.sql("""select *  from cleaned_df
            where {} is not null""".format(exclude_null_field))

    # TODO: add mogrify values, not table_name, fields, etc
    assert table_name or sql_temp
    if sql_temp is None:
        sql_temp = 'INSERT INTO {table_name}({fields}) VALUES {values}'

    if on_conflict_do_nothing:
        sql_temp += ' ON CONFLICT DO NOTHING'

    if on_conflict_do_update:
        sql_temp += on_conflict_do_update

    if fields is None:
        fields = cleaned_df.columns
    fields_stmt = ','.join(fields)

    if values_temp is None:
        values_temp = ','.join('{' + field + '}' for field in fields)
        values_temp = "({})".format(values_temp)

    cleaned_df.foreachPartition(
        functools.partial(insert_values_partition,
                          sql_temp=sql_temp, values_temp=values_temp, fields_stmt=fields_stmt, table_name=table_name,
                          config=config, batch_size=batch_size, logger=logger))
    cleaned_df.unpersist()
