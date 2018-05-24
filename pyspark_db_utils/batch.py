""" Utils for Postgres.

    Most useful are: :func:`read_from_pg`, :func:`write_to_pg`, :func:`execute_batch`
"""

import functools
from logging import Logger
from typing import Dict, List, Optional, Iterator, Iterable, Any

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext

from pyspark_db_utils.mogrify import mogrify, mogrifier
from pyspark_db_utils.utils.db_connect import db_connect
from pyspark_db_utils.utils.ensure_columns_in_table import ensure_columns_in_table


def batcher(iterable: Iterable, batch_size: int):
    """ yields batches of iterable

    Args:
        iterable: something to batch
        batch_size: batch size

    Yields:
        batch, until end of iterable
    """
    batch = []
    for obj in iterable:
        batch.append(obj)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _execute_batch_partition(partition: Iterator, sql_temp: str, con_info: Dict, batch_size: int) -> None:
    """ execute sql_temp for rows in partition in batch """
    with db_connect(con_info=con_info) as (conn, curs):
        for batch in batcher(partition, batch_size):
            sql = ';'.join(
                mogrifier.format(sql_temp, row)
                for row in batch
            )
            curs.execute(sql)
        conn.commit()


def execute_batch(df: DataFrame, sql_temp: str, con_info: Dict, batch_size: int=1000) -> None:
    """
    Very useful function to run custom SQL on each rows in DataFrame by batches.

    For example UPDATE / DELETE / etc

    Attention!
    It's expecting that sql_temp string using {} like formatting (because it's easy to overload it by custom formatter.
    execute_batch replace {field} by field value for each row in DataFrame.
    So, if you want to make some formatting (such as table_name or constant values) you should use %()s formatting.


    Examples:
        update table rows by id and values for DF records::

            >> execute_batch(df, con_info=con_info,
                sql_temp='update %(table_name)s set out_date=%(filename_date)s where id={id}'
                % {'table_name': table_name, 'filename_date': filename_date})

        update table rows fields by complex sql expression::

            >> execute_batch(df=df, sql_temp='''
                UPDATE reporting.cases c
                     SET
                          close_date = {check_date_time},
                          status = 2,
                          lost_sales = EXTRACT(epoch FROM {check_date_time} - c.start_date) * (3.0 / 7) / (24 * 3600)
                     WHERE
                          c.id = {id}
               ''', con_info=con_info)

    """
    df.foreachPartition(
        functools.partial(_execute_batch_partition, sql_temp=sql_temp, con_info=con_info, batch_size=batch_size))


def _update_many_partition(partition: Iterator,
                           table_name: str,
                           set_to: Dict[str, Any],
                           con_info: Dict,
                           batch_size: int,
                           id_field: str='id'
                           ) -> None:
    """ Update rows in partition. Set some fields to the new same-values.

    Args:
        partition: DataFrame partition
        table_name: table name
        set_to: dict such as {'field_name1': new_value1, 'field_name2': new_value2}
        con_info: con_info
        batch_size: batch size
        id_field: id field
    """
    field_stmt_list = []
    for field_name, new_value in set_to.items():
        field_stmt_list.append('{}={}'.format(field_name, mogrify(new_value)))
    fields_stmt = ', '.join(field_stmt_list)
    with db_connect(con_info) as (conn, curs):
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
                con_info: Dict,
                batch_size: int=1000,
                id_field: str='id'
                ) -> None:
    """ Update rows in DataFrame. Set some fields to the new constant same-values.

    Note:
        this function update fields to constant values,
        if you want to make some update-sql-expression, use execute_batch

    Args:
        df: DataFrame
        table_name: table name
        set_to: dict such as {'field_name1': new_const_value1, 'field_name2': new_const_value2}
        con_info: con_info
        batch_size: batch size
        id_field: id field
    """
    df.foreachPartition(
        functools.partial(_update_many_partition, table_name=table_name, set_to=set_to, con_info=con_info,
                          batch_size=batch_size, id_field=id_field))


def _insert_values_partition(partition: Iterator,
                             sql_temp: str,
                             values_temp: str,
                             con_info: Dict,
                             batch_size: int,
                             fields_stmt: Optional[str]=None,
                             table_name: Optional[str]=None,
                             logger: Optional[Logger]=None):
    """ Insert rows from partition.

    Args:
        partition: DataFrame partition
        sql_temp: sql template (may consist values, fields, table_name formatting-arguments)
        values_temp: string template for values
        con_info: con_info
        batch_size: batch size
        fields_stmt: string template for fields
        table_name: table name argument for string-formatting
    """

    with db_connect(con_info) as (conn, curs):
        for batch in batcher(partition, batch_size):
            values = ','.join(
                mogrifier.format(values_temp, row)
                for row in batch
            )
            sql = sql_temp.format(values=values, fields=fields_stmt, table_name=table_name)
            if logger:
                max_len = 1024
                logger.info('_insert_values_partition sql[:{}]: {}'.format(max_len, sql[:max_len]))
            curs.execute(sql)
        conn.commit()


def insert_values(df: DataFrame,
                  con_info: Dict,
                  batch_size: int=1000,
                  fields: Optional[List[str]]=None,
                  values_temp: Optional[str]=None,
                  sql_temp: Optional[str]=None,
                  table: Optional[str]=None,
                  on_conflict_do_nothing: bool=False,
                  on_conflict_do_update: bool=False,
                  drop_duplicates: bool=False,
                  exclude_null_field: Optional[str]=None,
                  logger: Optional[Logger]=None,
                  sc: SparkContext=None
                  ) -> None:
    """ Insert rows from DataFrame.

    Note:
        Use write_to_pg as often as possible.

        Unfortunately, it's not able to use ON CONFLICT and ON UPDATE statements,
        so we are forced to write custom function.


    Args:
        df: DataFrame
        sql_temp: sql template (may consist values, fields, table_name formatting-arguments)
        values_temp: string template for values
        con_info: con_info
        fields: list of columns for insert (if None, all olumns will be used)
        batch_size: batch size
        table: table name argument for string-formatting
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
    if table:
        cleaned_df = ensure_columns_in_table(con_info=con_info, table=table, df=df)

    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates(drop_duplicates)

    if exclude_null_field:
        spark = SQLContext(sc)
        cleaned_df.createOrReplaceTempView('cleaned_df')
        cleaned_df = spark.sql("""select *  from cleaned_df
            where {} is not null""".format(exclude_null_field))

    # TODO: add mogrify values, not table_name, fields, etc
    assert table or sql_temp
    if sql_temp is None:
        sql_temp = 'INSERT INTO {table}({fields}) VALUES {values}'

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
        functools.partial(_insert_values_partition,
                          sql_temp=sql_temp, values_temp=values_temp, fields_stmt=fields_stmt, table=table,
                          con_info=con_info, batch_size=batch_size, logger=logger))
    cleaned_df.unpersist()
