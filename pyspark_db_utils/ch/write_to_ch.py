from typing import Union

from infi.clickhouse_orm.database import Database, DatabaseException
from infi.clickhouse_orm.models import ModelBase
from pyspark.sql.types import DateType

from pyspark_db_utils.ch.make_ch_model_for_df import make_ch_model_for_df
from pyspark_db_utils.ch.smart_ch_fillna import smart_ch_fillna


class CustomDatabase(Database):
    def check_table_exist(self, table: Union[str, ModelBase]) -> bool:
        """ check if table exists

        Args:
            table: table to check
        """
        if isinstance(table, ModelBase):
            table_name = table.table_name()
        elif isinstance(table, str):
            table_name = table
        else:
            raise TypeError

        try:
            # TODO: use EXISTS statement in CHSQL
            resp = self.raw('select * from {} limit 0'.format(table_name))
            assert resp == ''
        except DatabaseException:
            exists = False
        else:
            exists = True
        return exists

    def describe(self, table: Union[ModelBase, str]) -> str:
        """ Returns result for DESCRIBE statement on table

        Args:
            table: table

        Returns:
            describe table
            i.e.:
                plu	Int64
                shop_id	Int64
                check_date_time	DateTime
                clickhouse_date	Date
                created	DateTime
                type	UInt8
        """
        if isinstance(table, ModelBase):
            table_name = table.table_name()
        elif isinstance(table, str):
            table_name = table
        else:
            raise TypeError
        resp = self.raw('describe table {}'.format(table_name))
        return resp


def make_sure_exsit(df, date_field_name, table_name, mode, config, logger, pk_columns=None):
    """ drop and create table if need """
    Model = make_ch_model_for_df(df, date_field_name, table_name, pk_columns=pk_columns)
    db = CustomDatabase(db_name=config["CH_DB_NAME"], db_url=config["CH_URL"])
    if db.check_table_exist(Model):
        if mode == 'fail':
            raise Exception('table {}.{} already exists and mode={}'.format(db.db_name, Model.table_name(), mode))
        elif mode == 'overwrite':
            db.drop_table(Model)
            logger.info('DROP TABLE {}.{}'.format(db.db_name, table_name))
            db.create_table(Model)
            logger.info('CREATE TABLE {}.{}'.format(db.db_name, table_name))
        elif mode == 'append':
            pass
    else:
        db.create_table(Model)
        logger.info('CREATE TABLE {}.{}'.format(db.db_name, table_name))
    db.describe(Model)


def write_to_ch(df, date_field_name, table_name, mode, config, logger, pk_columns=None):
    """
    Dumps PySpark DataFrame to ClickHouse,
        create or recreate table if needed.
    :param df: PySpark DataFrame
    :param mode: 'overwrite' / 'append' / 'fail'
        describe, what do if table already exists
            overwrite: drop and create table and insert rows (CH hasn't truncate operator)
            append: insert rows to exist table
            fail: raise Exception
    :param table_name: table name
    :param date_field_name: date field for partitioning
    :param pk_columns: list/tuple of primary key columns (None for all columns)
    :return:
    """
    assert mode in ['overwrite', 'append', 'fail'], "mode must be 'overwrite' / 'append' / 'fail'"
    assert '.' not in table_name, 'dots are not allowed in table_name'
    date_field = next(field for field in df.schema.fields if field.name == date_field_name)
    assert type(date_field.dataType) == DateType, \
        "df['{}'].dataType={} must be DateType".format(date_field_name, date_field.dataType)
    make_sure_exsit(df, date_field_name, table_name, mode, config=config, logger=logger, pk_columns=pk_columns)
    full_table_name = '{}.{}'.format(config["CH_DB_NAME"], table_name)
    # Spark JDBC CH Driver works correctly only in append mode
    # and without NULL-s
    df = smart_ch_fillna(df)
    df.write.jdbc(url=config['CH_JDBC_URL'], table=full_table_name, mode='append',
                  properties=config['CH_JDBC_PROPERTIES'])
