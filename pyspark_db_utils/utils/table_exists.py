import jaydebeapi
from pyspark_db_utils.utils.execute_sql import execute_sql


def table_exists(con_info, table) -> bool:
    """ table exists or not

    Notes:
        actually, just select LIMIT 0 from table and returns was it successful or not
    """
    try:
        execute_sql(con_info, 'SELECT * FROM {} LIMIT 0'.format(table))
        return True
    except jaydebeapi.DatabaseError:
        return False
