from typing import Optional, List, Tuple

from pyspark_db_utils.utils.db_connect import db_connect


def execute_sql(con_info, sql) -> Optional[List[Tuple]]:
    """ execute sql and return rows if possible

    Args:
        con_info: db connection info
        sql: sql to execute

    Returns:
        fetched
    """
    with db_connect(con_info, autocommit=True) as (con, cur):
        cur.execute(sql)
        if cur.description:
            return cur.fetchall()
