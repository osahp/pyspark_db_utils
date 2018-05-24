from typing import Optional, List

from pyspark.sql import SparkSession

from pyspark_db_utils.read_from_db import read_from_db


def get_field_names(con_info: dict,
                    table: str,
                    spark_session: Optional[SparkSession]=None) -> List[str]:
    """ get field names of table
        TODO: remove spark
    """
    sql = '(select * from {} limit 0) as foo'.format(table)
    df_tmp = read_from_db(con_info, sql, spark_session=spark_session)
    return df_tmp.columns
