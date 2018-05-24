from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from pyspark_db_utils.utils.get_field_names import get_field_names
from pyspark_db_utils.utils.table_exists import table_exists


def ensure_columns_in_table(con_info: dict,
                            df: DataFrame,
                            table: str,
                            spark_session: Optional[SparkSession] = None
                            ) -> DataFrame:
    """ drop columns in `df` which does not exist in DB table,
        do nothing if DB table does not exist
    """
    if not table_exists(con_info=con_info, table=table):
        return df
    table_columns = get_field_names(con_info=con_info, table=table, spark_session=spark_session)
    columns_to_drop = set(df.columns) - set(table_columns)
    df = df.drop(*columns_to_drop)
    return df
