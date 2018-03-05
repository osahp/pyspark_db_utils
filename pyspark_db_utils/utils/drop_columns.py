from typing import List

from pyspark.sql import DataFrame


def drop_other_columns(df: DataFrame, template_schema: List[str]) -> DataFrame:
    """ drop all df columns that are absent in template_schema """
    columns_to_drop = set(df.schema.names) - set(template_schema)
    for column in columns_to_drop:
        df = df.drop(column)
    return df
