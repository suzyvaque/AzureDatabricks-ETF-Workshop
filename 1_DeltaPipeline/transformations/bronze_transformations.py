from pyspark.sql import SparkSession

from utilities.common_utils import TABLES, bronze_table_name
from utilities.bronze_utils import (
    validate_catalog,
    create_bronze_schema,
    read_landing_table,
    add_bronze_metadata,
)


def build_bronze(
    spark: SparkSession,
    usernumber: str,
    source_fmt: str = "parquet",
    bronze_source_value: str = "landing"
) -> None:
    validate_catalog(spark)
    create_bronze_schema(spark, usernumber)

    for table_name in TABLES:
        df = read_landing_table(spark, table_name, fmt=source_fmt)
        bronze_df = add_bronze_metadata(df, source_value=bronze_source_value)

        (
            bronze_df.write
            .mode("append")
            .format("delta")
            .saveAsTable(bronze_table_name(usernumber, table_name))
        )

        print(f"[BRONZE] loaded: {table_name}")