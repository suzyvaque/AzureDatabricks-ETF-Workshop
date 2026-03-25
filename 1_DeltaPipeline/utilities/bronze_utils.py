from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utilities.common_utils import Env, landing_path


def validate_catalog(spark: SparkSession) -> None:
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    if Env.CATALOG not in catalogs:
        raise ValueError(
            f"Catalog {Env.CATALOG} does not exist. "
            f"Please use the pre-created external-location catalog."
        )


def create_bronze_schema(spark: SparkSession, usernumber: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Env.CATALOG}.sch_user{usernumber}_bronze")


def read_landing_table(spark: SparkSession, table_name: str, fmt: str = "parquet") -> DataFrame:
    path = landing_path(table_name)
    return spark.read.format(fmt).load(path)


def add_bronze_metadata(df: DataFrame, source_value: str = "landing") -> DataFrame:
    return (
        df.withColumn("_ingestion_ts", F.current_timestamp())
          .withColumn("_bronze_source", F.lit(source_value))
    )