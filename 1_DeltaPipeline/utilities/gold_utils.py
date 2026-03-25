from pyspark.sql import SparkSession

from utilities.common_utils import Env


def create_gold_schema(spark: SparkSession, usernumber: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Env.CATALOG}.sch_user{usernumber}_gold")