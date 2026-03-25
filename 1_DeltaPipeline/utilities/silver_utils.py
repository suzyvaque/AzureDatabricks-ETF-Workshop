from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from utilities.common_utils import Env, PRIMARY_KEYS, silver_table_name


def create_silver_schema(spark: SparkSession, usernumber: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {Env.CATALOG}.sch_user{usernumber}_silver")


def dedup_by_keys_latest(df: DataFrame, keys: list[str], order_col: str = "load_ts") -> DataFrame:
    cols = set(df.columns)

    if order_col not in cols:
        if "bas_dd" in cols:
            order_col = "bas_dd"
        else:
            order_col = keys[0]

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def merge_insert_only_to_silver(
    spark: SparkSession,
    usernumber: str,
    table_name: str,
    source_df: DataFrame,
) -> None:
    target_table = silver_table_name(usernumber, table_name)
    keys = PRIMARY_KEYS[table_name]

    if not spark.catalog.tableExists(target_table):
        source_df.write.mode("append").format("delta").saveAsTable(target_table)
        return

    delta_target = DeltaTable.forName(spark, target_table)
    merge_cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    (
        delta_target.alias("t")
        .merge(source_df.alias("s"), merge_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )