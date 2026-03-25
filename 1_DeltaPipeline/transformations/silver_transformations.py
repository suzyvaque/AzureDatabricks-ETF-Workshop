from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utilities.common_utils import TABLES, PRIMARY_KEYS, bronze_table_name
from utilities.silver_utils import (
    create_silver_schema,
    dedup_by_keys_latest,
    merge_insert_only_to_silver,
)




from pyspark.sql import functions as F

def safe_double(col_name: str):
    return (
        F.when(F.trim(F.col(col_name)) == "", None)
         .otherwise(F.col(col_name))
         .cast("double")
    )




def cast_customer_cash_portfolio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("bas_dd").cast("string").alias("bas_dd"),
            F.col("load_ts").cast("timestamp").alias("load_ts"),
            F.col("source_sys").cast("string").alias("source_sys"),
            F.col("cash_amt").cast("double").alias("cash_amt"),
            F.col("_ingestion_ts"),
            F.col("_bronze_source"),
        )
        .filter(F.col("customer_id").isNotNull() & F.col("bas_dd").isNotNull())
    )


def cast_customer_etf_portfolio(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("bas_dd").cast("string").alias("bas_dd"),
            F.col("load_ts").cast("timestamp").alias("load_ts"),
            F.col("source_sys").cast("string").alias("source_sys"),
            F.col("etf_code").cast("string").alias("etf_code"),
            F.col("qty").cast("double").alias("qty"),
            F.col("_ingestion_ts"),
            F.col("_bronze_source"),
        )
        .filter(
            F.col("customer_id").isNotNull() &
            F.col("bas_dd").isNotNull() &
            F.col("etf_code").isNotNull()
        )
    )


def cast_customer_master(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("bas_dd").cast("string").alias("bas_dd"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("birth_year").cast("int").alias("birth_year"),
            F.col("risk_profile_level").cast("int").alias("risk_profile_level"),
            F.col("risk_profile_name").cast("string").alias("risk_profile_name"),
            F.col("join_date").cast("string").alias("join_date"),
            F.col("customer_grade").cast("string").alias("customer_grade"),
            F.col("load_ts").cast("timestamp").alias("load_ts"),
            F.col("source_sys").cast("string").alias("source_sys"),
            F.col("_ingestion_ts"),
            F.col("_bronze_source"),
        )
        .filter(F.col("customer_id").isNotNull() & F.col("bas_dd").isNotNull())
    )


def cast_etf_master(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("bas_dd").cast("string").alias("bas_dd"),
            F.col("isu_cd").cast("string").alias("isu_cd"),
            F.col("isu_nm").cast("string").alias("isu_nm"),
            F.col("idx_ind_nm").cast("string").alias("idx_ind_nm"),
            F.col("list_shrs").cast("double").alias("list_shrs"),
            F.col("load_ts").cast("timestamp").alias("load_ts"),
            F.col("source_sys").cast("string").alias("source_sys"),
            F.col("_ingestion_ts"),
            F.col("_bronze_source"),
        )
        .filter(F.col("isu_cd").isNotNull() & F.col("bas_dd").isNotNull())
    )


def cast_etf_trades(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("bas_dd").cast("string").alias("bas_dd"),
            F.col("isu_cd").cast("string").alias("isu_cd"),
            F.col("isu_nm").cast("string").alias("isu_nm"),
            safe_double("tdd_clsprc").alias("tdd_clsprc"),
            safe_double("cmpprevdd_prc").cast("double").alias("cmpprevdd_prc"),
            safe_double("fluc_rt").alias("fluc_rt"),
            safe_double("nav").alias("nav"),
            safe_double("tdd_opnprc").alias("tdd_opnprc"),
            safe_double("tdd_hgprc").alias("tdd_hgprc"),
            safe_double("tdd_lwprc").alias("tdd_lwprc"),
            safe_double("acc_trdvol").alias("acc_trdvol"),
            safe_double("acc_trdval").alias("acc_trdval"),
            safe_double("mktcap").alias("mktcap"),
            safe_double("invstasst_netasst_totamt").alias("invstast_netast_totamt"),
            safe_double("list_shrs").alias("list_shrs"),
            F.col("idx_ind_nm").cast("string").alias("idx_ind_nm"),
            safe_double("obj_stkprc_idx").alias("obj_stkprc_idx"),
            safe_double("cmpprevdd_idx").alias("cmpprevdd_idx"),
            safe_double("fluc_rt_idx").alias("fluc_rt_idx"),
            F.col("bas_dd_api").cast("string").alias("bas_dd_api"),
            F.col("_ingestion_ts"),
            F.col("_bronze_source"),
        )
        .filter(F.col("isu_cd").isNotNull() & F.col("bas_dd").isNotNull())
    )


CASTERS = {
    "customer_cash_portfolio": cast_customer_cash_portfolio,
    "customer_etf_portfolio": cast_customer_etf_portfolio,
    "customer_master": cast_customer_master,
    "etf_master": cast_etf_master,
    "etf_trades": cast_etf_trades,
}


def build_silver(spark: SparkSession, usernumber: str) -> None:
    create_silver_schema(spark, usernumber)

    for table_name in TABLES:
        bronze_df = spark.table(bronze_table_name(usernumber, table_name))
        transformed = CASTERS[table_name](bronze_df)
        deduped = dedup_by_keys_latest(transformed, PRIMARY_KEYS[table_name], order_col="load_ts")

        merge_insert_only_to_silver(
            spark=spark,
            usernumber=usernumber,
            table_name=table_name,
            source_df=deduped
        )

        print(f"[SILVER] loaded: {table_name}")