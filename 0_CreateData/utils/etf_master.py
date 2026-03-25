from utils.krx_etf_api import call_krx_etf_by_date
import pandas as pd

MASTER_COLUMNS = {
    "BAS_DD": "bas_dd_api",     # keep API date separate (yyyymmdd)
    "ISU_CD": "isu_cd",
    "ISU_NM": "isu_nm",
    "IDX_IND_NM": "idx_ind_nm",
    "LIST_SHRS": "list_shrs",
}

FINAL_COLUMNS = ["bas_dd", "isu_cd", "isu_nm", "idx_ind_nm", "list_shrs"]

def extract_etf_master(df_api: pd.DataFrame) -> pd.DataFrame:
    """Extract ETF master table from KRX API response (pandas)."""
    df = (
        df_api[list(MASTER_COLUMNS.keys())]
        .rename(columns=MASTER_COLUMNS)
        .drop_duplicates(subset=["isu_cd"])
        .reset_index(drop=True)
    )
    return df

def _fallback_master(etf_codes: list[str]) -> pd.DataFrame:
    """Create a schema-consistent fallback master (pandas)."""
    return pd.DataFrame(
        {
            "isu_cd": etf_codes,
            "isu_nm": [None] * len(etf_codes),
            "idx_ind_nm": [None] * len(etf_codes),
            "list_shrs": [None] * len(etf_codes),
        }
    )

def run(base_date: str, cfg: dict) -> None:
    """
    Monthly ETF master refresh (overwrite).
    - Try KRX API (if auth key exists)
    - Fallback to synthetic ETF codes
    Ensures consistent schema across months.
    """
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()

    bas_dd_api = base_date.replace("-", "")  # for KRX API request only (yyyymmdd)
    bronze_path = cfg["paths"]["etf_master"]
    auth_key = cfg.get("krx_auth_key", "")

    df_api = None
    if auth_key:
        df_api = call_krx_etf_by_date(bas_dd=bas_dd_api, auth_key=auth_key)

    used_api = df_api is not None and len(df_api) > 0
    source_sys = "krx_api" if used_api else "local_fallback"

    if used_api:
        df_master_pd = extract_etf_master(df_api)  # has: bas_dd_api, isu_cd, ...
    else:
        codes = cfg.get("etf_codes") or [f"ETF{str(i).zfill(4)}" for i in range(1, 21)]
        df_master_pd = _fallback_master(list(codes))
        df_master_pd["bas_dd_api"] = bas_dd_api

    # Create Spark DF and standardize final schema
    sdf = spark.createDataFrame(df_master_pd)

    out = (
        sdf.withColumn("bas_dd", F.lit(base_date))          # normalized (yyyy-mm-dd)
           .withColumn("load_ts", F.current_timestamp())
           .withColumn("source_sys", F.lit(source_sys))
           # Ensure required columns exist even if API schema changes
           .withColumn("isu_nm", F.col("isu_nm").cast("string"))
           .withColumn("idx_ind_nm", F.col("idx_ind_nm").cast("string"))
           .withColumn("list_shrs", F.col("list_shrs").cast("string"))
           .select(*FINAL_COLUMNS, "load_ts", "source_sys")
    )

    (out.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path))
