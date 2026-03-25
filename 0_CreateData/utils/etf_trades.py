from utils.krx_etf_api import call_krx_etf_by_date
import pandas as pd
from datetime import datetime, timezone, timedelta

def _now_kst_iso() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="seconds")

def _normalize_columns_lower(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize pandas columns to lower-case for consistency."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df

def generate_etf_trades_daily(df_api: pd.DataFrame | None, bas_dd_api: str) -> tuple[pd.DataFrame, str]:
    """
    Generate daily ETF trades snapshot from KRX API response.
    Returns:
      (pandas_df, source_sys)
    - Keeps API columns (lowercased) and adds metadata columns.
    """
    used_api = df_api is not None and len(df_api) > 0
    source_sys = "krx_api" if used_api else "local_fallback"

    if not used_api:
        return pd.DataFrame(), source_sys

    df = _normalize_columns_lower(df_api)
    df["bas_dd_api"] = bas_dd_api
    df["load_ts"] = _now_kst_iso()
    df["source_sys"] = source_sys
    return df, source_sys

def run(base_date: str, cfg: dict) -> None:
    """
    Daily ETF trades load (idempotent per bas_dd).
    Writes Delta partitioned by bas_dd and overwrites that partition on rerun.
    """
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()

    bas_dd_api = base_date.replace("-", "")  # yyyymmdd for API
    bronze_path = cfg["paths"]["etf_trades"]
    auth_key = cfg.get("krx_auth_key", "")

    df_api = None
    if auth_key:
        df_api = call_krx_etf_by_date(bas_dd=bas_dd_api, auth_key=auth_key)

    df_trades_pd, source_sys = generate_etf_trades_daily(df_api, bas_dd_api)

    if df_trades_pd is None or len(df_trades_pd) == 0:
        # Fallback: minimal schema aligned with downstream usage
        codes = cfg.get("etf_codes") or [f"ETF{str(i).zfill(4)}" for i in range(1, 21)]
        df_trades_pd = pd.DataFrame(
            {
                "bas_dd_api": [bas_dd_api] * len(codes),
                "isu_cd": codes,
            }
        )
        source_sys = "local_fallback"

    # Ensure columns are normalized
    df_trades_pd = _normalize_columns_lower(df_trades_pd)

    sdf = spark.createDataFrame(df_trades_pd)

    # Standardize required columns
    out = (
        sdf.withColumn("bas_dd", F.lit(base_date))              # partition key (yyyy-mm-dd)
           .withColumn("load_ts", F.current_timestamp())        # authoritative load timestamp
           .withColumn("source_sys", F.lit(source_sys))
    )

    # Idempotent write per day: overwrite only that partition
    # NOTE: This requires Delta and works well for daily snapshots.
    (out.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"bas_dd = '{base_date}'")
        .partitionBy("bas_dd")
        .save(bronze_path))
