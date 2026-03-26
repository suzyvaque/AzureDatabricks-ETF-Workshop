from pyspark.sql import SparkSession

from utilities.common_utils import gold_view_name, Env
from utilities.gold_utils import create_gold_schema


def build_gold_views(spark: SparkSession, usernumber: str) -> None:
    create_gold_schema(spark, usernumber)

    silver_schema = f"{Env.CATALOG}.sch_user{usernumber}_silver"

    spark.sql(f"""
    CREATE OR REPLACE VIEW {gold_view_name(usernumber, "vw_customer_portfolio_daily")} AS
    SELECT
        cm.bas_dd,
        cm.customer_id,
        cm.customer_grade,
        cm.risk_profile_level,
        cm.risk_profile_name,
        cp.cash_amt,
        ep.etf_code,
        em.isu_nm AS etf_name,
        em.idx_ind_nm,
        ep.qty,
        tr.tdd_clsprc,
        (ep.qty * tr.tdd_clsprc) AS etf_eval_amt,
        (COALESCE(cp.cash_amt, 0) + COALESCE(ep.qty * tr.tdd_clsprc, 0)) AS est_total_asset
    FROM {silver_schema}.customer_master cm
    LEFT JOIN {silver_schema}.customer_cash_portfolio cp
        ON cm.customer_id = cp.customer_id
       AND cm.bas_dd = cp.bas_dd
    LEFT JOIN {silver_schema}.customer_etf_portfolio ep
        ON cm.customer_id = ep.customer_id
       AND cm.bas_dd = ep.bas_dd
    LEFT JOIN {silver_schema}.etf_master em
        ON ep.etf_code = em.isu_cd
       AND ep.bas_dd = em.bas_dd
    LEFT JOIN {silver_schema}.etf_trades tr
        ON ep.etf_code = tr.isu_cd
       AND ep.bas_dd = tr.bas_dd
    """)

    spark.sql(f"""
    CREATE OR REPLACE VIEW {gold_view_name(usernumber, "vw_customer_asset_allocation_daily")} AS
    SELECT
        bas_dd,
        customer_id,
        customer_grade,
        risk_profile_name,
        SUM(COALESCE(cash_amt, 0)) AS cash_amt,
        SUM(COALESCE(etf_eval_amt, 0)) AS etf_eval_amt,
        SUM(COALESCE(est_total_asset, 0)) AS total_asset
    FROM {gold_view_name(usernumber, "vw_customer_portfolio_daily")}
    GROUP BY
        bas_dd,
        customer_id,
        customer_grade,
        risk_profile_name
    """)

    spark.sql(f"""
    CREATE OR REPLACE VIEW {gold_view_name(usernumber, "vw_etf_market_daily")} AS
    SELECT
        tr.bas_dd,
        tr.isu_cd,
        tr.isu_nm,
        tr.idx_ind_nm,
        tr.tdd_clsprc,
        tr.nav,
        tr.fluc_rt,
        tr.acc_trdvol,
        tr.acc_trdval,
        tr.mktcap
    FROM {silver_schema}.etf_trades tr
    """)

    print("[GOLD] views created")