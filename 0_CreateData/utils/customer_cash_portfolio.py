from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run(base_date: str, cfg: dict) -> None:
    spark = SparkSession.builder.getOrCreate()

    run_date = date.fromisoformat(base_date)
    yday = (run_date - timedelta(days=1)).isoformat()

    bronze_path = cfg["paths"]["customer_cash_portfolio"]
    customer_master_path = cfg["paths"]["customer_master"]

    seed = int(cfg.get("seed", 42))
    prob_cash_change = float(cfg.get("prob_cash_change", 0.35))
    max_change_ratio = float(cfg.get("max_change_ratio", 0.25))

    # Grade-based cash ranges (KRW)
    grade_ranges = [
        ("VVIP", 50_000_000, 800_000_000),
        ("VIP",  20_000_000, 300_000_000),
        ("GOLD",  5_000_000, 120_000_000),
        ("일반",     100_000,  30_000_000),
    ]

    # Read customer master (keep it distributed)
    cust = (
        spark.read.format("delta").load(customer_master_path)
        .select(
            F.col("customer_id"),
            F.coalesce(F.col("customer_grade"), F.lit("일반")).alias("customer_grade")
        )
        .cache()
    )

    # Build a mapping DF for grade -> min/max cash
    ranges_df = spark.createDataFrame(grade_ranges, ["customer_grade", "min_cash", "max_cash"])

    cust_with_range = (
        cust.join(ranges_df, on="customer_grade", how="left")
            .withColumn("min_cash", F.coalesce(F.col("min_cash"), F.lit(100_000)))
            .withColumn("max_cash", F.coalesce(F.col("max_cash"), F.lit(30_000_000)))
    )

    # Load yesterday snapshot (IMPORTANT: use bas_dd, not base_date)
    try:
        y = (
            spark.read.format("delta").load(bronze_path)
            .where(F.col("bas_dd") == F.lit(yday))
            .select("customer_id", "cash_amt")
        )
        has_yesterday = y.limit(1).count() > 0  # triggers only a tiny job
    except Exception:
        y = None
        has_yesterday = False

    if not has_yesterday:
        # Day-0 initializer: lognormal-like via exp(randn)
        # Note: exact distribution match is not critical for synthetic data; keep it stable & fast.
        df = (
            cust_with_range
            .withColumn("bas_dd", F.lit(base_date))
            .withColumn("source_sys", F.lit("local"))
            .withColumn("load_ts", F.current_timestamp())
            # Generate a skewed positive value and clip to [min_cash, max_cash]
            .withColumn(
                "cash_raw",
                F.exp(F.randn(seed) * F.lit(0.8) + F.log(F.greatest(F.col("min_cash").cast("double"), F.lit(1.0))))
            )
            .withColumn(
                "cash_amt",
                F.least(F.col("max_cash"), F.greatest(F.col("min_cash"), F.col("cash_raw").cast("long")))
            )
            .drop("cash_raw")
            .select("customer_id", "bas_dd", "load_ts", "source_sys", "cash_amt")
        )
    else:
        # Daily update: join yesterday cash with current grade range
        base = (
            y.join(cust_with_range, on="customer_id", how="inner")
        )

        # Decide whether to change cash for each row
        # delta = cash_amt * uniform(-max_change_ratio, +max_change_ratio)
        u = F.rand(seed)
        change_flag = (u < F.lit(prob_cash_change))

        # Uniform in [-max_change_ratio, +max_change_ratio]
        u2 = F.rand(seed + 1) * 2 - 1

        delta = (F.col("cash_amt").cast("double") * u2 * F.lit(max_change_ratio)).cast("long")

        new_cash = F.when(change_flag, F.col("cash_amt") + delta).otherwise(F.col("cash_amt"))

        df = (
            base
            .withColumn("cash_amt_new", new_cash)
            # Clip to grade-based range
            .withColumn(
                "cash_amt",
                F.least(F.col("max_cash"), F.greatest(F.col("min_cash"), F.col("cash_amt_new"))).cast("long")
            )
            .withColumn("bas_dd", F.lit(base_date))
            .withColumn("source_sys", F.lit("local"))
            .withColumn("load_ts", F.current_timestamp())
            .select("customer_id", "bas_dd", "load_ts", "source_sys", "cash_amt")
        )

    # Reduce small files: ensure reasonable partitioning before write
    out = df.repartition(F.col("bas_dd"))

    (out.write
        .format("delta")
        .mode("append")
        .partitionBy("bas_dd")
        .save(bronze_path))
