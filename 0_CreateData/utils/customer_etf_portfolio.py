from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run(base_date: str, cfg: dict) -> None:
    """
    Customer ETF portfolio daily generator (Spark-native).
    Preserves:
      - yesterday-based update
      - grade-based ETF count & qty range
      - risk-based probability of buying a new ETF
      - probabilistic qty change on existing holdings
      - enforce min ETF count by adding new ETFs (excluding already owned)
    """

    spark = SparkSession.builder.getOrCreate()

    run_date = date.fromisoformat(base_date)
    yday = (run_date - timedelta(days=1)).isoformat()

    bronze_path = cfg["paths"]["customer_etf_portfolio"]
    customer_master_path = cfg["paths"]["customer_master"]
    etf_master_path = cfg["paths"]["etf_master"]

    seed = int(cfg.get("seed", 42))
    prob_qty_change = float(cfg.get("prob_qty_change", 0.30))
    source_sys = cfg.get("source_sys", "local")

    # -----------------------------
    # Static configs (same intent as your pandas version)
    # -----------------------------
    grade_etf_range = cfg.get("grade_etf_range") or {
        "VVIP": (7, 10),
        "VIP": (5, 7),
        "GOLD": (3, 5),
        "일반": (1, 3),
    }
    grade_qty_range = cfg.get("grade_qty_range") or {
        "VVIP": (50, 300),
        "VIP": (30, 200),
        "GOLD": (10, 120),
        "일반": (1, 50),
    }
    risk_new_etf_prob = cfg.get("risk_new_etf_prob") or {
        1: 0.30,
        2: 0.22,
        3: 0.15,
        4: 0.12,
        5: 0.10,
    }

    grade_rng_df = spark.createDataFrame(
        [(k, int(v[0]), int(v[1])) for k, v in grade_etf_range.items()],
        ["customer_grade", "min_etf", "max_etf"],
    )
    qty_rng_df = spark.createDataFrame(
        [(k, int(v[0]), int(v[1])) for k, v in grade_qty_range.items()],
        ["customer_grade", "qty_min", "qty_max"],
    )
    risk_prob_df = spark.createDataFrame(
        [(int(k), float(v)) for k, v in risk_new_etf_prob.items()],
        ["risk_profile_level", "prob_new_etf"],
    )

    # -----------------------------
    # Customer master (distributed)
    # -----------------------------
    cust = (
        spark.read.format("delta").load(customer_master_path)
        .select(
            "customer_id",
            F.coalesce(F.col("customer_grade"), F.lit("일반")).alias("customer_grade"),
            F.coalesce(F.col("risk_profile_level").cast("int"), F.lit(3)).alias("risk_profile_level"),
        )
        .join(grade_rng_df, on="customer_grade", how="left")
        .join(qty_rng_df, on="customer_grade", how="left")
        .join(risk_prob_df, on="risk_profile_level", how="left")
        .withColumn("min_etf", F.coalesce(F.col("min_etf"), F.lit(1)))
        .withColumn("max_etf", F.coalesce(F.col("max_etf"), F.lit(3)))
        .withColumn("qty_min", F.coalesce(F.col("qty_min"), F.lit(1)))
        .withColumn("qty_max", F.coalesce(F.col("qty_max"), F.lit(50)))
        .withColumn("prob_new_etf", F.coalesce(F.col("prob_new_etf"), F.lit(0.15)))
        .cache()
    )

    # -----------------------------
    # ETF universe (distributed)
    # -----------------------------
    try:
        etf_universe = (
            spark.read.format("delta").load(etf_master_path)
            .select(F.col("isu_cd").alias("etf_code"))
            .where(F.col("etf_code").isNotNull())
            .distinct()
            .limit(5000)
        )
    except Exception:
        # Fallback to cfg list
        fallback = cfg.get("etf_codes") or [f"ETF{str(i).zfill(4)}" for i in range(1, 21)]
        etf_universe = spark.createDataFrame([(x,) for x in fallback], ["etf_code"])

    # -----------------------------
    # Load yesterday portfolio (IMPORTANT: bas_dd, not base_date)
    # -----------------------------
    try:
        y = (
            spark.read.format("delta").load(bronze_path)
            .where(F.col("bas_dd") == F.lit(yday))
            .select("customer_id", "etf_code", "qty")
        )
        has_yesterday = y.limit(1).count() > 0
    except Exception:
        y = None
        has_yesterday = False

    # -----------------------------
    # Day-0: create initial holdings per customer (Spark-native)
    # -----------------------------
    if not has_yesterday:
        # Choose k ETFs per customer where k ~ Uniform[min_etf, max_etf] (matches pandas intent)
        cust_k = cust.withColumn(
            "k",
            (F.floor(F.rand(seed) * (F.col("max_etf") - F.col("min_etf") + F.lit(1))) + F.col("min_etf")).cast("int"),
        )

        # Cross join each customer with ETF universe and randomly rank ETFs per customer
        w = Window.partitionBy("customer_id").orderBy(F.rand(seed + 1))
        ranked = (
            cust_k.select("customer_id", "customer_grade", "qty_min", "qty_max", "k")
            .crossJoin(etf_universe)
            .withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") <= F.col("k"))
        )

        # Assign initial qty within [qty_min, qty_max]
        df = (
            ranked
            .withColumn(
                "qty",
                (F.floor(F.rand(seed + 2) * (F.col("qty_max") - F.col("qty_min") + F.lit(1))) + F.col("qty_min")).cast("int"),
            )
            .select("customer_id", "etf_code", "qty")
        )

    # -----------------------------
    # Daily: update qty + add new ETFs (Spark-native)
    # -----------------------------
    else:
        # Join yesterday with customer attributes/ranges
        base = (
            y.join(
                cust.select("customer_id", "min_etf", "max_etf", "qty_min", "qty_max", "prob_new_etf"),
                on="customer_id",
                how="inner",
            )
        )

        # 1) Existing ETF quantity change with prob_qty_change
        change_flag = (F.rand(seed) < F.lit(prob_qty_change))
        # delta ratio ~ Uniform(-0.3, +0.3) as in your pandas version
        u = (F.rand(seed + 1) * 2 - 1) * F.lit(0.3)
        delta = (F.col("qty").cast("double") * u).cast("int")
        qty_new = F.when(change_flag, F.col("qty") + delta).otherwise(F.col("qty"))
        qty_new = F.least(F.col("qty_max"), F.greatest(F.col("qty_min"), qty_new)).cast("int")

        updated = (
            base.select("customer_id", "etf_code", qty_new.alias("qty"), "min_etf", "max_etf", "qty_min", "qty_max", "prob_new_etf")
        )

        # Current holding count per customer
        cnt = updated.groupBy("customer_id").agg(F.count("*").alias("hold_cnt"))

        # Decide whether to buy ONE additional new ETF (risk-based) if hold_cnt < max_etf
        cust_add = (
            updated.select("customer_id", "max_etf", "qty_min", "qty_max", "prob_new_etf").distinct()
            .join(cnt, on="customer_id", how="left")
            .withColumn("hold_cnt", F.coalesce(F.col("hold_cnt"), F.lit(0)))
            .withColumn(
                "add_one",
                (F.col("hold_cnt") < F.col("max_etf")) & (F.rand(seed + 2) < F.col("prob_new_etf")),
            )
        )

        # Compute shortage to enforce min_etf after optional +1
        cust_short = (
            cust_add
            .withColumn("hold_after_add", F.col("hold_cnt") + F.when(F.col("add_one"), F.lit(1)).otherwise(F.lit(0)))
            .join(cust.select("customer_id", "min_etf"), on="customer_id", how="left")
            .withColumn("min_etf", F.coalesce(F.col("min_etf"), F.lit(1)))
            .withColumn("shortage", F.greatest(F.col("min_etf") - F.col("hold_after_add"), F.lit(0)).cast("int"))
            .withColumn("n_new", (F.col("shortage") + F.when(F.col("add_one"), F.lit(1)).otherwise(F.lit(0))).cast("int"))
            .select("customer_id", "n_new", "qty_min", "qty_max")
        )

        # Create candidate new ETFs by excluding already owned
        owned = updated.select("customer_id", "etf_code").distinct()

        candidates = (
            cust_short.where(F.col("n_new") > 0)
            .join(etf_universe, how="cross")
            .join(owned, on=["customer_id", "etf_code"], how="left_anti")
            .select("customer_id", "etf_code", "n_new", "qty_min", "qty_max")
        )


        # Randomly pick n_new ETFs per customer
        w2 = Window.partitionBy("customer_id").orderBy(F.rand(seed + 3))
        picked = (
            candidates
            .withColumn("rn", F.row_number().over(w2))
            .where(F.col("rn") <= F.col("n_new"))
        )


        # Assign qty for new ETFs
        new_rows = (
            picked
            .withColumn(
                "qty",
                (F.floor(F.rand(seed + 4) * (F.col("qty_max") - F.col("qty_min") + F.lit(1))) + F.col("qty_min")).cast("int"),
            )
            .select("customer_id", "etf_code", "qty")
        )

        # Union updated + new additions
        df = updated.select("customer_id", "etf_code", "qty").unionByName(new_rows)

    # -----------------------------
    # Add metadata columns and write
    # -----------------------------
    out = (
        df.withColumn("bas_dd", F.lit(base_date))
          .withColumn("load_ts", F.current_timestamp())
          .withColumn("source_sys", F.lit(source_sys))
          # Optional: reduce small files; tune partitions if needed
          .repartition(F.col("bas_dd"))
          .select("customer_id", "bas_dd", "load_ts", "source_sys", "etf_code", "qty")
    )

    (out.write
        .format("delta")
        .mode("append")
        .partitionBy("bas_dd")
        .save(bronze_path))
