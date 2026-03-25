from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run(base_date: str, cfg: dict) -> None:
    """
    Monthly customer master refresh (overwrite) - Spark-native.
    Avoids pandas/faker loops to prevent driver bottlenecks.
    """

    spark = SparkSession.builder.getOrCreate()

    run_date = date.fromisoformat(base_date)
    n = int(cfg.get("customer_n", 500000))
    seed = int(cfg.get("seed", 42))
    bronze_path = cfg["paths"]["customer_master"]

    # Risk profile mapping
    risk_map = [
        (1, "공격투자형"),
        (2, "적극투자형"),
        (3, "위험중립형"),
        (4, "안정추구형"),
        (5, "안정형"),
    ]
    risk_df = spark.createDataFrame(risk_map, ["risk_profile_level", "risk_profile_name"])

    # Generate n customers distributed using Spark range
    df = (
        spark.range(1, n + 1)
        .withColumn("customer_id", F.format_string("C%06d", F.col("id")))
        .withColumn("bas_dd", F.lit(base_date))
        .withColumn("load_ts", F.current_timestamp())
        .withColumn("source_sys", F.lit("local"))
        # Birth year: [1950, 1990]
        .withColumn("birth_year", (F.floor(F.rand(seed + 1) * 41) + 1950).cast("int"))
        # Grade: age-weighted random (older → lower score → higher grade)
        .withColumn("_grade_score",
            F.rand(seed) * (F.lit(0.3) + F.lit(0.7) * (F.col("birth_year") - 1950) / 40.0))
        .withColumn("customer_grade",
            F.when(F.col("_grade_score") < 0.05, F.lit("VVIP"))
             .when(F.col("_grade_score") < 0.20, F.lit("VIP"))
             .when(F.col("_grade_score") < 0.50, F.lit("GOLD"))
             .otherwise(F.lit("일반")))
        .drop("_grade_score")
        # Join date: between -10y and -1d (approximate, good enough for synthetic)
        .withColumn("join_date", F.date_sub(F.lit(base_date), (F.floor(F.rand(seed + 3) * 3650) + 1).cast("int")))
        # Risk level based on customer grade
        .withColumn("_risk_rand", F.rand(seed + 2))
        .withColumn("risk_profile_level",
            F.when(F.col("customer_grade").isin("VVIP", "VIP"),
                   F.when(F.col("_risk_rand") < 0.5, F.lit(1)).otherwise(F.lit(2)))
             .when(F.col("customer_grade") == "GOLD",
                   F.when(F.col("_risk_rand") < 0.5, F.lit(2)).otherwise(F.lit(4)))
             .otherwise(
                   F.when(F.col("_risk_rand") < 0.5, F.lit(3)).otherwise(F.lit(5))))
        .drop("_risk_rand")
        .join(risk_df, on="risk_profile_level", how="left")
        .select(
            "bas_dd",
            "customer_id",
            "birth_year",
            "risk_profile_level",
            "risk_profile_name",
            "join_date",
            "customer_grade",
            "load_ts",
            "source_sys",
        )
    )

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path))
