from pyspark.sql import Window
from pyspark.sql import functions as F


def _get_widget_or_env(name: str, env_var: str) -> str:
    value = None
    if "dbutils" in globals():
        try:
            value = dbutils.widgets.get(name).strip()
        except Exception:
            value = None
    if not value:
        import os

        value = os.getenv(env_var, "").strip()
    if not value:
        raise ValueError(f"Missing configuration for {name} / {env_var}")
    return value


def main() -> None:
    catalog = _get_widget_or_env("catalog", "CATALOG")
    schema_bronze = _get_widget_or_env("schema_bronze", "SCHEMA_BRONZE")
    schema_silver = _get_widget_or_env("schema_silver", "SCHEMA_SILVER")

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema_silver}")

    bronze_table = f"{catalog}.{schema_bronze}.claims"
    silver_table = f"{catalog}.{schema_silver}.claims_dedup"

    claims_df = spark.table(bronze_table)

    stats_before = (
        claims_df.agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("claim_no").alias("unique_claims"),
        )
        .collect()[0]
    )
    duplicate_rows = stats_before["total_rows"] - stats_before["unique_claims"]

    window_spec = Window.partitionBy("claim_no").orderBy(F.col("claim_date").desc_nulls_last())

    dedup_df = (
        claims_df.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .cache()
    )

    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    dedup_df.write.mode("overwrite").saveAsTable(silver_table)

    stats_after = (
        dedup_df.agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("claim_no").alias("unique_claims"),
        )
        .collect()[0]
    )

    metrics = {
        "total_rows_before": stats_before["total_rows"],
        "unique_claims_before": stats_before["unique_claims"],
        "duplicate_rows_removed": duplicate_rows,
        "total_rows_after": stats_after["total_rows"],
        "unique_claims_after": stats_after["unique_claims"],
    }

    if "dbutils" in globals():
        try:
            for key, value in metrics.items():
                dbutils.jobs.taskValues.set(key=key, value=value)
        except Exception:
            pass

    for key, value in metrics.items():
        print(f"{key}: {value}")

    dedup_df.unpersist()


if __name__ == "__main__":
    main()

