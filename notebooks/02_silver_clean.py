# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: typed, validated collisions
# MAGIC
# MAGIC Converts the bronze text columns to proper types, decodes collision severity, and runs
# MAGIC data-quality rules. Every rule's failure count is written to `silver_dq_log`, so a
# MAGIC change in the source data shows up as a number rather than as a silently wrong result.
# MAGIC Rows that fail a hard rule (duplicate ID, unreadable date, invalid severity) are excluded.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG, SCHEMA = "workspace", "road_safety"
bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_collisions")

typed = (
    bronze.select(
        F.col("collision_index"),
        F.col("collision_year").cast("int").alias("collision_year"),
        F.to_date("date", "dd/MM/yyyy").alias("collision_date"),
        F.col("time").alias("collision_time"),
        F.col("collision_severity").cast("int").alias("severity_code"),
        F.col("number_of_vehicles").cast("int").alias("number_of_vehicles"),
        F.col("number_of_casualties").cast("int").alias("number_of_casualties"),
        F.col("speed_limit").cast("int").alias("speed_limit"),
        F.col("urban_or_rural_area").cast("int").alias("urban_or_rural_code"),
        F.col("light_conditions").cast("int").alias("light_code"),
        F.col("weather_conditions").cast("int").alias("weather_code"),
        F.col("police_force").cast("int").alias("police_force_code"),
        F.col("local_authority_ons_district").alias("la_code"),
        F.col("lsoa_of_accident_location").alias("lsoa_code"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("_source_file"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardise small-area codes to the 2021 boundaries
# MAGIC
# MAGIC Some collisions in the 2021-2023 files carry 2011 LSOA codes, which no longer exist in the
# MAGIC 2021 boundary set. Codes already valid in 2021 are kept; 2011 codes are converted with the
# MAGIC ONS best-fit lookup, and the row is flagged so the conversion stays visible.

# COMMAND ----------

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
lsoa21 = (
    spark.read.option("header", True).csv(f"{VOL}/lsoa_msoa_lookup.csv")
    .select(F.col("LSOA21CD").alias("lsoa21_valid")).distinct()
)
l11 = spark.read.option("header", True).csv(f"{VOL}/lsoa11_lsoa21_lookup.csv")
lsoa11_to_21 = l11.select(F.col("LSOA11CD").alias("lsoa11"), F.col("LSOA21CD").alias("lsoa21_from_2011"))

typed = (
    typed.join(F.broadcast(lsoa21), typed.lsoa_code == lsoa21.lsoa21_valid, "left")
    .join(F.broadcast(lsoa11_to_21), typed.lsoa_code == lsoa11_to_21.lsoa11, "left")
    .withColumn("lsoa_converted_from_2011", F.col("lsoa21_valid").isNull() & F.col("lsoa21_from_2011").isNotNull())
    .withColumn("lsoa21_code", F.coalesce("lsoa21_valid", "lsoa21_from_2011"))
    .drop("lsoa21_valid", "lsoa11", "lsoa21_from_2011")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data-quality rules

# COMMAND ----------

dup_window = Window.partitionBy("collision_index")
checked = (
    typed.withColumn("_dup_count", F.count("*").over(dup_window))
    .withColumn("dq_duplicate_id", F.col("_dup_count") > 1)
    .withColumn("dq_bad_date", F.col("collision_date").isNull())
    .withColumn("dq_bad_severity", ~F.col("severity_code").isin(1, 2, 3) | F.col("severity_code").isNull())
    .withColumn(
        "dq_year_mismatch",
        F.col("collision_date").isNotNull() & (F.year("collision_date") != F.col("collision_year")),
    )
    .withColumn("dq_missing_coords", F.col("longitude").isNull() | F.col("latitude").isNull())
    .withColumn(
        "dq_coords_outside_gb",
        ~F.col("dq_missing_coords")
        & ~(F.col("latitude").between(49.8, 61.0) & F.col("longitude").between(-8.7, 2.0)),
    )
    .withColumn(
        "dq_missing_lsoa",
        F.col("lsoa_code").isNull() | (F.col("lsoa_code") == "-1") | (F.col("lsoa_code") == ""),
    )
    .withColumn("dq_lsoa_2011_code", F.col("lsoa_converted_from_2011"))
    .withColumn("dq_lsoa_unresolved", ~F.col("dq_missing_lsoa") & F.col("lsoa21_code").isNull())
    .drop("_dup_count")
)

rules = {
    "duplicate_id": "hard",
    "bad_date": "hard",
    "bad_severity": "hard",
    "year_mismatch": "soft",
    "missing_coords": "soft",
    "coords_outside_gb": "soft",
    "missing_lsoa": "soft",
    "lsoa_2011_code": "soft",
    "lsoa_unresolved": "soft",
}
total = checked.count()
log = [
    (rule, level, checked.filter(F.col(f"dq_{rule}")).count(), total)
    for rule, level in rules.items()
]
dq = (
    spark.createDataFrame(log, "rule string, level string, failing_rows long, total_rows long")
    .withColumn("pct_failing", F.round(100 * F.col("failing_rows") / F.col("total_rows"), 3))
    .withColumn("checked_at", F.current_timestamp())
)
dq.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_dq_log")
display(dq)

# COMMAND ----------

hard_fail = F.col("dq_duplicate_id") | F.col("dq_bad_date") | F.col("dq_bad_severity")
silver = (
    checked.filter(~hard_fail)
    .drop("dq_duplicate_id", "dq_bad_date", "dq_bad_severity")
    .withColumn(
        "severity",
        F.when(F.col("severity_code") == 1, "Fatal")
        .when(F.col("severity_code") == 2, "Serious")
        .otherwise("Slight"),
    )
)
(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_collisions")
)
print(f"silver rows: {silver.count():,} of {total:,} bronze rows")
