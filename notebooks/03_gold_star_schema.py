# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: star schema for reporting
# MAGIC
# MAGIC One fact table (one row per collision) and three dimension tables:
# MAGIC
# MAGIC | table | grain | used for |
# MAGIC |---|---|---|
# MAGIC | `fact_collision` | one collision | counts, casualties, vehicles |
# MAGIC | `dim_date` | one calendar day | year, month, weekday, weekend |
# MAGIC | `dim_severity` | one severity level | Fatal / Serious / Slight, killed-or-seriously-injured flag |
# MAGIC | `dim_area` | one small area (LSOA) | roll-up to MSOA and its name |
# MAGIC
# MAGIC The area lookup (`lsoa_msoa_lookup.csv`, ONS 2021 LSOA to MSOA) is uploaded to the
# MAGIC `raw` volume. The notebook ends with referential-integrity checks: every fact row should
# MAGIC find its date and severity, and the share of collisions that match an area is reported.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "road_safety"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
silver = spark.table(T("silver_collisions"))

# COMMAND ----------

# dim_date: one row per day across the full range of the data
bounds = silver.agg(F.min("collision_date").alias("lo"), F.max("collision_date").alias("hi")).first()
dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{bounds.lo}', DATE'{bounds.hi}')) AS date")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMM"))
    .withColumn("weekday", F.date_format("date", "EEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
)
dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("dim_date"))

# dim_severity: small fixed list
dim_severity = spark.createDataFrame(
    [(1, "Fatal", True), (2, "Serious", True), (3, "Slight", False)],
    "severity_code int, severity string, is_ksi boolean",
)
dim_severity.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("dim_severity"))

# dim_area: ONS LSOA 2021 -> MSOA 2021 lookup
dim_area = (
    spark.read.option("header", True).csv(f"{VOL}/lsoa_msoa_lookup.csv")
    .select(
        F.col("LSOA21CD").alias("lsoa_code"),
        F.col("MSOA21CD").alias("msoa_code"),
        F.col("MSOA21NM").alias("msoa_name"),
    )
    .dropDuplicates(["lsoa_code"])
)
dim_area.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("dim_area"))

# COMMAND ----------

fact = silver.select(
    "collision_index",
    F.date_format("collision_date", "yyyyMMdd").cast("int").alias("date_key"),
    "severity_code",
    F.col("lsoa21_code").alias("lsoa_code"),  # standardised to 2021 boundaries in silver
    "number_of_vehicles",
    "number_of_casualties",
    "speed_limit",
    "urban_or_rural_code",
    "light_code",
    "weather_code",
    "police_force_code",
    "la_code",
)
fact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("fact_collision"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential-integrity checks

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          count(*)                                                AS fact_rows,
          count_if(d.date_key IS NULL)                            AS rows_without_date,
          count_if(s.severity_code IS NULL)                       AS rows_without_severity,
          count_if(a.lsoa_code IS NULL)                           AS rows_without_area,
          round(100 * count_if(a.lsoa_code IS NOT NULL) / count(*), 2) AS pct_matched_to_area
        FROM {T('fact_collision')} f
        LEFT JOIN {T('dim_date')} d     ON f.date_key = d.date_key
        LEFT JOIN {T('dim_severity')} s ON f.severity_code = s.severity_code
        LEFT JOIN {T('dim_area')} a     ON f.lsoa_code = a.lsoa_code
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold summary: collisions by area and year (feeds the dashboard)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {T('gold_msoa_year')} AS
    SELECT a.msoa_code, a.msoa_name, d.year,
           count(*)                              AS collisions,
           count_if(s.is_ksi)                    AS ksi_collisions,
           sum(f.number_of_casualties)           AS casualties
    FROM {T('fact_collision')} f
    JOIN {T('dim_date')} d     ON f.date_key = d.date_key
    JOIN {T('dim_severity')} s ON f.severity_code = s.severity_code
    JOIN {T('dim_area')} a     ON f.lsoa_code = a.lsoa_code
    GROUP BY a.msoa_code, a.msoa_name, d.year
    """
)
display(spark.table(T("gold_msoa_year")).orderBy(F.desc("ksi_collisions")).limit(20))
