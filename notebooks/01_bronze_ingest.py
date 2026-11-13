# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: ingest raw STATS19 collision files
# MAGIC
# MAGIC Downloads the Department for Transport STATS19 collision files for 2021-2024 into a
# MAGIC Unity Catalog volume and loads them **unchanged** (every column kept as text) into a
# MAGIC bronze Delta table, with the source file and load time recorded on every row.
# MAGIC Nothing is cleaned here, so any later step can be rerun from the original data.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "road_safety"
VOLUME = "raw"
YEARS = [2021, 2022, 2023, 2024]
URL = (
    "https://data.dft.gov.uk/road-accidents-safety-data/"
    "dft-road-casualty-statistics-collision-{year}.csv"
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

import os
import urllib.request

for year in YEARS:
    dest = f"{VOL}/stats19_collision_{year}.csv"
    if os.path.exists(dest):
        print("already present:", dest)
        continue
    try:
        urllib.request.urlretrieve(URL.format(year=year), dest)
        print("downloaded:", dest)
    except Exception as e:  # e.g. no outbound internet on this compute
        print(f"download failed for {year} ({e}); upload the file to {VOL} by hand")

# ONS best-fit lookup from 2011 to 2021 small areas (LSOA), used in silver.
# Read page by page from the ONS Open Geography API (1,000 rows per request).
import csv
import json

LSOA_API = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "LSOA11_LSOA21_LAD22_EW_LU_v2/FeatureServer/0/query"
    "?where=1%3D1&outFields=LSOA11CD,LSOA21CD&returnGeometry=false"
    "&orderByFields=ObjectId&resultRecordCount=1000&f=json&resultOffset={offset}"
)
dest = f"{VOL}/lsoa11_lsoa21_lookup.csv"
rows, offset = [], 0
while True:
    with urllib.request.urlopen(LSOA_API.format(offset=offset)) as resp:
        page = json.load(resp)
    rows += [f["attributes"] for f in page["features"]]
    if not page.get("exceededTransferLimit"):
        break
    offset += len(page["features"])
with open(dest, "w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=["LSOA11CD", "LSOA21CD"])
    writer.writeheader()
    writer.writerows(rows)
print(f"lookup rows written: {len(rows):,}")

# COMMAND ----------

from pyspark.sql import functions as F

raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)  # bronze keeps everything as text
    .csv(f"{VOL}/stats19_collision_*.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    raw.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_collisions")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: one row count per source file

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT regexp_extract(_source_file, 'collision_(\\\\d{{4}})', 1) AS year,
               count(*) AS rows_loaded
        FROM {CATALOG}.{SCHEMA}.bronze_collisions
        GROUP BY 1 ORDER BY 1
        """
    )
)
