# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — Customers (Free Edition)
# MAGIC
# MAGIC **Prerequisite:** notebook `00_setup` must be executed first.
# MAGIC
# MAGIC Incremental ingestion of customer records via Auto Loader.
# MAGIC Raw data is preserved without any transformation.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("execution_date", "", "Date (yyyy-MM-dd) — empty = today")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

# Unity Catalog Volumes (replaces dbfs:/FileStore/ — disabled in Free Edition)
UC_CATALOG       = "main"
UC_VOLUME_SCHEMA = f"ecommerce_{ENV}"
VOLUME_BASE      = f"/Volumes/{UC_CATALOG}/{UC_VOLUME_SCHEMA}/landing"

LANDING_PATH    = f"{VOLUME_BASE}/customers"
CHECKPOINT_PATH = f"{VOLUME_BASE}/checkpoints/customers/bronze"
SCHEMA_PATH     = f"{VOLUME_BASE}/checkpoints/customers/schema"
BRONZE_TABLE    = f"{ENV}_bronze.customers"

print(f"ENV            : {ENV}")
print(f"execution_date : {execution_date}")
print(f"Landing        : {LANDING_PATH}")
print(f"Checkpoint     : {CHECKPOINT_PATH}")
print(f"Table          : {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## Auto Loader — incremental ingestion

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_bronze")

stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      SCHEMA_PATH)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes",    "true")
    # directory listing — compatible with UC Volumes in Free Edition
    .option("cloudFiles.useNotifications",    "false")
    .load(LANDING_PATH)
    # traceability metadata columns
    .withColumn("_source_file",    F.col("_metadata.file_path"))
    .withColumn("_ingested_at",    F.current_timestamp())
    .withColumn("_execution_date", F.lit(execution_date))
    .withColumn("_env",            F.lit(ENV))
    # partition columns based on ingestion date
    .withColumn("ingest_year",     F.year(F.current_date()))
    .withColumn("ingest_month",    F.month(F.current_date()))
)

query = (
    stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema",        "true")
    .outputMode("append")
    .partitionBy("ingest_year", "ingest_month")
    .trigger(availableNow=True)   # process all pending files and stop
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print("[OK] Stream completed")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

df    = spark.read.format("delta").table(BRONZE_TABLE)
count = df.count()

print(f"Table : {BRONZE_TABLE}")
print(f"Rows  : {count:,}")

print("\nRow distribution by segment:")
display(
    df.groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

dbutils.notebook.exit(f"success|rows={count}")
