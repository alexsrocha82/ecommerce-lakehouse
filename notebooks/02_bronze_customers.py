# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — Customers (Free Edition)
# MAGIC
# MAGIC **Pré-requisito:** notebook `00_setup` executado.
# MAGIC
# MAGIC Ingestão incremental de clientes via Auto Loader.
# MAGIC Dado bruto preservado sem transformação.

# COMMAND ----------

dbutils.widgets.text("env",            "dev", "Ambiente")
dbutils.widgets.text("execution_date", "",    "Data (yyyy-MM-dd)")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

BASE_PATH       = f"dbfs:/FileStore/ecommerce/{ENV}"
LANDING_PATH    = f"{BASE_PATH}/landing/customers"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/customers/bronze"
SCHEMA_PATH     = f"{BASE_PATH}/checkpoints/customers/schema"
BRONZE_TABLE    = f"{ENV}_bronze.customers"

print(f"ENV          : {ENV}")
print(f"Landing      : {LANDING_PATH}")
print(f"Tabela       : {BRONZE_TABLE}")

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
    .option("cloudFiles.useNotifications",    "false")
    .load(LANDING_PATH)
    .withColumn("_source_file",    F.input_file_name())
    .withColumn("_ingested_at",    F.current_timestamp())
    .withColumn("_execution_date", F.lit(execution_date))
    .withColumn("_env",            F.lit(ENV))
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
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

df    = spark.read.format("delta").table(BRONZE_TABLE)
count = df.count()

print(f"Tabela : {BRONZE_TABLE}")
print(f"Linhas : {count:,}")

# distribuição por segmento (preview dos dados)
print("\nDistribuição por segmento:")
display(
    df.groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

dbutils.notebook.exit(f"success|rows={count}")
