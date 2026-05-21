# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Orders (Free Edition)
# MAGIC
# MAGIC **Pré-requisito:** notebook `00_setup` executado.
# MAGIC
# MAGIC O que este notebook faz:
# MAGIC - Lê arquivos JSON da pasta landing no DBFS via **Auto Loader**
# MAGIC - Adiciona metadados de rastreabilidade obrigatórios
# MAGIC - Grava em Delta Table particionada (camada bronze)
# MAGIC - Valida o resultado

# COMMAND ----------

# MAGIC %md ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("env",            "dev",  "Ambiente")
dbutils.widgets.text("execution_date", "",     "Data (yyyy-MM-dd) — vazio = hoje")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

# Caminhos DBFS (Free Edition — sem ADLS)
BASE_PATH       = f"dbfs:/FileStore/ecommerce/{ENV}"
LANDING_PATH    = f"{BASE_PATH}/landing/orders"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/orders/bronze"
SCHEMA_PATH     = f"{BASE_PATH}/checkpoints/orders/schema"
BRONZE_TABLE    = f"{ENV}_bronze.orders"

print(f"ENV            : {ENV}")
print(f"execution_date : {execution_date}")
print(f"Landing        : {LANDING_PATH}")
print(f"Tabela         : {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## Auto Loader — ingestão incremental

# COMMAND ----------

from pyspark.sql import functions as F

# cria schema da tabela bronze se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_bronze")

stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      SCHEMA_PATH)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes",    "true")
    # No Free Edition usamos directory listing (sem Event Grid)
    .option("cloudFiles.useNotifications",    "false")
    .load(LANDING_PATH)
    # ── metadados de rastreabilidade ─────────────────────────────────
    .withColumn("_source_file",    F.input_file_name())
    .withColumn("_ingested_at",    F.current_timestamp())
    .withColumn("_execution_date", F.lit(execution_date))
    .withColumn("_env",            F.lit(ENV))
    # ── particionamento por data de ingestão ─────────────────────────
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
    .trigger(availableNow=True)   # processa tudo pendente e encerra
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print("[OK] Stream concluído")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

df    = spark.read.format("delta").table(BRONZE_TABLE)
count = df.count()

print(f"Tabela  : {BRONZE_TABLE}")
print(f"Linhas  : {count:,}")
print(f"Colunas : {len(df.columns)}")
print(f"\nColunas de metadados presentes:")
for c in ["_source_file", "_ingested_at", "_execution_date", "ingest_year", "ingest_month"]:
    print(f"  {'OK' if c in df.columns else 'FALTANDO'} : {c}")

display(df.orderBy(F.col("_ingested_at").desc()).limit(5))

# MAGIC %md ### Histórico Delta
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(5, truncate=False)

dbutils.notebook.exit(f"success|rows={count}")
