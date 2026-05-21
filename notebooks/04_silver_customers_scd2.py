# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver — Customers SCD Type 2 + Type 6 + PII (Free Edition)
# MAGIC
# MAGIC O que este notebook faz:
# MAGIC 1. Lê bronze customers
# MAGIC 2. Valida qualidade (completeness + validity)
# MAGIC 3. Mascara PII (CPF, email, telefone)
# MAGIC 4. Pseudonimiza customer_id (SHA-256 + salt — sem Key Vault no Free Edition)
# MAGIC 5. SCD Type 2: nova linha por mudança de atributo
# MAGIC 6. SCD Type 6: atualiza `current_segment` em todas as linhas
# MAGIC
# MAGIC > **Nota Free Edition:** o salt de pseudonimização fica em variável de
# MAGIC > sessão em vez do Key Vault. Em produção real sempre use Key Vault.

# COMMAND ----------

# MAGIC %md ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("env",            "dev", "Ambiente")
dbutils.widgets.text("execution_date", "",    "Data (yyyy-MM-dd)")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

BRONZE_TABLE     = f"{ENV}_bronze.customers"
SILVER_TABLE     = f"{ENV}_silver.customers_scd2"
QUARANTINE_TABLE = f"{ENV}_quarantine.customers"

print(f"ENV            : {ENV}")
print(f"execution_date : {execution_date}")

# COMMAND ----------

# MAGIC %md ## Salt de pseudonimização
# MAGIC
# MAGIC > Em produção: `SALT = dbutils.secrets.get("kv-scope", "pii-salt")`  
# MAGIC > No Free Edition: variável de sessão (não exponha em repositório público)

# COMMAND ----------

import hashlib

# No Free Edition, usamos uma variável local
# Em produção: buscar do Key Vault via dbutils.secrets
SALT = "ecommerce-study-salt-2024-changeme"

print("[WARN] Usando salt local — em produção use dbutils.secrets.get()")
print(f"Salt configurado: {'*' * len(SALT)}")

# COMMAND ----------

# MAGIC %md ## Funções de PII

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(StringType())
def pseudonymize(value):
    """Token SHA-256 truncado — reversível apenas com o salt."""
    if value is None:
        return None
    return hashlib.sha256(f"{SALT}:{value}".encode()).hexdigest()[:16]

def mask_pii(df):
    """Mascara colunas PII. Não reversível por analistas."""
    return (
        df
        .withColumn("cpf",
            F.when(F.col("cpf").isNotNull(),
                F.regexp_replace("cpf", r"\d{3}\.\d{3}\.\d{3}-(\d{2})", "***.***.***-$1")
            ).otherwise(F.lit(None))
        )
        .withColumn("email",
            F.when(F.col("email").isNotNull(),
                F.regexp_replace("email", r"(.).+(@.+)", "$1***$2")
            ).otherwise(F.lit(None))
        )
        .withColumn("phone",
            F.when(F.col("phone").isNotNull(),
                F.regexp_replace("phone", r"(\(\d{2}\))\s*\d+(\d{4})", "$1 *****$2")
            ).otherwise(F.lit(None))
        )
    )

# COMMAND ----------

# MAGIC %md ## Leitura, validação e preparação

# COMMAND ----------

from pyspark.sql.window import Window

bronze_raw = (
    spark.read.format("delta").table(BRONZE_TABLE)
    .filter(F.col("_execution_date") == execution_date)
)

print(f"Bronze carregado: {bronze_raw.count():,} linhas")

# ── validação básica ──────────────────────────────────────────────────
invalid_mask = (
    F.col("customer_id").isNull()
    | ~F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+")
)
valid_raw    = bronze_raw.filter(~invalid_mask)
quarantine_raw = (
    bronze_raw.filter(invalid_mask)
    .withColumn("_dq_errors",      F.lit("customer_id_null or invalid_email"))
    .withColumn("_quarantine_ts",  F.current_timestamp())
)

q_count = quarantine_raw.count()
if q_count > 0:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_quarantine")
    quarantine_raw.write.format("delta").mode("append").toTable(QUARANTINE_TABLE)
    print(f"[WARN] {q_count} registros em quarentena")

# ── deduplicar e enriquecer ───────────────────────────────────────────
incoming = (
    valid_raw
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    ))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("customer_name", F.initcap(F.trim("customer_name")))
    .withColumn("email_clean",   F.lower(F.trim("email")))
    .withColumn("segment",       F.upper(F.trim("segment")))
    .withColumn("country",       F.upper(F.trim("country")))
    .withColumn("customer_token", pseudonymize(F.col("customer_id")))
)

incoming = mask_pii(incoming)
v_count  = incoming.count()
print(f"Incoming válidos: {v_count:,}")

# COMMAND ----------

# MAGIC %md ## Criar tabela silver se não existir

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        customer_token   STRING      NOT NULL
      , customer_id      STRING      NOT NULL
      , customer_name    STRING
      , email            STRING
      , cpf              STRING
      , phone            STRING
      , country          STRING
      , segment          STRING
      , current_segment  STRING
      , is_current       BOOLEAN     NOT NULL
      , valid_from       TIMESTAMP   NOT NULL
      , valid_to         TIMESTAMP
      , record_source    STRING
      , _load_date       TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (country)
""")

silver = DeltaTable.forName(spark, SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ## SCD Type 2 — Passo 1: fechar registros que mudaram

# COMMAND ----------

changed_condition = """
    t.customer_id = s.customer_id
    AND t.is_current = true
    AND (
          t.email   <> s.email_clean
       OR t.segment <> s.segment
       OR t.country <> s.country
       OR t.customer_name <> s.customer_name
    )
"""

(
    silver.alias("t")
    .merge(incoming.alias("s"), changed_condition)
    .whenMatchedUpdate(set={
        "t.is_current": "false",
        "t.valid_to":   "current_timestamp()",
    })
    .execute()
)

print("[OK] Passo 1: registros alterados fechados")

# COMMAND ----------

# MAGIC %md ## SCD Type 2 — Passo 2: inserir novas versões e clientes novos

# COMMAND ----------

# anti-join: só quem não tem versão ativa inalterada
current_active = (
    silver.toDF()
    .filter("is_current = true")
    .select("customer_id")
)

new_versions = (
    incoming.alias("s")
    .join(current_active.alias("t"), "customer_id", "left_anti")
    .withColumn("is_current",     F.lit(True))
    .withColumn("valid_from",     F.current_timestamp())
    .withColumn("valid_to",       F.lit(None).cast("timestamp"))
    .withColumn("current_segment", F.col("segment"))   # SCD Type 6
    .withColumn("record_source",  F.lit("crm_bronze"))
    .withColumn("_load_date",     F.current_timestamp())
    .select(
        "customer_token", "customer_id", "customer_name",
        F.col("email_clean").alias("email"),
        "cpf", "phone", "country", "segment", "current_segment",
        "is_current", "valid_from", "valid_to",
        "record_source", "_load_date",
    )
)

nv_count = new_versions.count()
new_versions.write.format("delta").mode("append").toTable(SILVER_TABLE)
print(f"[OK] Passo 2: {nv_count:,} novas versões inseridas")

# COMMAND ----------

# MAGIC %md ## SCD Type 6 — Passo 3: sincronizar current_segment em TODAS as linhas

# COMMAND ----------

(
    silver.alias("t")
    .merge(
        incoming.select("customer_id", "segment").alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(set={"t.current_segment": "s.segment"})
    .execute()
)

print("[OK] Passo 3: current_segment atualizado (SCD Type 6)")

# COMMAND ----------

# MAGIC %md ## Resultado final

# COMMAND ----------

final_df   = spark.read.format("delta").table(SILVER_TABLE)
total      = final_df.count()
active     = final_df.filter("is_current = true").count()
historical = total - active

print(f"\n[OK] {SILVER_TABLE}")
print(f"     Total de linhas  : {total:,}")
print(f"     Registros ativos : {active:,}")
print(f"     Histórico        : {historical:,}")
print(f"     Novas versões    : {nv_count:,}")
print(f"\n[PII] Verificação:")
print(f"     customer_id na silver   : {'customer_id' in final_df.columns}")
print(f"     customer_token presente : {'customer_token' in final_df.columns}")

# preview: mostrar mascaramento funcionando
print("\n[PII] Amostra dos dados mascarados:")
display(
    final_df.filter("is_current = true")
    .select("customer_token", "customer_name", "email", "cpf",
            "segment", "current_segment", "country", "valid_from")
    .limit(5)
)

dbutils.notebook.exit(
    f"success|active={active}|historical={historical}|new_versions={nv_count}"
)
