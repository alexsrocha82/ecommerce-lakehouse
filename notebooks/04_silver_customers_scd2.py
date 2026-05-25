# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver — Customers SCD Type 2 + Type 6 + PII (Free Edition)
# MAGIC
# MAGIC What this notebook does:
# MAGIC 1. Reads bronze customers
# MAGIC 2. Validates data quality (completeness + validity)
# MAGIC 3. Tokenizes ALL PII fields via SHA-256 + salt (email, cpf, phone, customer_id)
# MAGIC 4. SCD Type 2: inserts a new row for each attribute change
# MAGIC 5. SCD Type 6: updates `current_segment` across all historical rows
# MAGIC
# MAGIC PII strategy — tokenization instead of masking:
# MAGIC   - Masking destroys data irreversibly (u***@gmail.com tells you nothing useful)
# MAGIC   - Tokenization replaces PII with a deterministic SHA-256 token
# MAGIC   - Anyone with the salt can reverse a token back to the original value
# MAGIC   - Analysts see only tokens — no raw PII exposed in silver or gold
# MAGIC   - In production the salt lives in Azure Key Vault (dbutils.secrets)

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("execution_date", "", "Date (yyyy-MM-dd) — empty = today")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

BRONZE_TABLE     = f"{ENV}_bronze.customers"
SILVER_TABLE     = f"{ENV}_silver.customers_scd2"
QUARANTINE_TABLE = f"{ENV}_quarantine.customers"

print(f"ENV            : {ENV}")
print(f"execution_date : {execution_date}")

# COMMAND ----------

# MAGIC %md ## Pseudonymization salt
# MAGIC
# MAGIC > In production: `SALT = dbutils.secrets.get("kv-scope", "pii-salt")`
# MAGIC > In Free Edition: local session variable (never commit to a public repo)

# COMMAND ----------

import hashlib

# Local variable — in production fetch from Azure Key Vault via dbutils.secrets
SALT = "ecommerce-study-salt-2024-changeme"

print("[WARN] Using local salt — in production use dbutils.secrets.get()")
print(f"Salt configured: {'*' * len(SALT)}")

# COMMAND ----------

# MAGIC %md ## PII tokenization
# MAGIC
# MAGIC All PII fields are tokenized using the same SHA-256 + salt function.
# MAGIC Reversible by anyone who holds the salt — in production only the pipeline
# MAGIC service principal and the security team have access to the Key Vault secret.
# MAGIC
# MAGIC Tokenization vs masking:
# MAGIC   masking  → destroys data (u***@gmail.com) — not useful for any lookup
# MAGIC   tokenize → deterministic token, same input always produces same output
# MAGIC              allows joins across tables using the token as a key

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(StringType())
def pseudonymize(value):
    """
    SHA-256 truncated token — reversible only with the correct salt.
    Deterministic: same input + same salt always produces the same token.
    """
    if value is None:
        return None
    return hashlib.sha256(f"{SALT}:{value}".encode()).hexdigest()[:16]

# COMMAND ----------

# MAGIC %md ## Read, validate and prepare incoming records

# COMMAND ----------

from pyspark.sql.window import Window

bronze_raw = (
    spark.read.format("delta").table(BRONZE_TABLE)
    .filter(F.col("_execution_date") == execution_date)
)

print(f"Bronze rows loaded: {bronze_raw.count():,}")

# basic validation — quarantine nulls and malformed emails
invalid_mask = (
    F.col("customer_id").isNull()
    | ~F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+")
)

valid_raw      = bronze_raw.filter(~invalid_mask)
quarantine_raw = (
    bronze_raw.filter(invalid_mask)
    .withColumn("_dq_errors",     F.lit("customer_id_null or invalid_email"))
    .withColumn("_quarantine_ts", F.current_timestamp())
)

q_count = quarantine_raw.count()
if q_count > 0:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_quarantine")
    quarantine_raw.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
    print(f"[WARN] {q_count} records written to quarantine")

# deduplicate, normalize and tokenize ALL PII in a single chain
# important: tokenization happens BEFORE email_clean alias to avoid column confusion
incoming = (
    valid_raw
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    # normalize text fields
    .withColumn("customer_name", F.initcap(F.trim("customer_name")))
    .withColumn("email_clean",   F.lower(F.trim("email")))
    .withColumn("segment",       F.upper(F.trim("segment")))
    .withColumn("country",       F.upper(F.trim("country")))
    # tokenize ALL PII fields using the same SHA-256 + salt function
    # each token is deterministic: same input always produces the same 16-char hex
    .withColumn("customer_token", pseudonymize(F.col("customer_id")))
    .withColumn("email_token",    pseudonymize(F.col("email_clean")))
    .withColumn("cpf_token",      pseudonymize(F.col("cpf")))
    .withColumn("phone_token",    pseudonymize(F.col("phone")))
)

print(f"Valid incoming records: {incoming.count():,}")

# quick verification — show token vs original for 3 records
print("\n[PII] Tokenization sample (dev only — never log this in production):")
display(
    incoming.select(
        "customer_id", "customer_token",
        "email_clean", "email_token",
        "cpf",         "cpf_token",
        "phone",       "phone_token",
    ).limit(3)
)

# COMMAND ----------

# MAGIC %md ## Create silver table if it does not exist
# MAGIC
# MAGIC PII columns store tokens only — no raw values.
# MAGIC customer_id is kept for SCD join logic and audit purposes (silver is restricted access).

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_silver")

# drop and recreate if schema changed (only safe in dev — remove in prod)
spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        customer_token   STRING    NOT NULL
      , customer_id      STRING    NOT NULL
      , customer_name    STRING
      , email_token      STRING
      , cpf_token        STRING
      , phone_token      STRING
      , country          STRING
      , segment          STRING
      , current_segment  STRING
      , is_current       BOOLEAN   NOT NULL
      , valid_from       TIMESTAMP NOT NULL
      , valid_to         TIMESTAMP
      , record_source    STRING
      , _load_date       TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (country)
""")

silver = DeltaTable.forName(spark, SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ## SCD Type 2 — Step 1: close changed records
# MAGIC
# MAGIC Tracked attributes: email_token, segment, country, customer_name.
# MAGIC If any of these changed, the current row is closed:
# MAGIC   is_current = false, valid_to = now()

# COMMAND ----------

changed_condition = """
    t.customer_id  = s.customer_id
    AND t.is_current = true
    AND (
          t.email_token   <> s.email_token
       OR t.segment       <> s.segment
       OR t.country       <> s.country
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

print("[OK] Step 1: changed records closed (is_current = false)")

# COMMAND ----------

# MAGIC %md ## SCD Type 2 — Step 2: insert new versions and brand-new customers
# MAGIC
# MAGIC Anti-join pattern: only inserts records with no active version in silver.
# MAGIC Covers two cases in one pass:
# MAGIC   - brand-new customer (never existed in silver)
# MAGIC   - returning customer whose record was just closed in Step 1

# COMMAND ----------

# anti-join: only records without an active (is_current=true) row in silver
current_active = (
    silver.toDF()
    .filter("is_current = true")
    .select("customer_id")
)

new_versions = (
    incoming.alias("s")
    .join(current_active.alias("t"), "customer_id", "left_anti")
    .withColumn("is_current",      F.lit(True))
    .withColumn("valid_from",      F.current_timestamp())
    .withColumn("valid_to",        F.lit(None).cast("timestamp"))
    .withColumn("current_segment", F.col("segment"))
    .withColumn("record_source",   F.lit("crm_bronze"))
    .withColumn("_load_date",      F.current_timestamp())
    .select(
        "customer_token",
        "customer_id",
        "customer_name",
        "email_token",
        "cpf_token",
        "phone_token",
        "country",
        "segment",
        "current_segment",
        "is_current",
        "valid_from",
        "valid_to",
        "record_source",
        "_load_date",
    )
)

nv_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
print(f"[OK] Step 2: {nv_count:,} new versions inserted")

# COMMAND ----------

# MAGIC %md ## SCD Type 6 — Step 3: sync current_segment across ALL rows
# MAGIC
# MAGIC Type 6 = Type 2 (new row per change) + Type 1 (overwrite current_segment).
# MAGIC current_segment is updated on ALL rows — historical and current alike.
# MAGIC This gives analysts two answers from one table:
# MAGIC   segment         = "what was the segment during THIS specific period?"
# MAGIC   current_segment = "what is the segment TODAY regardless of the period?"

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

print("[OK] Step 3: current_segment synced across all rows (SCD Type 6)")

# COMMAND ----------

# MAGIC %md ## Final summary + PII verification

# COMMAND ----------

final_df   = spark.read.format("delta").table(SILVER_TABLE)
total      = final_df.count()
active     = final_df.filter("is_current = true").count()
historical = total - active

print(f"\n[OK] {SILVER_TABLE}")
print(f"     Total rows      : {total:,}")
print(f"     Active records  : {active:,}")
print(f"     Historical rows : {historical:,}")
print(f"     New versions    : {nv_count:,}")

# verify no raw PII columns exist in silver
raw_pii_cols = ["email", "cpf", "phone"]
leaked = [c for c in raw_pii_cols if c in final_df.columns]
print(f"\n[PII] Raw PII columns in silver : {leaked if leaked else 'none — OK'}")
print(f"      Token columns present      : email_token={('email_token' in final_df.columns)}, cpf_token={('cpf_token' in final_df.columns)}, phone_token={('phone_token' in final_df.columns)}")
print(f"      customer_id in silver      : {'customer_id' in final_df.columns} (expected — audit access only)")
print(f"      customer_token present     : {'customer_token' in final_df.columns}")

print("\n[PII] Token sample (active records):")
display(
    final_df.filter("is_current = true")
    .select(
        "customer_token", "customer_name",
        "email_token", "cpf_token", "phone_token",
        "segment", "current_segment", "country", "valid_from"
    )
    .limit(5)
)

dbutils.notebook.exit(
    f"success|active={active}|historical={historical}|new_versions={nv_count}"
)
