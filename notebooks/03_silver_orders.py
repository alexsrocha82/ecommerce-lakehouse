# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — Orders (Free Edition)
# MAGIC
# MAGIC What this notebook does:
# MAGIC 1. Reads bronze orders for the execution date
# MAGIC 2. Applies explicit casting and cleaning
# MAGIC 3. Validates data quality (6 rules) and isolates quarantine records
# MAGIC 4. Incremental MERGE with partition pruning into silver
# MAGIC 5. Persists quality report to governance log

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("execution_date", "", "Date (yyyy-MM-dd) — empty = today")

ENV            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date") or \
                 __import__("datetime").date.today().isoformat()

BRONZE_TABLE     = f"{ENV}_bronze.orders"
SILVER_TABLE     = f"{ENV}_silver.orders"
QUARANTINE_TABLE = f"{ENV}_quarantine.orders"
QUALITY_LOG      = f"{ENV}_governance.quality_log"

print(f"ENV              : {ENV}")
print(f"execution_date   : {execution_date}")
print(f"BRONZE_TABLE     : {BRONZE_TABLE}")
print(f"SILVER_TABLE     : {SILVER_TABLE}")
print(f"QUARANTINE_TABLE : {QUARANTINE_TABLE}")
print(f"QUALITY_LOG      : {QUALITY_LOG}")

# COMMAND ----------

# MAGIC %md ## Quality framework (inline — no external dependency)

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from dataclasses import dataclass
from typing import List
from datetime import datetime

@dataclass
class QualityRule:
    name:        str
    dimension:   str
    expression:  str
    is_critical: bool = False

def check_and_split(df: DataFrame, rules: List[QualityRule], table_name: str):
    """
    Validates quality rules and splits the dataframe into valid and quarantine.
    Returns (valid_df, quarantine_df, report_dict).
    """
    total   = df.count()
    results = []

    for rule in rules:
        passed = df.filter(F.expr(rule.expression)).count()
        failed = total - passed
        results.append({
            "rule":      rule.name,
            "dimension": rule.dimension,
            "passed":    passed,
            "failed":    failed,
            "pass_rate": round(passed / total, 6) if total > 0 else 0.0,
            "ok":        failed == 0,
            "critical":  rule.is_critical,
        })

    # overall score = average of all pass rates
    score         = round(sum(r["pass_rate"] for r in results) / len(results) * 100, 2)
    critical_fail = any(r["critical"] and not r["ok"] for r in results)

    # combine all rule expressions to split valid vs invalid rows
    all_valid_expr = " AND ".join(f"({r.expression})" for r in rules)
    valid_df       = df.filter(F.expr(all_valid_expr))
    quarantine_df  = (
        df
        .filter(~F.expr(all_valid_expr))
        .withColumn(
            "_dq_errors", 
            F.array_compact(F.array(*[
              F.when(~F.expr(r.expression), F.lit(r.name))
              for r in rules
            ])
            )
        )
        .withColumn("_quarantine_ts",    F.current_timestamp())
        .withColumn("_quarantine_table", F.lit(table_name))
    )

    report = {
        "table":           table_name,
        "checked_at":      datetime.now().isoformat(),
        "total_rows":      total,
        "valid_rows":      valid_df.count(),
        "quarantine_rows": quarantine_df.count(),
        "quality_score":   score,
        "critical_fail":   critical_fail,
        "status":          "FAIL" if critical_fail or score < 95 else "PASS",
    }

    print(f"\n[Quality] {table_name} | Score: {score}% | {report['status']}")
    for r in results:
        icon = "OK  " if r["ok"] else "FAIL"
        print(f"  [{icon}] {r['dimension']:15s} {r['rule']:30s} failures={r['failed']}")

    return valid_df, quarantine_df, report

# COMMAND ----------

# MAGIC %md ## Read and cast bronze data

# COMMAND ----------

from pyspark.sql.window import Window

bronze = (
    spark.read.format("delta").table(BRONZE_TABLE)
    .filter(F.col("_execution_date") == execution_date)
    # explicit casting for each column
    .withColumn("order_id",     F.col("order_id").cast("string"))
    .withColumn("customer_id",  F.col("customer_id").cast("string"))
    .withColumn("product_id",   F.col("product_id").cast("string"))
    .withColumn("quantity",     F.col("quantity").cast("integer"))
    .withColumn("unit_price",   F.col("unit_price").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("order_date",   F.to_date("order_date"))
    .withColumn("status",       F.upper(F.trim("status")))
    # deduplicate by PK — keep the most recently ingested record
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Bronze rows loaded: {bronze.count():,}")

# COMMAND ----------

# MAGIC %md ## Data quality validation

# COMMAND ----------

rules = [
    QualityRule("order_id_not_null",    "completeness", "order_id IS NOT NULL",    is_critical=True),
    QualityRule("customer_id_not_null", "completeness", "customer_id IS NOT NULL", is_critical=True),
    QualityRule("amount_positive",      "validity",     "total_amount > 0"),
    QualityRule("quantity_positive",    "validity",     "quantity > 0"),
    QualityRule("valid_status",         "validity",
                "status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED')"),
    QualityRule("date_not_future",      "validity",     "order_date <= current_date()"),
]

valid_df, quarantine_df, report = check_and_split(bronze, rules, SILVER_TABLE)

# persist quarantine records if any
q_count = quarantine_df.count()
if q_count > 0:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_quarantine")
    quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
    print(f"\n[WARN] {q_count} quarantine rows written to {QUARANTINE_TABLE}")

# halt pipeline if any critical rule failed
if report["critical_fail"]:
    dbutils.notebook.exit(f"critical_fail|score={report['quality_score']}")

# COMMAND ----------

# MAGIC %md ## Incremental MERGE into Silver
# MAGIC
# MAGIC Partition pruning on (order_year, order_month) avoids full table scans.
# MAGIC Only updates rows where status or total_amount changed.

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        order_id      STRING   NOT NULL
      , customer_id   STRING   NOT NULL
      , product_id    STRING   NOT NULL
      , quantity      INT
      , unit_price    DOUBLE
      , total_amount  DOUBLE
      , order_date    DATE
      , status        STRING
      , updated_at    TIMESTAMP
      , _load_date    TIMESTAMP
      , order_year    INT
      , order_month   INT
    )
    USING DELTA
    PARTITIONED BY (order_year, order_month)
""")

target = DeltaTable.forName(spark, SILVER_TABLE)

source = (
    valid_df
    .withColumn("_load_date",  F.current_timestamp())
    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .select(
        "order_id", 
        "customer_id", 
        "product_id", 
        "quantity",
        "unit_price", 
        "total_amount", 
        "order_date", 
        "status",
        "updated_at", 
        "_load_date", 
        "order_year", 
        "order_month"
    )
)

(
    target.alias("t")
    .merge(
        source.alias("s"),
        # include partition columns in join predicate for partition pruning
        "t.order_id    = s.order_id    "
        "AND t.order_year  = s.order_year  "
        "AND t.order_month = s.order_month"
    )
    .whenMatchedUpdate(
        condition = "t.status <> s.status OR t.total_amount <> s.total_amount",
        set = {
            "t.status":       "s.status",
            "t.total_amount": "s.total_amount",
            "t.updated_at":   "s.updated_at",
            "t._load_date":   "s._load_date",
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md ## Persist quality log and print summary

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_governance")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {QUALITY_LOG} (
        table           STRING
      , execution_date  STRING
      , quality_score   DOUBLE
      , valid_rows      LONG
      , quarantine_rows LONG
      , status          STRING
      , checked_at      STRING
    )
    USING DELTA
""")

spark.createDataFrame([{
    "table":           SILVER_TABLE,
    "execution_date":  execution_date,
    "quality_score":   report["quality_score"],
    "valid_rows":      report["valid_rows"],
    "quarantine_rows": report["quarantine_rows"],
    "status":          report["status"],
    "checked_at":      report["checked_at"],
}]).write.format("delta").mode("append").saveAsTable(QUALITY_LOG)

final_count = spark.read.format("delta").table(SILVER_TABLE).count()

print(f"\n[OK] {SILVER_TABLE}")
print(f"     Total rows      : {final_count:,}")
print(f"     Valid processed : {report['valid_rows']:,}")
print(f"     Quarantine      : {q_count:,}")
print(f"     Quality score   : {report['quality_score']}%")

display(spark.read.format("delta").table(SILVER_TABLE).limit(5))

dbutils.notebook.exit(
    f"success|valid={report['valid_rows']}|quarantine={q_count}|score={report['quality_score']}"
)
