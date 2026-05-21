# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Quality Monitoring + Analytics (Free Edition)
# MAGIC
# MAGIC Dois blocos neste notebook:
# MAGIC
# MAGIC **Parte A — Quality Monitoring**
# MAGIC - Verifica SLOs de todas as tabelas críticas
# MAGIC - Detecta degradação com tendência histórica
# MAGIC - Exibe dashboard de saúde das tabelas
# MAGIC
# MAGIC **Parte B — Analytics queries**
# MAGIC - Queries analíticas sobre a camada gold
# MAGIC - Simula o que o Power BI ou Synapse Serverless consumiria
# MAGIC - Window functions, ranking, MoM, segmentação VIP

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Ambiente")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md ## Parte A — Quality Monitoring

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

TABLES_TO_MONITOR = [
    {
        "table":       f"{ENV}_bronze.orders",
        "pk":          "order_id",
        "sla_h":        1,
        "min_rows":     100,
        "null_checks":  ["order_id"],
        "ts_col":       "_ingested_at",
    },
    {
        "table":       f"{ENV}_bronze.customers",
        "pk":          "customer_id",
        "sla_h":        2,
        "min_rows":     50,
        "null_checks":  ["customer_id"],
        "ts_col":       "_ingested_at",
    },
    {
        "table":       f"{ENV}_silver.orders",
        "pk":          "order_id",
        "sla_h":        2,
        "min_rows":     100,
        "null_checks":  ["order_id", "customer_id", "total_amount"],
        "ts_col":       "_load_date",
        "valid_status": {
            "col":    "status",
            "values": ["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"],
        },
    },
    {
        "table":       f"{ENV}_silver.customers_scd2",
        "pk":          "customer_id",
        "sla_h":        4,
        "min_rows":     50,
        "null_checks":  ["customer_id", "email"],
        "ts_col":       "_load_date",
    },
    {
        "table":       f"{ENV}_gold.fact_order_line",
        "pk":          "order_id",
        "sla_h":        2,
        "min_rows":     100,
        "null_checks":  ["order_id", "customer_token", "total_amount"],
        "ts_col":       "_load_ts",
    },
]

def check_table(cfg: dict) -> dict:
    table = cfg["table"]
    result = {
        "table":      table,
        "checked_at": datetime.now().isoformat(),
        "checks":     {},
        "status":     "HEALTHY",
    }

    try:
        df    = spark.read.format("delta").table(table)
        total = df.count()
    except Exception as e:
        return {**result, "status": "ERROR", "error": str(e)}

    # volume
    result["checks"]["volume_ok"]  = total >= cfg["min_rows"]
    result["checks"]["row_count"]  = total

    # freshness
    ts_col = cfg.get("ts_col")
    if ts_col and ts_col in df.columns:
        max_ts = df.select(F.max(ts_col)).collect()[0][0]
        if max_ts:
            elapsed = (datetime.now() - max_ts.replace(tzinfo=None)).total_seconds() / 3600
            result["checks"]["freshness_ok"]       = elapsed <= cfg["sla_h"]
            result["checks"]["hours_since_update"] = round(elapsed, 2)

    # unicidade da PK
    unique = df.select(cfg["pk"]).distinct().count()
    result["checks"]["pk_unique_ok"]    = unique == total
    result["checks"]["duplicate_count"] = total - unique

    # nulos críticos
    for col in cfg.get("null_checks", []):
        if col in df.columns:
            n = df.filter(F.col(col).isNull()).count()
            result["checks"][f"null_{col}_ok"] = n == 0
            if n > 0:
                result["checks"][f"null_{col}_count"] = n

    # status válidos
    vs = cfg.get("valid_status")
    if vs and vs["col"] in df.columns:
        invalid = df.filter(~F.col(vs["col"]).isin(vs["values"])).count()
        result["checks"]["valid_status_ok"]    = invalid == 0
        result["checks"]["invalid_status_cnt"] = invalid

    # status geral
    failing = [k for k, v in result["checks"].items() if k.endswith("_ok") and v is False]
    if failing:
        result["status"]         = "DEGRADED"
        result["failing_checks"] = failing

    return result

# COMMAND ----------

reports = [check_table(cfg) for cfg in TABLES_TO_MONITOR]

print("\n" + "="*60)
print("QUALITY MONITORING REPORT")
print("="*60)
for r in reports:
    icon = "✓" if r["status"] == "HEALTHY" else ("✗" if r["status"] == "DEGRADED" else "!")
    print(f" {icon}  {r['table']:<45} {r['status']}")
    if r.get("failing_checks"):
        for fc in r["failing_checks"]:
            print(f"       ↳ FAIL: {fc}")

healthy  = sum(1 for r in reports if r["status"] == "HEALTHY")
degraded = sum(1 for r in reports if r["status"] == "DEGRADED")
errors   = sum(1 for r in reports if r["status"] == "ERROR")
print(f"\nResumo: {healthy} saudáveis | {degraded} degradadas | {errors} erros")

# COMMAND ----------

# MAGIC %md ## Persistir log de monitoramento

# COMMAND ----------

import json

LOG_TABLE = f"{ENV}_governance.quality_monitoring_log"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_governance")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        table       STRING
      , checked_at  STRING
      , status      STRING
      , checks_json STRING
      , error       STRING
    )
    USING DELTA
""")

log_rows = [{
    "table":       r["table"],
    "checked_at":  r["checked_at"],
    "status":      r["status"],
    "checks_json": json.dumps(r.get("checks", {})),
    "error":       r.get("error", ""),
} for r in reports]

spark.createDataFrame(log_rows).write.format("delta").mode("append").toTable(LOG_TABLE)

# COMMAND ----------

# MAGIC %md ## Tendência histórica

# COMMAND ----------

history = spark.read.format("delta").table(LOG_TABLE)
w       = Window.partitionBy("table").orderBy("checked_at")

regressions = (
    history
    .withColumn("prev_status", F.lag("status").over(w))
    .filter(
        F.col("prev_status").isNotNull()
        & (F.col("status") != F.col("prev_status"))
    )
    .select("table", "checked_at", "prev_status", "status")
)

rc = regressions.count()
if rc > 0:
    print(f"\n[WARN] {rc} mudança(s) de status detectada(s):")
    display(regressions)
else:
    print("\n[OK] Nenhuma regressão de qualidade detectada.")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## Parte B — Analytics queries (simula Power BI / Synapse Serverless)

# COMMAND ----------

GOLD_FACT = f"{ENV}_gold.fact_order_line"
fact      = spark.read.format("delta").table(GOLD_FACT)

print(f"fact_order_line: {fact.count():,} linhas")

# COMMAND ----------

# MAGIC %md ### B1 — Receita mensal com MoM e acumulado YTD

# COMMAND ----------

from pyspark.sql.window import Window as W

monthly = (
    fact.groupBy("order_year", "order_month", "customer_segment")
    .agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.count("*").alias("orders"),
        F.countDistinct("customer_token").alias("unique_customers"),
        F.round(F.avg("margin_pct"), 2).alias("avg_margin_pct"),
    )
)

w_seg     = W.partitionBy("customer_segment").orderBy("order_year", "order_month")
w_seg_yr  = W.partitionBy("customer_segment", "order_year").orderBy("order_month") \
             .rowsBetween(W.unboundedPreceding, W.currentRow)

monthly_enriched = (
    monthly
    .withColumn("prev_month_rev",  F.lag("monthly_revenue").over(w_seg))
    .withColumn("mom_growth_pct",  F.round(
        100.0 * (F.col("monthly_revenue") - F.col("prev_month_rev"))
              / F.nullif(F.col("prev_month_rev"), F.lit(0)), 2
    ))
    .withColumn("ytd_revenue", F.sum("monthly_revenue").over(w_seg_yr))
)

print("\nReceita mensal por segmento (com MoM e YTD):")
display(monthly_enriched.orderBy("order_year", "order_month", "customer_segment"))

# COMMAND ----------

# MAGIC %md ### B2 — Ranking de clientes por receita (top 10 com DENSE_RANK)

# COMMAND ----------

customer_revenue = (
    fact.groupBy("customer_token", "current_segment", "customer_country")
    .agg(
        F.sum("total_amount").alias("ltm_revenue"),
        F.count("*").alias("total_orders"),
        F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
        F.max("order_date").alias("last_order"),
    )
)

w_rank = W.orderBy(F.col("ltm_revenue").desc())

top_customers = (
    customer_revenue
    .withColumn("revenue_rank",     F.dense_rank().over(w_rank))
    .withColumn("revenue_pct_rank", F.round(F.percent_rank().over(w_rank), 4))
    .withColumn("vip_tier", F.when(F.col("revenue_pct_rank") >= 0.99, "VVVIP")
                             .when(F.col("revenue_pct_rank") >= 0.95, "VVIP")
                             .when(F.col("revenue_pct_rank") >= 0.80, "VIP")
                             .otherwise("Standard"))
    .filter(F.col("revenue_rank") <= 10)
    .orderBy("revenue_rank")
)

print("\nTop 10 clientes por receita:")
display(top_customers)

# COMMAND ----------

# MAGIC %md ### B3 — Receita por categoria com % do total

# COMMAND ----------

category_revenue = (
    fact.groupBy("category", "subcategory", "brand")
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.sum("margin_amount").alias("margin"),
        F.count("*").alias("orders"),
    )
    .withColumn("total_revenue_all", F.sum("revenue").over(W.rowsBetween(W.unboundedPreceding, W.unboundedFollowing)))
    .withColumn("pct_of_total",      F.round(F.col("revenue") / F.col("total_revenue_all") * 100, 2))
    .withColumn("margin_pct",        F.round(F.col("margin")  / F.col("revenue") * 100, 2))
    .drop("total_revenue_all")
    .orderBy(F.col("revenue").desc())
)

print("\nReceita por categoria:")
display(category_revenue)

# COMMAND ----------

# MAGIC %md ### B4 — Gap analysis: clientes em risco de churn

# COMMAND ----------

w_cust    = W.partitionBy("customer_token").orderBy("order_date")

gap_df = (
    fact.select("customer_token", "current_segment", "order_date", "total_amount")
    .withColumn("prev_order",      F.lag("order_date").over(w_cust))
    .withColumn("days_since_prev", F.datediff("order_date", "prev_order"))
)

churn_risk = (
    gap_df.groupBy("customer_token", "current_segment")
    .agg(
        F.max("order_date").alias("last_order"),
        F.avg("days_since_prev").alias("avg_cadence_days"),
        F.count("*").alias("total_orders"),
        F.sum("total_amount").alias("total_spent"),
    )
    .withColumn("days_since_last",
        F.datediff(F.current_date(), F.col("last_order"))
    )
    .withColumn("churn_risk",
        F.when(F.col("days_since_last") > F.col("avg_cadence_days") * 3, "High")
        .when(F.col("days_since_last") > F.col("avg_cadence_days") * 2, "Medium")
        .when(F.col("days_since_last") > 30, "Watch")
        .otherwise("Active")
    )
    .orderBy(F.col("days_since_last").desc())
)

print("\nAnálise de risco de churn:")
display(
    churn_risk.groupBy("churn_risk")
    .agg(F.count("*").alias("customers"), F.round(F.sum("total_spent"), 2).alias("at_risk_revenue"))
    .orderBy("customers")
)

# COMMAND ----------

# MAGIC %md ## Resumo do pipeline completo

# COMMAND ----------

print("\n" + "="*60)
print("PIPELINE ECOMMERCE LAKEHOUSE — STATUS FINAL")
print("="*60)

tables = {
    "Bronze": [f"{ENV}_bronze.orders", f"{ENV}_bronze.customers"],
    "Silver": [f"{ENV}_silver.orders", f"{ENV}_silver.customers_scd2", f"{ENV}_silver.products"],
    "Gold":   [f"{ENV}_gold.fact_order_line", f"{ENV}_gold.dim_customer",
               f"{ENV}_gold.dim_date", f"{ENV}_gold.dim_product"],
}

for layer, tbls in tables.items():
    print(f"\n  {layer}:")
    for t in tbls:
        try:
            cnt = spark.read.format("delta").table(t).count()
            print(f"    ✓  {t:<50} {cnt:>8,} linhas")
        except Exception as e:
            print(f"    ✗  {t:<50} ERRO")

print("\n[OK] Pipeline concluído com sucesso!")
print("     Próximo passo: Microsoft Fabric Data Factory para orquestração")

dbutils.notebook.exit("success")
