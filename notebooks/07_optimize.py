# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Optimize & Maintenance (Free Edition)
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook performs weekly maintenance on all Delta tables in the gold layer.
# MAGIC It is designed to run as a **separate Databricks Job**, independent of the daily
# MAGIC pipeline (notebooks 01-06).
# MAGIC
# MAGIC ## Why not run OPTIMIZE on every pipeline execution?
# MAGIC
# MAGIC Each incremental append adds a small Parquet file to the table.
# MAGIC `OPTIMIZE + ZORDER` physically rewrites **all files in the table** — not just the new ones.
# MAGIC Running it daily on a small append means paying the cost of rewriting 10M rows
# MAGIC just to optimize 500 new ones. That is wasteful.
# MAGIC
# MAGIC The right pattern:
# MAGIC   - Daily pipeline  → append new data (fast, cheap)
# MAGIC   - Weekly job      → OPTIMIZE + ZORDER (consolidates accumulated small files)
# MAGIC
# MAGIC ## Auto Optimize vs manual OPTIMIZE
# MAGIC
# MAGIC Databricks has an Auto Optimize feature that runs compaction automatically
# MAGIC in the background after each write. It works well in paid tiers where compute
# MAGIC quota is not a concern.
# MAGIC
# MAGIC In Databricks Free Edition, Auto Optimize is intentionally disabled here because:
# MAGIC   - The workspace has a daily compute quota
# MAGIC   - Auto Optimize fires on every write — including small incremental appends
# MAGIC   - This would consume quota on low-value compactions
# MAGIC   - Manual weekly OPTIMIZE gives full control over when resources are used
# MAGIC
# MAGIC In production (paid tier), you can enable Auto Optimize per table:
# MAGIC   ALTER TABLE dev_gold.fact_order_line
# MAGIC   SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,
# MAGIC                      delta.autoOptimize.autoCompact   = true);
# MAGIC
# MAGIC ## Scheduling recommendation
# MAGIC
# MAGIC Create a dedicated Databricks Job for this notebook:
# MAGIC   Schedule : Weekly, Sunday at 02:00
# MAGIC   Cluster  : Single node (optimization is I/O-bound, not compute-bound)
# MAGIC   Timeout  : 60 minutes
# MAGIC   Alerts   : on failure → email/Slack
# MAGIC
# MAGIC This job runs independently of the daily pipeline job.
# MAGIC If it fails, the daily pipeline is not affected — data is still queryable,
# MAGIC just with slightly more files than optimal.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env",          "dev",  "Environment")
dbutils.widgets.text("tables",       "all",  "Tables: all | gold | silver | comma-separated")
dbutils.widgets.text("dry_run",      "false","Dry run: true = show plan only, no execution")
dbutils.widgets.text("vacuum_hours", "168",  "Vacuum retention hours (168 = 7 days minimum)")

ENV          = dbutils.widgets.get("env")
TABLES_SCOPE = dbutils.widgets.get("tables")
DRY_RUN      = dbutils.widgets.get("dry_run").lower() == "true"
VACUUM_HOURS = int(dbutils.widgets.get("vacuum_hours"))

print(f"ENV          : {ENV}")
print(f"Tables scope : {TABLES_SCOPE}")
print(f"Dry run      : {DRY_RUN}")
print(f"Vacuum hours : {VACUUM_HOURS}h ({VACUUM_HOURS // 24} days)")

if DRY_RUN:
    print("\n[DRY RUN] No changes will be made — showing plan only")

# COMMAND ----------

# MAGIC %md ## Table catalog
# MAGIC
# MAGIC Defines ZORDER columns per table.
# MAGIC ZORDER columns should match the most common filter/join patterns in analytics queries:
# MAGIC   fact_order_line  → filtered by customer, date, category (B1-B4 analytics)
# MAGIC   dim_customer     → looked up by customer_token
# MAGIC   silver.orders    → filtered by order_date, status (MERGE partition pruning)

# COMMAND ----------

# table catalog with ZORDER strategy per table
# zorder_cols: ordered by query selectivity (most selective first)
TABLE_CATALOG = {
    "gold": [
        {
            "table":       f"{ENV}_gold.fact_order_line",
            "zorder_cols": ["customer_token", "order_date", "category"],
            "description": "main fact table — filtered by customer, date, category",
        },
        {
            "table":       f"{ENV}_gold.dim_customer",
            "zorder_cols": ["customer_token"],
            "description": "customer dimension — looked up by token",
        },
        {
            "table":       f"{ENV}_gold.dim_product",
            "zorder_cols": ["category", "brand"],
            "description": "product dimension — filtered by category/brand",
        },
        {
            "table":       f"{ENV}_gold.dim_date",
            "zorder_cols": ["full_date"],
            "description": "date dimension — joined by full_date",
        },
    ],
    "silver": [
        {
            "table":       f"{ENV}_silver.orders",
            "zorder_cols": ["order_date", "status"],
            "description": "silver orders — MERGE partition pruning + analytics filters",
        },
        {
            "table":       f"{ENV}_silver.customers_scd2",
            "zorder_cols": ["customer_token", "is_current"],
            "description": "SCD2 customers — filtered by is_current + token lookups",
        },
    ],
}

# resolve which tables to process based on the widget
if TABLES_SCOPE == "all":
    tables_to_process = TABLE_CATALOG["gold"] + TABLE_CATALOG["silver"]
elif TABLES_SCOPE == "gold":
    tables_to_process = TABLE_CATALOG["gold"]
elif TABLES_SCOPE == "silver":
    tables_to_process = TABLE_CATALOG["silver"]
else:
    # comma-separated list of explicit table names
    explicit = [t.strip() for t in TABLES_SCOPE.split(",")]
    tables_to_process = [
        t for layer in TABLE_CATALOG.values()
        for t in layer
        if t["table"] in explicit
    ]

print(f"\nTables to process: {len(tables_to_process)}")
for t in tables_to_process:
    print(f"  {t['table']:<50} ZORDER: {t['zorder_cols']}")

# COMMAND ----------

# MAGIC %md ## OPTIMIZE + ZORDER

# COMMAND ----------

from datetime import datetime

optimize_results = []

for cfg in tables_to_process:
    table       = cfg["table"]
    zorder_cols = ", ".join(cfg["zorder_cols"])
    start       = datetime.now()

    print(f"\n{'[DRY RUN] ' if DRY_RUN else ''}OPTIMIZE {table}")
    print(f"  ZORDER BY ({zorder_cols})")

    result = {
        "table":       table,
        "zorder_cols": zorder_cols,
        "status":      "skipped (dry run)",
        "duration_s":  0,
        "files_before": 0,
        "files_after":  0,
    }

    if not DRY_RUN:
        try:
            # file count before optimize
            detail_before = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
            files_before  = detail_before["numFiles"]

            # run optimize + zorder
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")

            # file count after optimize
            detail_after = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
            files_after  = detail_after["numFiles"]

            duration = (datetime.now() - start).total_seconds()

            result.update({
                "status":       "OK",
                "duration_s":   round(duration, 1),
                "files_before": files_before,
                "files_after":  files_after,
                "files_saved":  files_before - files_after,
            })

            print(f"  OK  {duration:.1f}s | files: {files_before} → {files_after} ({files_before - files_after} removed)")

        except Exception as e:
            result.update({"status": f"ERROR: {e}"})
            print(f"  ERROR: {e}")

    optimize_results.append(result)

# COMMAND ----------

# MAGIC %md ## VACUUM — remove old file versions
# MAGIC
# MAGIC Delta Lake keeps old file versions for time travel.
# MAGIC VACUUM removes files older than the retention window.
# MAGIC
# MAGIC Default retention: 7 days (168 hours) — this is the minimum safe value.
# MAGIC Reducing below 168h breaks time travel and is blocked by Delta by default.
# MAGIC
# MAGIC In production: align retention with your SLA for point-in-time recovery.
# MAGIC   - Need to recover data from 30 days ago? Set retention to 720h.
# MAGIC   - Storage cost concern? Keep 168h (minimum) and rely on backups instead.

# COMMAND ----------

vacuum_results = []

for cfg in tables_to_process:
    table = cfg["table"]
    start = datetime.now()

    print(f"\n{'[DRY RUN] ' if DRY_RUN else ''}VACUUM {table} RETAIN {VACUUM_HOURS} HOURS")

    result = {
        "table":  table,
        "status": "skipped (dry run)",
    }

    if not DRY_RUN:
        try:
            spark.sql(f"VACUUM {table} RETAIN {VACUUM_HOURS} HOURS")
            duration = (datetime.now() - start).total_seconds()
            result.update({"status": "OK", "duration_s": round(duration, 1)})
            print(f"  OK  {duration:.1f}s")
        except Exception as e:
            result.update({"status": f"ERROR: {e}"})
            print(f"  ERROR: {e}")

    vacuum_results.append(result)

# COMMAND ----------

# MAGIC %md ## Summary report

# COMMAND ----------

print("\n" + "=" * 65)
print("MAINTENANCE REPORT")
print("=" * 65)

print(f"\n{'[DRY RUN MODE — no changes applied]' if DRY_RUN else ''}")

print("\nOPTIMIZE results:")
print(f"  {'table':<45} {'status':<10} {'duration':>10} {'files saved':>12}")
print(f"  {'-'*45} {'-'*10} {'-'*10} {'-'*12}")
for r in optimize_results:
    dur   = f"{r.get('duration_s', 0):.1f}s" if r.get("duration_s") else "-"
    saved = str(r.get("files_saved", "-"))
    print(f"  {r['table']:<45} {r['status']:<10} {dur:>10} {saved:>12}")

print("\nVACUUM results:")
print(f"  {'table':<45} {'status':<10} {'duration':>10}")
print(f"  {'-'*45} {'-'*10} {'-'*10}")
for r in vacuum_results:
    dur = f"{r.get('duration_s', 0):.1f}s" if r.get("duration_s") else "-"
    print(f"  {r['table']:<45} {r['status']:<10} {dur:>10}")

# Delta history for fact_order_line (most important table)
print(f"\nDelta history — {ENV}_gold.fact_order_line (last 5 operations):")
display(
    spark.sql(f"DESCRIBE HISTORY {ENV}_gold.fact_order_line")
    .select("version", "timestamp", "operation", "operationMetrics")
    .limit(5)
)

ok_count  = sum(1 for r in optimize_results if r["status"] == "OK")
err_count = sum(1 for r in optimize_results if r["status"].startswith("ERROR"))

dbutils.notebook.exit(
    f"success|tables={len(tables_to_process)}|optimized={ok_count}|errors={err_count}|dry_run={DRY_RUN}"
)
