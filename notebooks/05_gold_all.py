# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold — Dimensions + fact_order_line (Free Edition)
# MAGIC
# MAGIC Builds the entire gold layer in sequence:
# MAGIC 1. `dim_date`         — programmatically generated (2020-2030)
# MAGIC 2. `dim_customer`     — derived from silver (current version, no real PII)
# MAGIC 3. `dim_product`      — derived from silver products reference table
# MAGIC 4. `fact_order_line`  — silver orders joined with dimensions + ZORDER

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("env",  "dev", "Environment")
dbutils.widgets.text("mode", "incremental", "Mode: full or incremental")

ENV  = dbutils.widgets.get("env")
mode = dbutils.widgets.get("mode")

print(f"ENV  : {ENV}")
print(f"mode : {mode}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_gold")

# COMMAND ----------

# MAGIC %md ## 1. dim_date — programmatic generation (2020-2030)

# COMMAND ----------

import datetime
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DateType,
    StringType, BooleanType
)
from pyspark.sql import functions as F

def build_dim_date(start_year: int, end_year: int):
    """Generates one row per calendar day between start_year and end_year."""
    rows = []
    d    = datetime.date(start_year, 1, 1)
    end  = datetime.date(end_year, 12, 31)

    # Brazilian national holidays (month, day) — fixed dates only
    holidays_br = {
        (1,1),(4,21),(5,1),(9,7),(10,12),(11,2),(11,15),(12,25)
    }

    while d <= end:
        quarter = (d.month - 1) // 3 + 1
        rows.append((
            int(d.strftime("%Y%m%d")),        # date_sk
            d,                                 # full_date
            d.year,
            quarter,
            d.month,
            d.strftime("%B"),                 # month_name
            d.isocalendar()[1],               # week_of_year
            d.weekday() + 1,                  # day_of_week (1=Mon)
            d.strftime("%A"),                 # day_name
            d.weekday() >= 5,                 # is_weekend
            (d.month, d.day) in holidays_br,  # is_holiday_br
            d.strftime("%Y-%m"),              # year_month
            f"{d.year}-Q{quarter}",           # year_quarter
        ))
        d += datetime.timedelta(days=1)

    schema = StructType([
        StructField("date_sk",       IntegerType(), False),
        StructField("full_date",     DateType(),    False),
        StructField("year",          IntegerType(), False),
        StructField("quarter",       IntegerType(), False),
        StructField("month",         IntegerType(), False),
        StructField("month_name",    StringType(),  False),
        StructField("week_of_year",  IntegerType(), False),
        StructField("day_of_week",   IntegerType(), False),
        StructField("day_name",      StringType(),  False),
        StructField("is_weekend",    BooleanType(), False),
        StructField("is_holiday_br", BooleanType(), False),
        StructField("year_month",    StringType(),  False),
        StructField("year_quarter",  StringType(),  False),
    ])
    return spark.createDataFrame(rows, schema)

DIM_DATE = f"{ENV}_gold.dim_date"

dim_date = build_dim_date(2020, 2030)
(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_DATE)
)

print(f"[OK] {DIM_DATE} | {dim_date.count():,} dates (2020-2030)")

# COMMAND ----------

# MAGIC %md ## 2. dim_customer
# MAGIC
# MAGIC Reads only current records from SCD2 silver table.
# MAGIC customer_id is NOT exposed in gold — only the pseudonymized customer_token.
# MAGIC
# MAGIC **No surrogate key (SK):** Kimball-style integer SKs were designed for
# MAGIC legacy columnar databases (Teradata, Netezza, circa 1990-2000) where
# MAGIC integer joins were significantly faster than string joins.
# MAGIC In modern Delta Lake, ZORDER + predicate pushdown + bloom filters make
# MAGIC string joins on customer_token equally performant.
# MAGIC The natural key (customer_token) also serves as the anonymization boundary,
# MAGIC making a separate SK redundant. SCD isolation is handled in the silver layer.

# COMMAND ----------

DIM_CUSTOMER     = f"{ENV}_gold.dim_customer"
SILVER_CUSTOMERS = f"{ENV}_silver.customers_scd2"

dim_customer = (
    spark.read.format("delta").table(SILVER_CUSTOMERS)
    .filter("is_current = true")
    .select(
        "customer_token",  # natural key — stable, anonymous, no SK needed
        "customer_name",
        "email_token",     # tokenized in silver — no raw PII in gold
        "cpf_token",
        "phone_token",
        "country",
        "segment",
        "current_segment",
        "valid_from",
    )
)

(
    dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_CUSTOMER)
)

print(f"[OK] {DIM_CUSTOMER} | {dim_customer.count():,} customers")

# COMMAND ----------

# MAGIC %md ## 3. dim_product

# COMMAND ----------

DIM_PRODUCT     = f"{ENV}_gold.dim_product"
SILVER_PRODUCTS = f"{ENV}_silver.products"

dim_product = (
    spark.read.format("delta").table(SILVER_PRODUCTS)
    .filter("is_current = true")
    .select(
        "product_id",   # natural key — no SK needed in modern lakehouse
        "product_name",
        "brand",
        "category",
        "subcategory",
        "unit_cost",
    )
)

(
    dim_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_PRODUCT)
)

print(f"[OK] {DIM_PRODUCT} | {dim_product.count():,} products")

# COMMAND ----------

# MAGIC %md ## 4. fact_order_line
# MAGIC
# MAGIC Joins silver orders with all three dimensions.
# MAGIC PII is excluded — only customer_token is carried to gold.
# MAGIC Derived metrics: cost_amount, margin_amount, margin_pct.

# COMMAND ----------

GOLD_FACT        = f"{ENV}_gold.fact_order_line"
SILVER_ORDERS    = f"{ENV}_silver.orders"
SILVER_CUSTOMERS = f"{ENV}_silver.customers_scd2"

silver_orders = spark.read.format("delta").table(SILVER_ORDERS)

if mode == "incremental":
    # only process the latest order date available in silver
    max_date      = silver_orders.select(F.max("order_date")).collect()[0][0]
    silver_orders = silver_orders.filter(F.col("order_date") == max_date)
    print(f"Incremental mode: processing date {max_date}")

orders_count = silver_orders.count()
print(f"Orders to process: {orders_count:,}")

if orders_count == 0:
    print("[INFO] No orders to process.")
    dbutils.notebook.exit("success|rows=0")

# bridge: resolve customer_id -> customer_token
# dim_customer no longer carries customer_id (PII excluded from gold)
# silver_customers has both customer_id and customer_token (restricted access layer)
silver_customers_bridge = (
    spark.read.format("delta").table(SILVER_CUSTOMERS)
    .filter("is_current = true")
    .select("customer_id", "customer_token")
)

silver_orders = silver_orders.join(silver_customers_bridge, "customer_id", "left")

# join dimensions using customer_token — no raw customer_id reaches gold
gold_fact = (
    silver_orders.alias("o")
    .join(dim_customer.alias("c"), "customer_token", "left")
    .join(dim_product.alias("p"),  "product_id",     "left")
    # partition columns
    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    # derived financial metrics
    .withColumn("cost_amount",
        F.when(F.col("p.unit_cost").isNotNull(),
               F.col("o.quantity") * F.col("p.unit_cost")
        ).otherwise(F.lit(None))
    )
    .withColumn("margin_amount",
        F.when(F.col("cost_amount").isNotNull(),
               F.col("o.total_amount") - F.col("cost_amount")
        ).otherwise(F.lit(None))
    )
    .withColumn("margin_pct",
        F.when(F.col("o.total_amount") > 0,
               F.round(F.col("margin_amount") / F.col("o.total_amount") * 100, 2)
        ).otherwise(F.lit(None))
    )
    .withColumn("_load_ts", F.current_timestamp())
    # gold layer: expose customer_token only — no raw customer_id
    .select(
        F.col("o.order_id"),
        F.col("customer_token"),                        # join key — no alias needed
        F.col("o.product_id"),
        F.col("c.segment").alias("customer_segment"),
        F.col("c.current_segment"),
        F.col("c.country").alias("customer_country"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("p.subcategory"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.total_amount"),
        F.col("cost_amount"),
        F.col("margin_amount"),
        F.col("margin_pct"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("order_year"),
        F.col("order_month"),
        F.col("_load_ts"),
    )
)

# create table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_FACT} (
        order_id         STRING
      , customer_token   STRING
      , product_id       STRING
      , customer_segment STRING
      , current_segment  STRING
      , customer_country STRING
      , product_name     STRING
      , brand            STRING
      , category         STRING
      , subcategory      STRING
      , quantity         INT
      , unit_price       DOUBLE
      , total_amount     DOUBLE
      , cost_amount      DOUBLE
      , margin_amount    DOUBLE
      , margin_pct       DOUBLE
      , order_date       DATE
      , status           STRING
      , order_year       INT
      , order_month      INT
      , _load_ts         TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (order_year, order_month)
""")

write_mode = "overwrite" if mode == "full" else "append"

(
    gold_fact.write
    .format("delta")
    .mode(write_mode)
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("order_year", "order_month")
    .saveAsTable(GOLD_FACT)
)

print(f"[OK] {GOLD_FACT} written | mode={write_mode} | {gold_fact.count():,} rows")

# COMMAND ----------

# MAGIC %md ## OPTIMIZE + ZORDER (full load only)
# MAGIC
# MAGIC ZORDER co-locates data by the most common filter/join columns,
# MAGIC reducing files read by subsequent queries.

# COMMAND ----------

if mode == "full":
    print("Running OPTIMIZE + ZORDER...")
    spark.sql(f"OPTIMIZE {GOLD_FACT} ZORDER BY (customer_token, order_date, category)")
    print("[OK] OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md ## PII verification + revenue summary

# COMMAND ----------

final = spark.read.format("delta").table(GOLD_FACT)

print(f"\n[OK] {GOLD_FACT}")
print(f"     Total rows          : {final.count():,}")
print(f"     customer_id in gold : {'customer_id' in final.columns}")
print(f"     customer_token      : {'customer_token' in final.columns}")

print("\nRevenue by customer segment:")
display(
    final.groupBy("customer_segment")
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.count("*").alias("orders"),
        F.round(F.avg("margin_pct"), 2).alias("avg_margin_pct"),
    )
    .orderBy(F.col("revenue").desc())
)

dbutils.notebook.exit(f"success|rows={final.count()}")
