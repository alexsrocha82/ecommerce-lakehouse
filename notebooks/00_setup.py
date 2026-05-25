# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Databricks Free Edition Environment
# MAGIC
# MAGIC Run this notebook **once** before any other.
# MAGIC
# MAGIC What it does:
# MAGIC - Creates the `ecommerce_dev` schema and `landing` Volume in catalog `main`
# MAGIC - Creates Medallion schemas (bronze, silver, gold, quarantine, governance)
# MAGIC - Generates synthetic orders and customers data
# MAGIC - Validates the environment is ready for notebooks 01-06

# COMMAND ----------

# MAGIC %md ## 1. Global parameters

# COMMAND ----------

# Environment — change to "hml" or "prod" to simulate other environments
ENV = "dev"

# Unity Catalog: catalog and schema for Volumes (file storage)
UC_CATALOG       = "main"
UC_VOLUME_SCHEMA = f"ecommerce_{ENV}"   # main.ecommerce_dev
UC_VOLUME_NAME   = "landing"            # main.ecommerce_dev.landing

# Paths via UC Volumes (replaces dbfs:/FileStore/ — disabled in Free Edition)
VOLUME_BASE       = f"/Volumes/{UC_CATALOG}/{UC_VOLUME_SCHEMA}/{UC_VOLUME_NAME}"
LANDING_ORDERS    = f"{VOLUME_BASE}/orders"
LANDING_CUSTOMERS = f"{VOLUME_BASE}/customers"
CHECKPOINT_BASE   = f"{VOLUME_BASE}/checkpoints"

# Medallion layer schemas
SCHEMAS = ["bronze", "silver", "gold", "quarantine", "governance"]

print(f"ENV              : {ENV}")
print(f"UC Catalog       : {UC_CATALOG}")
print(f"Volume path      : {VOLUME_BASE}")
print(f"Landing orders   : {LANDING_ORDERS}")
print(f"Landing customers: {LANDING_CUSTOMERS}")

# COMMAND ----------

# MAGIC %md ## 2. Create schema + Volume in Unity Catalog

# COMMAND ----------

# Schema that hosts the Volume for raw files
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_VOLUME_SCHEMA}")
print(f"Schema created: {UC_CATALOG}.{UC_VOLUME_SCHEMA}")

# Managed Volume — UC handles the underlying storage
# Functional equivalent of an ADLS container for Auto Loader purposes
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_VOLUME_SCHEMA}.{UC_VOLUME_NAME}
""")
print(f"Volume created: {UC_CATALOG}.{UC_VOLUME_SCHEMA}.{UC_VOLUME_NAME}")
print(f"Accessible path: {VOLUME_BASE}")

# COMMAND ----------

# MAGIC %md ## 3. Create Medallion schemas in catalog main

# COMMAND ----------

# Delta table schemas live under main.<env>_<layer>
# e.g.: main.dev_bronze.orders, main.dev_silver.orders
for layer in SCHEMAS:
    schema_fqn = f"{UC_CATALOG}.{ENV}_{layer}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    print(f"Schema created: {schema_fqn}")

# COMMAND ----------

# MAGIC %md ## 4. Create folder structure inside the Volume

# COMMAND ----------

dirs_to_create = [
    LANDING_ORDERS,
    LANDING_CUSTOMERS,
    f"{CHECKPOINT_BASE}/orders/bronze",
    f"{CHECKPOINT_BASE}/orders/schema",
    f"{CHECKPOINT_BASE}/customers/bronze",
    f"{CHECKPOINT_BASE}/customers/schema",
]

for path in dirs_to_create:
    dbutils.fs.mkdirs(path)
    print(f"Directory created: {path}")

# COMMAND ----------

# MAGIC %md ## 5. Generate synthetic data — Customers

# COMMAND ----------

import json
import random
from datetime import date, timedelta

random.seed(42)

SEGMENTS  = ["VIP", "Standard", "New", "Churned"]
COUNTRIES = ["BR", "AR", "CL", "MX", "CO", "PE"]
DOMAINS   = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com"]

def random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    delta = random.randint(end_days_ago, start_days_ago)
    return (date.today() - timedelta(days=delta)).isoformat()

def gen_customers(n: int = 200) -> list:
    customers = []
    for i in range(1, n + 1):
        name_parts = [
            random.choice([
                "Ana","Bruno","Carlos","Diana","Eduardo","Fernanda",
                "Gabriel","Helena","Igor","Julia","Lucas","Maria",
                "Nicolas","Olivia","Pedro","Renata","Sofia","Thiago"
            ]),
            random.choice([
                "Silva","Santos","Oliveira","Souza","Lima","Costa",
                "Pereira","Ferreira","Alves","Rodrigues"
            ]),
        ]
        customers.append({
            "customer_id":   f"CUST-{i:04d}",
            "customer_name": " ".join(name_parts),
            "email":         f"user{i}@{random.choice(DOMAINS)}",
            "cpf":           (
                f"{random.randint(100,999)}."
                f"{random.randint(100,999)}."
                f"{random.randint(100,999)}-"
                f"{random.randint(10,99):02d}"
            ),
            "phone":      f"({random.randint(11,99)}) 9{random.randint(1000,9999)}-{random.randint(1000,9999)}",
            "segment":    random.choice(SEGMENTS),
            "country":    random.choice(COUNTRIES),
            "created_at": random_date(365, 30),
            "updated_at": random_date(30, 0),
        })
    return customers

customers      = gen_customers(200)
batch1, batch2 = customers[:100], customers[100:]

for idx, batch in enumerate([batch1, batch2], 1):
    content   = "\n".join(json.dumps(c) for c in batch)
    file_path = f"{LANDING_CUSTOMERS}/customers_batch_{idx:02d}.json"
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Written: {file_path}  ({len(batch)} records)")

print(f"\nTotal customers generated: {len(customers)}")

# COMMAND ----------

# MAGIC %md ## 6. Generate synthetic data — Orders

# COMMAND ----------

PRODUCTS = [
    {"id": "PROD-001", "name": "Notebook Pro",  "category": "Electronics", "subcategory": "Computers", "brand": "TechBrand", "price_range": (2500, 8000)},
    {"id": "PROD-002", "name": "Smartphone X",  "category": "Electronics", "subcategory": "Phones",    "brand": "PhoneCo",   "price_range": (800,  3500)},
    {"id": "PROD-003", "name": "Headphones BT", "category": "Electronics", "subcategory": "Audio",     "brand": "SoundPro",  "price_range": (150,  900)},
    {"id": "PROD-004", "name": "Running Shoes", "category": "Sports",      "subcategory": "Footwear",  "brand": "RunFast",   "price_range": (200,  600)},
    {"id": "PROD-005", "name": "Coffee Maker",  "category": "Home",        "subcategory": "Kitchen",   "brand": "HomePro",   "price_range": (100,  400)},
    {"id": "PROD-006", "name": "Gaming Chair",  "category": "Furniture",   "subcategory": "Gaming",    "brand": "ComfortX",  "price_range": (600,  2000)},
    {"id": "PROD-007", "name": "Yoga Mat",      "category": "Sports",      "subcategory": "Fitness",   "brand": "FitLife",   "price_range": (50,   200)},
    {"id": "PROD-008", "name": "Smart Watch",   "category": "Electronics", "subcategory": "Wearables", "brand": "TimeTech",  "price_range": (400,  1500)},
]

STATUSES       = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [0.05, 0.10, 0.15, 0.60, 0.10]

def gen_orders(n: int = 500) -> list:
    orders       = []
    customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]

    for i in range(1, n + 1):
        product    = random.choice(PRODUCTS)
        qty        = random.randint(1, 5)
        unit_price = round(random.uniform(*product["price_range"]), 2)
        total      = round(qty * unit_price * random.uniform(0.85, 1.0), 2)
        order_date = random_date(180, 0)

        orders.append({
            "order_id":     f"ORD-{i:06d}",
            "customer_id":  random.choice(customer_ids),
            "product_id":   product["id"],
            "quantity":     qty,
            "unit_price":   unit_price,
            "total_amount": total,
            "order_date":   order_date,
            "status":       random.choices(STATUSES, STATUS_WEIGHTS)[0],
            "updated_at":   order_date,
        })
    return orders

orders  = gen_orders(500)
batches = [orders[:200], orders[200:400], orders[400:]]

for idx, batch in enumerate(batches, 1):
    content   = "\n".join(json.dumps(o) for o in batch)
    file_path = f"{LANDING_ORDERS}/orders_batch_{idx:02d}.json"
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Written: {file_path}  ({len(batch)} records)")

print(f"\nTotal orders generated: {len(orders)}")

# COMMAND ----------

# MAGIC %md ## 7. Create products reference table (dev_silver.products)
# MAGIC
# MAGIC Products are a static reference entity — they are created directly in silver
# MAGIC because they arrive already clean and structured (no bronze ingestion needed).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

products_rows = [
    (
        p["id"],
        p["name"],
        p["brand"],
        p["category"],
        p["subcategory"],
        round((p["price_range"][0] + p["price_range"][1]) / 2 * 0.55, 2),
        True,
    )
    for p in PRODUCTS
]

products_schema = StructType([
    StructField("product_id",   StringType(),  False),
    StructField("product_name", StringType(),  False),
    StructField("brand",        StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("subcategory",  StringType(),  True),
    StructField("unit_cost",    DoubleType(),  True),
    StructField("is_current",   BooleanType(), False),
])

products_df = spark.createDataFrame(products_rows, products_schema)

(
    products_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ENV}_silver.products")
)

print(f"Products saved to {ENV}_silver.products: {products_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## 8. Final validation

# COMMAND ----------

print("\n" + "=" * 60)
print("SETUP COMPLETE — VALIDATION")
print("=" * 60)

# Volume: landing files
print("\n[VOLUMES]")
for label, path in [
    ("Orders landing",    LANDING_ORDERS),
    ("Customers landing", LANDING_CUSTOMERS),
]:
    try:
        files    = dbutils.fs.ls(path)
        total_kb = sum(f.size for f in files) / 1024
        print(f"\n  {label}  ({len(files)} file(s), {total_kb:.1f} KB total):")
        for f in files:
            print(f"    {f.name:45s} {f.size/1024:6.1f} KB")
    except Exception as e:
        print(f"  [ERROR] {label}: {e}")

# Medallion schemas
print("\n[MEDALLION SCHEMAS]")
for layer in SCHEMAS:
    schema_fqn = f"{ENV}_{layer}"
    try:
        spark.sql(f"SHOW TABLES IN {schema_fqn}").count()
        print(f"  OK  {schema_fqn}")
    except Exception as e:
        print(f"  ERROR  {schema_fqn}: {e}")

# Products table
print("\n[PRODUCTS TABLE]")
try:
    cnt = spark.read.format("delta").table(f"{ENV}_silver.products").count()
    print(f"  OK  {ENV}_silver.products  ({cnt} rows)")
except Exception as e:
    print(f"  ERROR  {ENV}_silver.products: {e}")

print(f"""
[OK] Environment ready!

  Volume path      : {VOLUME_BASE}
  Landing orders   : {LANDING_ORDERS}
  Landing customers: {LANDING_CUSTOMERS}
  Checkpoints      : {CHECKPOINT_BASE}

  Next step: run notebooks 01 through 06 in order.
  Remember to set ENV = '{ENV}' in each notebook widget.
""")

