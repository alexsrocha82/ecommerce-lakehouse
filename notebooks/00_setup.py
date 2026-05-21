# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Ambiente Databricks Free Edition
# MAGIC
# MAGIC Execute este notebook **uma única vez** antes de qualquer outro.
# MAGIC
# MAGIC O que ele faz:
# MAGIC - Cria os schemas (bronze, silver, gold, quarantine, governance)
# MAGIC - Cria pastas no DBFS para simular o landing zone
# MAGIC - Gera dados sintéticos de pedidos e clientes
# MAGIC - Valida que tudo está pronto para os próximos notebooks
# MAGIC
# MAGIC > **Nota:** No lugar do ADLS usamos o DBFS (`dbfs:/FileStore/ecommerce/`).
# MAGIC > No Free Edition o DBFS é o storage disponível — comportamento idêntico
# MAGIC > ao ADLS para fins de aprendizado com Delta Lake e Auto Loader.

# COMMAND ----------

# MAGIC %md ## 1. Parâmetros globais

# COMMAND ----------

# Ambiente — mude para "hml" ou "prod" quando quiser simular outros ambientes
ENV = "dev"

# Caminhos base no DBFS (substitui ADLS no Free Edition)
BASE_PATH    = f"dbfs:/FileStore/ecommerce/{ENV}"
LANDING_ORDERS    = f"{BASE_PATH}/landing/orders"
LANDING_CUSTOMERS = f"{BASE_PATH}/landing/customers"
CHECKPOINT_BASE   = f"{BASE_PATH}/checkpoints"

print(f"ENV          : {ENV}")
print(f"BASE_PATH    : {BASE_PATH}")
print(f"Orders       : {LANDING_ORDERS}")
print(f"Customers    : {LANDING_CUSTOMERS}")

# COMMAND ----------

# MAGIC %md ## 2. Criar schemas no Unity Catalog / Hive Metastore

# COMMAND ----------

# O Free Edition usa Hive Metastore por padrão (ou Unity Catalog se habilitado)
# Os schemas abaixo simulam as camadas da arquitetura Medallion

for schema in ["bronze", "silver", "gold", "quarantine", "governance"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ENV}_{schema}")
    print(f"Schema criado: {ENV}_{schema}")

# alias para facilitar referência nos outros notebooks
# ex: bronze_orders fica em dev_bronze.orders

# COMMAND ----------

# MAGIC %md ## 3. Criar pastas no DBFS (simula landing zone)

# COMMAND ----------

import os

paths = [
    LANDING_ORDERS,
    LANDING_CUSTOMERS,
    f"{CHECKPOINT_BASE}/orders/bronze",
    f"{CHECKPOINT_BASE}/orders/schema",
    f"{CHECKPOINT_BASE}/customers/bronze",
    f"{CHECKPOINT_BASE}/customers/schema",
]

for path in paths:
    dbutils.fs.mkdirs(path)
    print(f"Pasta criada: {path}")

# COMMAND ----------

# MAGIC %md ## 4. Gerar dados sintéticos — Clientes

# COMMAND ----------

import json
import random
import uuid
from datetime import date, timedelta

random.seed(42)

SEGMENTS  = ["VIP", "Standard", "New", "Churned"]
COUNTRIES = ["BR", "AR", "CL", "MX", "CO", "PE"]
DOMAINS   = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com"]

def random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    delta = random.randint(end_days_ago, start_days_ago)
    return (date.today() - timedelta(days=delta)).isoformat()

def gen_customers(n: int = 200) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        name_parts = [
            random.choice(["Ana","Bruno","Carlos","Diana","Eduardo","Fernanda",
                           "Gabriel","Helena","Igor","Julia","Lucas","Maria",
                           "Nicolas","Olivia","Pedro","Renata","Sofia","Thiago"]),
            random.choice(["Silva","Santos","Oliveira","Souza","Lima","Costa",
                           "Pereira","Ferreira","Alves","Rodrigues"])
        ]
        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_name": " ".join(name_parts),
            "email": f"user{i}@{random.choice(DOMAINS)}",
            "cpf": f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(100,999)}-{random.randint(10,99):02d}",
            "phone": f"({random.randint(11,99)}) 9{random.randint(1000,9999)}-{random.randint(1000,9999)}",
            "segment": random.choice(SEGMENTS),
            "country": random.choice(COUNTRIES),
            "created_at": random_date(365, 30),
            "updated_at": random_date(30, 0),
        })
    return customers

customers = gen_customers(200)

# Salva em 2 arquivos JSON para simular chegada em lotes
batch1 = customers[:100]
batch2 = customers[100:]

for i, batch in enumerate([batch1, batch2], 1):
    content   = "\n".join(json.dumps(c) for c in batch)
    file_path = f"{LANDING_CUSTOMERS}/customers_batch_{i:02d}.json"
    dbutils.fs.put(file_path, content, overwrite=True)

print(f"Clientes gerados: {len(customers)}")
print(f"  Batch 1: {len(batch1)} registros → customers_batch_01.json")
print(f"  Batch 2: {len(batch2)} registros → customers_batch_02.json")

# COMMAND ----------

# MAGIC %md ## 5. Gerar dados sintéticos — Pedidos

# COMMAND ----------

PRODUCTS = [
    {"id": "PROD-001", "name": "Notebook Pro", "category": "Electronics",
     "subcategory": "Computers", "brand": "TechBrand", "price_range": (2500, 8000)},
    {"id": "PROD-002", "name": "Smartphone X",  "category": "Electronics",
     "subcategory": "Phones",    "brand": "PhoneCo",   "price_range": (800, 3500)},
    {"id": "PROD-003", "name": "Headphones BT", "category": "Electronics",
     "subcategory": "Audio",     "brand": "SoundPro",  "price_range": (150, 900)},
    {"id": "PROD-004", "name": "Running Shoes", "category": "Sports",
     "subcategory": "Footwear",  "brand": "RunFast",   "price_range": (200, 600)},
    {"id": "PROD-005", "name": "Coffee Maker",  "category": "Home",
     "subcategory": "Kitchen",   "brand": "HomePro",   "price_range": (100, 400)},
    {"id": "PROD-006", "name": "Gaming Chair",  "category": "Furniture",
     "subcategory": "Gaming",    "brand": "ComfortX",  "price_range": (600, 2000)},
    {"id": "PROD-007", "name": "Yoga Mat",      "category": "Sports",
     "subcategory": "Fitness",   "brand": "FitLife",   "price_range": (50, 200)},
    {"id": "PROD-008", "name": "Smart Watch",   "category": "Electronics",
     "subcategory": "Wearables", "brand": "TimeTech",  "price_range": (400, 1500)},
]

STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [0.05, 0.10, 0.15, 0.60, 0.10]

def gen_orders(n: int = 500) -> list[dict]:
    orders = []
    customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]

    for i in range(1, n + 1):
        product     = random.choice(PRODUCTS)
        qty         = random.randint(1, 5)
        unit_price  = round(random.uniform(*product["price_range"]), 2)
        total       = round(qty * unit_price * random.uniform(0.85, 1.0), 2)
        order_date  = random_date(180, 0)

        orders.append({
            "order_id":    f"ORD-{i:06d}",
            "customer_id": random.choice(customer_ids),
            "product_id":  product["id"],
            "quantity":    qty,
            "unit_price":  unit_price,
            "total_amount": total,
            "order_date":  order_date,
            "status":      random.choices(STATUSES, STATUS_WEIGHTS)[0],
            "updated_at":  order_date,
        })
    return orders

orders = gen_orders(500)

# Salva em 3 arquivos para simular ingestão incremental
for i, batch in enumerate([orders[:200], orders[200:400], orders[400:]], 1):
    content   = "\n".join(json.dumps(o) for o in batch)
    file_path = f"{LANDING_ORDERS}/orders_batch_{i:02d}.json"
    dbutils.fs.put(file_path, content, overwrite=True)

print(f"Pedidos gerados: {len(orders)}")
print(f"  Batch 1: 200 | Batch 2: 200 | Batch 3: 100")

# COMMAND ----------

# MAGIC %md ## 6. Gerar tabela de produtos (silver.products mock)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

products_rows = [
    (p["id"], p["name"], p["brand"], p["category"], p["subcategory"],
     round((p["price_range"][0] + p["price_range"][1]) / 2 * 0.55, 2),
     True)
    for p in PRODUCTS
]

schema = StructType([
    StructField("product_id",   StringType(),  False),
    StructField("product_name", StringType(),  False),
    StructField("brand",        StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("subcategory",  StringType(),  True),
    StructField("unit_cost",    DoubleType(),  True),
    StructField("is_current",   BooleanType(), False),
])

products_df = spark.createDataFrame(products_rows, schema)

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{ENV}_silver.products")

print(f"Produtos salvos em {ENV}_silver.products: {products_df.count()} linhas")

# COMMAND ----------

# MAGIC %md ## 7. Validação final

# COMMAND ----------

print("\n" + "="*55)
print("SETUP CONCLUÍDO — VALIDAÇÃO")
print("="*55)

# arquivos no landing
for label, path in [("Orders landing", LANDING_ORDERS), ("Customers landing", LANDING_CUSTOMERS)]:
    files = dbutils.fs.ls(path)
    print(f"\n{label}:")
    for f in files:
        print(f"  {f.name:40s} {f.size/1024:.1f} KB")

# schemas
print("\nSchemas criados:")
for schema in ["bronze", "silver", "gold", "quarantine", "governance"]:
    print(f"  {ENV}_{schema}")

print("\n[OK] Pronto para executar os notebooks 01 → 07")
print(f"     Lembre de usar ENV = '{ENV}' em todos os notebooks")
