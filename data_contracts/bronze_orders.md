# Data Contract — dev_bronze.orders

## Overview

| Property | Value |
|----------|-------|
| Table | `dev_bronze.orders` |
| Layer | Bronze |
| Owner | Data Engineering |
| Source | Landing Volume — JSON files |
| Ingestion | Auto Loader (cloudFiles) |
| Write mode | Append only — never updated or deleted |
| Partition | `ingest_year`, `ingest_month` |
| SLA freshness | 1 hour |

## Purpose

Raw ingestion layer. Preserves source data exactly as it arrived.
Serves as the immutable audit trail for all downstream processing.
If silver or gold data is found to be incorrect, this table is the
source of truth for reprocessing.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | YES | Business order identifier (e.g. ORD-000001) |
| customer_id | STRING | YES | Business customer identifier (e.g. CUST-0001) |
| product_id | STRING | YES | Business product identifier (e.g. PROD-001) |
| quantity | INTEGER | YES | Number of units ordered |
| unit_price | DOUBLE | YES | Price per unit at time of order |
| total_amount | DOUBLE | YES | Total order value |
| order_date | STRING | YES | Order date as string (typed in silver) |
| status | STRING | YES | Raw order status string |
| updated_at | STRING | YES | Last update timestamp from source |
| _source_file | STRING | NO | Full path of the source file in the Volume |
| _ingested_at | TIMESTAMP | NO | Timestamp when the record was ingested |
| _execution_date | STRING | NO | Pipeline execution date (yyyy-MM-dd) |
| _env | STRING | NO | Environment identifier (dev / hml / prod) |
| ingest_year | INTEGER | NO | Partition column — year of ingestion |
| ingest_month | INTEGER | NO | Partition column — month of ingestion |

## Business Rules

- No transformations applied — data preserved exactly as received
- Duplicate order_ids may exist if source files are re-delivered
- Deduplication happens in the silver layer
- status values are raw strings — normalization happens in silver

## Quality Checks (applied in 06_quality_and_analytics)

| Check | Rule | SLA |
|-------|------|-----|
| volume_ok | rows >= 100 | alert if breached |
| freshness_ok | hours since last ingest <= 1h | alert if breached |
| pk_unique_ok | distinct(order_id) == total rows | alert if breached |
| null_order_id_ok | order_id IS NOT NULL | alert if breached |

## Consumers

| Consumer | Usage |
|----------|-------|
| `03_silver_orders` | reads filtered by `_execution_date` |
| `06_quality_and_analytics` | SLO monitoring |
