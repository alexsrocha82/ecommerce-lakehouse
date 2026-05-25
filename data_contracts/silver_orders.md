# Data Contract — dev_silver.orders

## Overview

| Property | Value |
|----------|-------|
| Table | `dev_silver.orders` |
| Layer | Silver |
| Owner | Data Engineering |
| Source | `dev_bronze.orders` |
| Write mode | MERGE (incremental, idempotent) |
| Partition | `order_year`, `order_month` |
| SLA freshness | 2 hours |

## Purpose

Typed, deduplicated, and quality-validated orders. The MERGE pattern
ensures idempotency — running the same pipeline twice produces the
same result. Failed records are isolated in `dev_quarantine.orders`.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | NO | Business order identifier — primary key |
| customer_id | STRING | NO | Business customer identifier |
| product_id | STRING | NO | Business product identifier |
| quantity | INT | YES | Number of units ordered |
| unit_price | DOUBLE | YES | Price per unit at time of order |
| total_amount | DOUBLE | YES | Total order value |
| order_date | DATE | YES | Order date — typed from source string |
| status | STRING | YES | Normalized status (UPPER TRIM applied) |
| updated_at | TIMESTAMP | YES | Last update timestamp from source |
| _load_date | TIMESTAMP | NO | Timestamp when record was written to silver |
| order_year | INT | NO | Partition column — year of order_date |
| order_month | INT | NO | Partition column — month of order_date |

## Business Rules

- `order_id` is the primary key — one row per order
- Deduplication: when the same order_id arrives multiple times,
  the most recently ingested record wins (ordered by `_ingested_at`)
- MERGE update condition: `status <> s.status OR total_amount <> s.total_amount`
- Only status and total_amount can change — other fields are immutable after insert
- `status` values: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED

## Quality Rules (applied in 03_silver_orders)

| Rule | Dimension | Critical | Action on failure |
|------|-----------|----------|-------------------|
| order_id IS NOT NULL | completeness | YES | pipeline halts |
| customer_id IS NOT NULL | completeness | YES | pipeline halts |
| total_amount > 0 | validity | no | row → quarantine |
| quantity > 0 | validity | no | row → quarantine |
| status IN (5 values) | validity | no | row → quarantine |
| order_date <= today | validity | no | row → quarantine |

**Quality score threshold:** 95% — below this triggers FAIL status in governance log.

## MERGE Strategy

```sql
MERGE ON order_id = s.order_id
         AND order_year  = s.order_year
         AND order_month = s.order_month
WHEN MATCHED AND (status changed OR amount changed) → UPDATE
WHEN NOT MATCHED → INSERT ALL
```

Partition columns included in join predicate to enable partition pruning
(avoids full table scan on every merge).

## Consumers

| Consumer | Usage |
|----------|-------|
| `05_gold_all` | source for fact_order_line |
| `06_quality_and_analytics` | SLO monitoring + analytics queries |
| `07_optimize` | weekly OPTIMIZE + ZORDER |
