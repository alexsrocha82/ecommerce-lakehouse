# Data Contract — dev_gold.fact_order_line

## Overview

| Property | Value |
|----------|-------|
| Table | `dev_gold.fact_order_line` |
| Layer | Gold |
| Owner | Data Engineering |
| Source | `dev_silver.orders` + `dev_silver.customers_scd2` + `dev_silver.products` |
| Write mode | Full (overwrite dynamic partition) or Incremental (append) |
| Partition | `order_year`, `order_month` |
| ZORDER | `customer_token`, `order_date`, `category` |
| SLA freshness | 2 hours |

## Purpose

Main analytical fact table for the star schema. No PII — only
customer_token. No surrogate keys — natural keys used directly
(Delta Lake + ZORDER makes string joins equally performant to
integer joins). Derived financial metrics pre-computed for analytics.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | YES | Business order identifier |
| customer_token | STRING | YES | Anonymized customer key — joins to dim_customer |
| product_id | STRING | YES | Product identifier — joins to dim_product |
| customer_segment | STRING | YES | Segment at time of order (from SCD2) |
| current_segment | STRING | YES | Customer's segment today |
| customer_country | STRING | YES | Customer country |
| product_name | STRING | YES | Product display name |
| brand | STRING | YES | Product brand |
| category | STRING | YES | Product category |
| subcategory | STRING | YES | Product subcategory |
| quantity | INT | YES | Units ordered |
| unit_price | DOUBLE | YES | Price per unit |
| total_amount | DOUBLE | YES | Total order value |
| cost_amount | DOUBLE | YES | Total cost (quantity × unit_cost) |
| margin_amount | DOUBLE | YES | Gross margin (total_amount − cost_amount) |
| margin_pct | DOUBLE | YES | Margin percentage (margin_amount / total_amount × 100) |
| order_date | DATE | YES | Order date |
| status | STRING | YES | Order status |
| order_year | INT | NO | Partition column |
| order_month | INT | NO | Partition column |
| _load_ts | TIMESTAMP | NO | When this record was written to gold |

## Business Rules

### No PII in Gold
- `customer_id` is excluded — only `customer_token` is exposed
- Analysts cannot join this table with external CRM systems to
  reconstruct full PII — the token is a joinability firewall
- `email_token`, `cpf_token`, `phone_token` are not included in fact

### No Surrogate Keys
Natural keys used directly. See README for full reasoning.

### Financial Metrics
```
cost_amount   = quantity × unit_cost (from dim_product)
margin_amount = total_amount − cost_amount
margin_pct    = round(margin_amount / total_amount × 100, 2)
unit_cost     = (price_min + price_max) / 2 × 0.55
```
The 0.55 factor simulates cost = 55% of average selling price
(45% gross margin target).

### Write Modes
- `full`: reads all silver orders, overwrites partitions dynamically,
  runs OPTIMIZE + ZORDER at end
- `incremental`: reads only latest order_date from silver, appends

## Analytics Queries

See `/sql/` folder for ready-to-run queries:
- `b1_monthly_revenue_mom.sql` — MoM growth + YTD
- `b2_customer_ranking.sql` — DENSE_RANK top 10
- `b3_revenue_by_category.sql` — % of total per category
- `b4_churn_risk.sql` — relative churn risk segmentation

## Consumers

| Consumer | Usage |
|----------|-------|
| `06_quality_and_analytics` | all B1–B4 analytics queries |
| `07_optimize` | weekly OPTIMIZE + ZORDER + VACUUM |
| Power BI / Synapse | analytical dashboards |
