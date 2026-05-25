# Data Contract — dev_silver.customers_scd2

## Overview

| Property | Value |
|----------|-------|
| Table | `dev_silver.customers_scd2` |
| Layer | Silver |
| Owner | Data Engineering |
| Source | `dev_bronze.customers` |
| Write mode | SCD Type 2+6 (three-step MERGE + anti-join + MERGE) |
| Partition | `country` |
| SLA freshness | 4 hours |

## Purpose

Full customer history with PII tokenization. Implements SCD Type 2
(new row per attribute change) combined with Type 6 (current_segment
overwrite on all rows). No raw PII stored — all sensitive fields
replaced with deterministic SHA-256 tokens.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_token | STRING | NO | SHA-256 token of customer_id — primary anonymous key |
| customer_id | STRING | NO | Original customer identifier — for audit only (restricted) |
| customer_name | STRING | YES | Customer full name |
| email_token | STRING | YES | SHA-256 token of normalized email address |
| cpf_token | STRING | YES | SHA-256 token of CPF |
| phone_token | STRING | YES | SHA-256 token of phone number |
| country | STRING | YES | Country code (partition key) |
| segment | STRING | YES | Customer segment at this specific time period |
| current_segment | STRING | YES | Current segment today — updated on all rows (Type 6) |
| is_current | BOOLEAN | NO | True = active record, False = historical |
| valid_from | TIMESTAMP | NO | When this version became active |
| valid_to | TIMESTAMP | YES | When this version was superseded (NULL if current) |
| record_source | STRING | YES | Source system identifier |
| _load_date | TIMESTAMP | YES | Timestamp when record was written to silver |

## Business Rules

### PII Tokenization
All sensitive fields are replaced with SHA-256 tokens:
```python
token = SHA256(f"{SALT}:{value}").hexdigest()[:16]
```
- Same salt produces the same token for the same input value (deterministic)
- Reversible only by someone who holds the salt
- In production: salt stored in Azure Key Vault

### SCD Type 2+6 — Three Steps
1. **Close changed records**: if email_token, segment, country, or
   customer_name changed → set `is_current=False`, `valid_to=NOW()`
2. **Insert new versions**: anti-join inserts rows with no active version
   (covers new customers and customers closed in step 1)
3. **Sync current_segment**: updates `current_segment` on ALL rows
   for each customer_id (historical + current)

### segment vs current_segment
- `segment` = value during that specific time period (point-in-time)
- `current_segment` = the customer's segment today (always up to date)

### Tracked attributes (trigger new SCD2 version)
email_token, segment, country, customer_name

## Point-in-time query pattern

```sql
-- what segment was this customer in on a specific date?
select segment
from dev_silver.customers_scd2
where customer_id = 'CUST-0001'
  and valid_from <= '2024-01-15'
  and (valid_to > '2024-01-15' or valid_to is null)
```

## Consumers

| Consumer | Usage |
|----------|-------|
| `05_gold_all` | dim_customer (current records only) |
| `05_gold_all` | bridge lookup: customer_id → customer_token for fact join |
| `06_quality_and_analytics` | SLO monitoring |
| `07_optimize` | weekly OPTIMIZE + ZORDER |
