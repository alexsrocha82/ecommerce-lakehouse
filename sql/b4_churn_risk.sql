-- ============================================================
-- B4 — Churn Risk Segmentation (Relative Threshold)
-- ============================================================
-- Source : dev_gold.fact_order_line
-- Window : LAG per customer (order gap calculation)
-- Output : churn_risk bucket per customer
-- ============================================================
-- Key design decision — RELATIVE threshold (not fixed):
--   A customer who buys every 7 days with 21 days gap = High risk
--   A customer who buys every 90 days with 21 days gap = Active
--   Fixed 90-day rules miss both cases — relative cadence is better.
-- ============================================================
-- Risk buckets:
--   High   : days_since_last > avg_cadence_days × 3
--   Medium : days_since_last > avg_cadence_days × 2
--   Watch  : days_since_last > 30 (regardless of cadence)
--   Active : none of the above
-- ============================================================

with order_gaps as (
    select
          customer_token
        , current_segment
        , order_date
        , total_amount
        -- days between this order and the previous order per customer
        , datediff(
            order_date,
            lag(order_date) over (
                partition by customer_token
                order by order_date
            )
          ) as days_since_prev
    from dev_gold.fact_order_line
)

, customer_cadence as (
    select
          customer_token
        , current_segment
        , max(order_date)              as last_order
        , avg(days_since_prev)         as avg_cadence_days
        , count(*)                     as total_orders
        , sum(total_amount)            as total_spent
    from order_gaps
    group by
          customer_token
        , current_segment
)

select
      customer_token
    , current_segment
    , last_order
    , round(avg_cadence_days, 1)       as avg_cadence_days
    , total_orders
    , round(total_spent, 2)            as total_spent
    , datediff(current_date(), last_order) as days_since_last
    , case
        when datediff(current_date(), last_order) > avg_cadence_days * 3 then 'High'
        when datediff(current_date(), last_order) > avg_cadence_days * 2 then 'Medium'
        when datediff(current_date(), last_order) > 30                   then 'Watch'
        else                                                                   'Active'
      end                              as churn_risk
from customer_cadence
order by days_since_last desc
;
