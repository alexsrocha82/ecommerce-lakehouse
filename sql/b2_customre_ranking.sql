-- ============================================================
-- B2 — Customer Revenue Ranking (Top 10 with DENSE_RANK)
-- ============================================================
-- Source : dev_gold.fact_order_line
-- Window : DENSE_RANK + PERCENT_RANK (global, no partition)
-- Output : revenue_rank, vip_tier, ltm_revenue, avg_ticket
-- ============================================================
-- Note on DENSE_RANK vs RANK:
--   RANK      : ties produce gaps  → 1, 1, 3, 4 (skips 2)
--   DENSE_RANK: ties produce no gap → 1, 1, 2, 3 (fairer for revenue ranking)
-- ============================================================

with customer_revenue as (
    select
          customer_token
        , current_segment
        , customer_country
        , sum(total_amount)         as ltm_revenue
        , count(*)                  as total_orders
        , round(avg(total_amount), 2) as avg_ticket
        , max(order_date)           as last_order
    from dev_gold.fact_order_line
    group by
          customer_token
        , current_segment
        , customer_country
)

, ranked as (
    select
          customer_token
        , current_segment
        , customer_country
        , ltm_revenue
        , total_orders
        , avg_ticket
        , last_order
        -- rank by revenue descending (no partition = global ranking)
        , dense_rank()   over (order by ltm_revenue desc) as revenue_rank
        , percent_rank() over (order by ltm_revenue desc) as revenue_pct_rank
    from customer_revenue
)

select
      revenue_rank
    , customer_token
    , current_segment
    , customer_country
    , ltm_revenue
    , total_orders
    , avg_ticket
    , last_order
    , round(revenue_pct_rank, 4) as revenue_pct_rank
    -- VIP tier classification based on revenue percentile
    , case
        when revenue_pct_rank >= 0.99 then 'VVVIP'
        when revenue_pct_rank >= 0.95 then 'VVIP'
        when revenue_pct_rank >= 0.80 then 'VIP'
        else                               'Standard'
      end                        as vip_tier
from ranked
where revenue_rank <= 10
order by revenue_rank
;
