-- ============================================================
-- B1 — Monthly Revenue with MoM Growth and YTD Accumulation
-- ============================================================
-- Source : dev_gold.fact_order_line
-- Window : LAG (MoM) + rowsBetween cumulative (YTD)
-- Output : monthly_revenue, prev_month_rev, mom_growth_pct, ytd_revenue
-- ============================================================

with monthly as (
    select
          order_year
        , order_month
        , customer_segment
        , sum(total_amount)              as monthly_revenue
        , count(*)                       as orders
        , count(distinct customer_token) as unique_customers
        , round(avg(margin_pct), 2)      as avg_margin_pct
    from dev_gold.fact_order_line
    group by
          order_year
        , order_month
        , customer_segment
)

, enriched as (
    select
          order_year
        , order_month
        , customer_segment
        , monthly_revenue
        , orders
        , unique_customers
        , avg_margin_pct
        -- previous month revenue per segment (MoM)
        , lag(monthly_revenue) over (
            partition by customer_segment
            order by order_year, order_month
          ) as prev_month_rev
        -- cumulative YTD revenue within year per segment
        , sum(monthly_revenue) over (
            partition by customer_segment, order_year
            order by order_month
            rows between unbounded preceding and current row
          ) as ytd_revenue
    from monthly
)

select
      order_year
    , order_month
    , customer_segment
    , monthly_revenue
    , orders
    , unique_customers
    , avg_margin_pct
    , prev_month_rev
    , round(
        100.0 * (monthly_revenue - prev_month_rev)
              / nullif(prev_month_rev, 0)
      , 2)                              as mom_growth_pct
    , ytd_revenue
from enriched
order by
      order_year
    , order_month
    , customer_segment
;
