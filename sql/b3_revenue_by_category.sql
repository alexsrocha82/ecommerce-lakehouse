-- ============================================================
-- B3 — Revenue by Category with Percentage of Total
-- ============================================================
-- Source : dev_gold.fact_order_line
-- Window : unbounded window (no partition) for grand total
-- Output : revenue, margin, pct_of_total, margin_pct
-- ============================================================
-- Trick: rowsBetween(unbounded preceding, unbounded following)
-- without partitionBy creates a window covering ALL rows —
-- every row gets the grand total for the division.
-- ============================================================

with category_agg as (
    select
          category
        , subcategory
        , brand
        , sum(total_amount)  as revenue
        , sum(margin_amount) as margin
        , count(*)           as orders
    from dev_gold.fact_order_line
    group by
          category
        , subcategory
        , brand
)

select
      category
    , subcategory
    , brand
    , revenue
    , orders
    , round(margin, 2)                                     as margin
    -- percentage of total revenue (grand total window)
    , round(
        revenue
        / sum(revenue) over (
            rows between unbounded preceding
                     and unbounded following
          ) * 100
      , 2)                                                 as pct_of_total
    -- margin percentage per category
    , round(margin / nullif(revenue, 0) * 100, 2)          as margin_pct
from category_agg
order by revenue desc
;
