/*
  mart_upselling.sql
  ------------------
  KPI dashboard: Upselling & Sales

  Key metrics:
  - Upsell revenue per month / country / sales rep
  - Upsell conversion rate (eligible customers who were upsold)
  - Plan upgrade volume vs add-on sales
  - Upsell attach rate by segment
  - Average incremental MRR per upsell
  - Customers eligible for upsell (active, tenure > threshold, on basic plan)
*/

with upsells as (
    select * from {{ ref('stg_upsell_events') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

-- monthly upsell performance
monthly_upsell as (
    select
        date_trunc('month', u.upsell_date)      as month,
        u.country,
        u.upsell_type,
        u.sales_rep,

        count(*)                                as upsell_count,
        sum(u.incremental_revenue)              as total_incremental_revenue,
        round(avg(u.incremental_revenue), 2)    as avg_incremental_revenue,

        -- plan upgrades specifically
        countif(u.upsell_type = 'plan_upgrade') as plan_upgrades,
        countif(u.upsell_type = 'add_on_sale')  as add_on_sales,

        -- revenue split
        sum(iff(u.upsell_type = 'plan_upgrade',
            u.incremental_revenue, 0))          as upgrade_revenue,
        sum(iff(u.upsell_type = 'add_on_sale',
            u.incremental_revenue, 0))          as add_on_revenue

    from upsells u
    group by 1, 2, 3, 4
),

-- upsell-eligible customers: active, basic plan, tenure > threshold
eligible_customers as (
    select
        c.customer_id,
        c.country,
        c.segment,
        c.plan_type,
        c.mrr,
        c.tenure_days
    from customers c
    inner join subscriptions s
        on c.customer_id = s.customer_id
        and s.is_active
    where
        c.plan_type = 'basic'
        and c.tenure_days >= {{ var('upsell_min_tenure_days') }}
),

-- which eligible customers have already been upsold
upsold_customers as (
    select distinct customer_id
    from upsells
),

-- attach rate by segment
attach_rate_by_segment as (
    select
        e.country,
        e.segment,
        count(*)                                                    as eligible_count,
        countif(uc.customer_id is not null)                        as upsold_count,
        round(
            countif(uc.customer_id is not null)
            / nullif(count(*), 0) * 100, 2
        )                                                           as upsell_attach_rate_pct,

        sum(e.mrr)                                                  as eligible_mrr_base,
        -- potential MRR if all eligible upgrade from basic to premium (~avg lift = 550)
        sum(e.mrr) * 0.9                                           as estimated_upsell_opportunity_mrr

    from eligible_customers e
    left join upsold_customers uc
        on e.customer_id = uc.customer_id
    group by 1, 2
)

select
    'monthly_upsell_performance'    as record_type,
    month,
    country,
    upsell_type,
    sales_rep,
    upsell_count,
    total_incremental_revenue,
    avg_incremental_revenue,
    plan_upgrades,
    add_on_sales,
    upgrade_revenue,
    add_on_revenue,
    null                            as segment,
    null                            as eligible_count,
    null                            as upsold_count,
    null                            as upsell_attach_rate_pct,
    null                            as estimated_upsell_opportunity_mrr
from monthly_upsell

union all

select
    'attach_rate_by_segment'        as record_type,
    null                            as month,
    country,
    null                            as upsell_type,
    null                            as sales_rep,
    upsold_count                    as upsell_count,
    null                            as total_incremental_revenue,
    null                            as avg_incremental_revenue,
    null                            as plan_upgrades,
    null                            as add_on_sales,
    null                            as upgrade_revenue,
    null                            as add_on_revenue,
    segment,
    eligible_count,
    upsold_count,
    upsell_attach_rate_pct,
    estimated_upsell_opportunity_mrr
from attach_rate_by_segment

order by record_type, month desc nulls last
