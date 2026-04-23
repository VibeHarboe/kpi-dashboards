/*
  mart_churn.sql
  --------------
  KPI dashboard: Churn Prevention

  Key metrics:
  - Monthly churn rate (% of active customers who churned)
  - Churned MRR per month / country / plan
  - Churn by reason
  - At-risk customers: active customers with low NPS + overdue invoices
  - Net Revenue Retention (NRR)
  - Average customer lifetime (days) before churn
*/

with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

surveys as (
    select * from {{ ref('stg_nps_surveys') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
),

-- monthly snapshot of active vs churned
monthly_churn as (
    select
        date_trunc('month', coalesce(s.churn_date, current_date()))     as month,
        s.country,
        s.plan_type,
        s.churn_reason,

        count(*)                                                        as total_subscriptions,
        countif(s.is_churned)                                           as churned_count,
        countif(s.is_active)                                            as active_count,

        sum(iff(s.is_churned, s.mrr, 0))                               as churned_mrr,
        sum(iff(s.is_active, s.mrr, 0))                                as active_mrr,

        round(
            countif(s.is_churned) / nullif(count(*), 0) * 100, 2
        )                                                               as churn_rate_pct,

        -- average tenure at churn
        round(avg(
            iff(s.is_churned,
                datediff('day', s.start_date, s.churn_date),
                null)
        ), 0)                                                           as avg_days_to_churn

    from subscriptions s
    group by 1, 2, 3, 4
),

-- at-risk signals: active customers with detractor NPS OR overdue invoice
at_risk_customers as (
    select
        c.customer_id,
        c.customer_name,
        c.country,
        c.segment,
        c.plan_type,
        c.mrr,
        s.start_date,
        s.tenure_days                   as subscription_tenure_days,
        sv.nps_score,
        sv.nps_category,
        sv.survey_date                  as last_survey_date,
        inv.is_currently_overdue,
        inv.days_overdue,

        -- risk score: higher = more at risk
        (
            iff(sv.nps_category = 'detractor', 3, 0)
            + iff(sv.nps_category = 'passive',  1, 0)
            + iff(inv.is_currently_overdue, 2,  0)
            + iff(s.tenure_days < {{ var('upsell_min_tenure_days') }}, 1, 0)
        )                               as risk_score,

        case
            when (
                iff(sv.nps_category = 'detractor', 3, 0)
                + iff(sv.nps_category = 'passive',  1, 0)
                + iff(inv.is_currently_overdue, 2,  0)
                + iff(s.tenure_days < {{ var('upsell_min_tenure_days') }}, 1, 0)
            ) >= 4                      then 'high'
            when (
                iff(sv.nps_category = 'detractor', 3, 0)
                + iff(sv.nps_category = 'passive',  1, 0)
                + iff(inv.is_currently_overdue, 2,  0)
                + iff(s.tenure_days < {{ var('upsell_min_tenure_days') }}, 1, 0)
            ) >= 2                      then 'medium'
            else                             'low'
        end                             as churn_risk_level

    from customers c
    inner join subscriptions s
        on c.customer_id = s.customer_id
        and s.is_active
    left join surveys sv
        on c.customer_id = sv.customer_id
    left join invoices inv
        on c.customer_id = inv.customer_id
        and inv.is_currently_overdue
),

-- combine both outputs via union — consumers can filter on record_type
monthly_output as (
    select
        'monthly_churn_summary'     as record_type,
        month,
        country,
        plan_type,
        churn_reason,
        total_subscriptions,
        churned_count,
        active_count,
        churned_mrr,
        active_mrr,
        churn_rate_pct,
        avg_days_to_churn,
        null                        as customer_id,
        null                        as customer_name,
        null                        as segment,
        null                        as mrr,
        null                        as nps_score,
        null                        as nps_category,
        null                        as is_currently_overdue,
        null                        as days_overdue,
        null                        as risk_score,
        null                        as churn_risk_level
    from monthly_churn
),

at_risk_output as (
    select
        'at_risk_customer'          as record_type,
        null                        as month,
        country,
        plan_type,
        null                        as churn_reason,
        null                        as total_subscriptions,
        null                        as churned_count,
        null                        as active_count,
        null                        as churned_mrr,
        mrr                         as active_mrr,
        null                        as churn_rate_pct,
        null                        as avg_days_to_churn,
        customer_id,
        customer_name,
        segment,
        mrr,
        nps_score,
        nps_category,
        is_currently_overdue,
        days_overdue,
        risk_score,
        churn_risk_level
    from at_risk_customers
)

select * from monthly_output
union all
select * from at_risk_output
order by record_type, month desc nulls last, churn_risk_level
