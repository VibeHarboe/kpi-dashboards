/*
  mart_customer_satisfaction.sql
  ------------------------------
  KPI dashboard: Kundetilfredshed

  Key metrics:
  - NPS score (promoters - detractors / total * 100)
  - CSAT score (% satisfied)
  - NPS breakdown by country / segment / plan
  - Detractor analysis: who are they, what's their MRR at risk
  - Survey response rate (surveys / active customers)
  - NPS trend over time
*/

with surveys as (
    select * from {{ ref('stg_nps_surveys') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

-- enrich surveys with customer data
enriched_surveys as (
    select
        sv.survey_id,
        sv.customer_id,
        sv.survey_date,
        sv.nps_score,
        sv.csat_score,
        sv.nps_category,
        sv.is_satisfied,
        sv.comment,
        c.customer_name,
        c.country,
        c.segment,
        c.plan_type,
        c.mrr,
        s.is_active
    from surveys sv
    left join customers c  on sv.customer_id = c.customer_id
    left join subscriptions s
        on sv.customer_id = s.customer_id
        and s.is_active
),

-- NPS by month and country
nps_monthly as (
    select
        date_trunc('month', survey_date)        as month,
        country,
        segment,
        plan_type,

        count(*)                                as survey_responses,

        -- NPS components
        countif(nps_category = 'promoter')      as promoters,
        countif(nps_category = 'passive')       as passives,
        countif(nps_category = 'detractor')     as detractors,

        -- NPS score: (promoters - detractors) / total * 100
        round(
            (
                countif(nps_category = 'promoter')
                - countif(nps_category = 'detractor')
            ) / nullif(count(*), 0) * 100,
            1
        )                                       as nps_score,

        -- CSAT: % satisfied
        round(
            countif(is_satisfied)
            / nullif(count(*), 0) * 100,
            1
        )                                       as csat_pct,

        -- average raw scores
        round(avg(nps_score), 2)                as avg_nps_raw,
        round(avg(csat_score), 2)               as avg_csat_raw,

        -- MRR at risk = MRR from detractors
        sum(iff(nps_category = 'detractor', mrr, 0))    as detractor_mrr_at_risk,
        sum(mrr)                                        as total_surveyed_mrr

    from enriched_surveys
    group by 1, 2, 3, 4
),

-- detractor deep-dive
detractor_detail as (
    select
        customer_id,
        customer_name,
        country,
        segment,
        plan_type,
        mrr,
        nps_score,
        csat_score,
        survey_date,
        comment,
        is_active,

        -- flag: detractor who is also active = churn risk
        (nps_category = 'detractor' and is_active)  as is_active_detractor

    from enriched_surveys
    where nps_category = 'detractor'
),

-- survey response rate vs active customers
response_rate as (
    select
        sv.country,
        count(distinct sv.customer_id)              as surveyed_customers,
        count(distinct c.customer_id)               as active_customers,
        round(
            count(distinct sv.customer_id)
            / nullif(count(distinct c.customer_id), 0) * 100,
            1
        )                                           as response_rate_pct
    from customers c
    left join enriched_surveys sv on c.customer_id = sv.customer_id
    group by 1
)

select
    'nps_monthly'               as record_type,
    month,
    country,
    segment,
    plan_type,
    survey_responses,
    promoters,
    passives,
    detractors,
    nps_score,
    csat_pct,
    avg_nps_raw,
    avg_csat_raw,
    detractor_mrr_at_risk,
    total_surveyed_mrr,
    null                        as customer_id,
    null                        as customer_name,
    null                        as mrr,
    null                        as nps_score_raw,
    null                        as csat_score_raw,
    null                        as comment,
    null                        as is_active,
    null                        as is_active_detractor,
    null                        as surveyed_customers,
    null                        as active_customers,
    null                        as response_rate_pct
from nps_monthly

union all

select
    'detractor_detail'          as record_type,
    null                        as month,
    country,
    segment,
    plan_type,
    null                        as survey_responses,
    null, null, null,
    null                        as nps_score,
    null                        as csat_pct,
    null, null,
    mrr                         as detractor_mrr_at_risk,
    mrr                         as total_surveyed_mrr,
    customer_id,
    customer_name,
    mrr,
    nps_score                   as nps_score_raw,
    csat_score                  as csat_score_raw,
    comment,
    is_active,
    is_active_detractor,
    null, null, null
from detractor_detail

union all

select
    'response_rate'             as record_type,
    null, country, null, null,
    surveyed_customers          as survey_responses,
    null, null, null, null, null, null, null, null, null,
    null, null, null, null, null, null, null,
    surveyed_customers,
    active_customers,
    response_rate_pct
from response_rate

order by record_type, month desc nulls last
