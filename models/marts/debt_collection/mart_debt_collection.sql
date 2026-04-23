/*
  mart_debt_collection.sql
  ------------------------
  KPI dashboard: Debt Collection

  Key metrics:
  - Outstanding debt per country / segment
  - Days Sales Outstanding (DSO)
  - Overdue rate (% invoices overdue)
  - Aging buckets: 0-30, 31-60, 61-90, 90+ days overdue
  - Recovery rate: paid_late / (paid_late + overdue)
  - Average days to payment for late payers
*/

with invoices as (
    select * from {{ ref('stg_invoices') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- join invoices to customers for segmentation
enriched_invoices as (
    select
        i.*,
        c.customer_name,
        c.segment,
        c.plan_type,
        c.mrr,
        c.tenure_days,

        -- aging bucket for overdue invoices
        case
            when i.is_currently_overdue and i.days_overdue between 1  and 30  then '1-30 days'
            when i.is_currently_overdue and i.days_overdue between 31 and 60  then '31-60 days'
            when i.is_currently_overdue and i.days_overdue between 61 and 90  then '61-90 days'
            when i.is_currently_overdue and i.days_overdue > 90               then '90+ days'
            else 'current'
        end                                 as aging_bucket

    from invoices i
    left join customers c on i.customer_id = c.customer_id
),

-- summary by country and month
debt_summary as (
    select
        date_trunc('month', e.issue_date)       as month,
        e.country,
        e.segment,

        count(*)                                as total_invoices,
        sum(e.amount)                           as total_invoiced,

        -- overdue
        countif(e.is_currently_overdue)         as overdue_count,
        sum(iff(e.is_currently_overdue,
            e.amount, 0))                       as overdue_amount,

        -- paid late
        countif(e.status = 'paid_late')         as paid_late_count,
        sum(iff(e.status = 'paid_late',
            e.amount, 0))                       as paid_late_amount,

        -- paid on time
        countif(e.status = 'paid')              as paid_ontime_count,
        sum(iff(e.status = 'paid',
            e.amount, 0))                       as paid_ontime_amount,

        -- rates
        round(
            countif(e.is_currently_overdue)
            / nullif(count(*), 0) * 100, 2
        )                                       as overdue_rate_pct,

        round(
            countif(e.status = 'paid_late')
            / nullif(
                countif(e.status = 'paid_late')
                + countif(e.is_currently_overdue),
                0
            ) * 100, 2
        )                                       as recovery_rate_pct,

        -- DSO approximation: (overdue_amount / total_invoiced) * 30
        round(
            sum(iff(e.is_currently_overdue, e.amount, 0))
            / nullif(sum(e.amount), 0) * 30,
            1
        )                                       as dso_days,

        -- avg days overdue for currently overdue invoices
        round(avg(iff(e.is_currently_overdue,
            e.days_overdue, null)), 1)          as avg_days_overdue

    from enriched_invoices e
    group by 1, 2, 3
),

-- aging bucket breakdown
aging_breakdown as (
    select
        e.country,
        e.segment,
        e.aging_bucket,
        count(*)                                as invoice_count,
        sum(e.amount)                           as total_amount,
        round(avg(e.days_overdue), 1)           as avg_days_overdue
    from enriched_invoices e
    where e.is_currently_overdue
    group by 1, 2, 3
),

-- highest-risk debtors (currently overdue, high amount)
high_risk_debtors as (
    select
        e.customer_id,
        e.customer_name,
        e.country,
        e.segment,
        e.plan_type,
        e.mrr,
        count(*)                                as overdue_invoice_count,
        sum(e.amount)                           as total_overdue_amount,
        max(e.days_overdue)                     as max_days_overdue,
        max(e.aging_bucket)                     as worst_aging_bucket
    from enriched_invoices e
    where e.is_currently_overdue
    group by 1, 2, 3, 4, 5, 6
    order by total_overdue_amount desc
)

-- primary output: monthly debt summary
select
    'monthly_debt_summary'      as record_type,
    month,
    country,
    segment,
    total_invoices,
    total_invoiced,
    overdue_count,
    overdue_amount,
    paid_late_count,
    paid_late_amount,
    paid_ontime_count,
    paid_ontime_amount,
    overdue_rate_pct,
    recovery_rate_pct,
    dso_days,
    avg_days_overdue,
    null                        as aging_bucket,
    null                        as customer_id,
    null                        as customer_name,
    null                        as plan_type,
    null                        as mrr,
    null                        as overdue_invoice_count,
    null                        as total_overdue_amount,
    null                        as max_days_overdue
from debt_summary

union all

select
    'aging_breakdown'           as record_type,
    null                        as month,
    country,
    segment,
    invoice_count               as total_invoices,
    total_amount                as total_invoiced,
    invoice_count               as overdue_count,
    total_amount                as overdue_amount,
    null, null, null, null,
    null                        as overdue_rate_pct,
    null                        as recovery_rate_pct,
    null                        as dso_days,
    avg_days_overdue,
    aging_bucket,
    null, null, null, null, null, null, null
from aging_breakdown

union all

select
    'high_risk_debtors'         as record_type,
    null                        as month,
    country,
    segment,
    overdue_invoice_count       as total_invoices,
    total_overdue_amount        as total_invoiced,
    overdue_invoice_count       as overdue_count,
    total_overdue_amount        as overdue_amount,
    null, null, null, null,
    null                        as overdue_rate_pct,
    null                        as recovery_rate_pct,
    null                        as dso_days,
    max_days_overdue            as avg_days_overdue,
    worst_aging_bucket          as aging_bucket,
    customer_id,
    customer_name,
    plan_type,
    mrr,
    overdue_invoice_count,
    total_overdue_amount,
    max_days_overdue
from high_risk_debtors

order by record_type, month desc nulls last
