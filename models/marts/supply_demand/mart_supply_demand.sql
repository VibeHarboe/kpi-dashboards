/*
  mart_supply_demand.sql
  ----------------------
  KPI dashboard: Supply & Demand

  Key metrics:
  - Total leads per country / service type / month
  - Lead assignment rate (% leads assigned within SLA)
  - Conversion rate (leads → customers)
  - Average time-to-assign (hours)
  - Average time-to-convert (days)
  - Unassigned lead count (supply gap indicator)
*/

with leads as (
    select * from {{ ref('stg_leads') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

lead_metrics as (
    select
        l.country,
        l.service_type,
        l.lead_source,
        date_trunc('month', l.lead_date)            as month,

        -- volume
        count(*)                                    as total_leads,
        countif(l.is_converted)                     as converted_leads,
        countif(l.lead_status = 'pending')          as pending_leads,
        countif(l.lead_status = 'unassigned')       as unassigned_leads,
        countif(l.lead_status = 'lost')             as lost_leads,

        -- rates
        round(
            countif(l.is_converted) / nullif(count(*), 0) * 100, 2
        )                                           as conversion_rate_pct,

        round(
            countif(l.assigned_at is not null)
            / nullif(count(*), 0) * 100, 2
        )                                           as assignment_rate_pct,

        -- speed
        round(avg(l.hours_to_assign), 1)            as avg_hours_to_assign,
        round(avg(l.days_to_convert), 1)            as avg_days_to_convert,

        -- SLA breach: assigned > 24h after lead received
        countif(l.hours_to_assign > 24)             as sla_breaches,
        round(
            countif(l.hours_to_assign > 24)
            / nullif(countif(l.assigned_at is not null), 0) * 100, 2
        )                                           as sla_breach_rate_pct

    from leads l
    group by 1, 2, 3, 4
),

-- supply gap: markets where unassigned > 10% of total
supply_gap_flag as (
    select
        *,
        round(
            unassigned_leads / nullif(total_leads, 0) * 100, 2
        )                                           as unassigned_rate_pct,
        (unassigned_leads / nullif(total_leads, 0) > 0.10) as has_supply_gap
    from lead_metrics
)

select * from supply_gap_flag
order by month desc, country, service_type
