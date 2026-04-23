with source as (
    select * from {{ ref('subscriptions') }}
),

renamed as (
    select
        subscription_id,
        customer_id,
        plan_type,
        cast(start_date as date)                    as start_date,
        cast(nullif(end_date, '')   as date)        as end_date,
        cast(nullif(churn_date, '') as date)        as churn_date,
        nullif(trim(churn_reason), '')              as churn_reason,
        mrr,
        country,

        -- is active = no churn date and no end date
        (churn_date is null and end_date is null)   as is_active,

        -- churned flag
        (churn_date is not null)                    as is_churned,

        -- days since churn (null if not churned)
        iff(
            churn_date is not null,
            datediff('day', cast(churn_date as date), current_date()),
            null
        )                                           as days_since_churn

    from source
)

select * from renamed
