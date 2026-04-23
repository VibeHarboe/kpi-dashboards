with source as (
    select * from {{ ref('leads') }}
),

renamed as (
    select
        lead_id,
        customer_id,
        country,
        service_type,
        cast(lead_date as date)                         as lead_date,
        cast(nullif(trim(assigned_at), '') as date)     as assigned_at,
        cast(nullif(trim(converted_at), '') as date)    as converted_at,
        trim(lead_status)                               as lead_status,
        lead_source,

        -- time-to-assign in hours (business metric)
        iff(
            assigned_at is not null,
            datediff('hour',
                cast(lead_date as date),
                cast(nullif(trim(assigned_at), '') as date)
            ),
            null
        )                                               as hours_to_assign,

        -- time-to-convert in days
        iff(
            converted_at is not null,
            datediff('day',
                cast(lead_date as date),
                cast(nullif(trim(converted_at), '') as date)
            ),
            null
        )                                               as days_to_convert,

        -- conversion flag
        (trim(lead_status) = 'converted')               as is_converted

    from source
)

select * from renamed
