with source as (
    select * from {{ ref('invoices') }}
),

renamed as (
    select
        invoice_id,
        customer_id,
        cast(issue_date as date)                    as issue_date,
        cast(due_date as date)                      as due_date,
        cast(nullif(trim(paid_date), '') as date)   as paid_date,
        amount,
        currency,
        status,
        country,

        -- days overdue (positive = overdue, null if paid on time)
        iff(
            trim(status) in ('overdue', 'paid_late'),
            datediff('day',
                cast(due_date as date),
                coalesce(cast(nullif(trim(paid_date), '') as date), current_date())
            ),
            0
        )                                           as days_overdue,

        -- binary overdue flag
        (trim(status) = 'overdue')                  as is_currently_overdue

    from source
)

select * from renamed
