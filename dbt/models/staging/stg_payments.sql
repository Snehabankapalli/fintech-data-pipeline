{{
    config(
        materialized = 'view',
        tags         = ['staging', 'payments', 'daily']
    )
}}

/*
  Staging model for ACH/wire/bill-pay events from Fiserv.
  Handles late-arriving payment settlements (T+1 to T+3 settlement windows).
*/

WITH raw AS (
    SELECT RAW_PAYLOAD, LOADED_AT
    FROM {{ source('raw', 'payments') }}
    WHERE LOADED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),

parsed AS (
    SELECT
        {{ parse_payment_event('RAW_PAYLOAD') }},
        LOADED_AT AS _loaded_at,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM raw
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY settled_at DESC NULLS LAST, _loaded_at DESC
        ) AS _row_num
    FROM parsed
    WHERE payment_id IS NOT NULL
)

SELECT * EXCLUDE (_row_num)
FROM deduplicated
WHERE _row_num = 1
