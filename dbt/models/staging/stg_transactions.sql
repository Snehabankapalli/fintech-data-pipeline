{{
    config(
        materialized = 'view',
        tags         = ['staging', 'transactions', 'daily']
    )
}}

/*
  Staging model for raw credit card transactions from Fiserv payment processor.
  Parses semi-structured JSON using custom macro — standardizes across 50+ models.

  Source: S3 → Snowpipe → RAW.TRANSACTIONS (VARIANT column)
  SLA: Must refresh within 4h for same-day credit reporting (CFPB compliance)
*/

WITH raw AS (
    SELECT
        _SNOWFLAKE_FILE_NAME                        AS source_file,
        _SNOWFLAKE_FILE_ROW_NUMBER                  AS source_row,
        LOADED_AT,
        RAW_PAYLOAD
    FROM {{ source('raw', 'transactions') }}
    WHERE LOADED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())   -- rolling 7-day window
),

parsed AS (
    SELECT
        {{ parse_fiserv_transaction('RAW_PAYLOAD') }},
        source_file,
        source_row,
        LOADED_AT                                   AS _loaded_at,
        CURRENT_TIMESTAMP()                         AS _dbt_updated_at
    FROM raw
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY authorized_at DESC, _loaded_at DESC
        ) AS _row_num
    FROM parsed
    WHERE transaction_id IS NOT NULL               -- enforce NOT NULL at staging
)

SELECT * EXCLUDE (_row_num)
FROM deduplicated
WHERE _row_num = 1
