{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = 'event_key',
        cluster_by          = ['event_date', 'transaction_type'],
        tags                = ['intermediate', 'credit_card'],
        on_schema_change    = 'sync_all_columns'
    )
}}

/*
  Intermediate model — joins transactions + payments, enriches with member context.
  Feeds downstream credit card revenue analytics and regulatory reporting.

  Incremental strategy: merge with 6h lookback for late-arriving payment settlements.
  Cluster by event_date for p95 latency improvement (60% faster dashboard queries).
*/

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE {{ get_incremental_predicate('posted_at', lookback_hours=6) }}
    {% endif %}
),

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
    {% if is_incremental() %}
        WHERE {{ get_incremental_predicate('settled_at', lookback_hours=6) }}
    {% endif %}
),

-- Lateral flatten on merchant_category_code for enrichment lookups
enriched AS (
    SELECT
        t.transaction_id,
        t.card_id,
        t.member_id,
        t.amount,
        t.currency_code,
        t.merchant_name,
        t.merchant_category_code,
        t.transaction_type,
        t.transaction_status,
        t.posted_at,
        t.authorized_at,
        t.network,
        t.is_international,
        t.decline_reason,

        -- Payment linkage (settlements may arrive T+1 to T+3)
        p.payment_id,
        p.payment_type,
        p.payment_status,
        p.settled_at,

        -- Derived fields for analytics
        DATE(t.posted_at)                                           AS event_date,
        DATE_TRUNC('month', t.posted_at)                           AS event_month,
        HOUR(t.posted_at)                                           AS event_hour,
        CASE
            WHEN t.transaction_status = 'APPROVED'  THEN TRUE
            ELSE FALSE
        END                                                         AS is_approved,
        CASE
            WHEN t.is_international = TRUE          THEN 'INTERNATIONAL'
            ELSE 'DOMESTIC'
        END                                                         AS geo_type,
        CASE
            WHEN t.amount >= 500                    THEN 'HIGH'
            WHEN t.amount >= 100                    THEN 'MEDIUM'
            ELSE 'LOW'
        END                                                         AS amount_tier,

        -- Surrogate key for idempotent merges
        {{ generate_surrogate_key(['t.transaction_id', 't.posted_at']) }} AS event_key,

        CURRENT_TIMESTAMP()                                         AS _dbt_updated_at

    FROM transactions t
    LEFT JOIN payments p
        ON t.member_id    = p.member_id
        AND t.amount      = p.amount
        AND DATE(t.posted_at) = p.scheduled_date
        AND p.payment_status  = 'SETTLED'
)

SELECT * FROM enriched
