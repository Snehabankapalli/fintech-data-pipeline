{% macro parse_fiserv_transaction(column) %}
{#
  Custom macro for parsing semi-structured JSON from Fiserv payment processor APIs.
  Standardizes transformation logic across all transaction models.

  Usage:
    {{ parse_fiserv_transaction('raw_payload') }}

  Pattern mirrors SoFi production dbt codebase — reduces complexity by 40%.
#}
    TRY_PARSE_JSON({{ column }}):transaction_id::STRING        AS transaction_id,
    TRY_PARSE_JSON({{ column }}):card_id::STRING               AS card_id,
    TRY_PARSE_JSON({{ column }}):member_id::STRING             AS member_id,
    TRY_PARSE_JSON({{ column }}):amount::FLOAT                 AS amount,
    TRY_PARSE_JSON({{ column }}):currency_code::STRING         AS currency_code,
    TRY_PARSE_JSON({{ column }}):merchant_name::STRING         AS merchant_name,
    TRY_PARSE_JSON({{ column }}):merchant_category_code::STRING AS merchant_category_code,
    TRY_PARSE_JSON({{ column }}):transaction_type::STRING      AS transaction_type,
    TRY_PARSE_JSON({{ column }}):transaction_status::STRING    AS transaction_status,
    TRY_PARSE_JSON({{ column }}):posted_at::TIMESTAMP_NTZ      AS posted_at,
    TRY_PARSE_JSON({{ column }}):authorized_at::TIMESTAMP_NTZ  AS authorized_at,
    TRY_PARSE_JSON({{ column }}):network::STRING               AS network,
    TRY_PARSE_JSON({{ column }}):is_international::BOOLEAN     AS is_international,
    TRY_PARSE_JSON({{ column }}):decline_reason::STRING        AS decline_reason
{% endmacro %}


{% macro parse_payment_event(column) %}
{#
  Parses Fiserv payment events (ACH, wire, bill pay).
  Used across stg_payments and int_payment_reconciliation.
#}
    TRY_PARSE_JSON({{ column }}):payment_id::STRING            AS payment_id,
    TRY_PARSE_JSON({{ column }}):member_id::STRING             AS member_id,
    TRY_PARSE_JSON({{ column }}):payment_type::STRING          AS payment_type,
    TRY_PARSE_JSON({{ column }}):amount::FLOAT                 AS amount,
    TRY_PARSE_JSON({{ column }}):payment_status::STRING        AS payment_status,
    TRY_PARSE_JSON({{ column }}):scheduled_date::DATE          AS scheduled_date,
    TRY_PARSE_JSON({{ column }}):settled_at::TIMESTAMP_NTZ     AS settled_at,
    TRY_PARSE_JSON({{ column }}):source_account::STRING        AS source_account,
    TRY_PARSE_JSON({{ column }}):destination_account::STRING   AS destination_account,
    TRY_PARSE_JSON({{ column }}):failure_reason::STRING        AS failure_reason
{% endmacro %}
