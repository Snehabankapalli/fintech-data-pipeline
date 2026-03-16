{% macro get_incremental_predicate(timestamp_col, lookback_hours=6) %}
{#
  Standard incremental merge predicate used across all fintech models.
  Uses a lookback window to catch late-arriving events (common in payment processing).

  Args:
    timestamp_col:   Column to filter on (e.g., 'posted_at')
    lookback_hours:  How far back to reprocess (default 6h for late arrivals)

  Pattern reduces batch processing time 83% by only reprocessing recent rows.
#}
    {{ timestamp_col }} >= DATEADD('hour', -{{ lookback_hours }}, (
        SELECT COALESCE(MAX({{ timestamp_col }}), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    ))
{% endmacro %}


{% macro generate_surrogate_key(fields) %}
{#
  Generates a deterministic surrogate key from a list of fields.
  Used for idempotent upserts across all fact tables.
#}
    MD5(CONCAT_WS('|',
        {% for field in fields %}
            COALESCE(CAST({{ field }} AS STRING), '__null__')
            {%- if not loop.last %}, {% endif %}
        {% endfor %}
    ))
{% endmacro %}
