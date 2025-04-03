SELECT
    _fivetran_id as event_id
    {% for col in get_filtered_columns(from=source("google_analytics_raw", "event"), except=["fivetran_id"])  %}
    , {{ col }} as {{ col }}
    {% endfor %}
FROM
    {{ source("google_analytics_raw", "event") }}