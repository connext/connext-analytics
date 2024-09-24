SELECT
    DATE_TRUNC(sr.xcall_timestamp, DAY) AS xcall_date,
    COUNT(DISTINCT sr.xcall_caller) AS callers
FROM {{ ref( "connext_dbt", "stg_transfers_raw_usd") }} sr
GROUP BY 1
