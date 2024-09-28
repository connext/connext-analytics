-- agg on token, chain, end of day: bridge volume only

SELECT
    DATE_TRUNC(sr.xcall_timestamp, DAY) AS xcall_date,
    sr.origin_chain,
    sr.destination_chain,
    sr.origin_asset,
    sr.destination_asset,
    COUNT(DISTINCT sr.transfer_id) AS transfers,
    COUNT(DISTINCT sr.xcall_caller) AS callers,
    SUM(sr.usd_bridged_amount) AS usd_bridged_amount
FROM {{ ref( "connext_dbt", "stg_transfers_raw_usd") }} sr
GROUP BY 1, 2, 3, 4, 5
