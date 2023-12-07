SELECT
    DATE_TRUNC(sr.xcall_timestamp, DAY) AS xcall_date,
    CASE 
        WHEN (CAST(is_origin_asset_xerc20 AS BOOL) IS TRUE OR  CAST(is_destination_asset_xerc20 AS BOOL) IS TRUE) THEN TRUE
    ELSE FALSE END
    AS x_erc_20_asset,
    sr.caller_type,
    sr.status,
    COUNT(DISTINCT sr.xcall_caller) AS callers
FROM {{ref( "connext_dbt", "stg_transfers_raw_usd")}} sr
GROUP BY 1,2,3,4