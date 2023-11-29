SELECT
DATE_TRUNC(sr.xcall_timestamp, WEEK) AS xcall_date,
sr.caller_type,
sr.contract_name,
sr.contract_author,
sr.is_origin_asset_xerc20,
sr.is_destination_asset_xerc20,
sr.origin_chain,
sr.destination_chain,
sr.origin_asset,
sr.destination_asset,
COUNT(DISTINCT sr.transfer_id) AS transfers,
COUNT(DISTINCT sr.xcall_caller) AS callers,
SUM(sr.usd_bridged_amount) AS usd_bridged_amount
-- FROM `mainnet-bigq`.`stage`.`stg_transfers_raw_usd` sr
FROM {{ref( "connext_dbt", "stg_transfers_raw_usd")}} sr
GROUP BY 1,2,3,4,5,6,7,8,9,10