-- get date, from, to, from_chain, to_chain, origin_asset, 
WITH raw AS (
SELECT
  transfer_id,
  TIMESTAMP_SECONDS(xcall_timestamp) AS `date`,
  origin_domain_name AS `from`,
  destination_domain_name AS `to`,
  origin_asset_name AS asset,
  usd_amount AS amount

FROM `y42_connext_y42_dev.transfers_mapped`
)

SELECT * FROM raw