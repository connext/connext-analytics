
WITH raw_agg AS (

SELECT
  tf.origin_chain,
  tf.destination_chain,
  tf.origin_asset,
  tf.destination_asset,
  
  SUM(CAST(tf.origin_transacting_amount AS FLOAT64)) AS origin_transacting_amount,
  SUM(CAST(tf.bridged_amt AS FLOAT64)) AS bridged_amt,
  SUM(CAST(tf.destination_transacting_amount AS FLOAT64)) AS destination_transacting_amount,
  min(tf.token_decimal) AS min_token_decimal,
  max(tf.token_decimal) AS max_token_decimal,
  SUM(tf.usd_origin_amount) AS usd_origin_amount,
  SUM(tf.usd_bridged_amount) AS usd_bridged_amount,
  SUM(tf.usd_destination_amount) AS usd_destination_amount


FROM `mainnet-bigq.public.transfer_final` tf
WHERE (CHAR_LENGTH(tf.origin_asset) <= 10)
AND (tf.origin_asset IS NOT NULL)
AND (tf.destination_asset IS NOT NULL)
AND (
  tf.origin_chain= "BNB"
  OR tf.destination_chain= "BNB")

GROUP BY 1,2,3,4)

, agg AS (
  SELECT 
    ra.*, 
    ROUND(100* (ra.usd_origin_amount - ra.usd_destination_amount) / ra.usd_origin_amount, 1) AS perct_diff_from_origin_dest,
    ROUND(100* (ra.usd_origin_amount - ra.usd_bridged_amount) / ra.usd_origin_amount, 1) AS perct_diff_from_origin_bridge,
    ROUND(100* (ra.usd_bridged_amount - ra.usd_destination_amount) / ra.usd_bridged_amount, 1) AS perct_diff_from_bridge_dest

  FROM raw_agg ra
)

SELECT agg.*
FROM agg
WHERE agg.perct_diff_from_origin_dest IS NOT NULL
ORDER BY 11,12,13 DESC