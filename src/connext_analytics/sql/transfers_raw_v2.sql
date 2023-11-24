-- final cron sql.
-- pull data based on last transfer pulled timestamp


WITH last_transfer_timestamp AS (
  SELECT max(xcall_timestamp) AS last_transfer_update_date FROM `mainnet-bigq.public.transfers_in_usd`
)


, connext_contracts AS (
  SELECT * FROM `mainnet-bigq.public.connext_contracts`
)

, connext_tokens AS (
  SELECT 
    ct.token_address,
    ct.token_name,
    ct.is_xerc20
  FROM `mainnet-bigq.public.connext_tokens` ct
)

, assets AS (
  SELECT DISTINCT
    da.canonical_id
  , da.domain
  , da.decimal
  FROM `mainnet-bigq.public.assets` da
)

, transfer_raw AS (
SELECT 
  t.transfer_id,
  t.canonical_id,
  t.xcall_transaction_hash,
  t.xcall_caller,
  t.`to`,
  t.origin_sender,
  t.bridged_amt,
  t.origin_transacting_asset,
  t.origin_bridged_amount,
  t.destination_transacting_asset,
  t.destination_transacting_amount,
  t.xcall_tx_origin,
  t.execute_tx_origin,
  t.xcall_timestamp AS xcall_timestamp,
  CAST(TIMESTAMP_SECONDS(t.execute_timestamp) AS STRING) AS execute_timestamp,
  CAST(TIMESTAMP_SECONDS(t.reconcile_timestamp) AS STRING) AS reconcile_timestamp,
  CASE
    WHEN t.origin_domain = '6648936' THEN 'Ethereum'
    WHEN t.origin_domain = '1869640809' THEN 'Optimism'
    WHEN t.origin_domain = '6450786' THEN 'BNB'
    WHEN t.origin_domain = '6778479' THEN 'Gnosis'
    WHEN t.origin_domain = '1886350457' THEN 'Polygon'
    WHEN t.origin_domain = '1634886255' THEN 'Arbitrum One'
    ELSE t.origin_domain
  END AS origin_chain,
  CASE
    WHEN t.destination_domain = '6648936' THEN 'Ethereum'
    WHEN t.destination_domain = '1869640809' THEN 'Optimism'
    WHEN t.destination_domain = '6450786' THEN 'BNB'
    WHEN t.destination_domain = '6778479' THEN 'Gnosis'
    WHEN t.destination_domain = '1886350457' THEN 'Polygon'
    WHEN t.destination_domain = '1634886255' THEN 'Arbitrum One'
    ELSE t.destination_domain
  END AS destination_chain,
  CASE
    WHEN LOWER(t.xcall_caller) != LOWER(t.xcall_tx_origin)
      THEN 'Contract'
    ELSE 'EOA'
  END AS caller_type,
  cc.contract_name,
  cc.contract_author,
  coalesce(cc_origin.token_address, t.origin_transacting_asset) AS origin_asset,
  coalesce(cc_destination.token_address, t.destination_transacting_asset) AS destination_asset,
  CAST(a_origin.decimal AS FLOAT64) AS token_origin_decimal,
  CAST(a_dest.decimal AS FLOAT64) AS token_destination_decimal,
  -- CAST(t.bridged_amt AS FLOAT64) 
  -- / pow(10, coalesce(CAST(a.decimal AS INT64), 0)) AS  d_bridged_amt,
  CAST(origin_bridged_amount AS FLOAT64) 
  / pow(10, coalesce(CAST(a_origin.decimal AS INT64), 0)) AS  d_origin_amount,
  CAST(destination_transacting_amount AS FLOAT64) 
  / pow(10, coalesce(CAST(a_dest.decimal AS INT64), 0)) AS  d_destination_amount

FROM `mainnet-bigq.public.transfers` t
LEFT JOIN connext_contracts cc
  ON LOWER(t.xcall_caller) = LOWER(cc.xcall_caller)
LEFT JOIN connext_tokens cc_origin
  ON t.origin_transacting_asset = cc_origin.token_address
LEFT JOIN connext_tokens cc_destination
  ON t.destination_transacting_asset = cc_destination.token_address
LEFT JOIN assets a_origin
  ON (
    t.canonical_id = a_origin.canonical_id
    AND t.origin_domain = a_origin.domain
    )
LEFT JOIN assets a_dest
  ON (
    t.canonical_id = a_dest.canonical_id
    AND t.destination_domain = a_dest.domain
    )

WHERE (
  EXTRACT(MONTH FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = 12
  AND EXTRACT(YEAR FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = 2022)


)

, hr_asset_price AS (
  SELECT 
    ap.canonical_id,
    ap.timestamp - MOD(ap.timestamp,1800) AS timestamp,
    MAX(ap.price) AS price
  FROM `mainnet-bigq.public.asset_prices` ap
  -- WHERE ap.timestamp > (SELECT last_transfer_update_date FROM last_transfer_timestamp)
  GROUP BY 1,2

)

, transfers_usd_price  AS (
  SELECT
    tr.*,         
    CAST(ap.price AS FLOAT64) AS price,
    ROW_NUMBER() OVER
          (
            PARTITION BY tr.transfer_id
            ORDER BY ap.timestamp DESC
          ) AS closet_price_rank
  FROM transfer_raw tr
  LEFT JOIN hr_asset_price ap
    ON (
      tr.canonical_id = ap.canonical_id
      AND ap.timestamp <= (tr.xcall_timestamp - MOD(tr.xcall_timestamp,1800))
      )
)

SELECT 
  tsp.*,
  -- tsp.d_bridged_amt * CAST(tsp.price AS FLOAT64) AS usd_bridged_amt,
  tsp.d_origin_amount * CAST(tsp.price AS FLOAT64) AS usd_origin_amount,
  tsp.d_destination_amount * CAST(tsp.price AS FLOAT64) AS usd_destination_amount
FROM transfers_usd_price tsp
WHERE closet_price_rank = 1