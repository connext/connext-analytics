  -- batch data for market maker with a max timeframe of 3hr
WITH
  chains_meta AS (
  SELECT
    DISTINCT domainid,
    chain_name AS chain
  FROM
    `mainnet-bigq.raw.stg__ninja_connext_prod_chains_tokens_clean` ct ),
  assets AS (
  SELECT
    DISTINCT da.domain,
    da.canonical_id,
    da.adopted_decimal AS decimal
  FROM
    `mainnet-bigq.public.assets` da ),
  tokens_meta AS (
  SELECT
    DISTINCT LOWER(token_address) AS local,
    token_name AS asset
  FROM
    `mainnet-bigq.stage.connext_tokens` ct ),
  tx AS (
  SELECT
    t.transfer_id,
    TIMESTAMP_SECONDS (t.xcall_timestamp) AS xcall_timestamp,
    t.origin_domain,
    t.destination_domain,
    t.destination_local_asset,
    a.decimal,
    ( CAST(destination_transacting_amount AS FLOAT64) / POW (10, COALESCE(CAST(a.decimal AS INT64), 0)) ) AS destination_transacting_amount
  FROM
    `public.transfers` t
  LEFT JOIN
    assets a
  ON
    ( t.canonical_id = a.canonical_id
      AND t.destination_domain = a.domain ) ),
  semi_final AS (
  SELECT
    t.transfer_id,
    t.xcall_timestamp,
    ocm.chain AS origin_chain,
    dcm.chain AS destination_chain,
    COALESCE(dtm.asset, t.destination_local_asset) AS asset,
    t.destination_transacting_amount AS amount
  FROM
    tx t
  LEFT JOIN
    chains_meta dcm
  ON
    t.destination_domain = dcm.domainid
  LEFT JOIN
    tokens_meta dtm
  ON
    (t.destination_local_asset = dtm.local)
  LEFT JOIN
    chains_meta ocm
  ON
    t.origin_domain = ocm.domainid ),
  clean_final AS (
  SELECT
    sf.transfer_id,
    sf.xcall_timestamp AS date,
    sf.origin_chain,
    sf.destination_chain,
    sf.asset,
    CASE
      WHEN asset = 'ETH' THEN 'WETH'
      WHEN asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (asset, 'next') THEN REGEXP_REPLACE (asset, '^next', '')
      ELSE asset
  END
    AS asset_group,
    CASE
      WHEN asset = 'ETH' THEN 'WETH'
      WHEN asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (asset, 'next') THEN REGEXP_REPLACE (asset, '^next', '')
      WHEN asset = 'alUSD' THEN 'USDT'
      WHEN asset = 'nextALUSD' THEN 'USDT'
      WHEN asset = 'instETH' THEN 'WETH'
      WHEN asset = 'ezETH' THEN 'WETH'
      WHEN asset = 'alETH' THEN 'WETH'
      WHEN asset = 'nextalETH' THEN 'WETH'
      ELSE asset
  END
    AS price_group,
    sf.amount
  FROM
    semi_final sf ),
  -- adding daily pricing to final
  daily_price AS (
  SELECT
    DATE_TRUNC (CAST(p.date AS TIMESTAMP), HOUR) AS date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
  GROUP BY
    1,
    2 ),
  usd_data AS (
  SELECT
    "Connext" AS bridge,
    cf.transfer_id,
    DATE_TRUNC(cf.date, DAY) AS date,
    cf.origin_chain,
    cf.destination_chain,
    cf.asset_group AS asset,
    -- USD values
    dp.price * cf.amount AS amount_usd
  FROM
    clean_final cf
  LEFT JOIN
    daily_price dp
  ON
    DATE_TRUNC(cf.date, HOUR) = dp.date
    AND cf.price_group = dp.asset )
SELECT
  "Connext" AS bridge,
  ud.date AS date,
  ud.asset AS currency_symbol,
  ud.origin_chain AS source_chain_name,
  ud.destination_chain AS destination_chain_name,
  COUNT(CASE
      WHEN ud.amount_usd >= 10000 THEN ud.transfer_id
      ELSE NULL
  END
    ) AS tx_count_10k_txs,
  SUM(CASE
      WHEN ud.amount_usd >= 10000 THEN ud.amount_usd
      ELSE NULL
  END
    ) AS volume_10k_txs,
  AVG(CASE
      WHEN ud.amount_usd >= 10000 THEN ud.amount_usd
      ELSE NULL
  END
    ) AS avg_volume_10k_txs,
  AVG(ud.amount_usd) AS avg_volume,
  COUNT(ud.transfer_id) AS total_txs,
  SUM(ud.amount_usd) AS total_volume
FROM
  usd_data ud
WHERE
  ud.amount_usd IS NOT NULL
GROUP BY
  1,
  2,
  3,
  4,
  5