WITH
  connext_tokens AS (
  SELECT
    DISTINCT ct.token_address,
    ct.token_name,
    ct.is_xerc20
  FROM
    `mainnet-bigq.stage.connext_tokens` ct ),
  sp AS (
  SELECT
    *,
    CASE
      WHEN domain = '6648936' THEN 'Ethereum'
      WHEN domain = '1869640809' THEN 'Optimism'
      WHEN domain = '6450786' THEN 'BNB'
      WHEN domain = '6778479' THEN 'Gnosis'
      WHEN domain = '1886350457' THEN 'Polygon'
      WHEN domain = '1634886255' THEN 'Arbitrum One'
      WHEN domain = '1818848877' THEN 'Linea'
      WHEN domain = '31338' THEN 'Local Optimism'
      WHEN domain = '31339' THEN 'Local Arbitrum One'
    ELSE
    CONCAT("Add this domain to Google sheet, not found for:", domain)
  END
    AS chain,
    JSON_EXTRACT_STRING_ARRAY(pooled_tokens)[0] AS token_1,
    JSON_EXTRACT_STRING_ARRAY(pooled_tokens)[1] AS token_2,
    CAST(JSON_EXTRACT_STRING_ARRAY(pool_token_decimals)[0] AS NUMERIC) AS pool_token_decimals_1,
    CAST(JSON_EXTRACT_STRING_ARRAY(pool_token_decimals)[1] AS NUMERIC) AS pool_token_decimals_2,
    CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS NUMERIC) AS balances_1,
    CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS NUMERIC) AS balances_2
  FROM
    `public.stableswap_pools`
  WHERE
    balances IS NOT NULL ),
  MaxAssetPrices AS (
  SELECT
    canonical_id,
    MAX(timestamp) AS max_timestamp
  FROM
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
  GROUP BY
    canonical_id )
SELECT
  sp.key AS pool_id,
  sp.chain,
  assets.canonical_id,
  COALESCE(ct_1.token_name, sp.token_1) AS token_1_name,
  COALESCE(ct_2.token_name, sp.token_2) AS token_2_name,
  sp.balances_1 / POW(10, sp.pool_token_decimals_1) AS pool_1_amount,
  sp.balances_2 / POW(10, sp.pool_token_decimals_2) AS pool_2_amount,
  -- USD
  asset_prices.price * sp.balances_1 / POW(10, sp.pool_token_decimals_1) AS usd_pool_1_amount,
  asset_prices.price * sp.balances_2 / POW(10, sp.pool_token_decimals_2) AS usd_pool_2_amount
FROM
  sp
LEFT JOIN
  connext_tokens ct_1
ON
  sp.token_1 = ct_1.token_address
LEFT JOIN
  connext_tokens ct_2
ON
  sp.token_2 = ct_2.token_address
LEFT JOIN
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets` assets
ON
  sp.token_1 = assets.id
  AND sp.domain = assets.domain
LEFT JOIN
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
ON
  assets.canonical_id = asset_prices.canonical_id
INNER JOIN
  MaxAssetPrices
ON
  asset_prices.canonical_id = MaxAssetPrices.canonical_id
  AND asset_prices.timestamp = MaxAssetPrices.max_timestamp