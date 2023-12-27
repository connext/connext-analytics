
WITH stableswap_pools_bal AS (
  SELECT
      DATE_TRUNC(TIMESTAMP_SECONDS(timestamp), DAY) AS date,
      pool_id, 
      domain, 
      (
        CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS FLOAT64) 
      + CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS FLOAT64)
      ) AS balances,
      (
        CAST(JSON_EXTRACT_STRING_ARRAY(fees)[0] AS FLOAT64) 
      + CAST(JSON_EXTRACT_STRING_ARRAY(fees)[1] AS FLOAT64)
      ) AS fee,
      0 as vol
  FROM `public.stableswap_pool_events`
)

, stableswap_exchanges_agg AS (
  SELECT 
    DATE_TRUNC(TIMESTAMP_SECONDS(timestamp), DAY) AS date,
    pool_id, 
    domain, 
    (
      CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS FLOAT64) 
      + CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS FLOAT64)
    ) AS balances,
    CAST(fee AS FLOAT64), 
    (CAST(tokens_sold AS FLOAT64) + CAST(tokens_bought AS FLOAT64)) / 2 as vol 
  FROM `public.stableswap_exchanges`
)

SELECT
  agg.date,
  agg.pool_id, 
    CASE
      WHEN agg.domain = '6648936' THEN 'Ethereum'
      WHEN agg.domain = '1869640809' THEN 'Optimism'
      WHEN agg.domain = '6450786' THEN 'BNB'
      WHEN agg.domain = '6778479' THEN 'Gnosis'
      WHEN agg.domain = '1886350457' THEN 'Polygon'
      WHEN agg.domain = '1634886255' THEN 'Arbitrum One'
      WHEN agg.domain = '1818848877' THEN 'Linea'
      WHEN agg.domain = '31338' THEN 'Local Optimism'
      WHEN agg.domain = '31339' THEN 'Local Arbitrum One'
    ELSE
      CONCAT("Add this domain to Google sheet, not found for:", agg.domain)
    END
      AS chain,
  SUM(balances) AS tvl,
  SUM(fee) AS fee,
  SUM(vol) AS vol
FROM (
  (SELECT  * FROM stableswap_pools_bal WHERE vol IS NOT NULL)
  UNION ALL 
  (SELECT * FROM stableswap_exchanges_agg WHERE vol IS NOT NULL)
) agg
GROUP BY 1,2,3
ORDER BY 1







-- alternate

-- WITH stableswap_pools_bal AS (
--   SELECT
--       DATE_TRUNC(TIMESTAMP_SECONDS(timestamp), DAY) AS date,
--       pool_id, 
--       domain, 
--       (
--         CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS FLOAT64) 
--       + CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS FLOAT64)
--       ) AS balances,
--       (
--         CAST(JSON_EXTRACT_STRING_ARRAY(fees)[0] AS FLOAT64) 
--       + CAST(JSON_EXTRACT_STRING_ARRAY(fees)[1] AS FLOAT64)
--       ) AS fee,
--       0 as vol
--   FROM `public.stableswap_pool_events`
-- )

-- , stableswap_exchanges_agg AS (
--   SELECT 
--     DATE_TRUNC(TIMESTAMP_SECONDS(timestamp), DAY) AS date,
--     pool_id, 
--     domain, 
--     (
--       CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS FLOAT64) 
--       + CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS FLOAT64)
--     ) AS balances,
--     CAST(fee AS FLOAT64), 
--     (CAST(tokens_sold AS FLOAT64) + CAST(tokens_bought AS FLOAT64)) / 2 as vol 
--   FROM `public.stableswap_exchanges`
-- )

-- SELECT
--   agg.date,
--   agg.pool_id, 
--     CASE
--       WHEN agg.domain = '6648936' THEN 'Ethereum'
--       WHEN agg.domain = '1869640809' THEN 'Optimism'
--       WHEN agg.domain = '6450786' THEN 'BNB'
--       WHEN agg.domain = '6778479' THEN 'Gnosis'
--       WHEN agg.domain = '1886350457' THEN 'Polygon'
--       WHEN agg.domain = '1634886255' THEN 'Arbitrum One'
--       WHEN agg.domain = '1818848877' THEN 'Linea'
--       WHEN agg.domain = '31338' THEN 'Local Optimism'
--       WHEN agg.domain = '31339' THEN 'Local Arbitrum One'
--     ELSE
--       CONCAT("Add this domain to Google sheet, not found for:", agg.domain)
--     END
--       AS chain,
--   SUM(balances) AS tvl,
--   SUM(fee) AS fee,
--   SUM(vol) AS vol
-- FROM (
--   (SELECT  * FROM stableswap_pools_bal WHERE vol IS NOT NULL)
--   UNION ALL 
--   (SELECT * FROM stableswap_exchanges_agg WHERE vol IS NOT NULL)
-- ) agg
-- WHERE pool_id = "0x6d9af4a33ed4034765652ab0f44205952bc6d92198d3ef78fe3fb2b078d0941c" AND agg.domain = '1886350457'
-- GROUP BY 1,2,3
-- ORDER BY 1 DESC

