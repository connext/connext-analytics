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
  FROM `mainnet-bigq.public.stableswap_pool_events`
),

pool_tvl AS (

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
FROM stableswap_pools_bal agg

GROUP BY 1,2,3
ORDER BY 1 DESC),

-- Calculate APR and APY
apr_apy_calculations AS (
  SELECT
    date,
    pool_id,
    chain,
    tvl,
    fee,
    vol,
    (fee / NULLIF(tvl, 0)) * 365 AS apr, -- Annualize the daily fee to get APR
    (POWER(1 + (fee / NULLIF(tvl, 0)), 365) - 1) AS apy -- Calculate APY using compound interest formula    
  FROM pool_tvl
)

SELECT
  *
FROM apr_apy_calculations
WHERE pool_id = "0x12acadfa38ab02479ae587196a9043ee4d8bf52fcb96b7f8d2ba240f03bcd08a"
AND chain ="Arbitrum One"
