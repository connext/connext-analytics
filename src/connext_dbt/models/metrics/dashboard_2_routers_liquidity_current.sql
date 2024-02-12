-- Requirements: Chain asset - router liquidity (locked, total), pool (next asset, adopted)

WITH connext_tokens AS (
    SELECT DISTINCT
    ct.token_address,
    ct.token_name,
    ct.is_xerc20
  FROM `mainnet-bigq.stage.connext_tokens` ct
)

, router_liquidity AS (
  SELECT
    l.adopted AS asset_address,
    coalesce(ct.token_name, l.adopted) AS asset,
    CAST(ct.is_xerc20 AS BOOL) AS is_xerc20,
        CASE
      WHEN l.domain = '6648936' THEN 'Ethereum'
      WHEN l.domain = '1869640809' THEN 'Optimism'
      WHEN l.domain = '6450786' THEN 'BNB'
      WHEN l.domain = '6778479' THEN 'Gnosis'
      WHEN l.domain = '1886350457' THEN 'Polygon'
      WHEN l.domain = '1634886255' THEN 'Arbitrum One'
      WHEN l.domain = '1818848877' THEN 'Linea'
      WHEN l.domain = '1835365481' THEN 'Metis'
    ELSE
      l.domain
    END
      AS domain,
    SUM(l.balance_usd) AS balance_usd,
    SUM(l.locked_usd) AS locked_usd,
    SUM(l.removed_usd) AS removed_usd,
    SUM(l.supplied_usd) AS supplied_usd
  FROM `mainnet-bigq.raw.source__cartographer_router_with_balances` l
  LEFT JOIN connext_tokens ct
  ON l.adopted = ct.token_address
  GROUP BY 1,2,3,4
)

SELECT *
FROM router_liquidity