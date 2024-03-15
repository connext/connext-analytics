WITH raw AS (
SELECT
  CASE 
    WHEN n.name = "xDAI Chain" THEN "Gnosis" 
    ELSE n.name
  END AS chain_name,
  n.domainid,
  n.asset_symbol,
  n.asset_key AS asset,
  n.asset_mainnetequivalent AS canonical_asset_address,
  LOWER(a.local) AS local,
  LOWER(a.adopted) AS adopted,
  COALESCE(
    CAST(a.decimal AS INT64),
    CAST(n.asset_decimals AS INT64)
  ) AS asset_decimals

FROM `public.assets` a
INNER JOIN `mainnet-bigq.raw.source_chaindata_nija__metadata` n
ON a.domain = n.domainid
AND a.adopted = LOWER(n.asset_key)
)

SELECT *
FROM raw

-- Testing chain count by canonnical asset address

-- SELECT canonical_asset_address, COUNT(DISTINCT chain_name)
-- FROM raw
-- GROUP BY 1
-- ORDER BY 2 DESC