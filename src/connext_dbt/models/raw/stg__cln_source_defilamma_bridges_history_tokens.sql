WITH raw AS (
SELECT
  ht.url AS api_url,
  ht.date AS date,
  CAST(REGEXP_EXTRACT(ht.url, r'id=(\d+)') AS INT64) AS bridge_id,
  REGEXP_EXTRACT(ht.url, r'/(\d+)/') AS timestamp,
   REGEXP_EXTRACT(url, r'([^/]+)\?') AS chain,
  REGEXP_EXTRACT(ht.key, r'([^:]+)') AS chain_slug,
  REGEXP_EXTRACT(ht.key, r':(.+)') AS token_address,
  CASE
    WHEN key_type = "totalTokensDeposited" THEN "deposit"
    WHEN key_type = "totalTokensWithdrawn" THEN "withdrawal"
    ELSE "needs inspections!!!!"
  END AS tx_type,
  ht.symbol,
  ht.decimals,
  ht.usd_value

FROM `mainnet-bigq.raw.source_defilamma_bridges_history_tokens` ht
)

SELECT 
  b.display_name AS name,
  r.*,
  c.chain_id
FROM raw r
INNER JOIN `mainnet-bigq.raw.source_defilamma_bridges` b
ON r.bridge_id = b.id
INNER JOIN `mainnet-bigq.raw.source_defilamma_chains` c
ON r.chain = c.name
