WITH raw AS (
SELECT
  hw.url AS api_url,
  hw.date AS date,
  CAST(REGEXP_EXTRACT(hw.url, r'id=(\d+)') AS INT64) AS bridge_id,
  REGEXP_EXTRACT(hw.url, r'/(\d+)/') AS timestamp,
   REGEXP_EXTRACT(url, r'([^/]+)\?') AS chain,
  REGEXP_EXTRACT(hw.key, r'([^:]+)') AS chain_slug,
  REGEXP_EXTRACT(hw.key, r':(.+)') AS wallet_address,
  CASE
    WHEN key_type = "totalAddressDeposited" THEN "deposit"
    WHEN key_type = "totalAddressWithdrawn" THEN "withdrawal"
    ELSE "needs inspections!!!!"
  END AS tx_type,
  hw.usd_value,
  hw.txs

FROM `mainnet-bigq.raw.source_defilamma_bridges_history_wallets` hw
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