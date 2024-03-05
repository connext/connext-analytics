SELECT
  hw.url AS api_url,
  hw.date AS date,
  REGEXP_EXTRACT(hw.url, r'id=(\d+)') AS bridge_id,
  REGEXP_EXTRACT(hw.url, r'/(\d+)/') AS timestamp,
   REGEXP_EXTRACT(url, r'([^/]+)\?') AS chain,
  REGEXP_EXTRACT(hw.key, r'([^:]+)') AS chain_slug,
  REGEXP_EXTRACT(hw.key, r':(.+)') AS wallet_address,
  hw.usd_value,
  hw.txs

FROM `mainnet-bigq.raw.source_defilamma_bridges_history_wallets` hw
