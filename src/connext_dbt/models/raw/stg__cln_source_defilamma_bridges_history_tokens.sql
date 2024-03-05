SELECT
  ht.url AS api_url,
  ht.date AS date,
  REGEXP_EXTRACT(ht.url, r'id=(\d+)') AS bridge_id,
  REGEXP_EXTRACT(ht.url, r'/(\d+)/') AS timestamp,
   REGEXP_EXTRACT(url, r'([^/]+)\?') AS chain,
  REGEXP_EXTRACT(ht.key, r'([^:]+)') AS chain_slug,
  REGEXP_EXTRACT(ht.key, r':(.+)') AS token_address,
  ht.symbol,
  ht.decimals,
  ht.usd_value,
  ht.amount AS token_amount,

FROM `mainnet-bigq.raw.source_defilamma_bridges_history_tokens` ht
