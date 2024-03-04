SELECT 
  b.id, 
  CAST(JSON_EXTRACT_SCALAR(chains, '$') AS STRING) AS name
FROM `mainnet-bigq.raw.source_defilamma_bridges` b,
UNNEST(JSON_EXTRACT_ARRAY(chains)) AS chains
