SELECT
  raw.timestamp AS last_tx_timestamp,
  id AS last_tx_hash
FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers_new` raw
WHERE raw.timestamp = (SELECT MAX(timestamp) FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers_new`)