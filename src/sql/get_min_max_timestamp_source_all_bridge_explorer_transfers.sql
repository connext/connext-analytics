SELECT
    MIN(timestamp) AS min_timestamp,
    MAX(timestamp) AS max_timestamp
FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers`
