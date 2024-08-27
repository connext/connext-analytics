-- DEbridge
SELECT 
"DEbridge" AS bridge_name,
TIMESTAMP_SECONDS(max(CAST(creationtimestamp AS INT64))) AS last_tx, 
TIMESTAMP_SECONDS(min(CAST(creationtimestamp AS INT64))) AS first_tx,
COUNT(orderid_stringvalue) AS tx_count
FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions`


UNION ALL 

-- AllBridge
SELECT 
"AllBridge" AS bridge_name,
TIMESTAMP_MILLIS(max(CAST(timestamp AS INT64))) AS last_tx, 
TIMESTAMP_MILLIS(min(CAST(timestamp AS INT64))) AS first_tx,
COUNT(id) AS tx_count
FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers`

UNION ALL

-- symbiosis
SELECT 
"Symbiosis" AS bridge_name,
max(CAST(success_at AS TIMESTAMP)) AS last_tx, 
min(CAST(success_at AS TIMESTAMP)) AS first_tx,
COUNT(id) AS tx_count
FROM `mainnet-bigq.raw.source_symbiosis_bridge_explorer_transactions`


UNION ALL

-- synapseses: mainnet-bigq.raw.source_synapseprotocol_explorer_transactions

SELECT 
"Synapse" AS bridge_name,
TIMESTAMP_SECONDS(max(CAST(to_time AS INT64))) AS last_tx, 
TIMESTAMP_SECONDS(min(CAST(to_time AS INT64))) AS first_tx,
COUNT(kappa) AS tx_count
FROM `mainnet-bigq.raw.source_synapseprotocol_explorer_transactions`