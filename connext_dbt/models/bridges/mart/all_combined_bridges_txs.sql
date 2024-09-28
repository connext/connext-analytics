SELECT * FROM {{ ref('cln_hop_txs') }}
UNION ALL
SELECT * FROM {{ ref('cln_debridge_txs') }}
UNION ALL
SELECT * FROM {{ ref('cln_symbiosis_txs') }}
UNION ALL
SELECT * FROM {{ ref('cln_synapse_txs') }}