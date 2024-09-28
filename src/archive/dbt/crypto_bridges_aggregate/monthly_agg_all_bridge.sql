-- get data on all bridges for this year


WITH raw AS (
  -- from dune
  SELECT 
    dune.bridge,
    DATE(dune.date) AS date,
    dune.currency_symbol,
    dune.source_chain_name,
    dune.destination_chain_name,
    dune.tx_count_10k_txs,
    dune.volume_10k_txs,
    dune.avg_volume_10k_txs,
    dune.avg_volume,
    dune.total_txs,
    dune.total_volume
  FROM `mainnet-bigq.dune.source_bridges_aggregate_flows_monthly` dune
  where bridge is NOT NULL
  UNION ALL 
  -- from synapses
  SELECT 
    "synapse" AS bridge,
    CAST(synapse.date AS DATE) AS date,
    synapse.currency_symbol,
    synapse.source_chain_name,
    synapse.destination_chain_name,
    synapse.tx_count_10k_txs,
    synapse.volume_10k_txs,
    synapse.avg_volume_10k_txs,
    synapse.avg_volume,
    synapse.total_txs,
    synapse.total_volume
  FROM `mainnet-bigq.crypto_bridges_aggregate.monthly_agg_synapseprotocol` synapse
  
  UNION ALL
  -- from debridge
  SELECT 
    debridge.bridge,
    CAST(debridge.date AS DATE) AS date,
    debridge.currency_symbol,
    debridge.source_chain_name,
    debridge.destination_chain_name,
    debridge.tx_count_10k_txs,
    debridge.volume_10k_txs,
    debridge.avg_volume_10k_txs,
    debridge.avg_volume,
    debridge.total_txs,
    debridge.total_volume
  FROM `mainnet-bigq.crypto_bridges_aggregate.monthly_agg_debridge` debridge

  UNION ALL
  -- from connext
  SELECT 
    connext.bridge,
    CAST(connext.date AS DATE) AS date,
    connext.currency_symbol,
    connext.source_chain_name,
    connext.destination_chain_name,
    connext.tx_count_10k_txs,
    connext.volume_10k_txs,
    connext.avg_volume_10k_txs,
    connext.avg_volume,
    connext.total_txs,
    connext.total_volume
  FROM `mainnet-bigq.crypto_bridges_aggregate.monthly_agg_connext` connext

  )


-- formulate consistency for tokens, chains, and bridges names
SELECT
    r.bridge,
    r.date,
    r.currency_symbol,
    CASE 
        WHEN r.source_chain_name = "Arbitrum" THEN "Arbitrum One" 
        WHEN r.source_chain_name = "arbitrum" THEN "Arbitrum One" 
        WHEN r.source_chain_name = "Base" THEN "Base Mainnet"
        WHEN r.source_chain_name = "base" THEN "Base Mainnet"
        WHEN r.source_chain_name = "Binance Smart Chain Mainnet" THEN "BNB Chain"
        WHEN r.source_chain_name = "Ethereum" THEN "Ethereum Mainnet"
        WHEN r.source_chain_name = "ethereum" THEN "Ethereum Mainnet"
        WHEN r.source_chain_name = "linea" THEN "Linea Mainnet"
        WHEN r.source_chain_name = "Linea" THEN "Linea Mainnet"
        WHEN r.source_chain_name = "Optimism" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "Optimisum" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "optimism" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "Optimism" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "Optimism Main" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "Optimistic Ethereum" THEN "Optimism Mainnet"
        WHEN r.source_chain_name = "Polygon" THEN "Polygon Mainnet"
        WHEN r.source_chain_name = "polygon" THEN "Polygon Mainnet"
        WHEN r.source_chain_name = "Matic Mainnet" THEN "Polygon Mainnet"
        WHEN r.source_chain_name = "zkEVM" THEN "Polygon zkEVM"
        WHEN r.source_chain_name = "zkSync" THEN "zkSync"
        WHEN r.source_chain_name = "zkSync Era" THEN "zkSync"
        WHEN r.source_chain_name = "zkSync Lite" THEN "zkSync"
        WHEN r.source_chain_name = "zksync" THEN "zkSync"
        WHEN r.source_chain_name = "Metis" THEN "Metis Mainnet"
        WHEN r.source_chain_name = "Metis Andromeda" THEN "Metis Mainnet"
        WHEN r.source_chain_name = "Metis Andromeda Mainnet" THEN "Metis Mainnet"
        WHEN r.source_chain_name = "Mode" THEN "Mode Mainnet"
        WHEN r.source_chain_name = "mode" THEN "Mode Mainnet"
        WHEN r.source_chain_name = "blast" THEN "Blast Mainnet"
        WHEN r.source_chain_name = "lisk" THEN "Lisk Mainnet"
        ELSE r.source_chain_name 
    END AS source_chain_name,
    -- destination_chain_name 
        CASE 
        WHEN r.destination_chain_name = "Arbitrum" THEN "Arbitrum One" 
        WHEN r.destination_chain_name = "arbitrum" THEN "Arbitrum One" 
        WHEN r.destination_chain_name = "Base" THEN "Base Mainnet"
        WHEN r.destination_chain_name = "base" THEN "Base Mainnet"
        WHEN r.destination_chain_name = "Binance Smart Chain Mainnet" THEN "BNB Chain"
        WHEN r.destination_chain_name = "Ethereum" THEN "Ethereum Mainnet"
        WHEN r.destination_chain_name = "ethereum" THEN "Ethereum Mainnet"
        WHEN r.destination_chain_name = "linea" THEN "Linea Mainnet"
        WHEN r.destination_chain_name = "Linea" THEN "Linea Mainnet"
        WHEN r.destination_chain_name = "Optimism" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "Optimisum" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "optimism" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "Optimism" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "Optimism Main" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "Optimistic Ethereum" THEN "Optimism Mainnet"
        WHEN r.destination_chain_name = "Polygon" THEN "Polygon Mainnet"
        WHEN r.destination_chain_name = "polygon" THEN "Polygon Mainnet"
        WHEN r.destination_chain_name = "Matic Mainnet" THEN "Polygon Mainnet"
        WHEN r.destination_chain_name = "zkEVM" THEN "Polygon zkEVM"
        WHEN r.destination_chain_name = "zkSync" THEN "zkSync"
        WHEN r.destination_chain_name = "zkSync Era" THEN "zkSync"
        WHEN r.destination_chain_name = "zkSync Lite" THEN "zkSync"
        WHEN r.destination_chain_name = "zksync" THEN "zkSync"
        WHEN r.destination_chain_name = "Metis" THEN "Metis Mainnet"
        WHEN r.destination_chain_name = "Metis Andromeda" THEN "Metis Mainnet"
        WHEN r.destination_chain_name = "Metis Andromeda Mainnet" THEN "Metis Mainnet"
        WHEN r.destination_chain_name = "Mode" THEN "Mode Mainnet"
        WHEN r.destination_chain_name = "mode" THEN "Mode Mainnet"
        WHEN r.destination_chain_name = "blast" THEN "Blast Mainnet"
        WHEN r.destination_chain_name = "lisk" THEN "Lisk Mainnet"
        ELSE r.destination_chain_name 
    END AS destination_chain_name,
    r.tx_count_10k_txs,
    r.volume_10k_txs,
    r.avg_volume_10k_txs,
    r.avg_volume,
    r.total_txs,
    r.total_volume
FROM raw r
