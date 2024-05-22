WITH raw AS (
-- TOKEN FLOW
SELECT
    date,
    fs_bridge,
    ts_bridge,
    COALESCE(fs_bridge, ts_bridge) AS bridge,
    
    -- if bridge sc address is in the from column, then it is a flow out
    CASE 
        WHEN fs_bridge = "Base Bridge" THEN "Base"
        WHEN fs_bridge = "Metis Bridge" THEN "Metis"
        WHEN fs_bridge = "zkEVM Bridge" THEN "Polygon zkEVM"
        WHEN fs_bridge = "Mantle Bridge" THEN "Mantle"
        WHEN fs_bridge = "zkSync Bridge" THEN "zkSync Era"
        WHEN fs_bridge = "Polygon Bridge" THEN "Polygon"
        WHEN fs_bridge = "Arbitrum Bridge" THEN "Arbitrum"
        WHEN fs_bridge = "StarkNet Bridge" THEN "StarkNet"
        WHEN fs_bridge = "Optimisum Bridge" THEN "Optimisum"
        WHEN fs_bridge IS NULL THEN "Ethereum"
    END AS src_chain,
    
    -- if bridge sc address is in the to column, then it is a flow in
    CASE 
        WHEN ts_bridge = "Base Bridge" THEN "Base"
        WHEN ts_bridge = "Metis Bridge" THEN "Metis"
        WHEN ts_bridge = "zkEVM Bridge" THEN "Polygon zkEVM"
        WHEN ts_bridge = "Mantle Bridge" THEN "Mantle"
        WHEN ts_bridge = "zkSync Bridge" THEN "zkSync Era"
        WHEN ts_bridge = "Polygon Bridge" THEN "Polygon"
        WHEN ts_bridge = "Arbitrum Bridge" THEN "Arbitrum"
        WHEN ts_bridge = "StarkNet Bridge" THEN "StarkNet"
        WHEN ts_bridge = "Optimisum Bridge" THEN "Optimisum"
        WHEN ts_bridge IS NULL THEN "Ethereum"
    END AS dst_chain,
    symbol AS token_symbol,
    usd_token_value AS value_usd
FROM `mainnet-bigq.dune.source_cannonical_bridges_flows_tokens_hourly`

UNION ALL

-- NATIVE FLOW
SELECT
    date,
    fs_bridge,
    ts_bridge,
    COALESCE(fs_bridge, ts_bridge) AS bridge,
    -- if bridge sc address is in the from column, then it is a flow out
    CASE 
        WHEN fs_bridge = "Base Bridge" THEN "Base"
        WHEN fs_bridge = "mode Bridge" THEN "Mode"
        WHEN fs_bridge = "Linea Bridge" THEN "Linea"
        WHEN fs_bridge = "metis Bridge" THEN "Metis"
        WHEN fs_bridge = "Arbitrum Bridge" THEN "Arbitrum"
        WHEN fs_bridge = "StarkNet Bridge" THEN "StarkNet"
        WHEN fs_bridge = "zkSync Era Bridge" THEN "zkSync Era"
        WHEN fs_bridge = "zkSync Lite Bridge" THEN "zkSync Lite"
        WHEN fs_bridge = "Polygon Bridge" THEN "Polygon"
        WHEN fs_bridge = "Optimism Main Bridge" THEN "Optimisum"
        WHEN fs_bridge = "Polygon zkEVM Bridge" THEN "Polygon zkEVM"
        WHEN fs_bridge IS NULL THEN "Ethereum"
    END AS src_chain,
    
    -- if bridge sc address is in the to column, then it is a flow in
    CASE 
        WHEN ts_bridge = "Base Bridge" THEN "Base"
        WHEN ts_bridge = "mode Bridge" THEN "Mode"
        WHEN ts_bridge = "Linea Bridge" THEN "Linea"
        WHEN ts_bridge = "metis Bridge" THEN "Metis"
        WHEN ts_bridge = "Arbitrum Bridge" THEN "Arbitrum"
        WHEN ts_bridge = "StarkNet Bridge" THEN "StarkNet"
        WHEN ts_bridge = "zkSync Era Bridge" THEN "zkSync Era"
        WHEN ts_bridge = "zkSync Lite Bridge" THEN "zkSync Lite"
        WHEN ts_bridge = "Polygon Bridge" THEN "Polygon"
        WHEN ts_bridge = "Optimism Main Bridge" THEN "Optimisum"
        WHEN ts_bridge = "Polygon zkEVM Bridge" THEN "Polygon zkEVM"
        WHEN ts_bridge IS NULL THEN "Ethereum"
    END AS dst_chain,
    symbol AS token_symbol,
    usd_token_value AS value_usd
FROM `mainnet-bigq.dune.source_cannonical_bridges_flows_native_hourly`
)

SELECT
    date,
    -- bridge name change: Optimism Main Bridge, mode Bridge, metis Bridge, Polygon zkEVM Bridge
    CASE 
        WHEN bridge = "Optimism Main Bridge" THEN "Optimisum Bridge"
        WHEN bridge = "mode Bridge" THEN "Mode Bridge"
        WHEN bridge = "metis Bridge" THEN "Metis Bridge"
        WHEN bridge = "zkEVM Bridge" THEN "Polygon zkEVM Bridge"
        WHEN bridge = "zkSync Bridge" THEN "zkSync Era Bridge"
        ELSE bridge
    END AS bridge,
    src_chain,
    dst_chain,
    token_symbol,
    SUM(value_usd) AS value_usd
FROM raw
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1