SELECT
    date,
    from_address,
    to_address,
    tc_from,
    tc_to,
    bridge,
    tx_type,
    CASE
    -- withdrawal from Ethereum
        WHEN tx_type = "withdrawal" THEN "Ethereum"
        ELSE REPLACE(bridge, ' Bridge', '')
    END AS source_chain_name,
    CASE
    -- Deposit to Ethereum
        WHEN tx_type = "deposit" THEN "Ethereum"
        ELSE REPLACE(bridge, ' Bridge', '')
    END AS destination_chain_name,
    value,
    value_usd,
    gas_used,
    fee_usd,
    tx_count

FROM `mainnet-bigq.dune.source_native_evm_eth_bridges`
