SELECT DISTINCT

    tx.kappa AS transfer_id,
    TIMESTAMP_SECONDS(CAST(tx.from_time AS INT64)) AS from_timestamp,
    TIMESTAMP_SECONDS(CAST(tx.to_time AS INT64)) AS to_timestamp,
    tx.from_address,
    tx.to_address,

    -- from
    tx.from_hash,
    tx.from_chain_id,
    from_chain.name AS from_chain_name,
    tx.from_token_address,
    tx.from_token_symbol,
    tx.from_formatted_value AS from_amount,
    
    -- to
    tx.to_hash,
    tx.to_chain_id,
    to_chain.name AS to_chain_name,
    tx.to_token_address,
    tx.to_token_symbol,
    tx.to_formatted_value AS to_amount,
    
    -- fees
    from_chain.nativeCurrency_symbol AS fee_token_symbol,
    NULL AS gas_fee,
    tx.from_token_symbol AS relayer_fee_symbol,
    (CAST(tx.from_formatted_value AS FLOAT64) - CAST(tx.to_formatted_value AS FLOAT64)) AS relayer_fee,

FROM `mainnet-bigq.raw.source_synapseprotocol_explorer_transactions` tx
LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS from_chain
  ON tx.from_chain_id = from_chain.chainId

LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS to_chain
  ON tx.to_chain_id = to_chain.chainId
