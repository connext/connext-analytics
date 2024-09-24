SELECT DISTINCT

    tx.id AS transfer_id,
    CAST(tx.created_at AS TIMESTAMP) AS from_timestamp,
    tx.from_address,
    tx.to_address,

    -- from
    tx.from_tx_hash AS from_hash,
    CAST(tx.success_at AS TIMESTAMP) AS to_timestamp,
    tx.from_chain_id,
    from_chain.name AS from_chain_name,
    tx.token_address AS from_token_address,
    COALESCE(tx.token_symbol, tx.token_address) AS from_token_symbol,
    NULL AS from_amount,
    tx.from_amount_usd,

    -- to
    tx.to_tx_hash AS to_hash,
    tx.to_chain_id,
    to_chain.name AS to_chain_name,
    tx.token_address AS to_token_address,
    COALESCE(tx.token_symbol, tx.token_address) AS to_token_symbol,
    NULL AS to_amount,
    tx.to_amount_usd,

    -- fees
    from_chain.nativeCurrency_symbol AS fee_token_symbol,
    NULL AS gas_fee,
    (tx.from_amount_usd - tx.to_amount_usd) AS fee_amount_usd,

FROM `mainnet-bigq.raw.source_symbiosis_bridge_explorer_transactions` tx
LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS from_chain
  ON tx.from_chain_id = from_chain.chainId
LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS to_chain
  ON tx.to_chain_id = to_chain.chainId