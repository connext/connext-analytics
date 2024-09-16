

SELECT DISTINCT
    r.transferid AS transfer_id,
    TIMESTAMP_SECONDS(CAST(r.timestamp AS INT64)) AS date,
    accountaddress AS from_address,
    recipientaddress AS to_address,

    -- from
    sourcechainid AS from_chain_id,
    sourcechainslug AS from_chain_name,
    token AS from_token_symbol,
    amount AS from_amount,
    amountusd AS from_amount_usd,
    -- to
    destinationchainid AS to_chain_id,
    destinationchainslug AS to_chain_name,
    token AS to_token_symbol,
    CAST(amount AS FLOAT64) - CAST(bonderfee AS FLOAT64) AS to_amount,
    CAST(amountusd AS FLOAT64) - CAST(bonderfeeusd AS FLOAT64) AS to_amount_usd,
    
    -- fees
    NULL AS gas_fee,
    token AS relayer_fee_symbol,
    bonderfee AS relayer_fee,
    bonderfeeusd AS relayer_fee_in_usd,
    amountusd AS amount_usd,
    
FROM `mainnet-bigq.stage.source_hop_explorer__transfers` r


-- SELECT * FROM `mainnet-bigq.stage.source_hop_explorer__transfers`
-- LIMIT 10