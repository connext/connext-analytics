SELECT DISTINCT
    r.transferid AS transfer_id,
    TIMESTAMP_SECONDS(CAST(r.timestamp AS INT64)) AS from_timestamp,
    TIMESTAMP_SECONDS(CAST(r.bondtimestamp AS INT64)) AS to_timestamp,
    accountaddress AS from_address,
    recipientaddress AS to_address,

    -- from
    r.transferid AS from_hash,
    sourcechainid AS from_chain_id,
    sourcechainslug AS from_chain_name,
    token AS from_token_symbol,
    CAST(amountdisplay AS FLOAT64) AS from_amount,
    CAST(REGEXP_REPLACE(amountusddisplay, r'\$|,', '') AS FLOAT64) AS from_amount_usd,
    -- to
    transactionhash AS to_hash,
    destinationchainid AS to_chain_id,
    destinationchainslug AS to_chain_name,
    token AS to_token_symbol,
    CAST(amountdisplay AS FLOAT64) - CAST(bonderfeedisplay AS FLOAT64) AS to_amount,
    CAST(CAST(REGEXP_REPLACE(amountusddisplay, r'\$|,', '') AS FLOAT64) - CAST(bonderfeeusd AS FLOAT64) AS FLOAT64) AS to_amount_usd,
    
    -- fees
    NULL AS gas_fee,
    token AS relayer_fee_symbol,
    CAST(bonderfeedisplay AS FLOAT64) AS relayer_fee,
    CAST(REGEXP_REPLACE(bonderfeeusddisplay, r'\$|,', '') AS FLOAT64) AS relayer_fee_in_usd
    
    
FROM `mainnet-bigq.stage.source_hop_explorer__transfers` r