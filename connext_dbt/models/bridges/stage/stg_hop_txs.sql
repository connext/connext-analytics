WITH raw_tx AS (
SELECT DISTINCT
    r.transferid AS id,
    TIMESTAMP_SECONDS(CAST(r.timestamp AS INT64)) AS from_timestamp,
    TIMESTAMP_SECONDS(CAST(NULLIF(r.bondtimestamp, 0) AS INT64)) AS to_timestamp,
    accountaddress AS from_address,
    recipientaddress AS to_address,

    -- from
    transactionhash AS from_hash,
    sourcechainid AS from_chain_id,
    COALESCE(from_chain.name, sourcechainslug) AS from_chain_name,
    token AS from_token_symbol,
    CAST(amountdisplay AS FLOAT64) AS from_amount,
    CAST(REGEXP_REPLACE(amountusddisplay, r'\$|,', '') AS FLOAT64) AS from_amount_usd,
    -- to
    r.bondtransactionhash AS to_hash,
    destinationchainid AS to_chain_id,
    COALESCE(to_chain.name, destinationchainslug) AS to_chain_name,
    token AS to_token_symbol,
    CAST(amountdisplay AS FLOAT64) - CAST(bonderfeedisplay AS FLOAT64) AS to_amount,
    CAST(REGEXP_REPLACE(amountusddisplay, r'\$|,', '') AS FLOAT64) - CAST(REGEXP_REPLACE(bonderfeeusddisplay, r'\$|,', '') AS FLOAT64) AS to_amount_usd,

    -- fees
    CAST(NULL AS STRING) AS gas_token_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    token AS relayer_fee_symbol,
    CAST(bonderfeedisplay AS FLOAT64) AS relayer_fee,
    CAST(REGEXP_REPLACE(bonderfeeusddisplay, r'\$|,', '') AS FLOAT64) AS relayer_fee_in_usd
    
FROM {{ source('stage', 'source_hop_explorer__transfers') }} r
LEFT JOIN {{ref('chains')}} AS from_chain
  ON r.sourcechainid = from_chain.chain_id
LEFT JOIN {{ref('chains')}} AS to_chain
  ON r.destinationchainid = to_chain.chain_id
WHERE r.id NOT IN (
    '207960',
    '110530',
    '132995',
    '88441',
    '126748',
    '126615'
)
)

SELECT * FROM raw_tx tx
WHERE tx.from_amount > 0 AND tx.to_amount > 0 AND tx.to_amount_usd > 0 AND tx.from_amount_usd > 0
