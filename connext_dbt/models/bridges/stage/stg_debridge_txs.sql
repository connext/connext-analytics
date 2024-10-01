-- INFO
-- This SQL script processes transaction data from the `mainnet-bigq.raw.source_de_bridge_explorer__transactions` table, 
-- extracting and transforming various fields related to token swaps and fees. 
-- It joins this data with chain metadata from `mainnet-bigq.raw.source_chainlist_network__chains` to enrich the transaction records with chain names and token symbols. 
-- The final output includes details about the transfer, such as chain IDs, token amounts, and associated fees.


WITH raw AS (
    SELECT *
    FROM {{ source('raw', 'source_de_bridge_explorer__transactions') }}
    WHERE state = 'ClaimedUnlock'
),

semi AS (
    -- adding distinct to avoid duplicates in semi to avoid distinct on large raw data "*"
    SELECT DISTINCT
        orderid_stringvalue AS transfer_id,
        NULL AS from_tx_hash,
        NULL AS to_tx_hash,
        unlockauthoritydst_stringvalue AS user_address_out,
        preswapdata_tokeninmetadata_symbol AS pre_swap_in_token_symbol,
        preswapdata_tokenoutmetadata_symbol AS pre_swap_out_token_symbol,
        giveofferwithmetadata_metadata_symbol AS from_actual_symbol,
        giveofferwithmetadata_tokenaddress_stringvalue AS from_actual_token_address,
        takeofferwithmetadata_metadata_symbol AS to_symbol,
        TIMESTAMP_SECONDS(CAST(creationtimestamp AS INT64)) AS date,
        SAFE_CAST(preswapdata_chainid_bigintegervalue AS INT64) AS pre_swap_chain_id,
        SAFE_CAST(preswapdata_inamount_bigintegervalue AS FLOAT64) AS pre_swap_in_amount,
        SAFE_CAST(preswapdata_outamount_bigintegervalue AS FLOAT64) AS pre_swap_out_amount,
        SAFE_CAST(giveofferwithmetadata_chainid_bigintegervalue AS INT64) AS from_chain_id,
        SAFE_CAST(giveofferwithmetadata_metadata_decimals AS INT64) AS from_actual_symbol_decimal,
        SAFE_CAST(takeofferwithmetadata_chainid_bigintegervalue AS INT64) AS to_chain_id,
        SAFE_CAST(takeofferwithmetadata_decimals AS INT64) AS to_symbol_decimal,
        SAFE_CAST(giveofferwithmetadata_finalamount_bigintegervalue AS FLOAT64) AS from_actual_value,
        SAFE_CAST(takeofferwithmetadata_amount_bigintegervalue AS FLOAT64) AS to_value,
        SAFE_CAST(fixfee_bigintegervalue AS FLOAT64) AS market_maker_gas_costs,
        SAFE_CAST(finalpercentfee_bigintegervalue AS FLOAT64) AS debridge_fee
    FROM raw
)

SELECT
    transfer_id AS id,
    date,
    from_chain.name AS from_chain_name,
    from_actual_token_address AS from_token_address,
    from_actual_symbol AS from_token_symbol,
    user_address_out,
    to_chain.name AS to_chain_name,
    to_symbol AS to_token_symbol,
    fee_chain.nativecurrency_symbol AS fee_token_symbol,
    from_actual_symbol AS protocol_fee_token_symbol,
    CAST(COALESCE(pre_swap_chain_id, from_chain_id) AS INT64) AS from_chain_id,
    CAST(from_tx_hash AS STRING) AS from_tx_hash,
    (from_actual_value + debridge_fee) / POW(10, from_actual_symbol_decimal) AS from_amount,
    CAST(to_chain_id AS INT64) AS to_chain_id,
    CAST(to_tx_hash AS STRING) AS to_tx_hash,
    to_value / POW(10, to_symbol_decimal) AS to_amount,
    market_maker_gas_costs / POW(10, fee_chain.nativecurrency_decimals) AS gas_fee,
    debridge_fee / POW(10, from_actual_symbol_decimal) AS protocol_fee_value

FROM semi AS s
LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS fee_chain
    ON s.from_chain_id = fee_chain.chainid

LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS from_chain
    ON s.from_chain_id = from_chain.chainid

LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS to_chain
    ON s.to_chain_id = to_chain.chainid
