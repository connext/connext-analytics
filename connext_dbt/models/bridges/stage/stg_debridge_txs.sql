WITH 
evm_chains_token_metadata AS (
    SELECT DISTINCT
        symbol, chain_id, CAST(decimals AS INT64) AS decimals
    FROM (
        SELECT
            c.chain_id,
            symbol,
            decimals,
            rank() over (partition by blockchain, symbol order by contract_address) as rnk
        FROM {{source('dune', 'evm_chains_token_metadata')}} tm
        INNER JOIN {{ref('chains')}} c
            ON tm.blockchain = c.name
    )
    WHERE rnk = 1
),

raw AS (
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
),


semi_raw AS (
SELECT
    transfer_id AS id,
    date,
    -- from
    CAST(from_tx_hash AS STRING) AS from_tx_hash,
    CAST(COALESCE(pre_swap_chain_id, from_chain_id) AS INT64) AS from_chain_id,
    from_chain.name AS from_chain_name,
    CAST(NULL AS STRING) AS from_address,
    from_actual_token_address AS from_token_address,
    COALESCE(pre_swap_in_token_symbol, from_actual_symbol) AS from_token_symbol,
    COALESCE(
        CAST(pre_swap_in_amount AS FLOAT64) / IFNULL(POW(10, tm.decimals), POW(10,18)),
        CAST(from_actual_value AS FLOAT64) / IFNULL(POW(10, from_actual_symbol_decimal), POW(10,18))
    ) AS from_amount,
    from_actual_symbol AS interim_symbol,
    CAST(from_actual_value AS FLOAT64) / IFNULL(POW(10, from_actual_symbol_decimal), POW(10,18)) AS interim_amount,
    -- to
    CAST(to_tx_hash AS STRING) AS to_tx_hash,
    CAST(to_chain_id AS INT64) AS to_chain_id,
    to_chain.name AS to_chain_name,
    user_address_out AS to_address,
    CAST(NULL AS STRING) AS to_token_address,
    to_symbol AS to_token_symbol,
    to_value / POW(10, to_symbol_decimal) AS to_amount,
    
    fee_chain.fee_token_symbol AS gas_token_symbol,
    market_maker_gas_costs / POW(10, fee_chain.fee_token_decimal) AS gas_amount,
    from_actual_symbol AS relayer_fee_token_symbol,
    debridge_fee / POW(10, from_actual_symbol_decimal) AS relay_fee_amount

FROM semi AS s
LEFT JOIN {{ref('chains')}} AS fee_chain
    ON s.from_chain_id = fee_chain.chain_id

LEFT JOIN {{ref('chains')}} AS from_chain
    ON s.from_chain_id = from_chain.chain_id

LEFT JOIN {{ref('chains')}} AS to_chain
    ON s.to_chain_id = to_chain.chain_id

LEFT JOIN evm_chains_token_metadata AS tm
    ON s.pre_swap_in_token_symbol = tm.symbol AND CAST(s.from_chain_id AS INT64) = tm.chain_id
)
SELECT * FROM semi_raw
WHERE from_amount > 0 AND to_amount > 0