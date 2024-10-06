WITH extracted_routes AS (
    SELECT
    -- Extract the JSON arrays once
        tx.id,
        tx.created_at,
        -- Pre-calculate the length of the to_route array once
        tx.from_address,
        tx.to_address,
        tx.from_tx_hash,
        tx.success_at,
        tx.from_chain_id,
        from_chain.name AS from_chain_name,
        tx.token_address,
        tx.from_amount_usd,
        tx.to_tx_hash,
        tx.to_chain_id,
        to_chain.name AS to_chain_name,
        tx.to_amount_usd,
        from_chain.fee_token_symbol AS fee_token_symbol,
        tx.state,
        tx.type,
        tx.event_type,
        JSON_EXTRACT_ARRAY(tx.from_route) AS from_route_array,
        JSON_EXTRACT_ARRAY(tx.to_route) AS to_route_array,
        ARRAY_LENGTH(JSON_EXTRACT_ARRAY(tx.to_route)) AS to_route_length,
        COALESCE(tx.token_symbol, tx.token_address) AS from_token_symbol,
        CAST(NULL AS FLOAT64) AS fee_amount_usd
    FROM
        {{source('raw', 'source_symbiosis_bridge_explorer_transactions')}} AS tx
    LEFT JOIN {{ref('chains')}} AS from_chain
    ON tx.from_chain_id = from_chain.chain_id
    LEFT JOIN {{ref('chains')}} AS to_chain
    ON tx.to_chain_id = to_chain.chain_id


    -- Keep only successful transfers
    WHERE
        state = 0
        AND type = 0
        AND event_type IN (1, 3)
),

semi_cleaned AS (
    SELECT DISTINCT
        -- Transfer details
        from_address,
        to_address,
        from_tx_hash AS from_hash,
        from_chain_name,
        from_amount_usd,
        to_tx_hash AS to_hash,

        -- from_route (first element of from_route_array)
        to_chain_name,
        to_amount_usd,
        fee_token_symbol,
        fee_amount_usd,
        CAST(id AS STRING) AS id,
        CAST(created_at AS TIMESTAMP) AS from_timestamp,

        -- to_route (last element of to_route_array)
        CAST(success_at AS TIMESTAMP) AS to_timestamp,
        CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.chain_id') AS INT64) AS from_chain_id,
        CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.amount') AS FLOAT64) AS from_amount,
        JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.symbol') AS from_token_symbol,
        JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.name') AS from_token_name,
        JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.address') AS from_token_address,

        -- Existing fields
        CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.decimals') AS FLOAT64)
            AS from_token_decimals,
        CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.chain_id') AS INT64)
            AS to_chain_id,
        CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.amount') AS FLOAT64) AS to_amount,
        JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.symbol') AS to_token_symbol,
        JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.name') AS to_token_name,
        JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.address') AS to_token_address,
        CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.decimals') AS FLOAT64)
            AS to_token_decimals,
        CAST(NULL AS STRING) AS gas_fee_symbol,
        CAST(NULL AS FLOAT64) AS gas_fee,
        CAST(NULL AS FLOAT64) AS gas_fee_usd

    FROM extracted_routes
)

SELECT
    id,
    -- from
    from_timestamp,
    from_address,
    from_hash,
    from_chain_id,
    from_chain_name,
    from_token_symbol,
    from_token_name,
    from_token_address,
    from_amount / POW(10, from_token_decimals) AS from_amount,
    from_amount_usd,
    
    -- to
    to_address,
    to_hash,
    to_timestamp,
    to_chain_id,
    to_chain_name,
    to_token_symbol,
    to_token_name,
    to_token_address,
    to_amount / POW(10, to_token_decimals) AS to_amount,
    to_amount_usd,
    
    -- fees
    gas_fee_symbol AS gas_token_symbol,
    gas_fee AS gas_amount,
    gas_fee_usd AS gas_amount_usd,
    fee_token_symbol AS relay_symbol,
    CAST(NULL AS FLOAT64) AS relay_amount,
    fee_amount_usd AS relay_amount_usd

FROM semi_cleaned
WHERE
    from_amount > 0
    AND to_amount > 0
