WITH raw AS (
    SELECT DISTINCT
        tx.from_address,
        tx.to_address,
        tx.from_hash,
        tx.from_chain_id,
        from_chain.name AS from_chain_name,

        -- from
        tx.from_token_address,
        tx.from_token_symbol,
        tx.from_formatted_value AS from_amount,
        tx.to_hash,
        tx.to_chain_id,
        to_chain.name AS to_chain_name,

        -- to
        tx.to_token_address,
        tx.to_token_symbol,
        tx.to_formatted_value AS to_amount,
        from_chain.nativecurrency_symbol AS fee_token_symbol,
        tx.from_token_symbol AS relayer_fee_symbol,
        CAST(tx.kappa AS STRING) AS id,

        -- fees
        TIMESTAMP_SECONDS(CAST(tx.from_time AS INT64)) AS from_timestamp,
        TIMESTAMP_SECONDS(CAST(tx.to_time AS INT64)) AS to_timestamp,
        CAST(NULL AS FLOAT64) AS gas_fee,
        (CAST(tx.from_formatted_value AS FLOAT64) - CAST(tx.to_formatted_value AS FLOAT64)) AS relayer_fee,
        RANK() OVER (PARTITION BY tx.kappa ORDER BY tx.from_time DESC) AS ranking_kappa

    FROM {{ source('raw', 'source_synapseprotocol_explorer_transactions') }} AS tx
    LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS from_chain
        ON tx.from_chain_id = from_chain.chainid

    LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS to_chain
        ON tx.to_chain_id = to_chain.chainid
)


SELECT * FROM raw
WHERE ranking_kappa = 1
