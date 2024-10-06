WITH raw AS (
    SELECT DISTINCT
        CAST(tx.kappa AS STRING) AS id,
        
        -- from
        tx.from_hash,
        RANK() OVER (PARTITION BY tx.kappa ORDER BY tx.from_time DESC) AS ranking_kappa,
        TIMESTAMP_SECONDS(CAST(tx.from_time AS INT64)) AS from_timestamp,
        tx.from_chain_id,
        from_chain.name AS from_chain_name,
        tx.from_address,
        tx.from_token_address,
        tx.from_token_symbol,
        CAST(tx.from_formatted_value AS FLOAT64) AS from_amount,

        -- to
        tx.to_hash,
        TIMESTAMP_SECONDS(CAST(tx.to_time AS INT64)) AS to_timestamp,
        tx.to_chain_id,
        to_chain.name AS to_chain_name,
        tx.to_address,
        tx.to_token_address,
        tx.to_token_symbol,
        CAST(tx.to_formatted_value AS FLOAT64) AS to_amount,

        -- fees
        from_chain.nativecurrency_symbol AS fee_token_symbol,
        CAST(NULL AS FLOAT64) AS gas_amount,
        tx.from_token_symbol AS relayer_fee_symbol,
        CAST(NULL AS FLOAT64) AS relay_amount

    FROM {{ source('raw', 'source_synapseprotocol_explorer_transactions') }} AS tx
    LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS from_chain
        ON tx.from_chain_id = from_chain.chainid

    LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS to_chain
        ON tx.to_chain_id = to_chain.chainid
)


SELECT * EXCEPT(ranking_kappa) FROM raw
WHERE ranking_kappa = 1
