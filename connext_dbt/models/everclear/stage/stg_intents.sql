-- TODO for stage:
-- remove padding of tokens
-- convert amounts into decimals
-- add chain and token info -> chain names and token symbols
-- convert date to timestamp
-- convert data types for amounts, dates, etc.

WITH 
    evm_chains_token_metadata AS (
        SELECT DISTINCT
            domain_id AS chain_id,
            symbol,
            decimals,
            LOWER(address) AS contract_address
        FROM {{ ref("everclear_tokens") }}
    ),
    raw AS (
    SELECT
        i.id,
        i.status,
        i.hub_status,
        CAST(i.uploaded_at AS TIMESTAMP) AS updated_at,
        i.origin_transaction_hash AS from_hash,
        TIMESTAMP_SECONDS(CAST(i.origin_timestamp AS INT64)) AS origin_timestamp,
        TIMESTAMP_SECONDS(CAST(i.hub_added_timestamp AS INT64)) AS hub_added_timestamp,
        TIMESTAMP_SECONDS(CAST(i.hub_settlement_enqueued_timestamp AS INT64)) AS hub_settlement_enqueued_timestamp,
        TIMESTAMP_SECONDS(CAST(i.settlement_timestamp AS INT64)) AS settlement_timestamp,
        CAST(i.origin_origin AS INT64) AS from_chain_id,
        fa.name AS from_chain_name,
        LOWER(
            CONCAT('0x', REGEXP_REPLACE(SUBSTR(i.origin_input_asset, 3), r'^0{24}', ''))
        ) AS from_asset_address,
        CAST(i.settlement_domain AS INT64) AS to_chain_id,
        ta.name AS to_chain_name,
        LOWER(i.settlement_asset) AS to_asset_address,
        LOWER(
            CONCAT('0x', REGEXP_REPLACE(SUBSTR(i.origin_initiator, 3), r'^0{24}', ''))
        ) AS origin_initiator,
        CAST(i.hub_settlement_enqueued_timestamp AS FLOAT64) AS hub_settlement_enqueued_timestamp_epoch,
        CAST(i.hub_added_timestamp AS FLOAT64) AS hub_added_timestamp_epoch,
        0.0001 AS fee_value,
        CAST(i.origin_amount AS FLOAT64) AS origin_amount,
        CAST(i.settlement_amount AS FLOAT64) AS settlement_amount,
        CAST(i.hub_settlement_amount AS FLOAT64) AS hub_settlement_amount
    FROM {{ source("everclear_prod_db", "intents") }} AS i
    LEFT JOIN `mainnet-bigq.metadata.chains` fa on CAST(i.origin_origin AS INT64) = fa.chain_id
    LEFT JOIN `mainnet-bigq.metadata.chains` ta on CAST(i.settlement_domain AS INT64) = ta.chain_id
    ),

    semi_raw AS (
        SELECT 
            r.updated_at,
            r.id,
            r.status,
            r.hub_status,
            r.origin_initiator,
            -- from
            r.from_hash,
            r.origin_timestamp,
            r.from_chain_id,
            r.from_chain_name,
            r.from_asset_address,
            f_metadata.symbol AS from_asset_symbol,
            18 AS from_asset_decimals,
            r.origin_amount / POWER(10, 18) AS from_asset_amount,
            
            -- to
            r.settlement_timestamp,
            r.to_chain_id,
            r.to_chain_name,
            r.to_asset_address,
            t_metadata.symbol AS to_asset_symbol,
            t_metadata.decimals AS to_asset_decimals,
            r.settlement_amount / POWER(10, t_metadata.decimals) AS to_asset_amount,
            -- misc
            r.hub_added_timestamp,
            r.hub_settlement_enqueued_timestamp,
            r.fee_value,
            r.hub_settlement_enqueued_timestamp_epoch,
            r.hub_settlement_amount / POWER(10, 18) AS hub_settlement_amount,
            r.hub_added_timestamp_epoch

        FROM raw r
        LEFT JOIN evm_chains_token_metadata AS f_metadata
            ON r.from_chain_id = f_metadata.chain_id
            AND r.from_asset_address = f_metadata.contract_address
        LEFT JOIN evm_chains_token_metadata AS t_metadata
            ON r.to_chain_id = t_metadata.chain_id
            AND r.to_asset_address = t_metadata.contract_address
    )

SELECT * 
FROM semi_raw