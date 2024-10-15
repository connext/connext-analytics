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
            inv.id,
            inv.hub_invoice_id,
            inv.hub_invoice_intent_id,
            inv.origin_status,
            CAST(inv.uploaded_at AS TIMESTAMP) AS updated_at,
            TIMESTAMP_SECONDS(CAST(inv.origin_timestamp AS INT64)) AS origin_timestamp,
            inv.hub_status,
            CAST(inv.hub_invoice_entry_epoch AS INT64) AS hub_invoice_entry_epoch,
            CAST(inv.hub_settlement_epoch AS INT64) AS hub_settlement_epoch,
            CAST(inv.hub_invoice_enqueued_timestamp AS INT64) AS hub_invoice_enqueued_timestamp,
            LOWER(
                CONCAT('0x', REGEXP_REPLACE(SUBSTR(inv.origin_initiator, 3), r'^0{24}', ''))
            ) AS origin_initiator,
            CAST(inv.origin_origin AS INT64) AS from_chain_id,
            fa.name AS from_chain_name,
            LOWER(
                CONCAT('0x', REGEXP_REPLACE(SUBSTR(inv.origin_input_asset, 3), r'^0{24}', ''))
            ) AS from_asset_address,
            CAST(inv.hub_invoice_amount AS FLOAT64) AS hub_invoice_amount

        FROM {{ source("everclear_prod_db", "invoices") }} inv
        LEFT JOIN `mainnet-bigq.metadata.chains` fa on CAST(inv.origin_origin AS INT64) = fa.chain_id
    ),
    semi_raw AS (
        SELECT 
            r.updated_at,
            r.id,
            r.hub_invoice_id,
            r.hub_invoice_intent_id,
            r.origin_status,
            r.hub_status,
            r.origin_timestamp,
            r.hub_invoice_enqueued_timestamp,
            r.hub_invoice_entry_epoch,
            r.hub_settlement_epoch,
            r.origin_initiator,
            r.from_chain_id,
            r.from_chain_name,
            r.from_asset_address,
            f_metadata.symbol AS from_asset_symbol,
            18 AS from_asset_decimals,
            r.hub_invoice_amount / POWER(10, 18) AS hub_invoice_amount
        FROM raw r
        LEFT JOIN evm_chains_token_metadata AS f_metadata
            ON r.from_chain_id = f_metadata.chain_id
            AND r.from_asset_address = f_metadata.contract_address
    )

SELECT *
FROM semi_raw