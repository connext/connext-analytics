WITH raw AS (
    SELECT DISTINCT *
    FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions`
)

SELECT
    orderid_stringvalue AS transfer_id,
    TIMESTAMP_SECONDS(CAST(creationtimestamp AS INT64)) AS date,

    SAFE_CAST(preswapdata_chainid_bigintegervalue AS INT64)
        AS pre_swap_chain_id,
    SAFE_CAST(preswapdata_inamount_bigintegervalue AS FLOAT64)
        AS pre_swap_in_amount,
    preswapdata_tokeninmetadata_symbol AS pre_swap_in_token_symbol,
    SAFE_CAST(preswapdata_outamount_bigintegervalue AS FLOAT64)
        AS pre_swap_out_amount,
    preswapdata_tokenoutmetadata_symbol AS pre_swap_out_token_symbol,

    SAFE_CAST(giveofferwithmetadata_chainid_bigintegervalue AS INT64)
        AS from_chain_id,
    SAFE_CAST(giveofferwithmetadata_metadata_decimals AS INT64)
        AS from_actual_symbol_decimal,
    giveofferwithmetadata_metadata_symbol AS from_actual_symbol,
    SAFE_CAST(takeofferwithmetadata_chainid_bigintegervalue AS INT64)
        AS to_chain_id,
    SAFE_CAST(takeofferwithmetadata_decimals AS INT64) AS to_symbol_decimal,
    takeofferwithmetadata_metadata_symbol AS to_symbol,
    SAFE_CAST(giveofferwithmetadata_finalamount_bigintegervalue AS FLOAT64)
        AS from_actual_value,
    SAFE_CAST(takeofferwithmetadata_amount_bigintegervalue AS FLOAT64)
        AS to_value
FROM raw
