-- with this query: 
-- 1. pull usd price by tx
-- 2. 


WITH connext_tokens AS (
    SELECT DISTINCT
        ct.token_address,
        ct.token_name,
        ct.is_xerc20
    FROM `mainnet-bigq.stage.connext_tokens` ct
),

semi_ready AS (
    SELECT

        tiu.transfer_id,
        tiu.canonical_id,
        tiu.xcall_transaction_hash,
        tiu.xcall_caller,
        tiu.to,
        tiu.origin_sender,
        tiu.bridged_amt,
        -- origin
        CAST(cc_origin.is_xerc20 AS BOOL) AS is_origin_asset_xerc20,
        COALESCE(cc_origin.token_name, tiu.origin_transacting_asset)
            AS origin_asset,
        tiu.origin_transacting_amount,

        -- destination
        cc_destination.is_xerc20 AS is_destination_asset_xerc20,
        COALESCE(cc_destination.token_name, tiu.destination_transacting_asset)
            AS destination_asset,
        tiu.destination_transacting_asset,
        tiu.destination_transacting_amount,
        tiu.xcall_tx_origin,
        tiu.execute_tx_origin,
        TIMESTAMP_SECONDS(tiu.xcall_timestamp) AS xcall_timestamp,
        CAST(tiu.execute_timestamp AS TIMESTAMP) AS execute_timestamp,
        CAST(tiu.reconcile_timestamp AS TIMESTAMP) AS reconcile_timestamp,
        tiu.origin_chain,
        tiu.destination_chain,
        tiu.caller_type,
        tiu.contract_name,
        tiu.contract_author,
        tiu.token_decimal,
        tiu.d_bridged_amt,
        tiu.d_origin_amount,
        tiu.d_destination_amount,
        tiu.price,
        tiu.closet_price_rank,
        tiu.usd_bridged_amt AS usd_bridged_amount,
        tiu.usd_origin_amount,
        tiu.usd_destination_amount,
        t.message_status,
        t.status,
        t.error_message,
        t.error_status

    FROM `mainnet-bigq.public.transfers_in_usd` tiu
    LEFT JOIN connext_tokens cc_origin
        ON tiu.origin_transacting_asset = cc_origin.token_address
    LEFT JOIN connext_tokens cc_destination
        ON tiu.destination_transacting_asset = cc_destination.token_address
    INNER JOIN `mainnet-bigq.public.transfers` t
        ON tiu.transfer_id = t.transfer_id

)


SELECT

    sr.transfer_id,
    sr.canonical_id,
    sr.xcall_transaction_hash,
    sr.xcall_caller,
    sr.to,
    sr.origin_sender,
    sr.bridged_amt,
    -- origin
    sr.is_origin_asset_xerc20,
    sr.origin_asset,
    sr.origin_transacting_amount,
    -- destination
    sr.is_destination_asset_xerc20,
    sr.destination_asset,
    sr.destination_transacting_asset,
    sr.destination_transacting_amount,
    sr.xcall_tx_origin,
    sr.execute_tx_origin,
    sr.xcall_timestamp,
    sr.execute_timestamp,
    sr.reconcile_timestamp,
    sr.origin_chain,
    sr.destination_chain,
    sr.caller_type,
    sr.contract_name,
    sr.contract_author,
    sr.message_status,
    sr.status,
    sr.error_message,
    sr.error_status,
    sr.token_decimal,
    sr.d_bridged_amt,
    sr.d_origin_amount,
    sr.d_destination_amount,
    sr.price,
    sr.closet_price_rank,
    sr.usd_origin_amount,
    sr.usd_bridged_amount,
    sr.usd_destination_amount
FROM semi_ready sr
