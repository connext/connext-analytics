WITH connext_tokens AS (
    SELECT DISTINCT
        ct.token_address,
        ct.token_name,
        ct.is_xerc20
    FROM `mainnet-bigq.stage.connext_tokens` ct
),

hourly_price AS (
    SELECT
        symbol,
        CASE
            WHEN symbol = 'ETH' THEN 'WETH'
            WHEN symbol = 'NEXT' THEN 'NEXT'
            WHEN STARTS_WITH (symbol, 'next') THEN REGEXP_REPLACE (symbol, '^next', '')
            WHEN symbol = 'alUSD' THEN 'USDT'
            WHEN symbol = 'nextALUSD' THEN 'USDT'
            WHEN symbol = 'instETH' THEN 'WETH'
            WHEN symbol = 'ezETH' THEN 'WETH'
            WHEN symbol = 'alETH' THEN 'WETH'
            WHEN symbol = 'nextalETH' THEN 'WETH'
            ELSE symbol
        END
            AS price_group,
        date,
        average_price AS price

    FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`
)

, semi_txs AS (
    SELECT

        t.transfer_id,
        t.canonical_id,
        t.xcall_transaction_hash,
        t.xcall_caller,
        t.to,
        t.origin_sender,
        t.xcall_tx_origin,
        t.execute_tx_origin,
        CAST(cc_origin.is_xerc20 AS BOOL) AS is_origin_asset_xerc20,
        COALESCE(cc_origin.token_name, t.origin_transacting_asset)
            AS origin_asset,
        CAST(cc_destination.is_xerc20 AS BOOL) AS is_destination_asset_xerc20,
        COALESCE(cc_destination.token_name, t.destination_transacting_asset)
            AS destination_asset,
        t.origin_transacting_asset,
        t.destination_transacting_asset,
        TIMESTAMP_SECONDS(tiu.xcall_timestamp) AS xcall_timestamp,
        CAST(tiu.execute_timestamp AS TIMESTAMP) AS execute_timestamp,
        CAST(tiu.reconcile_timestamp AS TIMESTAMP) AS reconcile_timestamp,
        tiu.origin_chain,
        tiu.destination_chain,
        tiu.caller_type,
        tiu.contract_name,
        tiu.contract_author,
        tiu.price,
        tiu.closet_price_rank,
        t.message_status,
        t.status,
        t.error_message,
        t.error_status,
        t.origin_domain,
        t.destination_domain,
        CASE
            WHEN t.origin_domain = '6648936' THEN 'Ethereum'
            WHEN t.origin_domain = '1869640809' THEN 'Optimism'
            WHEN t.origin_domain = '6450786' THEN 'BNB'
            WHEN t.origin_domain = '6778479' THEN 'Gnosis'
            WHEN t.origin_domain = '1886350457' THEN 'Polygon'
            WHEN t.origin_domain = '1634886255' THEN 'Arbitrum One'
            WHEN t.origin_domain = '1818848877' THEN 'Linea'
            ELSE
                t.origin_domain
        END
            AS origin_chain_name,
        CASE
            WHEN t.destination_domain = '6648936' THEN 'Ethereum'
            WHEN t.destination_domain = '1869640809' THEN 'Optimism'
            WHEN t.destination_domain = '6450786' THEN 'BNB'
            WHEN t.destination_domain = '6778479' THEN 'Gnosis'
            WHEN t.destination_domain = '1886350457' THEN 'Polygon'
            WHEN t.destination_domain = '1634886255' THEN 'Arbitrum One'
            WHEN t.destination_domain = '1818848877' THEN 'Linea'
            ELSE
                t.destination_domain
        END
            AS destination_chain_name,
        t.origin_transacting_amount,
        t.bridged_amt,
        t.destination_transacting_amount

    FROM `mainnet-bigq.public.transfers` t
    INNER JOIN `mainnet-bigq.public.transfers_in_usd` tiu
        ON t.transfer_id = tiu.transfer_id

    LEFT JOIN connext_tokens cc_origin
        ON t.origin_transacting_asset = cc_origin.token_address

    LEFT JOIN connext_tokens cc_destination
        ON t.destination_transacting_asset = cc_destination.token_address
)

-- adding token decimals and calculate USD value
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
    sr.origin_domain,
    sr.destination_domain,
    sr.origin_chain_name,
    sr.destination_chain_name,
    sr.caller_type,
    sr.contract_name,
    sr.contract_author,
    CASE
        WHEN
            (sr.contract_author = "LiFi") OR (sr.contract_author = "Socket")
            THEN True
        ELSE False
    END AS lifi_socket_indicator,
    CASE
        WHEN
            (
                STARTS_WITH(sr.origin_asset, 'next')
                OR STARTS_WITH(sr.destination_asset, 'next')
            )
            THEN "nextAsset"
        WHEN sr.contract_author = "LiFi" THEN "LiFi"
        WHEN sr.contract_author = "Socket" THEN "Socket"
        ELSE "Bridge - Other"
    END AS user_group,
    sr.message_status,
    sr.status,
    sr.error_message,
    sr.error_status,
    sr.closet_price_rank,
    hp.price,
    CAST(a_origin.adopted_decimal AS FLOAT64) AS token_decimal,
    CAST(sr.origin_transacting_amount AS FLOAT64)
    / POW(10, COALESCE(CAST(a_origin.adopted_decimal AS INT64), 0))
        AS d_origin_amount,
    CAST(sr.bridged_amt AS FLOAT64)
    / POW(10, COALESCE(CAST(a_origin.decimal AS INT64), 0)) AS d_bridged_amt,
    CAST(sr.destination_transacting_amount AS FLOAT64)
    / POW(10, COALESCE(CAST(a_dest.adopted_decimal AS INT64), 0))
        AS d_destination_amount,
    --USD amounts
    (
        CAST(sr.origin_transacting_amount AS FLOAT64)
        / POW(10, COALESCE(CAST(a_origin.adopted_decimal AS INT64), 0))
    )
    * hp.price AS usd_origin_amount,
    (
        CAST(sr.bridged_amt AS FLOAT64)
        / POW(10, COALESCE(CAST(a_origin.decimal AS INT64), 0))
    )
    * hp.price AS usd_bridged_amount,
    (
        CAST(sr.destination_transacting_amount AS FLOAT64)
        / POW(10, COALESCE(CAST(a_dest.adopted_decimal AS INT64), 0))
    )
    * hp.price AS usd_destination_amount
FROM semi_txs sr
LEFT JOIN `mainnet-bigq.public.assets` a_origin
    ON
        (
            sr.canonical_id = a_origin.canonical_id
            AND sr.origin_domain = a_origin.domain
        )
LEFT JOIN `mainnet-bigq.public.assets` a_dest
    ON
        (
            sr.canonical_id = a_dest.canonical_id
            AND sr.destination_domain = a_dest.domain
        )
LEFT JOIN hourly_price hp
    ON
        (
            sr.destination_asset = hp.price_group
            AND DATE_TRUNC(CAST(sr.xcall_timestamp AS TIMESTAMP), HOUR) = CAST(hp.date AS TIMESTAMP)
        )
