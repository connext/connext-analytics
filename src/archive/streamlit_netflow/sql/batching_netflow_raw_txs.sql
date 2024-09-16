-- batch data for market maker with a max timeframe of 3hr

WITH
    chains_meta AS (
        SELECT DISTINCT
            domainid,
            chain_name AS chain
        FROM
            `mainnet-bigq.raw.stg__ninja_connext_prod_chains_tokens_clean` ct
    ),
    assets AS (
        SELECT DISTINCT
            da.domain,
            da.canonical_id,
            da.adopted_decimal AS decimal
        FROM
            `mainnet-bigq.public.assets` da
    ),
    tokens_meta AS (
        SELECT DISTINCT
            LOWER(token_address) AS local,
            token_name AS asset
        FROM
            `mainnet-bigq.stage.connext_tokens` ct
    ),
    tx AS (
        SELECT
            TIMESTAMP_SECONDS (t.xcall_timestamp) AS xcall_timestamp,
            t.destination_domain,
            t.destination_local_asset,
            a.decimal,
            (
                CAST(destination_transacting_amount AS FLOAT64) 
                / 
                POW (10, COALESCE(CAST(a.decimal AS INT64), 0))
            ) AS destination_transacting_amount
        FROM
            `public.transfers` t
            LEFT JOIN assets a ON (
                t.canonical_id = a.canonical_id
                AND t.destination_domain = a.domain
            )
    ),
    semi_final AS (
        SELECT
            t.xcall_timestamp,
            cm.chain AS chain,
            COALESCE(tm.asset, t.destination_local_asset) AS asset,
            t.destination_transacting_amount AS amount
        FROM
            tx t
            LEFT JOIN chains_meta cm ON t.destination_domain = cm.domainid
            LEFT JOIN tokens_meta tm ON (t.destination_local_asset = tm.local)
    ),
    clean_final AS (
        SELECT
            sf.xcall_timestamp AS date,
            sf.chain,
            sf.asset,
            CASE
                WHEN asset = 'ETH' THEN 'WETH'
                WHEN asset = 'NEXT' THEN 'NEXT'
                WHEN STARTS_WITH (asset, 'next') THEN REGEXP_REPLACE (asset, '^next', '')
                ELSE asset
            END AS asset_group,
            CASE
                WHEN asset = 'ETH' THEN 'WETH'
                WHEN asset = 'NEXT' THEN 'NEXT'
                WHEN STARTS_WITH (asset, 'next') THEN REGEXP_REPLACE (asset, '^next', '')
                WHEN asset = 'alUSD' THEN 'USDT'
                WHEN asset = 'nextALUSD' THEN 'USDT'
                WHEN asset = 'instETH' THEN 'WETH'
                WHEN asset = 'ezETH' THEN 'WETH'
                WHEN asset = 'alETH' THEN 'WETH'
                WHEN asset = 'nextalETH' THEN 'WETH'
                ELSE asset
            END AS price_group,
            sf.amount
        FROM
            semi_final sf
    ),

    -- adding daily pricing to final
    daily_price AS (
        SELECT
            DATE_TRUNC (CAST(p.date AS TIMESTAMP), HOUR) AS date,
            p.symbol AS asset,
            AVG(p.average_price) AS price
        FROM 
            `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
        GROUP BY
            1,
            2
    ),
    usd_data AS (
        SELECT
            "Connext" AS bridge,
            cf.date,
            cf.chain,
            cf.asset_group,
            cf.asset,
            dp.price,
            cf.amount,
            -- USD values
            dp.price * cf.amount AS amount_usd
        FROM
            clean_final cf
            LEFT JOIN daily_price dp ON DATE_TRUNC(cf.date, HOUR) = dp.date
            AND cf.price_group = dp.asset
        ORDER BY
            1,
            2,
            3,
            4 DESC
    )
SELECT
    *
FROM
    usd_data
WHERE
    price IS NOT NULL