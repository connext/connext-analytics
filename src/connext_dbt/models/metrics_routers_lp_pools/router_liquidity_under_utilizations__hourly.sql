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
            JSON_EXTRACT_SCALAR (t.routers, '$[0]') AS router,
            TIMESTAMP_SECONDS (t.xcall_timestamp) AS xcall_timestamp,
            CASE
                WHEN t.status = "CompletedFast" THEN CAST(destination_transacting_amount AS FLOAT64) / POW (10, COALESCE(CAST(a.decimal AS INT64), 0))
            END AS destination_fast_amount,
            CASE
                WHEN t.status = "CompletedSlow" THEN CAST(destination_transacting_amount AS FLOAT64) / POW (10, COALESCE(CAST(a.decimal AS INT64), 0))
            END AS destination_slow_amount,
            t.destination_domain,
            t.destination_local_asset,
            a.decimal,
        FROM
            `public.transfers` t
            LEFT JOIN assets a ON (
                t.canonical_id = a.canonical_id
                AND t.destination_domain = a.domain
            ) -- TODO Remove Filter Later
            -- WHERE
            --     t.destination_local_asset = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            --     AND t.destination_domain = "6648936"
            --     AND JSON_EXTRACT_SCALAR (t.routers, '$[0]') = "0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598"
    ),
    tx_agg AS (
        SELECT
            DATE_TRUNC (xcall_timestamp, HOUR) AS xcall_timestamp,
            router,
            cm.chain AS chain,
            COALESCE(tm.asset, t.destination_local_asset) AS asset,
            SUM(destination_fast_amount) AS destination_fast_volume,
            SUM(destination_slow_amount) AS destination_slow_volume,
            SUM(destination_fast_amount * 0.0005) AS router_fee
        FROM
            tx t
            LEFT JOIN chains_meta cm ON t.destination_domain = cm.domainid
            LEFT JOIN tokens_meta tm ON (t.destination_local_asset = tm.local)
        GROUP BY
            1,
            2,
            3,
            4
    ),
    tx_liquidity_flow AS (
        SELECT
            router,
            chain,
            asset,
            CASE
                WHEN chain = 'Ethereum Mainnet' THEN TIMESTAMP_ADD (xcall_timestamp, INTERVAL 12 HOUR)
                ELSE TIMESTAMP_ADD (xcall_timestamp, INTERVAL 5 HOUR)
            END AS liquidity_filled_back_timestamp,
            SUM(destination_fast_volume) AS liquidity_filled_back
        FROM
            tx_agg
        GROUP BY
            1,
            2,
            3,
            4
    ),
    tx_liquidity_flow_agg AS (
        SELECT
            COALESCE(
                tx_agg.xcall_timestamp,
                tx_liquidity_flow.liquidity_filled_back_timestamp
            ) AS date,
            COALESCE(tx_agg.router, tx_liquidity_flow.router) AS router,
            COALESCE(tx_agg.chain, tx_liquidity_flow.chain) AS chain,
            COALESCE(tx_agg.asset, tx_liquidity_flow.asset) AS asset,
            tx_agg.router_fee AS router_fee,
            tx_agg.destination_fast_volume AS destination_fast_volume,
            tx_agg.destination_slow_volume AS destination_slow_volume,
            COALESCE(tx_agg.destination_fast_volume, 0) AS liquidity_locked,
            COALESCE(tx_liquidity_flow.liquidity_filled_back, 0) AS liquidity_fill_back
        FROM
            tx_agg
            FULL OUTER JOIN tx_liquidity_flow ON (
                tx_agg.xcall_timestamp = tx_liquidity_flow.liquidity_filled_back_timestamp
                AND tx_agg.router = tx_liquidity_flow.router
                AND tx_agg.chain = tx_liquidity_flow.chain
                AND tx_agg.asset = tx_liquidity_flow.asset
            )
    ),
    raw_router_tvl AS (
        SELECT
            DATE_TRUNC (TIMESTAMP_SECONDS (r.timestamp), HOUR) AS date,
            r.router,
            cm.chain,
            COALESCE(tm.asset, r.asset) AS asset,
            event,
            CASE
                WHEN event = 'Add' THEN CAST(r.amount AS FLOAT64)
                ELSE - CAST(r.amount AS FLOAT64)
            END AS amount,
            SUM(
                CASE
                    WHEN event = 'Add' THEN CAST(r.amount AS FLOAT64)
                    ELSE - CAST(r.amount AS FLOAT64)
                END
            ) OVER (
                PARTITION BY
                    r.router,
                    r.asset,
                    r.domain
                ORDER BY
                    r.timestamp
            ) AS running_amount
        FROM
            `mainnet-bigq.public.router_liquidity_events` r
            LEFT JOIN chains_meta cm ON r.domain = cm.domainid
            LEFT JOIN tokens_meta tm ON (r.asset = tm.local)
            -- WHERE
            --     r.domain = "6648936"
            --     AND r.asset = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            --     AND r.router = "0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598"
        ORDER BY
            1 DESC
    ),
    routers_tvl AS (
        SELECT
            date,
            router,
            chain,
            asset,
            SUM(amount) AS amount,
            SUM(running_amount) AS locked
        FROM
            raw_router_tvl
        GROUP BY
            1,
            2,
            3,
            4
        ORDER BY
            1 DESC
    ),
    -- Fill routers TVL for all dates
    cln_routers_tvl AS (
        SELECT
            rt.date AS date,
            rt.router AS router,
            rt.chain AS chain,
            rt.asset AS asset,
            rt.amount AS amount,
            -- # Total locked: filling with previous non zero value
            COALESCE(
                rt.locked,
                LAST_VALUE (rt.locked IGNORE NULLS) OVER (
                    PARTITION BY
                        rt.router,
                        rt.chain,
                        rt.asset
                    ORDER BY
                        rt.date
                )
            ) AS total_locked
        FROM
            routers_tvl rt
    ),
    -- Fill fees for all dates from tx_liquidity_flow_agg
    cln_fees AS (
        SELECT
            f.date AS date,
            f.router AS router,
            f.chain AS chain,
            f.asset AS asset,
            f.router_fee,
            f.destination_fast_volume AS router_volume,
            f.destination_slow_volume,
            COALESCE(f.liquidity_locked, 0) AS liquidity_locked,
            COALESCE(f.liquidity_fill_back, 0) AS liquidity_fill_back,
            SUM(COALESCE(f.router_fee, 0)) OVER (
                PARTITION BY
                    f.router,
                    f.chain,
                    f.asset
                ORDER BY
                    f.date
            ) AS total_fee_earned
        FROM
            tx_liquidity_flow_agg f
    ),
    combined_cln_router_tvl_fee AS (
        -- combine filled fee + router_tvl
        SELECT
            COALESCE(frt.date, ff.date) AS date,
            COALESCE(frt.router, ff.router) AS router,
            COALESCE(frt.chain, ff.chain) AS chain,
            COALESCE(frt.asset, ff.asset) AS asset,
            ff.destination_slow_volume AS slow_volume,
            frt.amount AS router_dw,
            -- fill with pre- value
            LAST_VALUE (frt.total_locked IGNORE NULLS) OVER (
                PARTITION BY
                    COALESCE(frt.router, ff.router),
                    COALESCE(frt.chain, ff.chain),
                    COALESCE(frt.asset, ff.asset)
                ORDER BY
                    COALESCE(frt.date, ff.date)
            ) AS initial_locked,
            ff.router_fee,
            ff.total_fee_earned,
            ff.router_volume,
            -- running locked liquidity
            SUM(liquidity_locked) OVER (
                PARTITION BY
                    COALESCE(frt.router, ff.router),
                    COALESCE(frt.chain, ff.chain),
                    COALESCE(frt.asset, ff.asset)
                ORDER BY
                    COALESCE(frt.date, ff.date)
            ) AS liquidity_locked,
            -- running liquidity fill back
            SUM(liquidity_fill_back) OVER (
                PARTITION BY
                    COALESCE(frt.router, ff.router),
                    COALESCE(frt.chain, ff.chain),
                    COALESCE(frt.asset, ff.asset)
                ORDER BY
                    COALESCE(frt.date, ff.date)
            ) AS liquidity_fill_back
        FROM
            cln_fees ff
            FULL OUTER JOIN cln_routers_tvl frt ON ff.date = frt.date
            AND ff.router = frt.router
            AND ff.chain = frt.chain
            AND ff.asset = frt.asset
    ),
    final_combined_cln_router_tvl_fee AS (
        SELECT
            *,
            -- total_locked = actual_total_locked(in/out) - locked Liqudity(liq Provided) + liquidity_fill_back(liq added back)
            COALESCE(initial_locked, 0) - COALESCE(liquidity_locked, 0) + COALESCE(liquidity_fill_back, 0) AS total_locked
        FROM
            combined_cln_router_tvl_fee
    ),
    date_range AS (
        SELECT
            router,
            chain,
            asset,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM
            final_combined_cln_router_tvl_fee
        GROUP BY
            router,
            chain,
            asset
    ),
    all_dates AS (
        SELECT
            router,
            chain,
            asset,
            DATE_ADD (min_date, INTERVAL seq HOUR) AS date
        FROM
            date_range
            CROSS JOIN UNNEST (
                GENERATE_ARRAY (0, DATE_DIFF (max_date, min_date, HOUR))
            ) AS seq
    ),
    clean_final AS (
        SELECT
            DATE_TRUNC (COALESCE(ad.date, f.date), HOUR) AS date,
            COALESCE(ad.router, f.router) AS router,
            COALESCE(r.name, f.router) AS router_name,
            COALESCE(ad.chain, f.chain) AS chain,
            COALESCE(ad.asset, f.asset) AS asset,
            -- asset group
            CASE
                WHEN COALESCE(ad.asset, f.asset) = 'ETH' THEN 'WETH'
                WHEN COALESCE(ad.asset, f.asset) = 'NEXT' THEN 'NEXT'
                WHEN STARTS_WITH (COALESCE(ad.asset, f.asset), 'next') THEN REGEXP_REPLACE (COALESCE(ad.asset, f.asset), '^next', '')
                ELSE COALESCE(ad.asset, f.asset)
            END AS asset_group,
            CASE
                WHEN COALESCE(ad.asset, f.asset) = 'ETH' THEN 'WETH'
                WHEN COALESCE(ad.asset, f.asset) = 'NEXT' THEN 'NEXT'
                WHEN STARTS_WITH (COALESCE(ad.asset, f.asset), 'next') THEN REGEXP_REPLACE (COALESCE(ad.asset, f.asset), '^next', '')
                WHEN COALESCE(ad.asset, f.asset) = 'alUSD' THEN 'USDT'
                WHEN COALESCE(ad.asset, f.asset) = 'nextALUSD' THEN 'USDT'
                WHEN COALESCE(ad.asset, f.asset) = 'instETH' THEN 'WETH'
                WHEN COALESCE(ad.asset, f.asset) = 'ezETH' THEN 'WETH'
                WHEN COALESCE(ad.asset, f.asset) = 'alETH' THEN 'WETH'
                WHEN COALESCE(ad.asset, f.asset) = 'nextalETH' THEN 'WETH'
                ELSE COALESCE(ad.asset, f.asset)
            END AS price_group,
            f.router_dw,
            f.initial_locked,
            f.slow_volume,
            f.router_volume,
            f.router_fee,
            f.liquidity_locked,
            f.liquidity_fill_back,
            f.total_fee_earned,
            f.total_locked,
        FROM
            all_dates ad
            LEFT JOIN final_combined_cln_router_tvl_fee f ON ad.date = f.date
            AND LOWER(ad.router) = LOWER(f.router)
            AND ad.chain = f.chain
            AND ad.asset = f.asset
            LEFT JOIN `mainnet-bigq.raw.dim_connext_routers_name` r ON LOWER(ad.router) = LOWER(r.router)
    ),
    pre_filled_clean_final AS (
        SELECT
            cf.date,
            cf.router,
            cf.router_name,
            cf.chain,
            cf.asset_group,
            cf.asset,
            cf.price_group,
            cf.router_dw,
            cf.initial_locked,
            cf.slow_volume,
            cf.router_volume,
            cf.router_fee,
            cf.liquidity_locked,
            cf.liquidity_fill_back,
            -- for total locked and fee pull in previous  none null value
            COALESCE(
                cf.total_locked,
                LAST_VALUE (cf.total_locked IGNORE NULLS) OVER (
                    PARTITION BY
                        cf.router,
                        cf.chain,
                        cf.asset
                    ORDER BY
                        cf.date
                )
            ) AS total_locked,
            COALESCE(
                cf.total_fee_earned,
                LAST_VALUE (cf.total_fee_earned IGNORE NULLS) OVER (
                    PARTITION BY
                        cf.router,
                        cf.chain,
                        cf.asset
                    ORDER BY
                        cf.date
                )
            ) AS total_fee_earned
        FROM
            clean_final cf
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
            pcf.date,
            pcf.router,
            pcf.router_name,
            pcf.chain,
            pcf.asset_group,
            pcf.asset,
            dp.price,
            pcf.router_dw,
            pcf.initial_locked,
            pcf.slow_volume,
            pcf.router_volume,
            pcf.router_fee,
            pcf.liquidity_locked,
            pcf.liquidity_fill_back,
            pcf.total_locked,
            pcf.total_fee_earned,
            COALESCE(pcf.total_locked, 0) + COALESCE(pcf.total_fee_earned, 0) AS total_balance,
            -- USD values
            dp.price * pcf.router_dw AS router_dw_usd,
            dp.price * pcf.initial_locked AS initial_locked_usd,
            dp.price * pcf.slow_volume AS slow_volume_usd,
            dp.price * pcf.router_volume AS router_volume_usd,
            dp.price * pcf.router_fee AS router_fee_usd,
            dp.price * pcf.liquidity_locked AS liquidity_locked_usd,
            dp.price * pcf.liquidity_fill_back AS liquidity_fill_back_usd,
            dp.price * pcf.total_locked AS total_locked_usd,
            dp.price * pcf.total_fee_earned AS total_fee_earned_usd,
            dp.price * (
                COALESCE(pcf.total_locked, 0) + COALESCE(pcf.total_fee_earned, 0)
            ) AS total_balance_usd
        FROM
            pre_filled_clean_final pcf
            LEFT JOIN daily_price dp ON pcf.date = dp.date
            AND pcf.price_group = dp.asset
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
    router_name = "Connext"
    AND chain = "Ethereum Mainnet"
    AND asset = "WETH"
    -- skip todays data
    -- DATE (DATE_TRUNC (date, DAY)) < DATE (DATE_TRUNC (CURRENT_DATE(), DAY))
ORDER BY
    1 DESC