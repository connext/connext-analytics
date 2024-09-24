WITH
connext_tokens AS (
    SELECT DISTINCT
        ct.token_address,
        ct.token_name,
        ct.is_xerc20
    FROM
        `mainnet-bigq.stage.connext_tokens` ct
),

router_liquidity AS (
    SELECT DISTINCT
        l.adopted AS asset_address,
        COALESCE(ct.token_name, l.adopted) AS asset,
        CAST(ct.is_xerc20 AS BOOL) AS is_xerc20,
        CASE
            WHEN l.domain = '6648936' THEN 'Ethereum'
            WHEN l.domain = '1869640809' THEN 'Optimism'
            WHEN l.domain = '6450786' THEN 'BNB'
            WHEN l.domain = '6778479' THEN 'Gnosis'
            WHEN l.domain = '1886350457' THEN 'Polygon'
            WHEN l.domain = '1634886255' THEN 'Arbitrum One'
            WHEN l.domain = '1818848877' THEN 'Linea'
            WHEN l.domain = '1835365481' THEN 'Metis'
            WHEN l.domain = '1650553709' THEN "Base Mainnet"
            WHEN l.domain = '31338' THEN 'Local Optimism'
            WHEN l.domain = '31339' THEN 'Local Arbitrum One'
            WHEN l.domain = '1836016741' THEN 'Mode'
            ELSE
                l.domain
        END
            AS domain_name,
        l.domain,
        l.canonical_id,
        SUM(l.balance_usd) AS balance_usd,
        SUM(l.locked_usd) AS locked_usd,
        SUM(l.removed_usd) AS removed_usd,
        SUM(l.supplied_usd) AS supplied_usd,
        SUM(l.fee_earned_usd) AS fee_usd
    FROM

        `mainnet-bigq.legacy.stg_source__cartographer_router_with_balances` l
    LEFT JOIN
        connext_tokens ct
        ON
            l.adopted = ct.token_address
    GROUP BY
        1, 2, 3, 4, 5, 6
),

sp AS (
    SELECT
        *,
        CASE
            WHEN domain = '6648936' THEN 'Ethereum'
            WHEN domain = '1869640809' THEN 'Optimism'
            WHEN domain = '6450786' THEN 'BNB'
            WHEN domain = '6778479' THEN 'Gnosis'
            WHEN domain = '1886350457' THEN 'Polygon'
            WHEN domain = '1634886255' THEN 'Arbitrum One'
            WHEN domain = '1818848877' THEN 'Linea'
            WHEN domain = '31338' THEN 'Local Optimism'
            WHEN domain = '31339' THEN 'Local Arbitrum One'
            WHEN domain = '1835365481' THEN 'Metis'
            WHEN domain = '1650553709' THEN "Base Mainnet"
            WHEN domain = '1836016741' THEN 'Mode'

            ELSE
                CONCAT(
                    "Add this domain to Google sheet, not found for:", domain
                )
        END
            AS chain,
        JSON_EXTRACT_STRING_ARRAY(pooled_tokens)[0] AS token_1,
        JSON_EXTRACT_STRING_ARRAY(pooled_tokens)[1] AS token_2,
        CAST(JSON_EXTRACT_STRING_ARRAY(pool_token_decimals)[0] AS NUMERIC)
            AS pool_token_decimals_1,
        CAST(JSON_EXTRACT_STRING_ARRAY(pool_token_decimals)[1] AS NUMERIC)
            AS pool_token_decimals_2,
        CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS FLOAT64) AS balances_1,
        CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS FLOAT64) AS balances_2
    FROM
        `public.stableswap_pools`
    WHERE
        balances IS NOT NULL
),

maxassetprices AS (
    SELECT
        canonical_id,
        MAX(timestamp) AS max_timestamp
    FROM
        `mainnet-bigq.public.asset_prices` asset_prices
    GROUP BY
        canonical_id
),

pools_tvl_usd AS (
    SELECT DISTINCT
        sp.key AS pool_id,
        sp.chain,
        assets.canonical_id,
        sp.domain,
        CASE
            WHEN
                ct_1.token_name = '0x609aefb9fb2ee8f2fdad5dc48efb8fa4ee0e80fb'
                THEN 'nextWETH'
            ELSE COALESCE(ct_1.token_name, sp.token_1)
        END AS token_1_name,
        COALESCE(ct_2.token_name, sp.token_2) AS token_2_name,
        sp.balances_1 / POW(10, sp.pool_token_decimals_1) AS pool_1_amount,
        sp.balances_2 / POW(10, sp.pool_token_decimals_2) AS pool_2_amount,
        -- USD
        CAST(asset_prices.price AS FLOAT64)
        * CAST(sp.balances_1 AS FLOAT64)
        / POW(10, sp.pool_token_decimals_1) AS usd_pool_1_amount,
        CAST(asset_prices.price AS FLOAT64)
        * CAST(sp.balances_2 AS FLOAT64)
        / POW(10, sp.pool_token_decimals_2) AS usd_pool_2_amount
    FROM
        sp
    LEFT JOIN
        connext_tokens ct_1
        ON
            sp.token_1 = ct_1.token_address
    LEFT JOIN
        connext_tokens ct_2
        ON
            sp.token_2 = ct_2.token_address
    LEFT JOIN
        `mainnet-bigq.public.assets` assets
        ON
            sp.token_1 = assets.id
            AND sp.domain = assets.domain
    LEFT JOIN
        `mainnet-bigq.public.asset_prices` asset_prices
        ON
            assets.canonical_id = asset_prices.canonical_id
    INNER JOIN
        maxassetprices
        ON
            asset_prices.canonical_id = maxassetprices.canonical_id
            AND asset_prices.timestamp = maxassetprices.max_timestamp
),

poolsrouters AS (
    SELECT DISTINCT
        r.asset,
        r.asset_address,
        r.is_xerc20,
        r.domain_name AS domain_name,
        r.domain,
        r.canonical_id,
        p.token_1_name,
        p.token_2_name,
        SUM(r.balance_usd) AS balance_usd,
        SUM(r.locked_usd) AS locked_usd,
        SUM(r.removed_usd) AS removed_usd,
        SUM(r.supplied_usd) AS supplied_usd,
        SUM(r.fee_usd) AS fee_usd,
        SUM(p.usd_pool_1_amount) AS usd_pool_1_amount,
        SUM(p.usd_pool_2_amount) AS usd_pool_2_amount,
        SUM(p.usd_pool_1_amount) + SUM(p.usd_pool_2_amount) AS pool_balance
    FROM
        router_liquidity r
    LEFT JOIN
        pools_tvl_usd p
        ON
            r.asset = p.token_2_name
            AND r.domain_name = p.chain
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
),

chainassetvolume AS (
    SELECT
        rl.asset_address_coalesced AS asset_address_rl,
        --    rl.adopted_decimal,
        rl.chain_domain_coalesced AS chain_domain,
        --    rl.asset_canonical_id,
        --    rl.decimal,
        --    rl.asset_canonical_domain,
        SUM(origin_usd_volume_last_1_day) AS origin_usd_volume_last_1_day,
        SUM(origin_usd_volume_last_7_days) AS origin_usd_volume_last_7_days,
        SUM(origin_usd_volume_last_30_days) AS origin_usd_volume_last_30_days,
        SUM(origin_volume_1_day) AS origin_volume_1_day,
        SUM(origin_volume_7_days) AS origin_volume_7_days,
        SUM(origin_volume_30_days) AS origin_volume_30_days,
        SUM(origin_fast_volume_1_day) AS origin_fast_volume_1_day,
        SUM(origin_fast_volume_7_days) AS origin_fast_volume_7_days,
        SUM(origin_fast_volume_30_days) AS origin_fast_volume_30_days,
        SUM(destination_usd_volume_last_1_day)
            AS destination_usd_volume_last_1_day,
        SUM(destination_usd_volume_last_7_days)
            AS destination_usd_volume_last_7_days,
        SUM(destination_usd_volume_last_30_days)
            AS destination_usd_volume_last_30_days,
        SUM(destination_volume_1_day) AS destination_volume_1_day,
        SUM(destination_volume_7_days) AS destination_volume_7_days,
        SUM(destination_volume_30_days) AS destination_volume_30_days,
        SUM(destination_fast_volume_1_day) AS destination_fast_volume_1_day,
        SUM(destination_fast_volume_7_days) AS destination_fast_volume_7_days,
        SUM(destination_fast_volume_30_days) AS destination_fast_volume_30_days,
        MAX(rl.last_txn_date_coalesced) AS last_txn_date,
        SUM(
            CASE
                WHEN
                    rl.last_txn_date_coalesced
                    <= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
                    THEN balance_usd
                ELSE 0
            END
        ) AS inactive_balance_usd,
        SUM(
            CASE
                WHEN
                    rl.last_txn_date_coalesced
                    <= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
                    THEN locked_usd
                ELSE 0
            END
        ) AS inactive_locked_usd,
        SUM(
            CASE
                WHEN
                    rl.last_txn_date_coalesced
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
                    THEN balance_usd
                ELSE 0
            END
        ) AS active_balance_usd,
        SUM(
            CASE
                WHEN
                    rl.last_txn_date_coalesced
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
                    THEN locked_usd
                ELSE 0
            END
        ) AS active_locked_usd,
        SUM(balance_usd) AS total_balance_usd,
        SUM(locked_usd) AS total_locked_usd,
        SUM(origin_usd_volume_last_1_day)
        / SUM(locked_usd) AS utilization_last_1_day,
        SUM(origin_usd_volume_last_7_days)
        / SUM(locked_usd) AS utilization_last_7_days,
        SUM(origin_usd_volume_last_30_days)
        / SUM(locked_usd) AS utilization_last_30_days,
        SUM(slow_tns) AS slow_tns
    FROM
        `mainnet-bigq.legacy.mt_router_metrics` rl
    --`mainnet-bigq.metrics.router_metrics_test` rl
    GROUP BY
        1, 2
--,3,4,5,6
),

poolsroutersvolumes AS (
    SELECT *
    FROM
        poolsrouters pr
    LEFT JOIN
        chainassetvolume cav
        ON
            pr.domain = cav.chain_domain
            AND pr.asset_address = cav.asset_address_rl

),

routervolume AS (
    SELECT DISTINCT
        COALESCE(pr.asset, rmt.asset) AS asset,
        COALESCE(pr.domain_name, rmt.domain_name) AS domain_name,
        SUM(origin_usd_volume_last_1_day) AS origin_usd_volume_last_1_day,
        SUM(origin_usd_volume_last_7_days) AS origin_usd_volume_last_7_days,
        SUM(origin_usd_volume_last_30_days) AS origin_usd_volume_last_30_days,
        SUM(origin_volume_1_day) AS origin_volume_1_day,
        SUM(origin_volume_7_days) AS origin_volume_7_days,
        SUM(origin_volume_30_days) AS origin_volume_30_days,
        SUM(origin_fast_usd_volume_1_day) AS origin_fast_usd_volume_1_day,
        SUM(origin_fast_usd_volume_7_days) AS origin_fast_usd_volume_7_days,
        SUM(origin_fast_usd_volume_30_days) AS origin_fast_usd_volume_30_days,
        SUM(destination_usd_volume_last_1_day)
            AS destination_usd_volume_last_1_day,
        SUM(destination_usd_volume_last_7_days)
            AS destination_usd_volume_last_7_days,
        SUM(destination_usd_volume_last_30_days)
            AS destination_usd_volume_last_30_days,
        SUM(destination_volume_1_day / POWER(10, rmt.adopted_decimal))
            AS destination_volume_1_day,
        SUM(destination_volume_7_days / POWER(10, rmt.adopted_decimal))
            AS destination_volume_7_days,
        SUM(destination_volume_30_days / POWER(10, rmt.adopted_decimal))
            AS destination_volume_30_days,
        SUM(destination_fast_usd_volume_1_day)
            AS destination_fast_usd_volume_1_day,
        SUM(destination_fast_usd_volume_7_days)
            AS destination_fast_usd_volume_7_days,
        SUM(destination_fast_usd_volume_30_days)
            AS destination_fast_usd_volume_30_days,
        MAX(CAST(rmt.last_txn_date AS DATE)) AS last_txn_date,
        SUM(
            CASE
                WHEN
                    CAST(rmt.last_txn_date AS DATE)
                    < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN rmt.balance_usd
                ELSE 0
            END
        ) AS inactive_balance_usd,
        SUM(
            CASE
                WHEN
                    CAST(rmt.last_txn_date AS DATE)
                    < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN rmt.locked_usd
                ELSE 0
            END
        ) AS inactive_locked_usd,
        SUM(
            CASE
                WHEN
                    CAST(rmt.last_txn_date AS DATE)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN rmt.balance_usd
                ELSE 0
            END
        ) AS active_balance_usd,
        SUM(
            CASE
                WHEN
                    CAST(rmt.last_txn_date AS DATE)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN rmt.locked_usd
                ELSE 0
            END
        ) AS active_locked_usd,
        SUM(rmt.balance_usd) AS total_balance_usd,
        SUM(rmt.locked_usd) AS total_locked_usd,
        SUM(rmt.balance / POWER(10, rmt.adopted_decimal))
            AS total_balance_asset,
        SUM(rmt.locked / POWER(10, rmt.adopted_decimal)) AS total_locked_asset,
        SUM(destination_usd_volume_last_1_day)
        / SUM(rmt.locked_usd) AS utilization_last_1_day,
        SUM(destination_usd_volume_last_7_days)
        / SUM(rmt.locked_usd) AS utilization_last_7_days,
        SUM(destination_usd_volume_last_30_days)
        / SUM(rmt.locked_usd) AS utilization_last_30_days,
        SUM(slow_tns) AS slow_tns,
        MAX(pr.usd_pool_1_amount) AS usd_pool_1_amount,
        MAX(pr.usd_pool_2_amount) AS usd_pool_2_amount,
        MAX(pr.pool_balance) AS pool_balance
    FROM
        poolsrouters pr
    FULL OUTER JOIN
        `mainnet-bigq.legacy.mt_router_metrics_test` rmt
        ON pr.asset = rmt.asset AND pr.domain_name = rmt.domain_name
    GROUP BY 1, 2
)

--SELECT * FROM PoolsRoutersVolumes
SELECT * FROM routervolume --where domain_name = 'Mode'
--SELECT * FROM PoolsRouters where domain_name = 'Mode'
--SELECT * FROM ChainAssetVolume ORDER BY 2,1
