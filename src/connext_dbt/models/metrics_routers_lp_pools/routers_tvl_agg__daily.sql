-- Metrics: TVL, APR, APY
WITH
chains_meta AS (
    SELECT DISTINCT
        domainid,
        chain_name AS chain
    FROM
        `mainnet-bigq.raw.stg__ninja_connext_prod_chains_tokens_clean` ct
),

tokens_meta AS (
    SELECT DISTINCT
        token_address AS local,
        token_name AS asset
    FROM
        `mainnet-bigq.stage.connext_tokens` ct
-- WHERE token_address = "0xb368ae21081709d03c00d7dc51737d8abd9384e6"
),

raw_router_tvl AS (
    SELECT
        DATE_TRUNC(TIMESTAMP_SECONDS(r.timestamp), DAY) AS date,
        r.router,
        cm.chain,
        COALESCE(tm.asset, r.asset) AS asset,
        event,
        CASE
            WHEN event = 'Add' THEN CAST(r.amount AS FLOAT64)
            ELSE -CAST(r.amount AS FLOAT64)
        END AS amount,
        SUM(
            CASE
                WHEN event = 'Add' THEN CAST(r.amount AS FLOAT64)
                ELSE -CAST(r.amount AS FLOAT64)
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

-- SELECT * FROM router_tvl
-- WHERE
-- router = "0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598"
-- AND asset = 'WETH'
-- AND chain = "Ethereum Mainnet"
fees AS (
    SELECT
        DATE_TRUNC(TIMESTAMP_SECONDS(xcall_timestamp), DAY) AS date,
        router,
        cm.chain AS chain,
        COALESCE(tm.asset, t.destination_local_asset) AS asset,
        SUM(destination_amount * 0.0005) AS router_fee
    FROM
        `y42_connext_y42_dev.transfers_mapped` t
    LEFT JOIN chains_meta cm ON t.destination_domain = cm.domainid
    LEFT JOIN tokens_meta tm ON (t.destination_local_asset = tm.local)
    WHERE
        status = "CompletedFast"
        AND message_status = "Processed"
    -- AND destination_asset_name = 'weth'
    -- AND destination_domain_name= 'Ethereum Mainnet'
    -- AND router = "0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4"
    GROUP BY
        1,
        2,
        3,
        4
),

date_range AS (
    SELECT
        router,
        chain,
        asset,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM
        fees
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
        DATE_ADD(min_date, INTERVAL seq DAY) AS date
    FROM
        date_range
    CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, DATE_DIFF(max_date, min_date, DAY))
    ) AS seq
),

-- Fill fees for all dates
filled_fees AS (
    SELECT
        ad.date,
        ad.router,
        ad.chain,
        ad.asset,
        COALESCE(f.router_fee, 0) AS router_fee,
        SUM(COALESCE(f.router_fee, 0)) OVER (
            PARTITION BY
                ad.router,
                ad.chain,
                ad.asset
            ORDER BY
                ad.date
        ) AS total_fee_earned
    FROM
        all_dates ad
    LEFT JOIN fees
        f ON ad.date = f.date
    AND ad.router = f.router
    AND ad.chain = f.chain
    AND ad.asset = f.asset
),

-- Fill routers TVL for all dates
filled_routers_tvl AS (
    SELECT
        ad.date,
        ad.router,
        ad.chain,
        ad.asset,
        rt.amount AS amount,
        -- # Total locked: filling with previous non zero value
        COALESCE(
            rt.locked,
            LAST_VALUE(rt.locked IGNORE NULLS) OVER (
                PARTITION BY
                    ad.router,
                    ad.chain,
                    ad.asset
                ORDER BY
                    ad.date
            ),
            0
        ) AS total_locked
    FROM
        all_dates ad
    LEFT JOIN routers_tvl
        rt ON ad.date = rt.date
    AND ad.router = rt.router
    AND ad.chain = rt.chain
    AND ad.asset = rt.asset
),

router_bal_hist AS (
    SELECT
        date,
        router,
        chain,
        asset,
        locked AS total_locked,
        fees_earned AS total_fees_earned
    FROM
        (
            SELECT
                DATE_TRUNC(ab.snapshot_time, DAY) AS date,
                ab.router_address AS router,
                cm.chain,
                tm.asset,
                ab.locked / POW(10, CAST(a.decimal AS INT64)) AS locked,
                ab.fees_earned
                / POW(10, CAST(a.decimal AS INT64)) AS fees_earned,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        ab.router_address,
                        cm.chain,
                        tm.asset,
                        DATE_TRUNC(ab.snapshot_time, DAY)
                    ORDER BY
                        ab.snapshot_time DESC
                ) AS rn
            FROM
                `mainnet-bigq.y42_connext_y42_dev.routers_assets_balance_hist`
                    ab
            LEFT JOIN `mainnet-bigq.public.assets` a
                ON
                    a.domain = ab.asset_domain
                    AND a.canonical_id = ab.asset_canonical_id
            LEFT JOIN chains_meta cm ON ab.asset_domain = cm.domainid
            LEFT JOIN tokens_meta tm ON a.local = tm.local
        )
    WHERE
        rn = 1
),

combined_router_tvl_fee AS (
    -- combine filled fee + router_tvl
    SELECT
        COALESCE(frt.date, ff.date) AS date,
        COALESCE(frt.router, ff.router) AS router,
        COALESCE(frt.chain, ff.chain) AS chain,
        COALESCE(frt.asset, ff.asset) AS asset,
        ff.router_fee,
        ff.total_fee_earned,
        frt.amount,
        frt.total_locked
    FROM
        filled_fees ff
    LEFT JOIN filled_routers_tvl
        frt ON ff.date = frt.date
    AND ff.router = frt.router
    AND ff.chain = frt.chain
    AND ff.asset = frt.asset
),

final AS (
    -- combine route bal hist to use metric for TVL coalesec with combined_router_tvl_fee
    SELECT
        COALESCE(rbh.date, ctv.date) AS date,
        COALESCE(rbh.router, ctv.router) AS router,
        COALESCE(rbh.chain, ctv.chain) AS chain,
        COALESCE(rbh.asset, ctv.asset) AS asset,
        ctv.router_fee,
        COALESCE(rbh.total_fees_earned, ctv.total_fee_earned)
            AS total_fee_earned,
        ctv.amount,
        COALESCE(rbh.total_locked, ctv.total_locked) AS total_locked,
        rbh.total_locked AS rbh_total_locked,
        ctv.total_locked AS ctv_total_locked
    FROM
        combined_router_tvl_fee ctv
    FULL OUTER JOIN router_bal_hist rbh
        ON
            ctv.date = rbh.date
            AND ctv.router = rbh.router
            AND ctv.chain = rbh.chain
            AND ctv.asset = rbh.asset
    ORDER BY
        1 DESC
)

SELECT
    *,
    f.total_locked + f.total_fee_earned AS total_balance
FROM
    final f
    -- WHERE
    -- router = "0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598"
    -- AND f.date = "2024-06-02 00:00:00 UTC"
    -- AND chain = "Gnosis"
    -- AND asset = "nextUSDC"
ORDER BY
    1,
    2,
    3,
    4 DESC
