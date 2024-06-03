WITH chain_metadata AS (
    SELECT DISTINCT
        domainid,
        name
    FROM `mainnet-bigq.raw.source_chaindata_nija__metadata`
    WHERE domainid IS NOT NULL AND name IS NOT NULL

),

pool_metadata AS (
    SELECT DISTINCT
        pool_id,
        CASE
            WHEN token_1_name IS NULL THEN "nextMETIS"
            ELSE token_1_name
        END AS token_1_name,
        CASE
            WHEN token_2_name IS NULL THEN "METIS"
            ELSE token_2_name
        END AS token_2_name

    FROM `mainnet-bigq.metrics_routers_lp_pools.lp_pools_current_agg`
    WHERE token_1_name != token_2_name
),

final AS (
    SELECT
        dst.*,
        pm.token_1_name,
        pm.token_2_name,
        COALESCE(name, CAST(dst.domain AS STRING)) AS chain
    FROM
        `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_daily_swap_tvl`
            dst
    LEFT JOIN chain_metadata cm
        ON dst.domain = cm.domainid
    LEFT JOIN pool_metadata pm
        ON dst.pool_id = pm.pool_id


),


apr_apy_calculations AS (
    SELECT
        day AS date,
        pool_id,
        chain,
        token_1_name,
        token_2_name,
        total_tvl AS tvl,
        total_fee AS fee,
        total_vol AS vol,
        -- Annualize the daily fee to get APR
        (total_fee / NULLIF(total_tvl, 0)) * 365 AS apr,
        -- Calculate APY using compound interest formula
        (POWER(1 + (total_fee / NULLIF(total_tvl, 0)), 365) - 1) AS apy
    FROM final
)

SELECT
    *,
    AVG(apr)
        OVER (
            PARTITION BY pool_id, chain
            ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
        AS avg_7d_apr,
    AVG(apy)
        OVER (
            PARTITION BY pool_id, chain
            ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
        AS avg_7d_apy,
    AVG(apr)
        OVER (
            PARTITION BY pool_id, chain
            ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )
        AS avg_14d_apr,
    AVG(apy)
        OVER (
            PARTITION BY pool_id, chain
            ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )
        AS avg_14d_apy
FROM apr_apy_calculations
