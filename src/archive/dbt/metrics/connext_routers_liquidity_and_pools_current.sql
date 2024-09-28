-- Requirements: Chain asset - router liquidity (locked, total), pool (next asset, adopted)
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
    SELECT
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
            ELSE
                l.domain
        END
            AS domain,
        SUM(l.balance_usd) AS balance_usd,
        SUM(l.locked_usd) AS locked_usd,
        SUM(l.removed_usd) AS removed_usd,
        SUM(l.supplied_usd) AS supplied_usd
    FROM
        `mainnet-bigq.raw.source__cartographer_router_with_balances` l
    LEFT JOIN
        connext_tokens ct
        ON
            l.adopted = ct.token_address
    GROUP BY
        1,
        2,
        3,
        4
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
        CAST(JSON_EXTRACT_STRING_ARRAY(balances)[0] AS NUMERIC) AS balances_1,
        CAST(JSON_EXTRACT_STRING_ARRAY(balances)[1] AS NUMERIC) AS balances_2
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
        `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices`
            asset_prices
    GROUP BY
        canonical_id
),

pools_tvl_usd AS (
    SELECT
        sp.key AS pool_id,
        sp.chain,
        assets.canonical_id,
        COALESCE(ct_1.token_name, sp.token_1) AS token_1_name,
        COALESCE(ct_2.token_name, sp.token_2) AS token_2_name,
        sp.balances_1 / POW(10, sp.pool_token_decimals_1) AS pool_1_amount,
        sp.balances_2 / POW(10, sp.pool_token_decimals_2) AS pool_2_amount,
        -- USD
        asset_prices.price
        * sp.balances_1
        / POW(10, sp.pool_token_decimals_1) AS usd_pool_1_amount,
        asset_prices.price
        * sp.balances_2
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
        `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets`
            assets
        ON
            sp.token_1 = assets.id
            AND sp.domain = assets.domain
    LEFT JOIN
        `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices`
            asset_prices
        ON
            assets.canonical_id = asset_prices.canonical_id
    INNER JOIN
        maxassetprices
        ON
            asset_prices.canonical_id = maxassetprices.canonical_id
            AND asset_prices.timestamp = maxassetprices.max_timestamp
)

SELECT
    r.asset,
    r.is_xerc20,
    r.domain,
    p.token_1_name,
    p.token_2_name,
    SUM(r.balance_usd) AS balance_usd,
    SUM(r.locked_usd) AS locked_usd,
    SUM(r.removed_usd) AS removed_usd,
    SUM(r.supplied_usd) AS supplied_usd,
    SUM(p.usd_pool_1_amount) AS usd_pool_1_amount,
    SUM(p.usd_pool_2_amount) AS usd_pool_2_amount
FROM
    router_liquidity r
LEFT JOIN
    pools_tvl_usd p
    ON
        r.asset = p.token_2_name
        AND r.domain = p.chain
GROUP BY
    1,
    2,
    3,
    4,
    5
