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
            -- craeate incremental batch based on 20 tx per 3hr max
            TIMESTAMP_SECONDS (t.xcall_timestamp) AS xcall_timestamp,
            t.destination_domain,
            t.destination_local_asset,
            a.decimal,
            t.destination_transacting_amount,
        FROM
            `public.transfers` t
            LEFT JOIN assets a ON (
                t.canonical_id = a.canonical_id
                AND t.destination_domain = a.domain
            ) -- TODO Remove Filter Later
      
    ),
    tx_agg AS (
        SELECT
            DATE_TRUNC (xcall_timestamp, HOUR) AS xcall_timestamp,
            router,
            cm.chain AS chain,
            COALESCE(tm.asset, t.destination_local_asset) AS asset,
            SUM(destination_fast_amount) AS destination_fast_volume,
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