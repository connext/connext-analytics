WITH metadata AS (
    SELECT Symbol AS symbol,
        CAST(Decimals AS INTEGER) AS decimal,
        CAST(DomainID AS INTEGER) AS domain_id,
        LOWER(Address) AS address,
        LOWER(
            CONCAT(
                '0x',
                LPAD(
                    SUBSTRING(
                        Address
                        FROM 3
                    ),
                    64,
                    '0'
                )
            )
        ) AS adopted_address
    FROM (
            VALUES (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    1,
                    '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    10,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    56,
                    '0x2170Ed0880ac9A755fd29B2688956BD959F933F8'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    8453,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    42161,
                    '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    1,
                    '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    10,
                    '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85'
                ),
                (
                    'USD Coin',
                    'USDC',
                    18,
                    56,
                    '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    8453,
                    '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    42161,
                    '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    1,
                    '0xdAC17F958D2ee523a2206206994597C13D831ec7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    10,
                    '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58'
                ),
                (
                    'Tether USD',
                    'USDT',
                    18,
                    56,
                    '0x55d398326f99059fF775485246999027B3197955'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'
                )
        ) AS asset_data (AssetName, Symbol, Decimals, DomainID, Address)
),
raw_6hr_all AS (
    SELECT (
            DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) + (
                FLOOR(
                    EXTRACT(
                        hour
                        FROM to_timestamp(i.origin_timestamp)
                    ) / 6
                ) * INTERVAL '6 hour'
            )
        ) AS six_hour_interval,
        CAST(i.origin_origin AS INTEGER) AS from_chain_id,
        i.origin_input_asset AS from_asset_address,
        fm.symbol AS from_asset_symbol,
        CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
        i.settlement_asset AS to_asset_address,
        tm.symbol AS to_asset_symbol,
        SUM(i.origin_amount::float) AS volume,
        COUNT(i.id) as total_intents,
        COUNT(
            CASE
                WHEN (
                    CAST(i.hub_settlement_enqueued_timestamp AS FLOAT) - i.origin_timestamp
                ) < 21600 THEN i.id
                ELSE NULL
            END
        ) as settled_in_6_hrs
    FROM public.intents i
    LEFT JOIN metadata fm ON (
        i.origin_input_asset = fm.adopted_address
        AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
    )
    LEFT JOIN metadata tm ON (
        LOWER(i.settlement_asset) = tm.address
        AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
    )
    WHERE i.status = 'SETTLED_AND_COMPLETED'
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7
),
final_6hr_all AS (
    SELECT
        from_chain_id,
        from_asset_address,
        from_asset_symbol,
        to_chain_id,
        to_asset_address,
        to_asset_symbol,
        AVG(settled_in_6_hrs) AS avg_intents_settled_in_6_hrs,
        AVG(total_intents) AS avg_intents_in_6_hrs,
        -- settlement rate
        ROUND(
            AVG(
                settled_in_6_hrs * 100.0 / NULLIF(total_intents, 0)
            ),
            2
        ) AS daily_avg_settlement_rate_6h
    FROM raw_6hr_all
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6
) -- 24 hr all
,
raw_24hr_all AS (
    SELECT DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
        CAST(i.origin_origin AS INTEGER) AS from_chain_id,
        i.origin_input_asset AS from_asset_address,
        fm.symbol AS from_asset_symbol,
        CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
        i.settlement_asset AS to_asset_address,
        tm.symbol AS to_asset_symbol,
        SUM(i.origin_amount::float) AS volume,
        COUNT(i.id) as total_intents,
        COUNT(
            CASE
                WHEN (
                    CAST(i.hub_settlement_enqueued_timestamp AS FLOAT) - i.origin_timestamp
                ) < 86400 THEN i.id
                ELSE NULL
            END
        ) as settled_in_24_hrs
    FROM public.intents i
    LEFT JOIN metadata fm ON (
        i.origin_input_asset = fm.adopted_address
        AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
    )
    LEFT JOIN metadata tm ON (
        LOWER(i.settlement_asset) = tm.address
        AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
    )
    WHERE i.status = 'SETTLED_AND_COMPLETED'
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7
),
final_24hr_all AS (
    SELECT
        from_chain_id,
        from_asset_address,
        from_asset_symbol,
        to_chain_id,
        to_asset_address,
        to_asset_symbol,
        AVG(settled_in_24_hrs) AS avg_intents_settled_in_24_hrs,
        AVG(total_intents) AS avg_intents_in_24_hrs,
        -- settlement rate
        ROUND(
            AVG(
                settled_in_24_hrs * 100.0 / NULLIF(total_intents, 0)
            ),
            2
        ) AS daily_avg_settlement_rate_24h
    FROM raw_24hr_all
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6
        
),
final_all AS (
    SELECT
        COALESCE(
            final_6hr_all.from_chain_id,
            final_24hr_all.from_chain_id
        ) AS from_chain_id,
        COALESCE(
            final_6hr_all.from_asset_address,
            final_24hr_all.from_asset_address
        ) AS from_asset_address,
        COALESCE(
            final_6hr_all.to_chain_id,
            final_24hr_all.to_chain_id
        ) AS to_chain_id,
        COALESCE(
            final_6hr_all.to_asset_address,
            final_24hr_all.to_asset_address
        ) AS to_asset_address,
        COALESCE(
            final_6hr_all.from_asset_symbol,
            final_24hr_all.from_asset_symbol
        ) AS from_asset_symbol,
        COALESCE(
            final_6hr_all.to_asset_symbol,
            final_24hr_all.to_asset_symbol
        ) AS to_asset_symbol,
        final_6hr_all.avg_intents_settled_in_6_hrs,
        final_24hr_all.avg_intents_settled_in_24_hrs,
        final_6hr_all.avg_intents_in_6_hrs,
        final_24hr_all.avg_intents_in_24_hrs,
        final_6hr_all.daily_avg_settlement_rate_6h,
        final_24hr_all.daily_avg_settlement_rate_24h
    FROM final_6hr_all
        FULL OUTER JOIN final_24hr_all 
        ON final_6hr_all.from_chain_id = final_24hr_all.from_chain_id
        AND final_6hr_all.from_asset_address = final_24hr_all.from_asset_address
        AND final_6hr_all.to_chain_id = final_24hr_all.to_chain_id
        AND final_6hr_all.to_asset_address = final_24hr_all.to_asset_address
        AND final_6hr_all.from_asset_symbol = final_24hr_all.from_asset_symbol
        AND final_6hr_all.to_asset_symbol = final_24hr_all.to_asset_symbol
) -- Market Maker 1hr
,
raw_1hr_mm AS (
    SELECT (
            DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) + (
                FLOOR(
                    EXTRACT(
                        hour
                        FROM to_timestamp(i.origin_timestamp)
                    ) / 1
                ) * INTERVAL '1 hour'
            )
        ) AS one_hour_interval,
        CAST(i.origin_origin AS INTEGER) AS from_chain_id,
        i.origin_input_asset AS from_asset_address,
        fm.symbol AS from_asset_symbol,
        CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
        i.settlement_asset AS to_asset_address,
        tm.symbol AS to_asset_symbol,
        -- 1hr -> when invoice enters the queue and till hub processes the invoice
        COUNT(
            CASE
                WHEN (
                    (
                        i.hub_settlement_enqueued_timestamp::float - inv.hub_invoice_enqueued_timestamp::float
                    )
                ) < 3600 THEN inv.id
                ELSE NULL
            END
        ) as settled_in_1_hr,
        COUNT(i.id) as total_intents
    FROM public.intents i
        INNER JOIN public.invoices inv ON i.id = inv.id
    LEFT JOIN metadata fm ON (
        i.origin_input_asset = fm.adopted_address
        AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
    )
    LEFT JOIN metadata tm ON (
        LOWER(i.settlement_asset) = tm.address
        AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
    )
    WHERE i.status = 'SETTLED_AND_COMPLETED'
    AND i.hub_status IN ('DISPATCHED', 'SETTLED')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')

    GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7
),
final_1hr_mm AS (
    SELECT
        from_chain_id,
        from_asset_address,
        from_asset_symbol,
        to_chain_id,
        to_asset_address,
        to_asset_symbol,
        AVG(settled_in_1_hr) AS avg_intents_settled_in_1_hr,
        AVG(total_intents) AS avg_intents_in_1_hr,
        -- settlement rate
        ROUND(
            AVG(
                settled_in_1_hr * 100.0 / NULLIF(total_intents, 0)
            ),
            2
        ) AS mm_daily_avg_settlement_rate_1h_percentage
    FROM raw_1hr_mm
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6
) -- Market Maker 3hr
,
raw_3hr_mm AS (
    SELECT (
            DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) + (
                FLOOR(
                    EXTRACT(
                        hour
                        FROM to_timestamp(i.origin_timestamp)
                    ) / 3
                ) * INTERVAL '3 hour'
            )
        ) AS three_hour_interval,
        CAST(i.origin_origin AS INTEGER) AS from_chain_id,
        i.origin_input_asset AS from_asset_address,
        fm.symbol AS from_asset_symbol,
        CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
        i.settlement_asset AS to_asset_address,
        tm.symbol AS to_asset_symbol,
        -- 3hr
        COUNT(
            CASE
                WHEN (
                    (
                        i.hub_settlement_enqueued_timestamp::float - inv.hub_invoice_enqueued_timestamp::float
                    )
                ) < 10800 THEN inv.id
                ELSE NULL
            END
        ) as settled_in_3_hr,
        COUNT(i.id) as total_intents
    FROM public.intents i
        INNER JOIN public.invoices inv ON i.id = inv.id
    LEFT JOIN metadata fm ON (
        i.origin_input_asset = fm.adopted_address
        AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
    )
    LEFT JOIN metadata tm ON (
        LOWER(i.settlement_asset) = tm.address
        AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
    )
    WHERE i.status = 'SETTLED_AND_COMPLETED'
    AND i.hub_status IN ('DISPATCHED', 'SETTLED')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}')
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7
),
final_3hr_mm AS (
    SELECT
        from_chain_id,
        from_asset_address,
        to_chain_id,
        to_asset_address,
        from_asset_symbol,
        to_asset_symbol,
        -- settlement rate
        AVG(settled_in_3_hr) AS avg_intents_settled_in_3_hr,
        AVG(total_intents) AS avg_intents_in_3_hr,
        ROUND(
            AVG(
                settled_in_3_hr * 100.0 / NULLIF(total_intents, 0)
            ),
            2
        ) AS mm_daily_avg_settlement_rate_3h_percentage
    FROM raw_3hr_mm
    GROUP BY 1,
        2,
        3,
        4,
        5,
        6
) -- Market Maker 1hr and 3hr
,
mm_final AS (
    SELECT
        COALESCE(
            final_1hr_mm.from_chain_id,
            final_3hr_mm.from_chain_id
        ) AS from_chain_id,
        COALESCE(
            final_1hr_mm.from_asset_address,
            final_3hr_mm.from_asset_address
        ) AS from_asset_address,
        COALESCE(
            final_1hr_mm.to_chain_id,
            final_3hr_mm.to_chain_id
        ) AS to_chain_id,
        COALESCE(
            final_1hr_mm.to_asset_address,
            final_3hr_mm.to_asset_address
        ) AS to_asset_address,
        COALESCE(
            final_1hr_mm.from_asset_symbol,
            final_3hr_mm.from_asset_symbol
        ) AS from_asset_symbol,
        COALESCE(
            final_1hr_mm.to_asset_symbol,
            final_3hr_mm.to_asset_symbol
        ) AS to_asset_symbol,
        final_1hr_mm.avg_intents_settled_in_1_hr AS mm_avg_intents_settled_in_1_hr,
        final_3hr_mm.avg_intents_settled_in_3_hr AS mm_avg_intents_settled_in_3_hr,
        final_1hr_mm.avg_intents_in_1_hr AS mm_avg_intents_in_1_hr,
        final_3hr_mm.avg_intents_in_3_hr AS mm_avg_intents_in_3_hr,
        final_1hr_mm.mm_daily_avg_settlement_rate_1h_percentage,
        final_3hr_mm.mm_daily_avg_settlement_rate_3h_percentage
    FROM final_1hr_mm
        FULL OUTER JOIN final_3hr_mm ON 
        final_1hr_mm.from_chain_id = final_3hr_mm.from_chain_id
        AND final_1hr_mm.from_asset_address = final_3hr_mm.from_asset_address
        AND final_1hr_mm.to_chain_id = final_3hr_mm.to_chain_id
        AND final_1hr_mm.to_asset_address = final_3hr_mm.to_asset_address
        AND final_1hr_mm.from_asset_symbol = final_3hr_mm.from_asset_symbol
        AND final_1hr_mm.to_asset_symbol = final_3hr_mm.to_asset_symbol
)
SELECT 
    COALESCE(final_all.from_chain_id, mm_final.from_chain_id) AS from_chain_id,
    COALESCE(
        final_all.from_asset_address,
        mm_final.from_asset_address
    ) AS from_asset_address,
    COALESCE(final_all.to_chain_id, mm_final.to_chain_id) AS to_chain_id,
    COALESCE(
        final_all.to_asset_address,
        mm_final.to_asset_address
    ) AS to_asset_address,
    COALESCE(final_all.from_asset_symbol, mm_final.from_asset_symbol) AS from_asset_symbol,
    COALESCE(final_all.to_asset_symbol, mm_final.to_asset_symbol) AS to_asset_symbol,
    -- settled
    final_all.avg_intents_settled_in_6_hrs,
    final_all.avg_intents_settled_in_24_hrs,
    mm_final.mm_avg_intents_settled_in_1_hr,
    mm_final.mm_avg_intents_settled_in_3_hr,
    -- avg total
    final_all.avg_intents_in_6_hrs,
    final_all.avg_intents_in_24_hrs,
    mm_final.mm_avg_intents_in_1_hr,
    mm_final.mm_avg_intents_in_3_hr,
    -- rate
    final_all.daily_avg_settlement_rate_6h,
    final_all.daily_avg_settlement_rate_24h,
    mm_final.mm_daily_avg_settlement_rate_1h_percentage,
    mm_final.mm_daily_avg_settlement_rate_3h_percentage
FROM final_all
    FULL OUTER JOIN mm_final ON final_all.from_chain_id = mm_final.from_chain_id
    AND final_all.from_asset_address = mm_final.from_asset_address
    AND final_all.to_chain_id = mm_final.to_chain_id
    AND final_all.to_asset_address = mm_final.to_asset_address
    AND final_all.from_asset_symbol = mm_final.from_asset_symbol
    AND final_all.to_asset_symbol = mm_final.to_asset_symbol