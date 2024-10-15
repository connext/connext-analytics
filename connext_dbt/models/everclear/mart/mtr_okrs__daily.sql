-- TODO
-- metric: 1 -> Volume(monthly)
-- metric: 2  -> Rebalancing fee(bps) + volume(daily)
-- metric: 3 -> Avg. Settlement time(minutes) - for Market makers
-- metric: 4 -> % intents settled within 6 Hrs. - for all
-- metric: 5 -> % of Intents Netted within 24hrs of subimission by a Solver/Rebalancer -> only netted intents
-- 

WITH daily_metrics AS (
    SELECT
        DATE_TRUNC(ii.settlement_timestamp, DAY) AS day,
        SUM(ii.from_asset_amount_usd) AS volume_usd,
        AVG(ii.discount_bps_rebalancer) AS avg_rebalancing_fee_bps,
        SUM(ii.rebalancing_fee_usd) AS rebalancing_fee_usd,
        
        -- avg. time to settlement in hrs -> only for mmarket Makers
        AVG(CASE WHEN ii.invoice_intent_id IS NOT NULL 
            THEN TIMESTAMP_DIFF(ii.settlement_timestamp, ii.origin_timestamp, SECOND) / 3600
            ELSE NULL 
        END) AS avg_settlement_time_hrs,
        
        -- % intents settled within 6 Hrs- for all intents
        (   100 *
            COUNT(CASE WHEN TIMESTAMP_DIFF(ii.settlement_timestamp, ii.origin_timestamp, SECOND) / 3600 <= 6 THEN ii.id ELSE NULL END) 
            / COUNT(ii.id)
        ) AS pct_intents_settled_within_6hrs,

        -- % of Intents Netted in 24hrs
        (   100 *
            COUNT(CASE WHEN ii.invoice_intent_id IS NULL THEN ii.id ELSE NULL END) 
            / COUNT(ii.id)
        ) AS pct_intents_netted_in_24hrs

    FROM {{ref('cln_intents_invoices')}} AS ii
    -- filter current date
    WHERE DATE(ii.settlement_timestamp) < CURRENT_DATE()
    GROUP BY 1
    ORDER BY 1
)

SELECT
    dm.day,
    dm.volume_usd,
    dm.rebalancing_fee_usd,
    dm.avg_rebalancing_fee_bps,
    dm.avg_settlement_time_hrs,
    dm.pct_intents_settled_within_6hrs,
    dm.pct_intents_netted_in_24hrs

FROM daily_metrics AS dm