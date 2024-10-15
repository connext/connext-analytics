WITH final AS (
SELECT
    --intents
    
    i.updated_at,
    i.id,
    i.status,
    i.hub_status,
    i.origin_initiator,
    -- from
    i.from_hash,
    i.origin_timestamp,
    i.from_chain_id,
    i.from_chain_name,
    i.from_asset_address,
    i.from_asset_symbol,
    i.from_asset_decimals,
    i.from_asset_amount,
    i.from_asset_price,
    i.from_asset_amount_usd,
    
    -- to
    i.settlement_timestamp,
    i.to_chain_id,
    i.to_chain_name,
    i.to_asset_address,
    i.to_asset_symbol,
    i.to_asset_decimals,
    i.to_asset_amount,
    i.to_asset_price,
    i.to_asset_amount_usd,
    -- misc
    i.hub_added_timestamp,
    i.hub_settlement_enqueued_timestamp,
    i.fee_value,
    i.hub_settlement_amount_usd,
    i.hub_settlement_enqueued_timestamp_epoch,
    i.hub_added_timestamp_epoch,

    -- invoices
    inv.id AS invoice_intent_id,
    inv.hub_invoice_id,
    inv.hub_invoice_intent_id,
    inv.hub_invoice_enqueued_timestamp,
    inv.hub_invoice_entry_epoch,
    inv.hub_settlement_epoch,
    inv.hub_invoice_amount_usd,

    -- calculation and flags
    CASE 
        WHEN inv.id IS NULL 
        THEN (10000 * (i.from_asset_amount_usd - i.to_asset_amount_usd) / i.from_asset_amount_usd)
        ELSE NULL
    END AS discount_bps_rebalancer,
    CASE 
        WHEN inv.id IS NOT NULL AND i.hub_status IN ('DISPATCHED', 'SETTLED')
        THEN (
            cast(i.hub_settlement_amount_usd as float64) / cast(i.from_asset_amount_usd as float64) - 1
        ) * 10000
        ELSE NULL
    END AS discount_bps_mm,
    TIMESTAMP_DIFF(i.hub_settlement_enqueued_timestamp, i.origin_timestamp, MINUTE) AS settlement_duration_minutes,
    (i.fee_value * i.from_asset_amount_usd) AS protocol_revenue_usd,
    CASE WHEN inv.id IS NOT NULL THEN (inv.hub_invoice_amount_usd - i.to_asset_amount_usd) ELSE 0 END AS discounts_by_mm_usd

FROM {{ref('cln_intents')}} i
LEFT JOIN {{ref('cln_invoices')}} inv
    ON i.id = inv.id
WHERE i.status = 'SETTLED_AND_COMPLETED'
    AND i.hub_status != 'DISPATCHED_UNSUPPORTED'

)

SELECT *,

    -- market makers flag on origin_initiator
    CASE
        WHEN discount_bps_mm < 0 THEN 'Negative'
        WHEN discount_bps_mm >= 0 AND discount_bps_mm < 0.8 THEN '0 - 0.79'
        WHEN discount_bps_mm >= 0.8 AND discount_bps_mm < 1.6 THEN '0.8 - 1.59'
        WHEN discount_bps_mm >= 1.6 AND discount_bps_mm < 2.4 THEN '1.6 - 2.39'
        WHEN discount_bps_mm >= 2.4 AND discount_bps_mm < 3.2 THEN '2.4 - 3.19'
        WHEN discount_bps_mm >= 3.2 AND discount_bps_mm < 4.0 THEN '3.2 - 3.99'
        WHEN discount_bps_mm >= 4.0 AND discount_bps_mm < 4.8 THEN '4.0 - 4.8'
        ELSE '4.8.0+'
    END AS discount_bucket,
    CASE
        WHEN discount_bps_rebalancer = 0 THEN 'netted'
        ELSE 'market_maker_filled'
    END AS netted_flag_discount_0,
    CASE
        WHEN discount_bps_rebalancer = 0
        AND settlement_duration_minutes <= 1440 THEN 'netted in 24hrs'
        ELSE 'not netted in 24hrs'
    END AS netted_in_24hrs,
    case
    when settlement_duration_minutes <= 360 then 'settled within 6hrs'
    else 'settled in greater than 6hrs'
    end as settled_in_6hrs_flag,
    -- rebalancer_fee_usd
    protocol_revenue_usd + discounts_by_mm_usd AS rebalancing_fee_usd

FROM final
