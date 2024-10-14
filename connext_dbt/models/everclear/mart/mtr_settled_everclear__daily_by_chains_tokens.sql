WITH settled AS (

    SELECT 
        DATE_TRUNC(i.origin_timestamp, DAY) AS day,
        i.from_chain_id,
        i.from_chain_name,
        i.from_asset_address,
        i.from_asset_symbol,
        i.to_chain_id,
        i.to_chain_name,
        i.to_asset_address,
        i.to_asset_symbol,
        -- origin amount -> from_asset_amount and settlement amount -> to_asset_amount
        COUNT(i.id) AS total_intents_by_mm,
        AVG(inv.hub_invoice_amount_usd - i.to_asset_amount_usd) AS avg_discounts_by_mm_usd,
        SUM(inv.hub_invoice_amount_usd - i.to_asset_amount_usd) AS discounts_by_mm_usd,
        AVG( inv.hub_invoice_amount_usd - i.from_asset_amount_usd) AS avg_rewards_by_invoice_usd,
        SUM( inv.hub_invoice_amount_usd - i.from_asset_amount_usd - i.fee_value * i.from_asset_amount_usd) AS rewards_for_invoices_usd,
        SUM(i.from_asset_amount_usd) AS volume_settled_by_mm_usd,
        -- proxy for system to settle invoices
        AVG(i.hub_settlement_enqueued_timestamp_epoch - i.hub_added_timestamp_epoch) AS avg_time_in_hrs,
        ROUND(AVG(inv.hub_settlement_epoch - inv.hub_invoice_entry_epoch),0) AS avg_discount_epoch,
        SUM(i.fee_value * i.from_asset_amount_usd) AS protocol_revenue_mm_usd
    
    FROM {{ref('cln_intents')}} i
    LEFT JOIN {{ref('cln_invoices')}} inv
        ON i.id = inv.id
    WHERE i.status = 'SETTLED_AND_COMPLETED'
        AND i.hub_status IN ('DISPATCHED', 'SETTLED')

    GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
    )

SELECT day,
    from_chain_id,
    from_chain_name,
    from_asset_address,
    from_asset_symbol,
    to_chain_id,
    to_chain_name,
    to_asset_address,
    to_asset_symbol,
    total_intents_by_mm,
    volume_settled_by_mm_usd,
    protocol_revenue_mm_usd,
    discounts_by_mm_usd,
    avg_discounts_by_mm_usd,
    rewards_for_invoices_usd,
    avg_rewards_by_invoice_usd,
    avg_time_in_hrs AS avg_settlement_time_in_hrs_by_mm,
    (discounts_by_mm_usd / volume_settled_by_mm_usd) * 365 * 100 AS apy,
    avg_discount_epoch AS avg_discount_epoch_by_mm
FROM settled