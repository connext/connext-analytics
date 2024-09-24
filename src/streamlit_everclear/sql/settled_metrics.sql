-- MARKET MAKER KPIs
-- 5. Settlement_Volume -> Volume settled by market makers
-- 11. Settlement_Time -> Time taken by Market Makers to settle a invoice
-- 14. Net profit: APY for Market Makers -> issue: fee_by_market_maker is not collected: gas_used
-- 15. Average amount of epochs
-- 19. Trading_Volume

WITH raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    AVG(CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.settlement_amount AS FLOAT)) AS avg_discounts_by_mm,
    SUM(CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.settlement_amount AS FLOAT)) AS discounts_by_mm,
    -- rewards
    AVG(CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.origin_amount AS FLOAT)) AS avg_rewards_by_invoices,
    SUM(CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.origin_amount AS FLOAT)) AS rewards_for_invoices,

    SUM(i.origin_amount::float) AS volume_settled_by_mm,
    COUNT(i.id) AS total_invoices_by_mm,
    AVG(
        (i.hub_settlement_enqueued_timestamp::FLOAT- i.hub_added_timestamp::FLOAT) / 3600
    ) AS alt_avg_time_in_hrs,
    AVG(
        (i.hub_settlement_enqueued_timestamp::FLOAT- i.origin_timestamp::FLOAT) / 3600
    ) AS avg_time_in_hrs,
    SUM(ABS(i.origin_amount::float - inv.hub_invoice_amount::float)) as fee_by_market_maker,
    
    -- fees paid by MM for the invoices
    SUM(i.origin_gas_limit::float * i.origin_gas_price::float) as fees_paid_by_mm,

    ROUND(AVG(inv.hub_settlement_epoch - inv.hub_invoice_entry_epoch), 0) AS avg_discount_epoch


FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status IN ('DISPATCHED', 'SETTLED')
GROUP BY 1,2,3,4,5
)

SELECT 
    day,
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    volume_settled_by_mm,
    total_invoices_by_mm,
    discounts_by_mm,
    avg_discounts_by_mm,
    rewards_for_invoices,
    avg_rewards_by_invoices,
    avg_time_in_hrs AS avg_settlement_time_in_hrs_by_mm,
    alt_avg_time_in_hrs AS alt_avg_settlement_time_in_hrs_by_mm,
    -- APY calculation as (fee/volume) * 365 based on daily fee to MM
    ((fee_by_market_maker) / volume_settled_by_mm) * 365 * 100 AS apy,
    avg_discount_epoch AS avg_discount_epoch_by_mm,
    fees_paid_by_mm
FROM raw