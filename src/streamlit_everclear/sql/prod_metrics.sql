-- Metrics

--1. Clearing_Volume

-- Summary:
-- This metric represents the aggregate value of all transactions that have been
-- fully processed and settled within the system. It includes both netted transactions
-- and those settled through market makers.
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS volume
FROM public.intents i
WHERE i.status= 'SETTLED_AND_COMPLETED' AND hub_status != 'DISPATCHED_UNSUPPORTED'
GROUP BY 1,2,3,4,5


--2. Netting_Volume: Volume of intents was netted (only Netting by other intents)
-- core logic, netted intents: these are the intents that are not in invoices table
 -- 2. Netting_Volume: Volume of intents that were netted (only Netting by other intents)
 -- 
 -- Definition:
 -- This metric represents the total volume of transactions that were settled through netting
 -- with other intents, without involving market makers. It is calculated by:
 -- 1. Selecting all intents that have been fully settled and completed
 -- 2. Excluding intents that appear in the invoices table (which represent market maker settlements)
 -- 3. Aggregating the total volume of these netted intents
 -- 
 -- The result is grouped by day, source chain, source asset, destination chain, and destination asset.

-- 10. Netting_Time
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS netting_volume,
    AVG(
        (i.settlement_timestamp::FLOAT- i.origin_timestamp::FLOAT) / 3600
    ) AS avg_netting_time_in_hrs

FROM public.intents i
LEFT JOIN public.invoices inv 
    ON i.id = inv.id
WHERE inv.id IS NULL
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
GROUP BY 1,2,3,4,5



--3. Settlement_Rate(1,3,6,24)
-- Definition:
WITH raw AS (
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    COUNT(i.id) as total_intents,
    -- 1hr in seconds
    COUNT(CASE
        WHEN i.status = 'SETTLED_AND_COMPLETED' AND (i.settlement_timestamp - i.origin_timestamp) < 3600
        AND i.hub_status = 'DISPATCHED'
        THEN id 
    END) as settled_in_1_hr,
    -- 3hrs in seconds
    COUNT(CASE
        WHEN i.status = 'SETTLED_AND_COMPLETED' AND (i.settlement_timestamp - i.origin_timestamp) < 10800
        AND i.hub_status = 'DISPATCHED'
        THEN id 
    END) as settled_in_3_hrs,
    --  6 hr in seconds
    COUNT(CASE
        WHEN i.status = 'SETTLED_AND_COMPLETED' AND (i.settlement_timestamp - i.origin_timestamp) < 21600
        AND i.hub_status = 'DISPATCHED'
        THEN id 
    END) as settled_in_6_hrs,
    -- 24 hr in seconds
    COUNT(CASE
        WHEN i.status = 'SETTLED_AND_COMPLETED' AND (i.settlement_timestamp - i.origin_timestamp) < 86400
        AND i.hub_status = 'DISPATCHED'
        THEN id 
    END) as settled_in_24_hrs

    
FROM public.intents i
WHERE hub_status != 'DISPATCHED_UNSUPPORTED'
GROUP BY 1)

-- settlement rate
SELECT 
    day,
    total_intents,
    settled_in_1_hr AS intents_settled_in_1_hr,
    settled_in_3_hrs AS intents_settled_in_3_hrs,
    settled_in_6_hrs AS intents_settled_in_6_hrs,
    settled_in_24_hrs AS intents_settled_in_24_hrs,
    ROUND(settled_in_1_hr * 100.0 / NULLIF(total_intents, 0), 2) AS settlement_rate_1h_percentage,
    ROUND(settled_in_3_hrs * 100.0 / NULLIF(total_intents, 0), 2) AS settlement_rate_3h_percentage,
    ROUND(settled_in_6_hrs * 100.0 / NULLIF(total_intents, 0), 2) AS settlement_rate_6h_percentage,
    ROUND(settled_in_24_hrs * 100.0 / NULLIF(total_intents, 0), 2) AS settlement_rate_24h_percentage
FROM raw;


-- MARKET MAKER KPIs

-- 5. Settlement_Volume -> Volume settled by market makers
-- 11. Settlement_Time -> Time taken by Market Makers to settle a invoice
-- 14. Net profit: APY for Market Makers
-- 15. Average amount of epochs
    -- each epoch is 30 mins so count avg epoch based on the time
-- 17. Settlement_Rate_1h
-- 18. Settlement_Rate_3h
-- 19. Trading_Volume

WITH raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS volume_settled_by_mm,
    COUNT(i.id) AS total_invoices_by_mm,
    AVG(
        (inv.hub_invoice_enqueued_timestamp::FLOAT- i.origin_timestamp::FLOAT) / 3600
    ) AS avg_time_in_hrs,

    SUM(ABS(i.origin_amount::float - inv.hub_invoice_amount::float)) as fee_by_market_maker,
    
    -- fees paid by MM for the invoices
    SUM(i.origin_gas_limit::float * i.origin_gas_price::float) as fees_paid_by_mm,

    AVG(CASE 
        WHEN inv.hub_status IN ('DISPATCHED', 'SETTLED') 
        THEN ROUND((inv.hub_settlement_epoch - inv.hub_invoice_entry_epoch), 0)
        ELSE NULL
    END ) AS avg_discount_epoch,


    -- settlement Rate: 1hr, 3hr, 6hr, 24hr

    -- 1hr in seconds
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp) < 3600
        AND inv.hub_status IN ('DISPATCHED', 'SETTLED')
        THEN id 
    END) as settled_in_1_hr,
    -- 3hrs in seconds
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp) < 10800
        AND inv.hub_status IN ('DISPATCHED', 'SETTLED')
        THEN id 
    END) as settled_in_3_hrs,
    --  6 hr in seconds
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp) < 21600
        AND inv.hub_status IN ('DISPATCHED', 'SETTLED')
        THEN id 
    END) as settled_in_6_hrs,
    -- 24 hr in seconds
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp) < 86400
        AND inv.hub_status IN ('DISPATCHED', 'SETTLED')
        THEN id 
    END) as settled_in_24_hrs



FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
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
    avg_time_in_hrs AS avg_settlement_time_in_hrs_by_mm,
    -- APY calculation as (fee/volume) * 365 based on daily fee to MM
    ((fee_by_market_maker - fees_paid_by_mm) / volume_settled_by_mm) * 365 * 100 AS apy,
    avg_discount_epoch AS avg_discount_epoch_by_mm
FROM raw



-- 6. Total_rebalaicing fee
-- 12. Total_Protocol_Revenue: Fee for the protocol
-- core logic: 
-- missing rewrds in the above query:
-- amounts from the hub invoice amount is thr rewards
-- rewards = origin_amount - hub_invoiced_amount -> accurate for intents that become invoices
-- discounts = hub_invoiced_amount - settlement_amount
-- rebalancing_fee = protocol_fee + discounts

WITH raw AS (
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    CASE 
        WHEN inv.id IS NULL THEN 'netted'
        WHEN inv.id IS NOT NULL THEN 'settled'
    END AS intent_settlement_type,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    0.001 AS fee_value,
    i.origin_amount::FLOAT AS origin_amount,
    -- SETTLED
    -- # discounts:
    CASE 
        WHEN inv.id IS NULL THEN 0
        WHEN inv.id IS NOT NULL THEN (CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.settlement_amount AS FLOAT))
    END AS intent_discounts_by_mm
FROM public.intents i
LEFT JOIN public.invoices inv ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
)

SELECT
    day,
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    intent_settlement_type,
    SUM(intent_discounts_by_mm) AS discount,
    SUM(fee_value * origin_amount) AS fee_amount,
    (SUM(intent_discounts_by_mm) + SUM(fee_value * origin_amount)) AS rebalancing_fee
FROM raw
GROUP BY 1,2,3,4,5,6


-- 8. Amount of intents
-- 9. Average intent size
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    AVG(i.origin_amount::float / POW(10, td.decimals)) AS avg_intent_size,
    COUNT(i.id) AS total_intents
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1,2,3,4,5;



-- 13. Wallet_retention rate
-- Metric 13: **Wallet_retention_rate**: Measures the frequency and consistency of user activity associated with specific wallet addresses over time
-- user 1: origin_intent -> initiator
-- Other can be added in similar way

-- weekly retention by origin wallet and its start date by first intent id
WITH user_activity AS (
  SELECT 
    initiator AS origin_wallet,
    DATE_TRUNC('week', to_timestamp(timestamp)) AS week
  FROM public.origin_intents
),
cohorts AS (
  SELECT
    origin_wallet,
    MIN(week) AS cohort_week
  FROM user_activity
  GROUP BY origin_wallet
),
user_retention AS (
  SELECT
    c.cohort_week,
    ua.week,
    COUNT(DISTINCT ua.origin_wallet) AS users
  FROM cohorts c
  JOIN user_activity ua ON c.origin_wallet = ua.origin_wallet
  GROUP BY c.cohort_week, ua.week
)
SELECT
  cohort_week,
  week,
  users,
  FLOOR((EXTRACT(EPOCH FROM week) - EXTRACT(EPOCH FROM cohort_week)) / 604800) AS weeks_since_cohort,
  users::FLOAT / FIRST_VALUE(users) OVER (PARTITION BY cohort_week ORDER BY week) AS retention_rate
FROM user_retention
ORDER BY cohort_week, week;

-- 16. Discount_value
-- [ ] Questions: {How is discount_value -ve sometimes: 0x9c1501b239bec1a99ea6fb6110ebe59c6f8bcd0df6c219c09d7e94d272f918c1}

SELECT
    inv.id,
    inv.origin_amount::float,
    inv.hub_invoice_amount::float,
    to_timestamp(inv.hub_invoice_enqueued_timestamp) AS hub_invoice_enqueued_timestamp,
    to_timestamp(inv.origin_timestamp) AS origin_timestamp,
    (inv.hub_invoice_enqueued_timestamp::float - inv.origin_timestamp::float) AS time_2_settle,
    60 *30 * (inv.hub_settlement_epoch - inv.hub_invoice_entry_epoch)
FROM invoices inv
