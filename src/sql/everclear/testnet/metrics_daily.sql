-- 1. Settlement Rate 24h
-- Definition: Percentage of intents that were settled within 24 hours, the start time
-- given there are two settrment strategy: direct settlement on intent queue and hub intent based clearing
-- timestamp start date and end date
-- clubbing them together

-- for MM -> Look at amounts
-- for users -> Look at count aswell
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    COUNT(i.id) as total_intent_count,
    COUNT(CASE WHEN i.settlement_status = 'SETTLED' THEN 1 END) as settled_count,
    100 * COUNT(CASE WHEN i.settlement_status = 'SETTLED' THEN 1 END) / COUNT(i.id) as prct_of_settled_count
FROM public.intents i
-- filter for date of 6 and 7th sep
WHERE TO_CHAR(TO_TIMESTAMP(i.origin_timestamp / 1000), 'DD/MM/YYYY HH24:MI:SS') IN ('2024-09-06', '2024-09-07')
GROUP BY 1


-- Metric 2: Invoices_1h_Retention_Rate- Percentage of invoices that remain in the system for ~1h
WITH tis AS (
SELECT
    -- calculate time in system for each invoice origin_timestamp till settlement_timestamp
    DATE_TRUNC('hour', to_timestamp(i.settlement_timestamp)) as settlement_hour,
    AVG(i.settlement_timestamp - i.origin_timestamp) as time_in_system,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
    END) AS count_of_invoices_within_1h,
    COUNT(id) AS total_count_of_invoices
FROM public.intents i
-- filter for date of 6 and 7th sep
WHERE i.origin_timestamp::date IN ('2024-09-06', '2024-09-07')
GROUP BY 1
)
SELECT 
    settlement_hour,
    time_in_system,
    total_count_of_invoices,
    count_of_invoices_within_1h,
    100 * count_of_invoices_within_1h / total_count_of_invoices AS retention_rate
FROM tis



-- Metric 3: Epoch_Discounts- Number of epoch discounts applied to the invoice before settlement
-- LOGIC: based on the epoch logic using the above intents table create this metric-> use hub_invoice and origin_intent columns
-- diff amounts: orgin_intent_amount - settled_amount
-- diff epoch: settlement_timestamp - origin_timestamp
-- Events from Queue -> SettlementEnqueued | DepositEnqueued
-- DepositEnqueued -->SettlementEnqueued or DepositProcessed->SettlementEnqueued

-- 3.1. EPOCH AMOUNT DISCOUNT: same as metric 5.
-- 3.2. EPOCH TIME DISCOUNT:
-- This is calculated based on hub entry and settlement epoch and only for those intents that are not netted ie origin_ttl > 0
-- discount is decided by hub
-- use the hub timestamp -> Settlement_enqueued_timestamp -> its the timestamp where the settlement is finalized

SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    AVG(i.origin_amount::float - i.settlement_amount::float) as discount_value,
    AVG(i.settlement_enqueued_timestamp -i.origin_timestamp) as discount_epoch,
    -- ??? making sure that invoice created doesn't alter the entry epoch time ie can a intent enter twice if not pickedup
    AVG(i.hub_settlement_epoch::integer - hi.entry_epoch::integer) as discount_epoch_alt
 FROM public.intents i
 LEFT JOIN public.hub_invoices hi ON i.id = hi.intent_id
 -- filter out the netted invoices
 WHERE i.origin_ttl > 0 AND i.settlement_status = 'SETTLED'
 GROUP BY 1


-- Metric 4: volume by market maker- Trading_Volume
-- ??? which amount to use origin_amount or hub_invoice_amount
-- ??? How to identify market maker
SELECT 
 DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
 SUM(i.origin_amount) as volume_by_market_maker
 FROM public.invoices i
 -- filter out the netted invoices
 WHERE i.origin_ttl > 0 AND i.settlement_status = 'SETTLED'
 GROUP BY 1

-- Metric 5: Discount_value- The daily average discount applied to invoices
-- todo: remove fee from the amount
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    -- gas fee + protocol fee
    -- protcol fee based on token- event: TokenConfigsSet | AssetConfigSet
    -- function that is called for protcol fee:
    AVG(i.origin_amount::float - i.settlement_amount::float) as discount_value
 FROM public.invoices i
 WHERE i.origin_ttl > 0 AND i.settlement_status = 'SETTLED'
 GROUP BY 1

-- Metric 6: 

-- **APY**: Annual Percentage Yield for MM
-- confirm the fee earned by market makers with Preetham
WITH raw AS (
  SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    SUM(i.settled_amount) as volume_by_market_maker,
    -- Assuming the fee is the difference between the origin amount and settled amount
    SUM(i.origin_amount::float - i.settlement_amount::float) as fee_by_market_maker
  FROM public.invoices i
  WHERE i.origin_ttl > 0 
    AND i.settlement_status = 'SETTLED'
  GROUP BY 1
)
SELECT 
  day,
  volume_by_market_maker,
  fee_by_market_maker,
  -- APY calculation as (fee/volume) * 365 based on daily fee to MM
  (fee_by_market_maker / volume_by_market_maker) * 365 as apy
FROM raw;

-- Metric 7: 

-- **KR1_Clearing_Volume**: Clearing volume (settlement + netted)
    -- settlement: sum of all settled amount in hub_intent table
    -- netted: double cehck for settlement only
    -- ttl is zero that is a netted order | Others are filled intent order or solver based order
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CASE WHEN i.origin_ttl = 0 THEN 'netted' ELSE 'filled' END as intent_type,
    SUM(i.settled_amount) as volume
  FROM public.intents i
  WHERE i.settlement_status = 'SETTLED'
  GROUP BY 1,2


-- - **KR2_Netting_Rate**
--   - Category: OKRs
--   - Description: Percentage of transactions netted within 24 hours
--   - Target: KR2: 60% netted within 24h
--   - Property: by chains; by assets
-- SQL-> hub_intent_table -> settled / dispatched and no hub invoice for the itent id
-- Settled event is emmitted 
-- when hub intent is settled that message is sent to destination, settled on destination then received the money
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS netted_count,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) AS count_of_intents_within_1h,
    -- Calculating the percentage of invoices netted within 24 hour
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS netting_rate_percentage
FROM public.intents i
WHERE i.settlement_status = 'SETTLED' 
  AND i.origin_ttl = 0
GROUP BY 1;


-- Metric 8: **KR3_Total_rebalancing_fee**: Total fee = Protocol fee + Discount
-- CAL; from intents table, get discount and get protocol from the token tables that are then matched to settlement tokens

WITH raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,    
    -- discount
    SUM(i.origin_amount - i.settlement_amount::numeric) AS discount,
    -- [ ] TODO ??? protocol fee: check if the fee_amounts in token table is perct
    (i.origin_amount / 100 * jsonb_array_elements_text(fee_amounts::jsonb)::numeric) AS fee_amount
FROM public.intents i
LEFT JOIN public.tokens t ON i.settlement_asset = t.id
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1
)

SELECT 
    day,
    discount,
    protocol_fee,
    -- rebalancing_fee: Total fee = Protocol fee + Discount
    (protocol_fee + discount) AS rebalancing_fee
FROM raw;





--Metric 9: **Settlement_Rate**: Percentage of transactions settled within 6 hours
-- 9.1.6 hrs
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_intent_count,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 21600 THEN i.id
    END) AS count_of_intents_within_6h,
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 21600 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS settlement_rate_percentage
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;

-- 9.2. 24 hrs
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_intent_count,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) AS count_of_intents_within_24h,
    -- Calculating the percentage of invoices netted within 24 hour
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS settlement_rate_percentage
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;





-- Metric 10: **Total_Protocol_Revenue**: Total revenue generated by the protocol
    -- calcuation logic: from the token table get the fee_amounts for the token for origin_amount in intents table
    -- Alternative way to calculate:
    -- - origin_intents amount  by each token fee percent
    -- -- ??? verfiy epoch for us is 30 mins
    -- -- EpochLengthUpdated

WITH raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    -- [ ] TODO ??? protocol fee: check if the fee_amounts in token table is perct
    SUM(i.origin_amount / 100 * jsonb_array_elements_text(fee_amounts::jsonb)::numeric) AS fee_amount
FROM public.intents i
LEFT JOIN public.tokens t ON i.settlement_asset = t.id
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1
)
SELECT 
    day,
    fee_amount AS protocol_fee
FROM raw





-- Metric 11: **Settlement_Time**: Average time taken to settle the intent
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    AVG(i.settlement_timestamp - i.origin_timestamp) AS avg_settlement_time
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;

-- Metric 12: **Wallet_retention_rate**: Measures the frequency and consistency of user activity associated with specific wallet addresses over time
-- user 1: origin_intent -> initiator
-- Other can be added in similar way

-- weekly retention by origin wallet and its start date by first intent id
WITH user_activity AS (
  SELECT 
    origin_wallet,
    DATE_TRUNC('week', to_timestamp(origin_timestamp)) AS week
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


-- Metric 13: **Average_intent_size**- Average value per intent on daily basis
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    AVG(i.origin_amount) AS avg_intent_size
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;

-- Metric 14: **Number of intents**: Number of intents on daily basis
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_intents
FROM public.intents i
GROUP BY 1;
SELECT 




-- rebalancing:

-- top players -> wallet address identifications
    --users
        -- Exchanges
    -- activity 
        -- volume
    -- if they use the same path as what we support
    -- cost for rebalancing
    -- how much will they save with us
