-- Metric 6: 
-- **APY**: Annual Percentage Yield for MM
-- confirm the fee earned by market makers with Preetham
WITH raw AS (
  SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    SUM(i.origin_amount::float) as volume_by_market_maker,
    -- Assuming the fee is the difference between the origin amount and settled amount
    -- settlement_amount: better option with settlement_status = 'SETTLED'
    SUM(ABS(i.origin_amount::float - i.hub_invoice_amount::float)) as fee_by_market_maker
  FROM public.invoices i
  WHERE i.origin_ttl > 0 
    AND i.hub_status = 'DISPATCHED'
  GROUP BY 1
)
SELECT 
  day,
  volume_by_market_maker,
  fee_by_market_maker,
  -- APY calculation as (fee/volume) * 365 based on daily fee to MM
  (fee_by_market_maker / volume_by_market_maker) * 365 as apy
FROM raw;