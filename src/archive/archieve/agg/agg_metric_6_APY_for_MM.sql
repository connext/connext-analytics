-- Metric 6: 
-- **APY**: Annual Percentage Yield for MM
-- confirm the fee earned by market makers with Preetham
WITH raw AS (
  SELECT 
    SUM(i.origin_amount::float) as volume_by_market_maker,
    -- Assuming the fee is the difference between the origin amount and settled amount
    -- settlement_amount: better option with settlement_status = 'SETTLED'
    SUM(ABS(i.origin_amount::float - i.hub_invoice_amount::float)) as fee_by_market_maker,
    -- fees paid by MM for the invoices
    SUM(i.origin_gas_limit::float * i.origin_gas_price::float) as fees_paid_by_mm
  FROM public.invoices i
  WHERE i.origin_ttl > 0 
    AND i.hub_status = 'DISPATCHED'
    AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
)
SELECT 
  volume_by_market_maker,
  -- APY calculation as (fee/volume) * 365 based on daily fee to MM
  ((fee_by_market_maker - fees_paid_by_mm) / volume_by_market_maker) * 365 * 100 as apy
FROM raw;