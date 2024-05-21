with relevant_usd_prices as (
SELECT  
 symbol AS token_name,
 CAST(date AS TIMESTAMP) AS timestamp,
 average_price AS price
FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`
WHERE symbol IN ('WETH', 'DAI', 'USDC', 'USDT')
),

debridge as (
  select TIMESTAMP_TRUNC(date, HOUR) as execute_day,
  UNIX_SECONDS(date) join_time,
CASE 
  WHEN from_chain_id = 1 THEN 'Ethereum Mainnet'
  WHEN from_chain_id = 42161 THEN 'Arbitrum One'
  WHEN from_chain_id = 137 THEN 'Matic Mainnet'
  WHEN from_chain_id = 10 THEN 'Optimistic Ethereum'
  WHEN from_chain_id = 8453 THEN 'Base Mainnet'
  ELSE CAST(from_chain_id AS STRING) END source_chain, 
upper(trim(from_actual_symbol)) source_token, 
from_actual_value / POW(10, from_actual_symbol_decimal)  source_amount, 
CASE 
  WHEN to_chain_id = 1 THEN 'Ethereum Mainnet'
  WHEN to_chain_id = 42161 THEN 'Arbitrum One'
  WHEN to_chain_id = 137 THEN 'Matic Mainnet'
  WHEN to_chain_id = 10 THEN 'Optimistic Ethereum'
  WHEN to_chain_id = 8453 THEN 'Base Mainnet'
  ELSE CAST(to_chain_id AS STRING) END target_chain,
  upper(trim(to_symbol)) target_token,
to_value / POW(10, to_symbol_decimal) target_amount

from mainnet-bigq.stage.stg_cln_de_bridge_explorer_transactions__dedup
where from_chain_id in(42161,137,10,8453,1) and to_chain_id in(42161,137,10,8453,1) 
and from_actual_symbol in('WETH', 'DAI', 'USDC', 'USDT') AND to_symbol in ('WETH', 'DAI', 'USDC', 'USDT') 
and lower(pre_swap_in_token_symbol) = 'nan' and lower(pre_swap_out_token_symbol) = 'nan'

order by 1 desc
),

intents_ as (select 
  d.execute_day, 
  d.source_chain,
  d.source_token, 
  d.source_amount, 
  d.target_chain, 
  d.target_token, 
  d.target_amount, 
  fp_t.price as target_token_price,
  (coalesce(d.target_amount,0) * coalesce(CAST(fp_t.price AS FLOAT64),0)) as destination_value_usd 
from debridge d left join relevant_usd_prices fp_s on d.source_token = fp_s.token_name and d.execute_day = fp_s.timestamp
  left join relevant_usd_prices fp_t on d.target_token = fp_t.token_name and d.execute_day = fp_t.timestamp),

-- SELECT * FROM intents_
-- check for missing price or zero value -> none so far in query
--  WHERE (destination_value_usd IS NULL) OR (destination_value_usd = 0)

inflow AS (
    SELECT
        execute_day AS date,
        source_token AS asset,
        source_chain AS chain,
        SUM(destination_value_usd) AS inflow
    FROM intents_
    where destination_value_usd > 0
    GROUP BY 1, 2, 3
),
outflow AS (
    SELECT
        execute_day AS date,
        target_token AS asset,
        target_chain AS chain,
        SUM(destination_value_usd) AS outflow
    FROM intents_
    where destination_value_usd > 0
    GROUP BY 1, 2, 3
),
daily_net_flow AS (
    SELECT
        COALESCE(i.date, o.date) AS date,
        COALESCE(i.chain, o.chain) AS chain,
        COALESCE(i.asset, o.asset) AS asset,
        COALESCE(i.inflow, 0) AS inflow_usd,
        COALESCE(o.outflow, 0) AS outflow_usd,
        COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0) AS net_amount_usd,
        abs(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) AS abs_net_amount_usd,
        COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0) total_volume_usd,
        1 - (ABS(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) / (COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0))) AS balance_ratio_usd
    FROM inflow i
    FULL OUTER JOIN outflow o ON i.date = o.date AND i.chain = o.chain AND i.asset = o.asset
    where (COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)) > 0
)

SELECT * FROM daily_net_flow
ORDER BY 1 desc