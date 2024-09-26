SELECT 
  p.symbol,
  DATE_TRUNC(CAST(p.date AS TIMESTAMP), DAY) AS date,
  AVG(p.average_price) AS price
FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`  p
WHERE p.symbol IN ('USDC', 'USDT', 'WETH')
AND DATE_TRUNC(CAST(p.date AS TIMESTAMP), DAY) >= TIMESTAMP('2024-09-17')
GROUP BY 1,2