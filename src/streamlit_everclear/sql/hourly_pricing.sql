SELECT 
  p.symbol,
  CAST(p.date AS TIMESTAMP) AS date,
  AVG(p.average_price) AS price
FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`  p
WHERE p.symbol IN ('USDC', 'USDT', 'WETH', 'BNB')
GROUP BY 1,2