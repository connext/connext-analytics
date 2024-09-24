SELECT 
  date,
  status,
  SUM(volume) AS volume,
  SUM(transfers) AS transfers,
  (SUM(volume) / SUM(SUM(volume)) OVER (PARTITION BY date)) * 100 AS perct_volume,
  (SUM(transfers) / SUM(SUM(transfers)) OVER (PARTITION BY date)) * 100 AS perct_transfers
FROM ${la_base_eth_flow}
WHERE status IN ('CompletedSlow', 'CompletedFast')
GROUP BY 1,2