-- Asset	Origin Chain	Destination Chain	Volume	Average tx size	# of txs	Share of 10k+ transfers

SELECT 
  b.currency_symbol AS asset,
  b.source_chain_name AS origin_chain,
  b.destination_chain_name AS destination_chain,
  SUM(b.total_volume) AS Volume,
  AVG(b.avg_volume) AS avg_tx_size,
  SUM(b.total_txs) AS no_of_txs,
  SUM(tx_count_10k_txs) AS no_of_10k_txs,
  100 * SUM(tx_count_10k_txs) / SUM(b.total_txs) AS Share_of_10k_transfers

FROM `mainnet-bigq.crypto_bridges_aggregate.monthly_agg_all_bridge` b
WHERE destination_chain_name IS NOT NULL AND b.date >= '2024-06-01'
GROUP BY 1,2,3
ORDER BY 5 DESC