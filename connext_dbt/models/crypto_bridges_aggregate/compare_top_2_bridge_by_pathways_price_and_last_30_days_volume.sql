-- Asset	Origin Chain	Destination Chain	Volume	Average tx size	# of txs	Share of 10k+ transfers
WITH volume AS (
  SELECT b.currency_symbol AS asset,
    b.source_chain_name AS origin_chain,
    b.destination_chain_name AS destination_chain,
    SUM(b.total_volume) AS Volume,
    AVG(b.avg_volume) AS avg_tx_size,
    SUM(b.total_txs) AS no_of_txs,
    SUM(b.volume_10k_txs) AS volume_of_10k_txs,
    100 * SUM(b.volume_10k_txs) / SUM(b.total_volume) AS Share_of_10k_transfers
  FROM `mainnet-bigq.crypto_bridges_aggregate.daily_agg_all_bridge` b -- date last 30 days
  WHERE b.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,
    2,
    3
)
SELECT COALESCE(volume.origin_chain, p.from_chain) AS from_chain,
  COALESCE(volume.destination_chain, p.to_chain) AS to_chain,
  COALESCE(volume.asset, p.token) AS token,
  p.input_amount,
  CAST(volume.Volume AS STRING) AS volume,
  -- cal bps 
  10000 * (1 - p.price / CAST(p.input_amount AS FLOAT64)) AS bps,
  p.bridge,
  p.price,
  p.best_bridge_rank

FROM volume
  FULL OUTER JOIN `mainnet-bigq.ad_hoc.stg_all_pathways__bridge_ranking` p 
  ON 
    LOWER(volume.origin_chain) = LOWER(p.from_chain)
    AND LOWER(volume.destination_chain) = LOWER(p.to_chain)
    AND LOWER(volume.asset) = LOWER(p.token)  
WHERE volume.Volume IS NOT NULL AND p.bridge IS NOT NULL