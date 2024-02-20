SELECT DISTINCT
  i AS aggregator,
  CAST(be.fromChainId AS INT64) AS fromChainId,
  from_c.name AS from_c_name,
  CAST(be.toChainId AS INT64) AS toChainId,
  to_c.name AS to_c_name
FROM
  `mainnet-bigq.stage.source_lifi__bridges_exchanges` be
LEFT JOIN
  `stage.source_lifi__chains` from_c
ON
  be.fromChainId = from_c.id
LEFT JOIN
  `stage.source_lifi__chains` to_c
ON
  be.toChainId = to_c.id
CROSS JOIN
  UNNEST(["socket", "lifi"]) AS i 
--WHERE (from_c.key IN ({{keys}}) ) OR (to_c.key IN ({{keys}}) )
