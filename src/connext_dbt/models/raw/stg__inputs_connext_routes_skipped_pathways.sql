
WITH
  connext_tokens AS (
  SELECT
    DISTINCT ct.token_address,
    ct.token_name,
    ct.is_xerc20
  FROM
    `mainnet-bigq.stage.connext_tokens` ct )
SELECT
  DISTINCT p.fromChainId,
  ct_from.token_name AS from_token_name,
  p.fromTokenAddress,
  p.toChainId,
  ct_to.token_name AS to_token_name,
  p.toTokenAddress
FROM
  `mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways` wp
RIGHT JOIN
  `mainnet-bigq.stage.source_lifi__pathways` p
ON
  ( wp.fromChainId = CAST(p.fromChainId AS FLOAT64)
    AND wp.fromTokenAddress = p.fromTokenAddress
    AND wp.toChainId= CAST(p.toChainId AS FLOAT64)
    AND wp.toTokenAddress = p.toTokenAddress )
LEFT JOIN
  connext_tokens ct_from
ON
  p.fromTokenAddress= ct_from.token_address
LEFT JOIN
  connext_tokens ct_to
ON
  p.fromTokenAddress= ct_to.token_address
WHERE
  wp.fromChainId IS NULL