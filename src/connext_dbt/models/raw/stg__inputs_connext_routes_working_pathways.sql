-- v2:
WITH raw AS (

SELECT DISTINCT

  r.route_fromchainid,
  r.route_fromtoken_symbol,
  r.route_fromtoken_address,

  r.route_tochainid,
  r.route_totoken_symbol,
  r.route_totoken_address,

  FORMAT("%.15f",
    POW(10, ROUND(LOG10( CAST(r.route_fromamount AS FLOAT64)))) 
  ) AS nearest_power_of_ten_amount

FROM `mainnet-bigq.stage.source_lifi__routes` r
)

-- stg inputs connext routes working pathways
-- Inputs needed for API call,
-- 'fromChainId', 'fromTokenAddress', 'fromAddress', 'toChainId', 'toTokenAddress', 'fromAmount'

SELECT DISTINCT
  rp.route_fromchainid AS fromChainId,
  rp.route_fromtoken_address AS fromTokenAddress,
  "0x32d222E1f6386B3dF7065d639870bE0ef76D3599" AS fromAddress,
  rp.route_tochainid AS toChainId,
  rp.route_totoken_address AS toTokenAddress,
  CAST(rp.nearest_power_of_ten_amount AS FLOAT64) AS fromAmount
FROM raw rp
WHERE rp.nearest_power_of_ten_amount IS NOT NULL




-- v1:

-- stg inputs connext routes working pathways
-- Inputs needed for API call,
-- 'fromChainId', 'fromTokenAddress', 'fromAddress', 'toChainId', 'toTokenAddress', 'fromAmount'
-- SELECT DISTINCT
--   rp.action_fromtoken_chainid AS fromChainId,
--   rp.action_fromtoken_address AS fromTokenAddress,
--   "0x32d222E1f6386B3dF7065d639870bE0ef76D3599" AS fromAddress,
--   rp.action_totoken_chainid AS toChainId,
--   rp.action_totoken_address AS toTokenAddress,
--   CAST(rp.nearest_power_of_ten_amount AS FLOAT64) AS fromAmount
-- FROM `mainnet-bigq.raw.source_routes__working_pathways_calculated` rp
-- WHERE rp.nearest_power_of_ten_amount IS NOT NULL