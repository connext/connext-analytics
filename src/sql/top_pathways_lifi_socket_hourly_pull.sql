-- use vatalik adress: 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045

WITH
  chain_transfers AS (
  SELECT
    'zkSync' AS origin,
    'Ethereum Mainnet' AS destination,
    324 AS origin_chain_id,
    1 AS destination_chain_id
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Arbitrum One',
    1,
    42161
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Base Mainnet',
    1,
    8453
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Linea Mainnet',
    1,
    59144
  UNION ALL
  SELECT
    'Arbitrum One',
    'Ethereum Mainnet',
    42161,
    1
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Arbitrum One',
    1,
    42161
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Optimism Mainnet',
    1,
    10
    -- UNION ALL
    -- SELECT 'Arbitrum One', 'Ethereum Mainnet', 42161, 1
    -- UNION ALL
    -- SELECT 'Linea Mainnet', 'Ethereum Mainnet', 59144, 1
    -- UNION ALL
    -- SELECT 'Base Mainnet', 'Ethereum Mainnet', 8453, 1
    ),
  tokens AS (
  SELECT
    'ETH' AS token
  UNION ALL
  SELECT
    'WETH'
  UNION ALL
  SELECT
    'USDC'
  UNION ALL
  SELECT
    'USDT' ),
  amounts AS (
  SELECT
    1000 AS amount
  UNION ALL
  SELECT
    10000
  UNION ALL
  SELECT
    100000 ),
aggregators AS (
    SELECT
        'lifi' AS aggregator
    UNION ALL
    SELECT 'socket'      
),

  final_raw AS (
  SELECT
    DATE_TRUNC(CURRENT_TIMESTAMP(), HOUR) AS hour,
    ct.origin,
    ct.destination,
    ct.origin_chain_id,
    ct.destination_chain_id,
    t.token AS asset,
    ag.aggregator,
    a.amount
  FROM
    chain_transfers ct
  CROSS JOIN
    tokens t
  CROSS JOIN
    amounts a 
  CROSS JOIN
    aggregators ag),
  daily_price AS (
  SELECT
    p.date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
    WHERE 
        date = (SELECT max(date) FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`)
        AND p.symbol IN (SELECT token FROM tokens)
  GROUP BY
    1,
    2 )

, final AS (
-- bring USD latest price in and send the request out to LIFI and Socket
SELECT
    f.hour,
    f.origin_chain_id AS fromChainId,
    f.destination_chain_id AS toChainId,
    CEIL(f.amount / dp.price) * pow(10, ft.decimals) AS fromAmount,
    f.asset,
    ft.address AS fromTokenAddress,
    ft.decimals AS from_decimal,
    tt.address AS toTokenAddress,
    tt.decimals AS to_decimal,
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045" AS fromAddress,
    f.aggregator,
    dp.price,
    f.amount / dp.price AS amount_value
FROM
  final_raw f
LEFT JOIN `mainnet-bigq.stage.source_lifi__tokens`  ft ON (f.asset = ft.symbol AND f.origin_chain_id = ft.chainId)
LEFT JOIN `mainnet-bigq.stage.source_lifi__tokens`  tt ON (f.asset = tt.symbol AND f.destination_chain_id = tt.chainId)
LEFT JOIN daily_price dp
ON
  (CASE WHEN f.asset = 'ETH' THEN 'WETH' ELSE f.asset END) = dp.asset
)

SELECT * FROM final
WHERE aggregator = "{{aggregator}}"