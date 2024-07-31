-- use vatalik adress: 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
WITH target_chain_ids AS (
  SELECT DISTINCT chain_id
  FROM (
      SELECT chain_id
      FROM -- connext chains
        UNNEST (
          [42161, 56, 1, 100, 10, 8453, 59144, 1088, 137, 324, 1101, 8453, 43114, 1088, 34443]
        ) chain_id
      UNION ALL
      SELECT chainid AS chain_id
      FROM `mainnet-bigq.raw.source_socket__chains`
      UNION ALL
      SELECT id AS chain_id
      FROM `mainnet-bigq.stage.source_lifi__chains`
    )
),
target_token_ids AS (
  SELECT symbol
  FROM UNNEST(['ETH', 'WETH', 'ezETH', 'WBTC', 'USDC', 'USDT']) symbol
),
tokens AS (
  SELECT DISTINCT chainId,
    coinKey,
    address,
    decimals
  FROM `mainnet-bigq.stage.source_lifi__tokens` tt
  WHERE chainId IN (
      42161,
      56,
      1,
      100,
      10,
      8453,
      59144,
      1088,
      137,
      324,
      1101,
      8453,
      43114,
      1088,
      34443
    ) -- chainId IN (42161)
    AND (
      (
        symbol = 'ETH'
        AND address IN ('0x0000000000000000000000000000000000000000')
      )
      OR (
        symbol = 'USDT'
        AND name IN ('USDT')
      )
      OR (
        symbol = 'DAI'
        AND name IN ('Dai Stablecoin', 'DAI Stablecoin')
      )
      OR (
        symbol = 'USDC'
        AND name IN ('USD Coin')
      )
      OR (
        symbol = 'WETH'
        AND name IN ('WETH', 'Wrapped ETH', 'Wrapped Ether')
      )
      OR (
        symbol = 'WBTC'
        AND name IN ('WBTC', 'Wrapped BTC')
      )
      OR (
        symbol = 'ezETH'
        AND name = 'Renzo Restaked ETH'
      )
    )
    AND logoURI IS NOT NULL
),
amounts AS (
  SELECT 1000 AS amount
  UNION ALL
  SELECT 10000
  UNION ALL
  SELECT 100000
),
chain_ids_token_symbols AS (
  SELECT DISTINCT from_chains.chain_id AS from_chain_id,
    to_chains.chain_id AS to_chain_id,
    target_token_ids.symbol,
    amounts.amount
  FROM target_chain_ids from_chains
    CROSS JOIN target_chain_ids to_chains
    CROSS JOIN target_token_ids
    CROSS JOIN amounts
  WHERE from_chains.chain_id != to_chains.chain_id
  ORDER BY 1,
    2
),
-- 2800 pair of from to token amounts
chains_tokens AS (
  SELECT DISTINCT cid.from_chain_id AS fromChainId,
    cid.to_chain_id AS toChainId,
    cid.symbol,
    cid.amount,
    ft.address AS fromTokenAddress,
    tt.address AS toTokenAddress,
    ft.decimals AS from_decimals
  FROM chain_ids_token_symbols cid -- from side token -> some tokens are just missing on some chains
    INNER JOIN tokens ft ON (
      cid.from_chain_id = ft.chainId
      AND cid.symbol = ft.coinKey
    ) -- to side token
    INNER JOIN tokens tt ON (
      cid.to_chain_id = tt.chainId
      AND cid.symbol = tt.coinKey
    ) -- WHERE (tt.chainId IS NULL OR ft.chainId IS NULL OR ft.address IS NULL OR tt.address IS NULL)
),
daily_price AS (
  SELECT p.date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
  WHERE date = (
      SELECT max(date)
      FROM `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`
    )
    AND p.symbol IN (
      SELECT symbol
      FROM target_token_ids
    )
  GROUP BY 1,
    2
),
final AS (
  SELECT ct.fromChainId,
    ct.toChainId,
    ct.amount,
    ROUND(
      ct.amount / dp.price * pow(10, ct.from_decimals),
      0
    ) AS fromAmount,
    ct.fromTokenAddress,
    ct.toTokenAddress,
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045" AS fromAddress,
    aggregator,
    FROM chains_tokens ct
    CROSS JOIN UNNEST(["socket", "lifi"]) AS aggregator
    LEFT JOIN daily_price dp ON (
      CASE
        WHEN ct.symbol = 'ETH' THEN 'WETH'
        WHEN ct.symbol = 'ezETH' THEN 'WETH'
        WHEN ct.symbol = 'nETH' THEN 'WETH'
        ELSE ct.symbol
      END
    ) = dp.asset
)
SELECT both.*
FROM (
    -- LIFI chains and TOken support filters
    SELECT DISTINCT lp.*
    FROM final lp -- chain filter
      INNER JOIN `mainnet-bigq.stage.source_lifi__tools` lt ON lp.aggregator = "lifi"
      AND lp.fromChainId = lt.fromChainId
      AND lp.toChainId = lt.toChainId -- from token filter
      INNER JOIN `mainnet-bigq.stage.source_lifi__tokens` ft ON lp.fromChainId = ft.chainId
      AND lp.fromTokenAddress = ft.address
      AND lp.aggregator = "lifi" -- to token filter
      INNER JOIN `mainnet-bigq.stage.source_lifi__tokens` tt ON lp.toChainId = tt.chainId
      AND lp.toTokenAddress = tt.address
      AND lp.aggregator = "lifi" -- do for Socket
    UNION ALL
    SELECT DISTINCT lp.*
    FROM final lp -- from chain
      INNER JOIN `mainnet-bigq.raw.source_socket__chains` sfc ON lp.fromChainId = sfc.chainid
      AND lp.aggregator = "socket" -- to chain
      INNER JOIN `mainnet-bigq.raw.source_socket__chains` stc ON lp.toChainId = stc.chainid
      AND lp.aggregator = "socket" -- from token
      INNER JOIN `mainnet-bigq.raw.source_socket__tokens` sft ON lp.fromChainId = sft.chainId
      AND LOWER(lp.fromTokenAddress) = LOWER(sft.address)
      AND lp.aggregator = "socket" -- to token
      INNER JOIN `mainnet-bigq.raw.source_socket__tokens` stt ON lp.toChainId = stt.chainId
      AND LOWER(lp.toTokenAddress) = LOWER(stt.address)
      AND lp.aggregator = "socket"
  ) both
WHERE both.aggregator = "{{aggregator}}" -- WHERE both.aggregator = "lifi"