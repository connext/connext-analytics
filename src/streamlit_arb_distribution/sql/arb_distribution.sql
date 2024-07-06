  -- batch data for market maker with a max timeframe of 3hr
WITH
  chains_meta AS (
  SELECT
    DISTINCT domainid,
    chain_name AS chain
  FROM
    `mainnet-bigq.raw.stg__ninja_connext_prod_chains_tokens_clean` ct ),
  assets AS (
  SELECT
    DISTINCT da.domain,
    da.canonical_id,
    da.adopted_decimal AS decimal
  FROM
    `mainnet-bigq.public.assets` da ),
  tokens_meta AS (
  SELECT
    DISTINCT LOWER(token_address) AS local,
    token_name AS asset
  FROM
    `mainnet-bigq.stage.connext_tokens` ct ),
  weth_txs AS (
  SELECT
    DISTINCT 
    wt.hash,
    wt.user_address,
    wt.date,
    CASE
      WHEN wt.origin_chain = 'bnb' THEN 'Binance Smart Chain'
      WHEN wt.origin_chain = 'base' THEN 'Base Mainnet'
      WHEN wt.origin_chain = 'linea' THEN 'Linea Mainnet'
      WHEN wt.origin_chain = 'gnosis' THEN 'xDAI Chain'
      WHEN wt.origin_chain = 'polygon' THEN 'Polygon Mainnet'
      WHEN wt.origin_chain = 'ethereum' THEN 'Ethereum Mainnet'
      WHEN wt.origin_chain = 'optimism' THEN 'Optimistic Ethereum'
      ELSE wt.origin_chain
  END
    AS origin_chain,
    wt.destination_chain,
    wt.volume_eth
  FROM
    `mainnet-bigq.dune.source_arb_weth_deposit_transactions` wt ),
  tx AS (
  SELECT
    transfer_id,
    xcall_transaction_hash,
    TIMESTAMP_SECONDS (t.xcall_timestamp) AS xcall_timestamp,
    CASE
      WHEN t.origin_domain = '6648936' THEN 'Ethereum Mainnet'
      WHEN t.origin_domain = '1869640809' THEN 'Optimistic Ethereum'
      WHEN t.origin_domain = '6450786' THEN 'Binance Smart Chain Mainnet'
      WHEN t.origin_domain = '6778479' THEN 'xDAI Chain'
      WHEN t.origin_domain = '1886350457' THEN 'Matic Mainnet'
      WHEN t.origin_domain = '1634886255' THEN 'Arbitrum One'
      WHEN t.origin_domain = '1818848877' THEN 'Linea Mainnet'
      WHEN t.origin_domain = '1835365481' THEN 'Metis Andromeda Mainnet'
      WHEN t.origin_domain = '1650553709' THEN 'Base Mainnet'
      WHEN t.origin_domain = '1836016741'THEN 'Mode Mainnet'
      ELSE t.origin_domain
  END
    AS origin_chain_name,
    CASE
      WHEN t.destination_domain = '6648936' THEN 'Ethereum Mainnet'
      WHEN t.destination_domain = '1869640809' THEN 'Optimistic Ethereum'
      WHEN t.destination_domain = '6450786' THEN 'Binance Smart Chain Mainnet'
      WHEN t.destination_domain = '6778479' THEN 'xDAI Chain'
      WHEN t.destination_domain = '1886350457' THEN 'Matic Mainnet'
      WHEN t.destination_domain = '1634886255' THEN 'Arbitrum One'
      WHEN t.destination_domain = '1818848877' THEN 'Linea Mainnet'
      WHEN t.destination_domain = '1835365481' THEN 'Metis Andromeda Mainnet'
      WHEN t.destination_domain = '1650553709' THEN 'Base Mainnet'
      WHEN t.destination_domain = '1836016741'THEN 'Mode Mainnet'
      ELSE t.destination_domain
  END
    AS destination_chain_name,
    t.xcall_caller,
    t.origin_sender,
    t.to,
    origin_transacting_asset,
    -- for testing
    destination_transacting_asset AS destination_transacting_asset,
    oa.domain AS origin_domain,
    CAST(oa.decimal AS INT64) AS origin_decimal,
    ( CAST(destination_transacting_amount AS FLOAT64) / POW (10, COALESCE(CAST(da.decimal AS INT64), 0)) ) AS destination_transacting_amount,
    -- Gas fee
    t.origin_domain AS gas_fee_domain,
    CASE
      WHEN t.origin_domain = '1869640809' THEN 'WETH' -- Optimistic WETHereum
      WHEN t.origin_domain = '1835365481' THEN 'METIS' -- Metis Andromeda Mainnet
      WHEN t.origin_domain = '1886350457' THEN 'MATIC' -- Matic Mainnet (Polygon)
      WHEN t.origin_domain = '1836016741' THEN 'WETH' -- Mode Mainnet
      WHEN t.origin_domain = '6450786' THEN 'BNB' -- Binance Smart Chain Mainnet
      WHEN t.origin_domain = '2020368761' THEN 'WETH' -- XLayer Mainnet
      WHEN t.origin_domain = '6778479' THEN 'DAI' -- Gnosis(xDIA is pegged to DAI)
      WHEN t.origin_domain = '1634886255' THEN 'WETH' -- Arbitrum One
      WHEN t.origin_domain = '6648936' THEN 'WETH' -- WETHereum Mainnet
      WHEN t.origin_domain = '1818848877' THEN 'WETH' -- Linea Mainnet
      WHEN t.origin_domain = '1650553709' THEN 'WETH' -- Base Mainnet
      ELSE 'WETH'
  END
    AS gas_fee_token_symbol,
    -- gas fee symbol: WETH, MATIC, METIS, BNB have gas fee decimal: 18
    CAST(t.xcall_gas_price AS FLOAT64) * CAST(t.xcall_gas_limit AS FLOAT64) / POW (10, 18) AS gas_fee_amount,
    -- Relay Fee
    -- 1st: Native Token
    REGEXP_EXTRACT(TO_JSON_STRING(t.relayer_fees), r'"([^"]+)":') AS relay_fee_token_1,
    CAST( REGEXP_EXTRACT(TO_JSON_STRING(t.relayer_fees), r':"([^"]+)"') AS FLOAT64 ) AS relay_fee_1,
    -- 2nd
    -- Extract the Relay token second address if it exists
    CASE
      WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t.relayer_fees), r'"([^"]+)":')) > 1 THEN REGEXP_EXTRACT(SUBSTR(TO_JSON_STRING(t.relayer_fees), REGEXP_INSTR(TO_JSON_STRING(t.relayer_fees), r'"[^"]+":".+?",') + 1), r'"([^"]+)":')
      ELSE NULL
  END
    AS relay_fee_token_2,
    -- Extract the value corresponding to the second address or set to zero if only one key-value pair exists
    CASE
      WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t.relayer_fees), r'"([^"]+)":')) > 1 THEN CAST( REGEXP_EXTRACT(SUBSTR(TO_JSON_STRING(t.relayer_fees), REGEXP_INSTR(TO_JSON_STRING(t.relayer_fees), r'"[^"]+":".+?",') + 1), r':"([^"]+)"' ) AS FLOAT64 )
      ELSE NULL
  END
    AS relay_fee_2,
    -- Router fee: `origin_transacting_amount * 5bps`
    CAST(t.origin_transacting_amount AS FLOAT64) * 0.0005 / POW (10, COALESCE(CAST(oa.decimal AS INT64), 18)) AS router_fee_amount,
    -- AMM fees
    CASE
      WHEN origin_domain IN ( '1869640809', '1835365481', '1886350457', '1836016741', '2020368761', '6778479', '1634886255', '1818848877', '1650553709' ) THEN CAST(t.origin_transacting_amount AS FLOAT64) * 0.0013 / POW (10, COALESCE(CAST(oa.decimal AS INT64), 18))
    -- BNB and ETH are L1 rest of above are l2
      WHEN origin_domain IN ('6450786',
      '6648936') THEN CAST(t.origin_transacting_amount AS FLOAT64) * 0.0008 / POW (10, COALESCE(CAST(oa.decimal AS INT64), 18))
      ELSE 0
  END
    AS amm_fees
  FROM
    `public.transfers` t
    --dest
  LEFT JOIN
    assets da
  ON
    ( t.canonical_id = da.canonical_id
      AND t.destination_domain = da.domain )
    -- origin
  LEFT JOIN
    assets oa
  ON
    ( t.canonical_id = oa.canonical_id
      AND t.origin_domain = oa.domain )
  WHERE
    -- FILTER FOR ARBITRUM DESTINATION ONLY
    t.status IN ('CompletedSlow',
      'CompletedFast')
    AND LOWER(t.destination_transacting_asset) = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
    AND t.destination_domain = "1634886255"
    AND t.xcall_timestamp >= 1718668800),
  clean_tx AS (
  SELECT
    t.transfer_id,
    t.xcall_transaction_hash,
    t.to,
    t.xcall_timestamp,
    t.origin_chain_name,
    t.destination_chain_name,
    t.xcall_caller,
    t.origin_sender,
    -- gas
    t.gas_fee_token_symbol AS gas_fee_asset,
    t.gas_fee_amount,
    -- relay fee -> if token is base address then its gas fee token else its the token from origin asset
    -- relay fee 1
    CASE
      WHEN (t.relay_fee_token_1 = t.origin_transacting_asset AND t.relay_fee_token_1 != "0x0000000000000000000000000000000000000000") THEN r1_tm.asset
      ELSE t.gas_fee_token_symbol
  END
    AS relay_fee_asset_1,
    CASE
      WHEN (t.relay_fee_token_1 = t.origin_transacting_asset AND t.relay_fee_token_1 != "0x0000000000000000000000000000000000000000") THEN t.relay_fee_1 / POW (10, COALESCE(CAST(t.origin_decimal AS INT64), 18))
      ELSE t.relay_fee_1 / POW (10, 18)
  END
    AS relay_fee_1,
    -- relay fee 2
    CASE
      WHEN t.relay_fee_token_2 IS NULL THEN NULL
      WHEN (t.relay_fee_token_2 = t.origin_transacting_asset
      AND t.relay_fee_token_2 != "0x0000000000000000000000000000000000000000") THEN r2_tm.asset
      ELSE t.gas_fee_token_symbol
  END
    AS relay_fee_asset_2,
    CASE
      WHEN t.relay_fee_2 IS NULL THEN NULL
      WHEN (t.relay_fee_token_2 = t.origin_transacting_asset
      AND t.relay_fee_token_2 != "0x0000000000000000000000000000000000000000") THEN t.relay_fee_2 / POW (10, COALESCE(CAST(t.origin_decimal AS INT64), 18))
      ELSE t.relay_fee_2 / POW (10, 18)
  END
    AS relay_fee_2,
    -- AMM fee
    COALESCE(amm_tm.asset, t.origin_transacting_asset) AS amm_fee_asset,
    t.amm_fees AS amm_fee,
    -- destination vol
    COALESCE(vol_tm.asset, t.destination_transacting_asset) AS destination_asset,
    t.destination_transacting_amount AS destination_amount,
    -- router fee
    COALESCE(router_tm.asset, t.origin_transacting_asset) AS router_fee_asset,
    t.router_fee_amount
  FROM
    tx t
    -- relay fee token
  LEFT JOIN
    tokens_meta r1_tm
  ON
    (LOWER(t.relay_fee_token_1) = r1_tm.local)
  LEFT JOIN
    tokens_meta r2_tm
  ON
    (LOWER(t.relay_fee_token_2) = r2_tm.local)
    -- amm fee token
  LEFT JOIN
    tokens_meta amm_tm
  ON
    (LOWER(t.origin_transacting_asset) = amm_tm.local)
    -- vol token
  LEFT JOIN
    tokens_meta vol_tm
  ON
    (LOWER(t.destination_transacting_asset) = vol_tm.local)
    -- router fee token
  LEFT JOIN
    tokens_meta router_tm
  ON
    (LOWER(t.origin_transacting_asset) = router_tm.local) ),
  clean_final AS (
  SELECT
    ct.transfer_id,
    ct.xcall_transaction_hash,
    ct.to,
    ct.xcall_timestamp AS date,
    ct.xcall_caller AS xcall_caller,
    ct.origin_sender AS origin_sender,
    ct.origin_chain_name AS origin_chain,
    ct.destination_chain_name AS destination_chain,
    -- price groups for gass_fee, relay_fee(1,2), amm_fee, router_fee, destination_vol
    -- 1. gas_fee
    CASE
      WHEN ct.gas_fee_asset = 'ETH' THEN 'WETH'
      WHEN ct.gas_fee_asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.gas_fee_asset, 'next') THEN REGEXP_REPLACE (ct.gas_fee_asset, '^next', '')
      WHEN ct.gas_fee_asset = 'alUSD' THEN 'USDT'
      WHEN ct.gas_fee_asset = 'nextALUSD' THEN 'USDT'
      WHEN ct.gas_fee_asset = 'instETH' THEN 'WETH'
      WHEN ct.gas_fee_asset = 'ezETH' THEN 'WETH'
      WHEN ct.gas_fee_asset = 'alETH' THEN 'WETH'
      WHEN ct.gas_fee_asset = 'nextalETH' THEN 'WETH'
      ELSE ct.gas_fee_asset
  END
    AS gas_fee_price_group,
    -- 2. relay_fee(1,2)
    CASE
      WHEN ct.relay_fee_asset_1 = 'ETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_1 = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.relay_fee_asset_1, 'next') THEN REGEXP_REPLACE (ct.relay_fee_asset_1, '^next', '')
      WHEN ct.relay_fee_asset_1 = 'alUSD' THEN 'USDT'
      WHEN ct.relay_fee_asset_1 = 'nextALUSD' THEN 'USDT'
      WHEN ct.relay_fee_asset_1 = 'instETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_1 = 'ezETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_1 = 'alETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_1 = 'nextalETH' THEN 'WETH'
      ELSE ct.relay_fee_asset_1
  END
    AS relay_fee_1_price_group,
    -- 3. relay_fee(2)
    CASE
      WHEN ct.relay_fee_asset_2 = 'ETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_2 = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.relay_fee_asset_2, 'next') THEN REGEXP_REPLACE (ct.relay_fee_asset_2, '^next', '')
      WHEN ct.relay_fee_asset_2 = 'alUSD' THEN 'USDT'
      WHEN ct.relay_fee_asset_2 = 'nextALUSD' THEN 'USDT'
      WHEN ct.relay_fee_asset_2 = 'instETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_2 = 'ezETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_2 = 'alETH' THEN 'WETH'
      WHEN ct.relay_fee_asset_2 = 'nextalETH' THEN 'WETH'
      ELSE ct.relay_fee_asset_2
  END
    AS relay_fee_2_price_group,
    -- 4. amm_fee
    CASE
      WHEN ct.amm_fee_asset = 'ETH' THEN 'WETH'
      WHEN ct.amm_fee_asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.amm_fee_asset, 'next') THEN REGEXP_REPLACE (ct.amm_fee_asset, '^next', '')
      WHEN ct.amm_fee_asset = 'alUSD' THEN 'USDT'
      WHEN ct.amm_fee_asset = 'nextALUSD' THEN 'USDT'
      WHEN ct.amm_fee_asset = 'instETH' THEN 'WETH'
      WHEN ct.amm_fee_asset = 'ezETH' THEN 'WETH'
      WHEN ct.amm_fee_asset = 'alETH' THEN 'WETH'
      WHEN ct.amm_fee_asset = 'nextalETH' THEN 'WETH'
      ELSE ct.amm_fee_asset
  END
    AS amm_fee_price_group,
    -- 5. router_fee
    CASE
      WHEN ct.router_fee_asset = 'ETH' THEN 'WETH'
      WHEN ct.router_fee_asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.router_fee_asset, 'next') THEN REGEXP_REPLACE (ct.router_fee_asset, '^next', '')
      WHEN ct.router_fee_asset = 'alUSD' THEN 'USDT'
      WHEN ct.router_fee_asset = 'nextALUSD' THEN 'USDT'
      WHEN ct.router_fee_asset = 'instETH' THEN 'WETH'
      WHEN ct.router_fee_asset = 'ezETH' THEN 'WETH'
      WHEN ct.router_fee_asset = 'alETH' THEN 'WETH'
      WHEN ct.router_fee_asset = 'nextalETH' THEN 'WETH'
      ELSE ct.router_fee_asset
  END
    AS router_fee_price_group,
    -- 6. destination_vol
    CASE
      WHEN ct.destination_asset = 'ETH' THEN 'WETH'
      WHEN ct.destination_asset = 'NEXT' THEN 'NEXT'
      WHEN STARTS_WITH (ct.destination_asset, 'next') THEN REGEXP_REPLACE (ct.destination_asset, '^next', '')
      WHEN ct.destination_asset = 'alUSD' THEN 'USDT'
      WHEN ct.destination_asset = 'nextALUSD' THEN 'USDT'
      WHEN ct.destination_asset = 'instETH' THEN 'WETH'
      WHEN ct.destination_asset = 'ezETH' THEN 'WETH'
      WHEN ct.destination_asset = 'alETH' THEN 'WETH'
      WHEN ct.destination_asset = 'nextalETH' THEN 'WETH'
      ELSE ct.destination_asset
  END
    AS destination_price_group,
    -- assets
    ct.gas_fee_asset AS gas_fee_asset,
    ct.relay_fee_asset_1 AS relay_fee_asset_1,
    ct.relay_fee_asset_2 AS relay_fee_asset_2,
    ct.amm_fee_asset AS amm_fee_asset,
    ct.router_fee_asset AS router_fee_asset,
    ct.destination_asset AS destination_asset,
    -- amounts
    ct.gas_fee_amount AS gas_fee_amount,
    ct.relay_fee_1 AS relay_fee_1_amount,
    ct.relay_fee_2 AS relay_fee_2_amount,
    ct.amm_fee AS amm_fee_amount,
    ct.router_fee_amount AS router_fee_amount,
    ct.destination_amount AS destination_amount
  FROM
    clean_tx ct
    -- filter for WETH inflows Only
  WHERE
    ct.destination_asset= 'WETH' ),
  -- adding daily pricing to final
  daily_price AS (
  SELECT
    DATE_TRUNC (CAST(p.date AS TIMESTAMP), HOUR) AS date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
  WHERE
    CAST(p.date AS TIMESTAMP) >= "2024-06-18"
  GROUP BY
    1,
    2 ),
  usd_data AS (
  SELECT
    cf.transfer_id,
    cf.xcall_transaction_hash,
    cf.date,
    cf.xcall_caller,
    cf.origin_sender,
    cf.to,
    cf.origin_chain,
    cf.destination_chain,
    -- assets
    cf.gas_fee_asset,
    cf.relay_fee_asset_1,
    cf.relay_fee_asset_2,
    cf.amm_fee_asset,
    cf.router_fee_asset,
    cf.destination_asset,
    -- amounts
    cf.gas_fee_amount,
    cf.relay_fee_1_amount,
    cf.relay_fee_2_amount,
    cf.amm_fee_amount,
    cf.router_fee_amount,
    cf.destination_amount,
    -- usd amounts
    cf.gas_fee_amount * gas_dp.price AS usd_gas_fee_amount,
    cf.relay_fee_1_amount * relay_1_dp.price AS usd_relay_fee_1_amount,
    cf.relay_fee_2_amount * relay_2_dp.price AS usd_relay_fee_2_amount,
    cf.amm_fee_amount * amm_dp.price AS usd_amm_fee_amount,
    cf.router_fee_amount * router_dp.price AS usd_router_fee_amount,
    cf.destination_amount * destination_dp.price AS usd_destination_amount
  FROM
    clean_final cf
    -- gas fee
  LEFT JOIN
    daily_price gas_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = gas_dp.date
    AND cf.gas_fee_price_group = gas_dp.asset
    -- relay 1
  LEFT JOIN
    daily_price relay_1_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = relay_1_dp.date
    AND cf.relay_fee_1_price_group = relay_1_dp.asset
    -- relay 2
  LEFT JOIN
    daily_price relay_2_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = relay_2_dp.date
    AND cf.relay_fee_2_price_group = relay_2_dp.asset
    -- amm fee
  LEFT JOIN
    daily_price amm_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = amm_dp.date
    AND cf.amm_fee_price_group = amm_dp.asset
    -- router fee
  LEFT JOIN
    daily_price router_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = router_dp.date
    AND cf.router_fee_price_group = router_dp.asset
    -- destination vol
  LEFT JOIN
    daily_price destination_dp
  ON
    DATE_TRUNC (cf.date, HOUR) = destination_dp.date
    AND cf.destination_price_group = destination_dp.asset
  ORDER BY
    1,
    2,
    3,
    4 DESC )
SELECT
  wt.user_address,
  ud.*
FROM
  usd_data ud
LEFT JOIN
  weth_txs wt
ON
  (ud.xcall_transaction_hash = wt.hash
    AND ud.origin_chain = wt.origin_chain)