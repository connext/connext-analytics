WITH 
daily_transfer_volume AS (
  SELECT * FROM 
  --`mainnet-bigq.y42_connext_y42_dev.daily_transfer_volume_y42_view`
  --`mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_daily_transfer_volume`
  `mainnet-bigq.legacy.stg_daily_transfer_volume`

),
VolumeMetrics AS (
  SELECT
    status,
    router,
    asset,
    origin_chain,
    destination_chain,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN usd_volume ELSE 0 END) AS usd_volume_last_1_day,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN usd_volume ELSE 0 END) AS usd_volume_last_7_days,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN usd_volume ELSE 0 END) AS usd_volume_last_30_days,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN volume ELSE 0 END) AS volume_last_1_day,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN volume ELSE 0 END) AS volume_last_7_days,
    SUM(CASE WHEN DATE(transfer_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN volume ELSE 0 END) AS volume_last_30_days,
    MAX(transfer_date) AS last_transfer_date,
    COUNTIF(status = 'CompletedSlow') AS slow_txns
    FROM
    daily_transfer_volume
  GROUP BY 
    1,2,3,4,5
),
CompletedFastOriginVolume AS (
  SELECT
    router,
    asset,
    origin_chain as chain,
    SUM(volume_last_1_day) AS origin_fast_volume_1_day,
    SUM(volume_last_7_days) AS origin_fast_volume_7_days,
    SUM(volume_last_30_days) AS origin_fast_volume_30_days,
    SUM(usd_volume_last_1_day) AS origin_fast_usd_volume_1_day,
    SUM(usd_volume_last_7_days) AS origin_fast_usd_volume_7_days,
    SUM(usd_volume_last_30_days) AS origin_fast_usd_volume_30_days
  FROM VolumeMetrics vm
  WHERE status = 'CompletedFast'
  GROUP BY 1,2,3
),
CompletedFastDestVolume AS (
  SELECT
    router,
    asset,
    destination_chain as chain,
    SUM(usd_volume_last_1_day) AS destination_fast_usd_volume_1_day,
    SUM(usd_volume_last_7_days) AS destination_fast_usd_volume_7_days,
    SUM(usd_volume_last_30_days) AS destination_fast_usd_volume_30_days,
    SUM(volume_last_1_day) AS destination_fast_volume_1_day,
    SUM(volume_last_7_days) AS destination_fast_volume_7_days,
    SUM(volume_last_30_days) AS destination_fast_volume_30_days
  FROM VolumeMetrics vm
  WHERE status = 'CompletedFast'
  GROUP BY 1,2,3
),
OriginVolume AS (
  SELECT
    vm.router,
    vm.asset,
    vm.origin_chain AS chain,
    SUM(usd_volume_last_1_day) AS origin_usd_volume_last_1_day,
    SUM(usd_volume_last_7_days) AS origin_usd_volume_last_7_days,
    SUM(usd_volume_last_30_days) AS origin_usd_volume_last_30_days,
    SUM(volume_last_1_day) AS origin_volume_1_day,
    SUM(volume_last_7_days) AS origin_volume_7_days,
    SUM(volume_last_30_days) AS origin_volume_30_days
  FROM 
    VolumeMetrics vm 
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
  GROUP BY 1,2,3
),
DestinationVolume AS (
  SELECT
    vm.router,
    vm.asset,
    vm.destination_chain AS chain,
    SUM(usd_volume_last_1_day) AS destination_usd_volume_last_1_day,
    SUM(usd_volume_last_7_days) AS destination_usd_volume_last_7_days,
    SUM(usd_volume_last_30_days) AS destination_usd_volume_last_30_days,
    SUM(volume_last_1_day) AS destination_volume_1_day,
    SUM(volume_last_7_days) AS destination_volume_7_days,
    SUM(volume_last_30_days) AS destination_volume_30_days,
    MAX(last_transfer_date) AS destination_last_transfer_date
  FROM 
    VolumeMetrics vm 
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
  GROUP BY 1,2,3
),
CombinedVolumes AS (
  SELECT
    COALESCE(ov.router, dv.router) AS router,
    COALESCE(ov.asset, dv.asset) AS asset,
    COALESCE(ov.chain, dv.chain) AS chain,
    ov.origin_usd_volume_last_1_day,
    ov.origin_usd_volume_last_7_days,
    ov.origin_usd_volume_last_30_days,
--    ov.origin_fast_volume_1_day,
--    ov.origin_fast_volume_7_days,
--    ov.origin_fast_volume_30_days,
    dv.destination_usd_volume_last_1_day,
    dv.destination_usd_volume_last_7_days,
    dv.destination_usd_volume_last_30_days,
--    dv.destination_fast_volume_1_day,
--    dv.destination_fast_volume_7_days,
--    dv.destination_fast_volume_30_days
  FROM OriginVolume ov
  FULL OUTER JOIN DestinationVolume dv ON ov.router = dv.router AND ov.asset = dv.asset AND ov.chain = dv.chain
),
CombinedView AS (
  SELECT 
  vm.router as router,
  vm.asset as asset,
  vm.origin_chain AS chain,
  SUM(ov.origin_usd_volume_last_1_day) AS ov1d,
  SUM(ov.origin_usd_volume_last_7_days) AS ov7d,
  SUM(ov.origin_usd_volume_last_30_days) AS ov30d,
--  SUM(ov.origin_fast_volume_1_day) AS ofv1d, 
--  SUM(ov.origin_fast_volume_7_days) AS ofv7d,
--  SUM(ov.origin_fast_volume_30_days) AS ofv30d,
  SUM(dv.destination_usd_volume_last_1_day) AS dv1d,
  SUM(dv.destination_usd_volume_last_7_days) AS dv7d,
  SUM(dv.destination_usd_volume_last_30_days) AS dv30d,
--  SUM(dv.destination_fast_volume_1_day) AS dfv1d,
--  SUM(dv.destination_fast_volume_7_days) AS dfv7d,
--  SUM(dv.destination_fast_volume_30_days) AS dfv30d,
  MAX(vm.last_transfer_date) as last_txn_date,
  SUM(vm.slow_txns) as slow_tns
FROM
  VolumeMetrics vm
  FULL OUTER JOIN OriginVolume ov ON vm.router = ov.router AND vm.asset = ov.asset AND vm.origin_chain = ov.chain 
  FULL OUTER JOIN DestinationVolume dv ON vm.router = dv.router AND vm.asset = dv.asset AND vm.origin_chain = dv.chain 
GROUP BY
  1,2,3
),
Combinations AS (
  SELECT DISTINCT destination_chain as chain, asset, router from VolumeMetrics
),
Metrics AS (
  SELECT 
    router,
    asset,
    destination_chain as chain,
    MAX(vm.last_transfer_date) as last_txn_date,
    SUM(vm.slow_txns) as slow_tns
  FROM VolumeMetrics vm
  GROUP BY 1,2,3
),
GroupedMetrics AS (
SELECT 
  cmbns.chain as chain_domain,
  cmbns.router as router_address,
  cmbns.asset as asset_address,
  ov.origin_usd_volume_last_1_day,
  ov.origin_usd_volume_last_7_days,
  ov.origin_usd_volume_last_30_days,
  ov.origin_volume_1_day,
  ov.origin_volume_7_days,
  ov.origin_volume_30_days,
  cfov.origin_fast_volume_1_day,
  cfov.origin_fast_volume_7_days,
  cfov.origin_fast_volume_30_days,
  cfov.origin_fast_usd_volume_1_day,
  cfov.origin_fast_usd_volume_7_days,
  cfov.origin_fast_usd_volume_30_days,
  dv.destination_usd_volume_last_1_day,
  dv.destination_usd_volume_last_7_days,
  dv.destination_usd_volume_last_30_days,
  dv.destination_volume_1_day,
  dv.destination_volume_7_days,
  dv.destination_volume_30_days,
  cfdv.destination_fast_usd_volume_1_day,
  cfdv.destination_fast_usd_volume_7_days,
  cfdv.destination_fast_usd_volume_30_days,
  cfdv.destination_fast_volume_1_day,
  cfdv.destination_fast_volume_7_days,
  cfdv.destination_fast_volume_30_days,
  last_txn_date,
  slow_tns
from 
  Combinations cmbns 
  LEFT JOIN Metrics mtr ON cmbns.chain = mtr.chain AND cmbns.asset = mtr.asset AND cmbns.router = mtr.router
  LEFT JOIN DestinationVolume dv ON cmbns.chain = dv.chain AND cmbns.asset = dv.asset AND cmbns.router = dv.router
  LEFT JOIN OriginVolume ov ON cmbns.chain = ov.chain AND cmbns.asset = ov.asset AND cmbns.router = ov.router
  LEFT JOIN CompletedFastOriginVolume cfov ON cmbns.chain = cfov.chain AND cmbns.asset = cfov.asset AND cmbns.router = cfov.router
  LEFT JOIN CompletedFastDestVolume cfdv ON cmbns.chain = cfdv.chain AND cmbns.asset = cfdv.asset AND cmbns.router = cfdv.router

),
LatestAssetPrices AS (
  SELECT
    ap.canonical_domain,
    ap.canonical_id,
    ap.price,
    ap.timestamp
  FROM `mainnet-bigq.public.asset_prices` ap
  Inner JOIN (
    SELECT
      canonical_domain,
      canonical_id,
      MAX(timestamp) AS max_timestamp
    FROM
      `mainnet-bigq.public.asset_prices`
    GROUP BY
      1, 2
  ) AS latest
  ON ap.canonical_domain = latest.canonical_domain
  AND ap.canonical_id = latest.canonical_id
  AND ap.timestamp = latest.max_timestamp
),
RouterLiquidity AS (
  SELECT
    COALESCE(rwb.domain, gv.chain_domain) as chain_domain_coalesced,
    COALESCE(rwb.adopted, gv.asset_address) as asset_address_coalesced,
    COALESCE(rwb.router_address, gv.router_address) as router_address_coalesced,
    COALESCE(DATE(gv.last_txn_date), DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) as last_txn_date_coalesced,
    rwb.canonical_domain as asset_canonical_domain,
    rwb.address as address,
    rwb.adopted as adopted,
    rwb.adopted_decimal as adopted_decimal,
    rwb.asset_canonical_id as asset_canonical_id,
    rwb.asset_domain as asset_domain_rwb,
    rwb.asset_usd_price as asset_usd_price,
    rwb.balance as balance,
    rwb.balance_usd as balance_usd,
    rwb.decimal as decimal,
    rwb.domain as domain,
    rwb.fees_earned as fees_earned,
    rwb.id as id,
    rwb.key as key,
    rwb.local as local,
    rwb.locked as locked,
    rwb.locked_usd as locked_usd,
    rwb.removed as removed,
    rwb.removed_usd as removed_usd,
    rwb.router_address as router_address_rwb,
    rwb.supplied as supplied,
    rwb.supplied_usd as supplied_usd,
    gv.*,
    lap.*

  FROM
    GroupedMetrics gv
    --`mainnet-bigq.raw.source__cartographer_router_with_balances` rwb
    --`mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb
    FULL OUTER JOIN 
    `mainnet-bigq.raw.source__cartographer_router_with_balances` rwb
    ON rwb.address = gv.router_address AND rwb.adopted = gv.asset_address AND rwb.domain = gv.chain_domain
    LEFT JOIN LatestAssetPrices lap ON rwb.asset_canonical_id = lap.canonical_id AND rwb.canonical_domain = lap.canonical_domain
  WHERE gv.destination_volume_30_days > 0 OR CAST(rwb.balance AS FLOAT64) > 0
),
connext_tokens AS (
  SELECT
    DISTINCT ct.token_address,
    ct.token_name,
    ct.is_xerc20
  FROM
    `mainnet-bigq.stage.connext_tokens` ct 
),
RouterMapping AS (
  SELECT 
    DISTINCT
    COALESCE(ct.token_name, asset_address_coalesced) as asset,
--    cav.domain_name,
    CASE 
        WHEN router_address_coalesced = '0x9584eb0356a380b25d7ed2c14c54de58a25f2581' THEN 'Mike Nai'
        WHEN router_address_coalesced = '0xc4ae07f276768a3b74ae8c47bc108a2af0e40eba' THEN 'P2P 2'
        WHEN router_address_coalesced = '0xeca085906cb531bdf1f87efa85c5be46aa5c9d2c' THEN 'Blocktech 2'
        WHEN router_address_coalesced = '0x22831e4f21ce65b33ef45df0e212b5bebf130e5a' THEN 'Blocktech 1'
        WHEN router_address_coalesced = '0xbe7bc00382a50a711d037eaecad799bb8805dfa8' THEN 'Minerva'
        WHEN router_address_coalesced = '0x63Cda9C42db542bb91a7175E38673cFb00D402b0' THEN 'Consensys Mesh'
        WHEN router_address_coalesced = '0xf26c772c0ff3a6036bddabdaba22cf65eca9f97c' THEN 'Connext'
        WHEN router_address_coalesced = '0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4' THEN 'P2P 1'
        WHEN router_address_coalesced = '0x8cb19ce8eedf740389d428879a876a3b030b9170' THEN 'BWare'
        WHEN router_address_coalesced = '0x0e62f9fa1f9b3e49759dc94494f5bc37a83d1fad' THEN 'Bazilik'
        WHEN router_address_coalesced = '0x58507fed0cb11723dfb6848c92c59cf0bbeb9927' THEN 'Hashquark'
        WHEN router_address_coalesced = '0x7ce49752ffa7055622f444df3c69598748cb2e5f' THEN 'Vault Staking'
        WHEN router_address_coalesced = '0x33b2ad85f7dba818e719fb52095dc768e0ed93ec' THEN 'Ethereal'
        WHEN router_address_coalesced = '0x048a5ecc705c280b2248aeff88fd581abbeb8587' THEN 'Gnosis'
        WHEN router_address_coalesced = '0x975574980a5Da77f5C90bC92431835D91B73669e' THEN '01 Node'
        WHEN router_address_coalesced = '0x6892d4D1f73A65B03063B7d78174dC6350Fcc406' THEN 'Unagii'
        WHEN router_address_coalesced = '0x32d63da9f776891843c90787cec54ada23abd4c2' THEN 'Ingag'
        WHEN router_address_coalesced = '0xfaab88015477493cfaa5dfaa533099c590876f21' THEN 'Paradox'
        WHEN router_address_coalesced = '0x49a9e7ec76bc8fdf658d09557305170d9f01d2fa' THEN 'InfraSingularity'
        WHEN router_address_coalesced = '0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598' THEN 'Dokia'
        WHEN router_address_coalesced = '0x5d527765252003acee6545416f6a9c8d15ae8402' THEN '01 Node'
        WHEN router_address_coalesced = '0xc82c7d826b1ed0b2a4e9a2be72b445416f901fd1' THEN 'Amber'
        WHEN router_address_coalesced = '0xc770ec66052fe77ff2ef9edf9558236e2d1c41ef' THEN 'Dialectic'
        

        ELSE router_address_coalesced
    END AS router_name,
    CASE 
        WHEN pr.chain_domain_coalesced = '6648936' THEN 'Ethereum'
        WHEN pr.chain_domain_coalesced = '1869640809' THEN 'Optimism'
        WHEN pr.chain_domain_coalesced = '6450786' THEN 'BNB'
        WHEN pr.chain_domain_coalesced = '6778479' THEN 'Gnosis'
        WHEN pr.chain_domain_coalesced = '1886350457' THEN 'Polygon'
        WHEN pr.chain_domain_coalesced = '1634886255' THEN 'Arbitrum One'
        WHEN pr.chain_domain_coalesced = '1818848877' THEN 'Linea'
        WHEN pr.chain_domain_coalesced = '31338' THEN 'Local Optimism'
        WHEN pr.chain_domain_coalesced = '31339' THEN 'Local Arbitrum One'
        WHEN pr.chain_domain_coalesced = '1835365481' THEN 'Metis'
        WHEN pr.chain_domain_coalesced = '1650553709' THEN "Base Mainnet"
        WHEN pr.chain_domain_coalesced = '1836016741' THEN 'Mode'
        ELSE pr.chain_domain_coalesced
    END AS domain_name,
    pr.*
  FROM 
    RouterLiquidity pr LEFT JOIN connext_tokens ct ON pr.asset_address_coalesced = ct.token_address
),
RouterGrouping AS (
  SELECT
  Distinct
    rm.asset,
    rm.router_name,
    rm.domain_name,
    AVG(asset_usd_price) as asset_price,
    AVG(CASE
      WHEN (rm.asset = 'ezETH' AND adopted_decimal is null) OR (rm.asset = 'WETH' AND adopted_decimal is null) THEN 18
      ELSE CAST(adopted_decimal AS INT64)
    END) as adopted_decimal,
    SUM(CAST(balance AS FLOAT64)) as balance,
    SUM(CAST(balance_usd AS FLOAT64)) as balance_usd,
    SUM(CAST(fees_earned AS FLOAT64)) as fees_earned,
    SUM(CAST(locked AS FLOAT64)) as locked,
    SUM(CAST(locked_usd AS FLOAT64)) as locked_usd,
    SUM(CAST(removed AS FLOAT64)) as removed,
    SUM(CAST(removed_usd AS FLOAT64)) as removed_usd,
    SUM(CAST(supplied AS FLOAT64)) as supplied,
    SUM(CAST(supplied_usd AS FLOAT64)) as supplied_usd,
    SUM(CAST(origin_usd_volume_last_1_day AS FLOAT64)) AS origin_usd_volume_last_1_day,
    SUM(CAST(origin_usd_volume_last_7_days AS FLOAT64)) AS origin_usd_volume_last_7_days,
    SUM(CAST(origin_usd_volume_last_30_days AS FLOAT64)) AS origin_usd_volume_last_30_days,
    SUM(CAST(origin_volume_1_day AS FLOAT64)) AS origin_volume_1_day,
    SUM(CAST(origin_volume_7_days AS FLOAT64)) AS origin_volume_7_days,
    SUM(CAST(origin_volume_30_days AS FLOAT64)) AS origin_volume_30_days,
    SUM(CAST(origin_fast_usd_volume_1_day AS FLOAT64)) AS origin_fast_usd_volume_1_day,
    SUM(CAST(origin_fast_usd_volume_7_days AS FLOAT64)) AS origin_fast_usd_volume_7_days,
    SUM(CAST(origin_fast_usd_volume_30_days AS FLOAT64)) AS origin_fast_usd_volume_30_days,
    SUM(CAST(origin_fast_volume_1_day AS FLOAT64)) AS origin_fast_volume_1_day,
    SUM(CAST(origin_fast_volume_7_days AS FLOAT64)) AS origin_fast_volume_7_days,
    SUM(CAST(origin_fast_volume_30_days AS FLOAT64)) AS origin_fast_volume_30_days,
    SUM(CAST(destination_usd_volume_last_1_day AS FLOAT64)) AS destination_usd_volume_last_1_day,
    SUM(CAST(destination_usd_volume_last_7_days AS FLOAT64)) AS destination_usd_volume_last_7_days,
    SUM(CAST(destination_usd_volume_last_30_days AS FLOAT64)) AS destination_usd_volume_last_30_days,
    SUM(CAST(destination_volume_1_day AS FLOAT64)) AS destination_volume_1_day,
    SUM(CAST(destination_volume_7_days AS FLOAT64)) AS destination_volume_7_days,
    SUM(CAST(destination_volume_30_days AS FLOAT64)) AS destination_volume_30_days,
    SUM(CAST(destination_fast_usd_volume_1_day AS FLOAT64)) AS destination_fast_usd_volume_1_day,
    SUM(CAST(destination_fast_usd_volume_7_days AS FLOAT64)) AS destination_fast_usd_volume_7_days,
    SUM(CAST(destination_fast_usd_volume_30_days AS FLOAT64)) AS destination_fast_usd_volume_30_days,
    SUM(CAST(destination_fast_volume_1_day AS FLOAT64)) AS destination_fast_volume_1_day,
    SUM(CAST(destination_fast_volume_7_days AS FLOAT64)) AS destination_fast_volume_7_days,
    SUM(CAST(destination_fast_volume_30_days AS FLOAT64)) AS destination_fast_volume_30_days,
    MAX(CAST(last_txn_date_coalesced AS DATE)) AS last_txn_date,
    SUM(CAST(slow_tns AS FLOAT64)) AS slow_tns
  FROM RouterMapping rm
  GROUP BY
    1,2,3
),
ChaihAssetLiquidity AS (
  SELECT
    rl.adopted,
    rl.adopted_decimal,
--    rl.domain,
    rl.asset_canonical_id,
    rl.decimal,
    rl.asset_canonical_domain,
    SUM(origin_usd_volume_last_1_day) AS origin_usd_volume_last_1_day,
    SUM(origin_usd_volume_last_7_days) AS origin_usd_volume_last_7_days,
    SUM(origin_usd_volume_last_30_days) AS origin_usd_volume_last_30_days,
    SUM(origin_volume_1_day) AS origin_volume_1_day,
    SUM(origin_volume_7_days) AS origin_volume_7_days,
    SUM(origin_volume_30_days) AS origin_volume_30_days,
    SUM(origin_fast_volume_1_day) AS origin_fast_volume_1_day,
    SUM(origin_fast_volume_7_days) AS origin_fast_volume_7_days,
    SUM(origin_fast_volume_30_days) AS origin_fast_volume_30_days,
    SUM(destination_usd_volume_last_1_day) AS destination_usd_volume_last_1_day,
    SUM(destination_usd_volume_last_7_days) AS destination_usd_volume_last_7_days,
    SUM(destination_usd_volume_last_30_days) AS destination_usd_volume_last_30_days,
    SUM(destination_volume_1_day) AS destination_volume_1_day,
    SUM(destination_volume_7_days) AS destination_volume_7_days,
    SUM(destination_volume_30_days) AS destination_volume_30_days,
    SUM(destination_fast_volume_1_day) AS destination_fast_volume_1_day,
    SUM(destination_fast_volume_7_days) AS destination_fast_volume_7_days,
    SUM(destination_fast_volume_30_days) AS destination_fast_volume_30_days,
    MAX(CAST(last_txn_date as DATE)) AS last_txn_date,
    SUM(slow_tns) AS slow_tns
  FROM
    RouterLiquidity rl
  GROUP BY
    1,2,3,4,5
--    ,6
),
ezETH_Pricing_Fix AS (
  SELECT 
    * EXCEPT (asset_price, balance_usd, locked_usd, removed_usd, supplied_usd, origin_usd_volume_last_1_day, origin_usd_volume_last_7_days, origin_usd_volume_last_30_days,
    origin_fast_usd_volume_1_day, origin_fast_usd_volume_7_days, origin_fast_usd_volume_30_days,
    destination_usd_volume_last_1_day, destination_usd_volume_last_7_days, destination_usd_volume_last_30_days,
    destination_fast_usd_volume_1_day, destination_fast_usd_volume_7_days, destination_fast_usd_volume_30_days
    ),
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t2.eth_price
      ELSE t1.asset_price
    END AS asset_price,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.balance / power(10,adopted_decimal) * t2.eth_price 
      ELSE t1.balance_usd
    END AS balance_usd,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.locked / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.locked_usd
    END AS locked_usd,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.removed / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.removed_usd
    END AS removed_usd,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.supplied / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.supplied_usd
    END AS supplied_usd,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_volume_1_day / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_usd_volume_last_1_day
    END AS origin_usd_volume_last_1_day,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_volume_7_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_usd_volume_last_7_days
    END AS origin_usd_volume_last_7_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_volume_30_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_usd_volume_last_30_days
    END AS origin_usd_volume_last_30_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_fast_volume_1_day / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_fast_usd_volume_1_day
    END AS origin_fast_usd_volume_1_day,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_fast_volume_7_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_fast_usd_volume_7_days
    END AS origin_fast_usd_volume_7_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.origin_fast_volume_30_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.origin_fast_usd_volume_30_days
    END AS origin_fast_usd_volume_30_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_volume_1_day / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_usd_volume_last_1_day
    END AS destination_usd_volume_last_1_day,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_volume_7_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_usd_volume_last_7_days
    END AS destination_usd_volume_last_7_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_volume_30_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_usd_volume_last_30_days
    END AS destination_usd_volume_last_30_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_fast_volume_1_day / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_fast_usd_volume_1_day
    END AS destination_fast_usd_volume_1_day,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_fast_volume_7_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_fast_usd_volume_7_days
    END AS destination_fast_usd_volume_7_days,
    CASE
      WHEN t1.asset = 'ezETH' OR (t1.asset = 'WETH' AND (t1.asset_price = 0)) THEN t1.destination_fast_volume_30_days / power(10,adopted_decimal) * t2.eth_price
      ELSE t1.destination_fast_usd_volume_30_days
    END AS destination_fast_usd_volume_30_days
  FROM RouterGrouping t1
  CROSS JOIN (
    SELECT asset_price as ETH_price
    FROM RouterGrouping
    WHERE asset = 'WETH' and asset_price > 0
    LIMIT 1  -- Replace 'ETH' with the asset you want to use for the price
  ) t2
)
--SELECT * FROM ChaihAssetLiquidity
--SELECT * from CompletedFastVolume
--select * from RouterMapping WHERE domain_name = 'Linea' --ORDER BY destination_usd_volume_last_1_day DESC
select * from ezETH_Pricing_Fix --WHERE domain_name = 'Linea' --ORDER BY destination_usd_volume_last_1_day DESC
--ezETH 0xbf5495efe5db9ce00f80364c8b423567e58d2110 0x2416092f143378750bb29b79ed961ab195cceea5
--select * from RouterMapping where asset_address_coalesced = '0xbf5495efe5db9ce00f80364c8b423567e58d2110' order by asset 
--SELECT * --SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18) FROM ezETH_Pricing_Fix WHere domain_name = 'Linea'
--SELECT * --asset, SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18),SUM(destination_usd_volume_last_1_day), SUM(destination_usd_volume_last_7_days), SUM(destination_usd_volume_last_30_days) 
--FROM RouterGrouping -- WHERE domain_name = 'Linea' -- group by asset
--SELECT asset, SUM(volume_last_1_day)/Power(10,18), SUM(volume_last_7_days)/Power(10,18), SUM(volume_last_30_days)/Power(10,18),SUM(usd_volume_last_1_day), SUM(usd_volume_last_7_days), SUM(usd_volume_last_30_days) FROM VolumeMetrics Where destination_chain = '1818848877' group by asset

--select * from RouterMapping WHERE RouterMapping.router_address_coalesced = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT asset_address_coalesced, SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18),SUM(destination_usd_volume_last_1_day), SUM(destination_usd_volume_last_7_days), SUM(destination_usd_volume_last_30_days) FROM RouterLiquidity where chain_domain_coalesced = '1818848877' group by asset_address_coalesced
--SELECT asset_address, SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18),SUM(destination_usd_volume_last_1_day), SUM(destination_usd_volume_last_7_days), SUM(destination_usd_volume_last_30_days)  from GroupedMetrics WHERE GroupedMetrics.chain_domain = '1818848877' group by asset_address
--SELECT * from GroupedMetrics WHERE router_address = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb WHERE address = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * from Combinations WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM DestinationVolume WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--ORDER BY destination_volume_last_1_day desc 
--SELECT * FROM CombinedVolumes
--ORDER BY destination_volume_last_1_day desc 

--SELECT * FROM OriginVolume
--ORDER BY origin_volume_last_1_day desc 
--SELECT DISTINCT asset, router, origin_chain, destination_chain from VolumeMetrics
--SELECT DISTINCT origin_chain from VolumeMetrics
--SELECT  GroupedMetrics.asset_address, 
--SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18) FROM GroupedMetrics WHERE chain_domain = '1836016741'  GROUP BY asset_address ORDER BY SUM(destination_volume_1_day)/Power(10,18) desc
--SELECT ROuterLiquidity.asset_address_coalesced, SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18) from ROuterLiquidity WHERE ROuterLiquidity.chain_domain_coalesced = '1836016741' GROUP BY asset_address_coalesced ORDER BY SUM(destination_volume_1_day)/Power(10,18) desc
--asset = '0x2416092f143378750bb29b79ed961ab195cceea5' and 
--0x4ecaba5870353805a9f068101a40e0f32ed605c6 - USDT
--SELECT * FROM Metrics WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * --asset, SUM(destination_volume_1_day)/Power(10,18), SUM(destination_volume_7_days)/Power(10,18), SUM(destination_volume_30_days)/Power(10,18) 
--FROM ezETH_Pricing_Fix where domain_name = 'Mode' 
--group by asset