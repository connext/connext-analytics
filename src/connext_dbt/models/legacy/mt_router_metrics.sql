-- mainnet-bigq.metrics.router_metrics

WITH VolumeMetrics AS (
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
    {{ ref('stg_daily_transfer_volume') }}
  GROUP BY 
    1,2,3,4,5
),
CompletedFastOriginVolume AS (
  SELECT
    router,
    asset,
    origin_chain as chain,
    SUM(usd_volume_last_1_day) AS origin_fast_volume_1_day,
    SUM(usd_volume_last_7_days) AS origin_fast_volume_7_days,
    SUM(usd_volume_last_30_days) AS origin_fast_volume_30_days
  FROM VolumeMetrics vm
  WHERE status = 'CompletedFast'
  GROUP BY 1,2,3
),
CompletedFastDestVolume AS (
  SELECT
    router,
    asset,
    destination_chain as chain,
    SUM(usd_volume_last_1_day) AS destination_fast_volume_1_day,
    SUM(usd_volume_last_7_days) AS destination_fast_volume_7_days,
    SUM(usd_volume_last_30_days) AS destination_fast_volume_30_days
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
  SELECT DISTINCT origin_chain as chain, asset, router from VolumeMetrics
),
Metrics AS (
  SELECT 
    router,
    asset,
    origin_chain as chain,
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
  dv.destination_usd_volume_last_1_day,
  dv.destination_usd_volume_last_7_days,
  dv.destination_usd_volume_last_30_days,
  dv.destination_volume_1_day,
  dv.destination_volume_7_days,
  dv.destination_volume_30_days,
  cfdv.destination_fast_volume_1_day,
  cfdv.destination_fast_volume_7_days,
  cfdv.destination_fast_volume_30_days,
  last_txn_date,
  slow_tns
from 
  Combinations cmbns 
  LEFT JOIN Metrics mtr ON cmbns.chain = mtr.chain AND cmbns.asset = mtr.asset AND cmbns.router = mtr.router
  LEFT JOIN OriginVolume ov ON cmbns.chain = ov.chain AND cmbns.asset = ov.asset AND cmbns.router = ov.router
  LEFT JOIN DestinationVolume dv ON cmbns.chain = dv.chain AND cmbns.asset = dv.asset AND cmbns.router = dv.router
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
  INNER JOIN (
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
    {{ ref('stg_source__cartographer_router_with_balances') }} rwb
--    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb
    FULL OUTER JOIN GroupedMetrics gv ON rwb.address = gv.router_address AND rwb.adopted = gv.asset_address AND rwb.domain = gv.chain_domain
    LEFT JOIN LatestAssetPrices lap ON rwb.asset_canonical_id = lap.canonical_id AND rwb.canonical_domain = lap.canonical_domain
  WHERE gv.destination_usd_volume_last_30_days > 0 OR gv.origin_usd_volume_last_30_days > 0 OR CAST(rwb.balance AS FLOAT64) > 0
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
        WHEN router_address_coalesced = '0x49a9e7ec76bc8fdf658d09557305170d9f01d2fa' THEN 'Jitin'
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
        ELSE pr.chain_domain_coalesced
    END AS domain_name,
    pr.*
  FROM 
    RouterLiquidity pr LEFT JOIN connext_tokens ct ON pr.asset_address_coalesced = ct.token_address
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
    MAX(last_txn_date) AS last_txn_date,
    SUM(slow_tns) AS slow_tns
  FROM
    RouterLiquidity rl
  GROUP BY
    1,2,3,4,5
--    ,6
)
--SELECT * FROM ChaihAssetLiquidity
--SELECT * from CompletedFastVolume
select * from RouterMapping ORDER BY destination_usd_volume_last_1_day DESC
--select * from RouterMapping WHERE RouterMapping.router_address_coalesced = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM RouterLiquidity
--SELECT * from GroupedMetrics
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
--SELECT * FROM VolumeMetrics WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--0x4ecaba5870353805a9f068101a40e0f32ed605c6 - USDT
--SELECT * FROM Metrics WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'


