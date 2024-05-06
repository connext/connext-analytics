-- mainnet-bigq.raw.source__cartographer_router_with_balances

WITH MaxAssetPricesTimestamp AS (
  SELECT canonical_id, MAX(timestamp) AS max_timestamp
  FROM `mainnet-bigq.public.asset_prices` asset_prices
  GROUP BY canonical_id
),
MaxAssetPrices AS (
  SELECT 
    asset_prices.canonical_domain,
    asset_prices.canonical_id,
    asset_prices.id,
    CAST(asset_prices.price AS FLOAT64) AS price,
    asset_prices.timestamp,
    FROM `mainnet-bigq.public.asset_prices` asset_prices
  JOIN MaxAssetPricesTimestamp ON asset_prices.canonical_id = MaxAssetPricesTimestamp.canonical_id AND asset_prices.timestamp = MaxAssetPricesTimestamp.max_timestamp
),

--JOIN `mainnet-bigq.public.asset_prices` asset_prices ON assets.canonical_id = asset_prices.canonical_id
--LEFT JOIN MaxAssetPrices ON asset_prices.canonical_id = MaxAssetPrices.canonical_id AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
--LEFT JOIN MaxAssetPrices asset_prices ON assets.canonical_id = asset_prices.canonical_id--MaxAssetPrices.canonical_id --AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
RWB AS (
SELECT
DISTINCT
  routers.address,
  asset_balances.asset_canonical_id,
  asset_balances.asset_domain,
  asset_balances.router_address,
  asset_balances.balance,
  assets.local,
  assets.adopted,
  assets.canonical_id,
  assets.canonical_domain,
  assets.domain,
  assets.key,
  assets.id,
  asset_balances.fees_earned,
  asset_balances.locked,
  asset_balances.supplied,
  asset_balances.removed,
  assets.decimal,
  assets.adopted_decimal,
  COALESCE(asset_prices.price, 0) AS asset_usd_price,
  asset_prices.price * (CAST(asset_balances.balance AS FLOAT64) / POW(10, CAST(assets.decimal AS INT64))) AS balance_usd,
  asset_prices.price * (CAST(asset_balances.fees_earned AS FLOAT64) / POW(10, CAST(assets.decimal AS INT64))) AS fee_earned_usd,
  asset_prices.price * ( CAST(asset_balances.locked AS FLOAT64) / POW(10, CAST(assets.decimal AS INT64))) AS locked_usd,
  asset_prices.price * (CAST(asset_balances.supplied AS FLOAT64)  / POW(10, CAST(assets.decimal AS INT64))) AS supplied_usd,
  asset_prices.price * (CAST(asset_balances.removed AS FLOAT64) / POW(10, CAST(assets.decimal AS INT64))) AS removed_usd
FROM
  (`mainnet-bigq.public.routers` routers
LEFT JOIN
  `mainnet-bigq.public.asset_balances` asset_balances
ON
  routers.address = asset_balances.router_address
LEFT JOIN
  `mainnet-bigq.public.assets` assets
ON
  asset_balances.asset_canonical_id = assets.canonical_id
  AND asset_balances.asset_domain = assets.domain)
LEFT JOIN MaxAssetPrices asset_prices ON assets.canonical_id = asset_prices.canonical_id
--JOIN `mainnet-bigq.public.asset_prices` asset_prices ON assets.canonical_id = asset_prices.canonical_id
--LEFT JOIN MaxAssetPrices ON asset_prices.canonical_id = MaxAssetPrices.canonical_id AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
--LEFT JOIN MaxAssetPrices asset_prices ON assets.canonical_id = asset_prices.canonical_id--MaxAssetPrices.canonical_id --AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
)

SELECT DISTINCT * FROM RWB --WHERE address is not null and asset_domain = '1836016741'
--SELECT * FROM MaxAssetPrices
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets` WHERE domain = '1836016741'
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_balances` WHERE asset_domain = '1836016741'
--SELECT * FROM `mainnet-bigq.public.asset_prices` WHERE  REGEXP_CONTAINS(canonical_id, '(?i)2416092f143378750bb29b79ed961ab195cceea5')
--asset_canonical_id = '0xbf5495efe5db9ce00f80364c8b423567e58d2110'


--WHERE adopted = '0xbf5495efe5db9ce00f80364c8b423567e58d2110'
