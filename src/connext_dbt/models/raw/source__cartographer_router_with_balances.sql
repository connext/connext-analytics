WITH MaxAssetPrices AS (
  SELECT canonical_id, MAX(timestamp) AS max_timestamp
  FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
  GROUP BY canonical_id
)

SELECT
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
  asset_prices.price * (asset_balances.balance / POW(10, CAST(assets.decimal AS INT64))) AS balance_usd,
  asset_prices.price * (asset_balances.fees_earned / POW(10, CAST(assets.decimal AS INT64))) AS fee_earned_usd,
  asset_prices.price * (asset_balances.locked / POW(10, CAST(assets.decimal AS INT64))) AS locked_usd,
  asset_prices.price * (asset_balances.supplied / POW(10, CAST(assets.decimal AS INT64))) AS supplied_usd,
  asset_prices.price * (asset_balances.removed / POW(10, CAST(assets.decimal AS INT64))) AS removed_usd
FROM
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers` routers
LEFT JOIN
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_balances` asset_balances
ON
  routers.address = asset_balances.router_address
LEFT JOIN
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets` assets
ON
  asset_balances.asset_canonical_id = assets.canonical_id
  AND asset_balances.asset_domain = assets.domain
LEFT JOIN
  `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
ON
  assets.canonical_id = asset_prices.canonical_id
INNER JOIN
  MaxAssetPrices
ON asset_prices.canonical_id = MaxAssetPrices.canonical_id AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
