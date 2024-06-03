WITH MaxAssetPrices AS (
    SELECT
        Canonical_Id,
        MAX(Timestamp) AS Max_Timestamp
    FROM
        `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices`
            Asset_Prices
    GROUP BY Canonical_Id
)

SELECT
    Routers.Address,
    Asset_Balances.Asset_Canonical_Id,
    Asset_Balances.Asset_Domain,
    Asset_Balances.Router_Address,
    Asset_Balances.Balance,
    Assets.Local,
    Assets.Adopted,
    Assets.Canonical_Id,
    Assets.Canonical_Domain,
    Assets.Domain,
    Assets.Key,
    Assets.Id,
    Asset_Balances.Fees_Earned,
    Asset_Balances.Locked,
    Asset_Balances.Supplied,
    Asset_Balances.Removed,
    Assets.Decimal,
    Assets.Adopted_Decimal,
    COALESCE(Asset_Prices.Price, 0) AS Asset_Usd_Price,
    Asset_Prices.Price
    * (Asset_Balances.Balance / POW(10, CAST(Assets.Decimal AS INT64)))
        AS Balance_Usd,
    Asset_Prices.Price
    * (Asset_Balances.Fees_Earned / POW(10, CAST(Assets.Decimal AS INT64)))
        AS Fee_Earned_Usd,
    Asset_Prices.Price
    * (Asset_Balances.Locked / POW(10, CAST(Assets.Decimal AS INT64)))
        AS Locked_Usd,
    Asset_Prices.Price
    * (Asset_Balances.Supplied / POW(10, CAST(Assets.Decimal AS INT64)))
        AS Supplied_Usd,
    Asset_Prices.Price
    * (Asset_Balances.Removed / POW(10, CAST(Assets.Decimal AS INT64)))
        AS Removed_Usd
FROM
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers`
        Routers
LEFT JOIN
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_balances`
        Asset_Balances
    ON
        Routers.Address = Asset_Balances.Router_Address
LEFT JOIN
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets`
        Assets
    ON
        Asset_Balances.Asset_Canonical_Id = Assets.Canonical_Id
        AND Asset_Balances.Asset_Domain = Assets.Domain
LEFT JOIN
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices`
        Asset_Prices
    ON
        Assets.Canonical_Id = Asset_Prices.Canonical_Id
-- [ ] TODO BY Adding a inner join here, we are effectively removing any token data where price is not avaiable
INNER JOIN
    MaxAssetPrices
    ON
        Asset_Prices.Canonical_Id = MaxAssetPrices.Canonical_Id
        AND Asset_Prices.Timestamp = MaxAssetPrices.Max_Timestamp
