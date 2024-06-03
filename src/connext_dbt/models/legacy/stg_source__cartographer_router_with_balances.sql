-- mainnet-bigq.raw.source__cartographer_router_with_balances

WITH MaxAssetPricesTimestamp AS (
    SELECT
        Canonical_Id,
        MAX(Timestamp) AS Max_Timestamp
    FROM `mainnet-bigq.public.asset_prices` Asset_Prices
    GROUP BY Canonical_Id
),

MaxAssetPrices AS (
    SELECT
        Asset_Prices.Canonical_Domain,
        Asset_Prices.Canonical_Id,
        Asset_Prices.Id,
        CAST(Asset_Prices.Price AS FLOAT64) AS Price,
        Asset_Prices.Timestamp
    FROM `mainnet-bigq.public.asset_prices` Asset_Prices
    JOIN
        MaxAssetPricesTimestamp
        ON
            Asset_Prices.Canonical_Id = MaxAssetPricesTimestamp.Canonical_Id
            AND Asset_Prices.Timestamp = MaxAssetPricesTimestamp.Max_Timestamp
),

--JOIN `mainnet-bigq.public.asset_prices` asset_prices ON assets.canonical_id = asset_prices.canonical_id
--LEFT JOIN MaxAssetPrices ON asset_prices.canonical_id = MaxAssetPrices.canonical_id AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
--LEFT JOIN MaxAssetPrices asset_prices ON assets.canonical_id = asset_prices.canonical_id--MaxAssetPrices.canonical_id --AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
RWB AS (
    SELECT DISTINCT
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
        * (
            CAST(Asset_Balances.Balance AS FLOAT64)
            / POW(10, CAST(Assets.Decimal AS INT64))
        ) AS Balance_Usd,
        Asset_Prices.Price
        * (
            CAST(Asset_Balances.Fees_Earned AS FLOAT64)
            / POW(10, CAST(Assets.Decimal AS INT64))
        ) AS Fee_Earned_Usd,
        Asset_Prices.Price
        * (
            CAST(Asset_Balances.Locked AS FLOAT64)
            / POW(10, CAST(Assets.Decimal AS INT64))
        ) AS Locked_Usd,
        Asset_Prices.Price
        * (
            CAST(Asset_Balances.Supplied AS FLOAT64)
            / POW(10, CAST(Assets.Decimal AS INT64))
        ) AS Supplied_Usd,
        Asset_Prices.Price
        * (
            CAST(Asset_Balances.Removed AS FLOAT64)
            / POW(10, CAST(Assets.Decimal AS INT64))
        ) AS Removed_Usd
    FROM
        (
            `mainnet-bigq.public.routers` Routers
            LEFT JOIN
                `mainnet-bigq.public.asset_balances` Asset_Balances
                ON
                    Routers.Address = Asset_Balances.Router_Address
            LEFT JOIN
                `mainnet-bigq.public.assets` Assets
                ON
                    Asset_Balances.Asset_Canonical_Id = Assets.Canonical_Id
                    AND Asset_Balances.Asset_Domain = Assets.Domain
        )
    LEFT JOIN
        MaxAssetPrices Asset_Prices
        ON Assets.Canonical_Id = Asset_Prices.Canonical_Id
--JOIN `mainnet-bigq.public.asset_prices` asset_prices ON assets.canonical_id = asset_prices.canonical_id
--LEFT JOIN MaxAssetPrices ON asset_prices.canonical_id = MaxAssetPrices.canonical_id AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
--LEFT JOIN MaxAssetPrices asset_prices ON assets.canonical_id = asset_prices.canonical_id--MaxAssetPrices.canonical_id --AND asset_prices.timestamp = MaxAssetPrices.max_timestamp
)

--WHERE address is not null and asset_domain = '1836016741'
SELECT DISTINCT * FROM RWB
--SELECT * FROM MaxAssetPrices
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_assets` WHERE domain = '1836016741'
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_balances` WHERE asset_domain = '1836016741'
--SELECT * FROM `mainnet-bigq.public.asset_prices` WHERE  REGEXP_CONTAINS(canonical_id, '(?i)2416092f143378750bb29b79ed961ab195cceea5')
--asset_canonical_id = '0xbf5495efe5db9ce00f80364c8b423567e58d2110'


--WHERE adopted = '0xbf5495efe5db9ce00f80364c8b423567e58d2110'
