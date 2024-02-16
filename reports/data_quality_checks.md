# Missing Data

## Pricing missing

Mentis has 4 tokens it supports, out of which 1 is missing from the asset price table.

The asset price table is also missing few rows as compared to Cartographer. 
- sql-BQ:
    ```sql
    SELECT canonical_id, MAX(timestamp) AS max_timestamp
    FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
    WHERE canonical_id IN (
    "0x0000000000000000000000009e32b13ce7f2e80a01932b42553652e053d6ed8e",
    "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7"
    )
    GROUP BY canonical_id
    ```

- Pricing not aviable for mentis token,
    ```sql
    SELECT * 
    FROM `y42_connext_y42_main.source__Carto__public_assets` assets
    LEFT JOIN
    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_asset_prices` asset_prices
    ON
    assets.canonical_id = asset_prices.canonical_id

    WHERE local = LOWER("0x1a1162304654A79B4b6A3aF0D564CA1E3cC7cc1B")
    ```
- `https://connextscan.io/metis` uses LOCAL asset while we are using adopted asset to calculate routers with balance

Action Items:
- Full Refresh Pull for the table

## Join Error? Why the data was not showing up
- INNER JOIN: efectively removed any tokens with null pricing
- Token metadata missing effectively removed all tokens as well

## TOken + Chains Metadata missing
- we need to add this endpoint to our BQ: ``