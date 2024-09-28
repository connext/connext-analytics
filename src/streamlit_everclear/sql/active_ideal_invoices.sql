-- MARKET MAKER KPIs
-- 5. Settlement_Volume -> Volume settled by market makers
-- 11. Settlement_Time -> Time taken by Market Makers to settle a invoice
-- 14. Net profit: APY for Market Makers -> issue: fee_by_market_maker is not collected: gas_used
-- 15. Average amount of epochs
-- 19. Trading_Volume
WITH metadata AS (
    SELECT Symbol AS symbol,
        CAST(Decimals AS INTEGER) AS decimal,
        CAST(DomainID AS INTEGER) AS domain_id,
        LOWER(Address) AS address,
        LOWER(
            CONCAT(
                '0x',
                LPAD(
                    SUBSTRING(
                        Address
                        FROM 3
                    ),
                    64,
                    '0'
                )
            )
        ) AS adopted_address
    FROM (
            VALUES (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    1,
                    '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    10,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    56,
                    '0x2170Ed0880ac9A755fd29B2688956BD959F933F8'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    8453,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    42161,
                    '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    1,
                    '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    10,
                    '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85'
                ),
                (
                    'USD Coin',
                    'USDC',
                    18,
                    56,
                    '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    8453,
                    '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    42161,
                    '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    1,
                    '0xdAC17F958D2ee523a2206206994597C13D831ec7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    10,
                    '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58'
                ),
                (
                    'Tether USD',
                    'USDT',
                    18,
                    56,
                    '0x55d398326f99059fF775485246999027B3197955'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'
                )
        ) AS asset_data (AssetName, Symbol, Decimals, DomainID, Address)
)

SELECT 
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    fm.symbol AS from_asset_symbol,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    tm.symbol AS to_asset_symbol,
    -- amounts,
    (i.origin_amount::float / 10 ^ 18) AS origin_amount,
    (CAST(inv.hub_invoice_amount AS FLOAT) / 10 ^ 18) AS hub_invoice_amount,
    (CAST(i.settlement_amount AS FLOAT) / 10 ^ tm.decimal) AS settlement_amount,
    (CAST(inv.hub_invoice_amount AS FLOAT) / 10 ^ 18) - (
        CAST(i.settlement_amount AS FLOAT) / 10 ^ tm.decimal
    ) AS discounts_by_mm
FROM public.intents i
INNER JOIN public.invoices inv ON i.id = inv.id
LEFT JOIN metadata fm ON (
    i.origin_input_asset = fm.adopted_address
    AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
)
LEFT JOIN metadata tm ON (
    LOWER(i.settlement_asset) = tm.address
    AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
)
WHERE inv.id = '0xd86d6efba7b376e7ddba3a8e2b8dc9fc969bb69226d0517fe5b7c3d733e1007e'
    AND inv.hub_status = 'INVOICED'