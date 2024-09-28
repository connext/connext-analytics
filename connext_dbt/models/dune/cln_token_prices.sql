SELECT
    symbol,
    CAST(date AS TIMESTAMP) AS date,
    avg(average_price) AS price
FROM {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }}
GROUP BY symbol, datew