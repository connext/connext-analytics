-- -- create price group and join on date to hour

WITH raw_tx AS (
SELECT
    all_bridge.*,
    DATE_TRUNC(all_bridge.date, HOUR) AS date_hour,
    relay_nt.price_symbol AS relay_fnt_price_group,
    relay_lts.price_symbol AS relay_ft_price_group,
    from_lts.price_symbol AS from_price_group,
    to_lts.price_symbol AS to_price_group,
FROM {{ ref('stg_all_bridge_txs') }} all_bridge
LEFT JOIN {{ ref('list_of_tokens_symbols') }} relay_nt ON all_bridge.relayer_fee_token_symbol = relay_nt.token_symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }} relay_lts ON all_bridge.relayer_fee_token_symbol = relay_lts.token_symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }} from_lts ON all_bridge.from_token_symbol = from_lts.token_symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }} to_lts ON all_bridge.to_token_symbol = to_lts.token_symbol
)

-- convert the amounts tousd price
semi_raw_tx AS (
SELECT
    rt.*,
    -- usd amounts
    rt.from_amount * fts_p.average_price AS from_price_usd,
    rt.to_amount * tts_p.average_price AS to_price_usd,
    rt.from_gas_amount * fts_fn.average_price AS from_gas_native_usd,
    rt.to_gas_amount * tts_fn.average_price AS to_gas_native_usd,
    rt.relayer_fee_in_native * relay_fnt_p.average_price AS relay_fee_native_usd,
    rt.relayer_fee_in_tokens * relay_ft_p.average_price AS relay_fee_tokens_usd
FROM raw_tx rt
-- get price for each token column    

LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} fts_p 
    ON rt.from_price_group = fts_p.symbol AND rt.date_hour = CAST(fts_p.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} tts_p 
    ON rt.to_price_group = tts_p.symbol AND rt.date_hour = CAST(tts_p.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} fts_fn 
    ON rt.from_native_token = fts_fn.symbol AND rt.date_hour = CAST(fts_fn.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} tts_fn 
    ON rt.to_native_token = tts_fn.symbol AND rt.date_hour = CAST(tts_fn.date AS TIMESTAMP)
-- relay fee native token price
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} relay_fnt_p 
    ON rt.relay_fnt_price_group = relay_fnt_p.symbol AND rt.date_hour = CAST(relay_fnt_p.date AS TIMESTAMP)
-- relay fee token price
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} relay_ft_p 
    ON rt.relay_ft_price_group = relay_ft_p.symbol AND rt.date_hour = CAST(relay_ft_p.date AS TIMESTAMP)
)


-- final table:
SELECT

s.transfer_id AS id
s.date AS timestamp_in
NULL AS timestamp_out
s.from_amount AS amount_in
s.to_amount AS amount_out
-- combine all gas fees to get this fee
NULL AS gas_fee
s.from_ga
gas_price_usd
relay_fee
relay_fee_price_usd
token_in
symbol_in
token_out
symbol_out
price_in_usd
price_out_usd 
txn_hash_in
txn_hash_out
token_address_in
token_address_out
chain_in
chain_out
bridge
user_address_in
user_address_out

FROM semi_raw_tx s