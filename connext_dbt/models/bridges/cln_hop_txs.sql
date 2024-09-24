
-- final table -> Cols needed for final table:
SELECT

s.transfer_id AS id,
s.from_timestamp AS timestamp_in,
s.to_timestamp AS timestamp_out,
s.from_amount AS amount_in,
s.to_amount AS amount_out,
NULL AS gas_fee_symbol,
NULL AS gas_fee,
NULL AS gas_price_usd,
s.relayer_fee_symbol AS relay_fee_symbol,
s.relayer_fee AS relay_fee,
s.relayer_fee_in_usd AS relay_fee_price_usd,
s.from_token_symbol AS symbol_in,
s.to_token_symbol AS symbol_out,
s.from_amount_usd AS price_in_usd,
s.to_amount_usd AS price_out_usd,
s.from_hash AS txn_hash_in,
s.to_hash AS txn_hash_out,
NULL AS token_address_in,
NULL AS token_address_out,
s.from_chain_id AS chain_in,
s.from_chain_name AS chain_in_name,
s.to_chain_id AS chain_out,
s.to_chain_name AS chain_out_name,
'hop' AS bridge,
s.from_address AS user_address_in,
s.to_address AS user_address_out

FROM {{ ref('stg_hop_txs') }} s