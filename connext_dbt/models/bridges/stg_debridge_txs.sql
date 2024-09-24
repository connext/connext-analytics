-- INFO
-- This SQL script processes transaction data from the `mainnet-bigq.raw.source_de_bridge_explorer__transactions` table, 
-- extracting and transforming various fields related to token swaps and fees. 
-- It joins this data with chain metadata from `mainnet-bigq.raw.source_chainlist_network__chains` to enrich the transaction records with chain names and token symbols. 
-- The final output includes details about the transfer, such as chain IDs, token amounts, and associated fees.


WITH raw AS (
  SELECT DISTINCT *
  FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions`
)

, semi AS (
SELECT  
  orderid_stringvalue AS transfer_id,
  TIMESTAMP_SECONDS(CAST(creationtimestamp AS INT64)) AS date,
  unlockauthoritydst_stringvalue AS user_address_out,
  SAFE_CAST(preswapdata_chainid_bigintegervalue AS INT64) AS pre_swap_chain_id,
  SAFE_CAST(preswapdata_inamount_bigintegervalue AS FLOAT64) AS pre_swap_in_amount,
  preswapdata_tokeninmetadata_symbol AS pre_swap_in_token_symbol,
  SAFE_CAST(preswapdata_outamount_bigintegervalue AS FLOAT64) AS pre_swap_out_amount,
  preswapdata_tokenoutmetadata_symbol AS pre_swap_out_token_symbol,
  SAFE_CAST(giveofferwithmetadata_chainid_bigintegervalue AS INT64) AS from_chain_id,
  SAFE_CAST(giveofferwithmetadata_metadata_decimals AS INT64) AS from_actual_symbol_decimal,
  giveofferwithmetadata_metadata_symbol AS from_actual_symbol,
  giveofferwithmetadata_tokenaddress_stringvalue AS from_actual_token_address,
  SAFE_CAST(takeofferwithmetadata_chainid_bigintegervalue AS INT64) AS to_chain_id,
  SAFE_CAST(takeofferwithmetadata_decimals AS INT64) AS to_symbol_decimal,
  takeofferwithmetadata_metadata_symbol AS to_symbol,
  SAFE_CAST(giveofferwithmetadata_finalamount_bigintegervalue AS FLOAT64) AS from_actual_value,
  SAFE_CAST(takeofferwithmetadata_amount_bigintegervalue AS FLOAT64) AS to_value,
  SAFE_CAST(fixfee_bigintegervalue AS FLOAT64) AS market_maker_gas_costs,
  SAFE_CAST(finalpercentfee_bigintegervalue AS FLOAT64) AS debridge_fee
FROM raw r
)

SELECT 
  transfer_id,
  date,
  
  -- from
  COALESCE(pre_swap_chain_id, from_chain_id) AS from_chain_id,
  from_chain.name AS from_chain_name,
  from_actual_token_address AS from_token_address,
  from_actual_symbol AS from_token_symbol,
  from_actual_value / POW(10, from_actual_symbol_decimal) AS from_amount,
  
  -- to
  user_address_out AS user_address_out,
  to_chain_id,
  to_chain.name AS to_chain_name,
  to_symbol AS to_token_symbol,
  to_value / POW(10, to_symbol_decimal) AS to_amount,
  
  -- fees
  -- fee 1 token symbol is the origin chain native token
  fee_chain.nativeCurrency_symbol AS fee_token_symbol,
  market_maker_gas_costs / POW(10, fee_chain.nativeCurrency_decimals) AS gas_fee,
  
  from_actual_symbol AS protocol_fee_token_symbol,
  debridge_fee / POW(10, from_actual_symbol_decimal) AS protocol_fee_value

FROM semi s
LEFT JOIN mainnet-bigq.raw.source_chainlist_network__chains AS fee_chain
  ON s.from_chain_id = fee_chain.chainId

LEFT JOIN mainnet-bigq.raw.source_chainlist_network__chains AS from_chain
  ON s.from_chain_id = from_chain.chainId

LEFT JOIN mainnet-bigq.raw.source_chainlist_network__chains AS to_chain
  ON s.to_chain_id = to_chain.chainId