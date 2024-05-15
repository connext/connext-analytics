SELECT  
  TIMESTAMP_SECONDS( creationtimestamp) AS date,
  giveofferwithmetadata_chainid_bigintegervalue AS from_chain_id,
  giveofferwithmetadata_metadata_decimals AS from_symbol_decimal,
  giveofferwithmetadata_metadata_symbol AS from_symbol,
  takeofferwithmetadata_chainid_bigintegervalue AS to_chain_id,
  takeofferwithmetadata_decimals AS to_symbol_decimal,
  takeofferwithmetadata_metadata_symbol AS to_symbol,
  giveofferwithmetadata_finalamount_bigintegervalue AS from_value,
  takeofferwithmetadata_amount_bigintegervalue AS to_value
FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions` 