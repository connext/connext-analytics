-- get list of txs in transfers for which we dont have data in `stage.source_mode_weth_arb_chain_deposits__transactions`

SELECT
  t.xcall_transaction_hash AS source_chain_hash
FROM
  `public.transfers` t
LEFT JOIN `stage.source_mode_weth_arb_chain_deposits__transactions` adt
ON t.xcall_transaction_hash = adt.hash
WHERE
  t.status IN ('CompletedSlow',
    'CompletedFast')
  AND LOWER(t.destination_transacting_asset) = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1" --weth
  AND t.destination_domain = "1634886255" -- arb
  AND t.origin_domain = "1836016741" -- mode
  AND TIMESTAMP_SECONDS(t.xcall_timestamp) >= (
  SELECT
    CAST(MAX(adt.timestamp) AS timestamp)
  FROM
    `stage.source_mode_weth_arb_chain_deposits__transactions` adt )
  AND adt.hash IS NULL