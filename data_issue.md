# Asset Decimal Issue

[Eg Transfer: Link to carto](https://postgrest.mainnet.connext.ninja/transfers?transfer_id=eq.0xd3b48a423a57829f3970b76c4546c9ac67eaa46589f0048eee9e6bf45adab806&limit=10&offset=0&order=xcall_timestamp.desc&select=transfer_id,nonce,to,call_data,origin_domain,canonical_domain,canonical_id,destination_domain,bridged_amt,normalized_in,origin_sender,origin_chain,origin_transacting_asset,origin_transacting_amount,origin_bridged_asset,origin_bridged_amount,xcall_caller,xcall_transaction_hash,xcall_timestamp,xcall_gas_price,xcall_gas_limit,xcall_block_number,xcall_tx_origin,destination_chain,receive_local,status,routers,delegate,slippage,updated_slippage,destination_transacting_asset,destination_transacting_amount,destination_local_asset,destination_local_amount,execute_caller,execute_transaction_hash,execute_timestamp,execute_gas_price,execute_gas_limit,execute_block_number,execute_origin_sender,execute_tx_origin,reconcile_caller,reconcile_transaction_hash,reconcile_timestamp,reconcile_gas_price,reconcile_gas_limit,reconcile_block_number,reconcile_tx_origin,relayer_fees,error_status,execute_simulation_input,execute_simulation_from,execute_simulation_to,execute_simulation_network)


The asset table show only 6 decimal for the above `canonical_id` for all domains. Based on that, amount will be as,

- Transfers Raw

  ```sql
  SELECT *
  FROM `mainnet-bigq.public.transfers` t
  LEFT JOIN assets a
    ON (
      t.canonical_id = a.canonical_id
      AND t.origin_domain = a.domain
      )
  WHERE t.transfer_id = "0xd3b48a423a57829f3970b76c4546c9ac67eaa46589f0048eee9e6bf45adab806"

  ```

  ```json
  [{
    "transfer_id": "0xd3b48a423a57829f3970b76c4546c9ac67eaa46589f0048eee9e6bf45adab806",
    "nonce": "7018",
    "to": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "call_data": "0x",
    "origin_domain": "6450786",
    "destination_domain": "1886350457",
    "receive_local": "false",
    "origin_chain": "56",
    "origin_transacting_asset": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "origin_transacting_amount": "221159783502651317642",
    "origin_bridged_asset": "0x5e7d83da751f4c9694b13af351b30ac108f32c38",
    "origin_bridged_amount": "220919091",
    "xcall_caller": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "xcall_transaction_hash": "0x62b65e4856bd4251be4a6627503498facf37edce9c0a0a2e21f55f2cf9ef5406",
    "xcall_timestamp": "1680682306",
    "xcall_gas_price": "5000000000",
    "xcall_gas_limit": "416417",
    "xcall_block_number": "27078828",
    "destination_chain": "137",
    "destination_transacting_asset": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
    "destination_transacting_amount": "220867466",
    "destination_local_asset": "0xf96c6d2537e1af1a9503852eb2a4af264272a5b6",
    "destination_local_amount": "220919091",
    "execute_caller": "0xabcc9b596420a9e9172fd5938620e265a0f9df92",
    "execute_transaction_hash": "0x8ffa442c63139c2981f2ac93827687e6e9b678a78bf11c499139677d6942692f",
    "execute_timestamp": "1680682391",
    "execute_gas_price": "154453899249",
    "execute_gas_limit": "6000000",
    "execute_block_number": "41165327",
    "execute_origin_sender": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "reconcile_caller": "0x96fddc1a6fbdb232e9ada1ffc1026799f85128e9",
    "reconcile_transaction_hash": "0x8d33540d6fa0b50236f43ec6f2e5c975dc59f2460976479b8f037aef31806a89",
    "reconcile_timestamp": "1680689350",
    "reconcile_gas_price": "137019990188",
    "reconcile_gas_limit": "6000000",
    "reconcile_block_number": "41168416",
    "update_time": "2023-07-11 19:32:25.101671 UTC",
    "delegate": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "message_hash": "0x4eb5fbeb7052d1bbdb433d319f0440a97d6a6bdfc0c6a62657bf1297cbb39273",
    "canonical_domain": "6648936",
    "slippage": "50",
    "origin_sender": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "bridged_amt": "220919091",
    "normalized_in": "221159783502651317642",
    "canonical_id": "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "router_fee": null,
    "xcall_tx_origin": "0x49de9ec7e35015799724897832c3c75ab78c55b3",
    "execute_tx_origin": "0xf1bea24376329e01c7c10a00a6a7329591168cf9",
    "reconcile_tx_origin": "0x3d529c760f3ec4c89bdd6549ddabe9097c1da6e9",
    "relayer_fee": null,
    "error_status": null,
    "backoff": "32",
    "next_execution_timestamp": "0",
    "updated_slippage": null,
    "execute_simulation_input": null,
    "execute_simulation_from": null,
    "execute_simulation_to": null,
    "execute_simulation_network": null,
    "error_message": null,
    "datastream_metadata": {
      "uuid": "f9545ad6-34ba-4d4b-9ec7-a8f511001010",
      "source_timestamp": "1689103946153"
    },
    "relayer_fees": "{\"0x0000000000000000000000000000000000000000\":\"259661419728533\"}",
    "message_status": "Processed",
    "status": "CompletedFast",
    "execute_tx_nonce": "16806823910398",
    "reconcile_tx_nonce": "16806893500230",
    "routers": null
  }]
  ```


- Asset
  ```sql
  SELECT DISTINCT
    da.canonical_id
    , da.decimal
  FROM `mainnet-bigq.public.assets` da
  WHERE da.canonical_id = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
  ```

  output:
  
  ```json
  [{
    "canonical_id": "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "decimal": "6"
  }]
  ```

- Transfers USD

  ```sql
  SELECT *
  FROM  `public.transfers_in_usd` t
  WHERE t.transfer_id = "0xd3b48a423a57829f3970b76c4546c9ac67eaa46589f0048eee9e6bf45adab806"
  ```
  output:
  
  ```json
  "token_decimal": "6.0",
  "origin_chain": "BNB",
  "destination_chain": "Polygon",
  "caller_type": "EOA",
  "contract_name": null,
  "contract_author": null,
  "origin_asset": "USDC",
  "destination_asset": "USDC",
  "origin_transacting_amount": "221159783502651317642",
  "destination_transacting_amount": "220867466",
  "price": "0.99973945493082361",
  "usd_bridged_amt": "220.86153162015304",
  "usd_origin_amount": "221102161411559.59",
  "usd_destination_amount": "220.80992007079223"
  ```