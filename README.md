# Connext Data pipline

List of integration and metadata on it.

## Integrations

- [x] Data Source: Dune

  - EVM Native Bridges

    - Tokens(Erc-20)

      ```sql
      SELECT * FROM `mainnet-bigq.dune.source_tokens_evm_eth_bridges` LIMIT 1000
      ```

    - ETH

      ```sql
      SELECT * FROM `mainnet-bigq.dune.source_native_evm_eth_bridges` LIMIT 1000
      ```

    - Clean Native EVM ETH
      This table is a view created from the ETH Table above. Same can be done with Tokens(Erc-20)

      ```sql
      SELECT * FROM `mainnet-bigq.dune.clean_native_evm_bridge_transfers__daily_agg`
      ```

      Output

      ````json
          [
              {
              "date": "2024-03-24 00:00:00.000000 UTC",
              "from_address": "0x337a81113f934b8522459f0ca207eee7aa0f4545",
              "to_address": "0xf8a16864d8de145a266a534174305f881ee2315e",
              "tc_from": "0x32400084c286cf3e17e7b677ea9583e60a000324",
              "tc_to": "0x9fbc5eb9186d1be59ee636d83b286af37aba2983",
              "bridge": "zkSync Era Bridge",
              "tx_type": "withdrawal",
              "source_chain_name": "Ethereum",
              "destination_chain_name": "zkSync Era",
              "value": "0.0103",
              "value_usd": "34.283962",
              "gas_used": "1.30728e-12",
              "fee_usd": "83.543808038340842",
              "tx_count": "1"
              }
          ]
          ```
      ````

  - Stargate

    ```sql
    SELECT * FROM `mainnet-bigq.dune.source_stargate_bridges` LIMIT 1000
    ```

    Output

    ```json
    [
      {
        "date": "2024-03-26 00:00:00.000000 UTC",
        "source_chain_name": "Optimism",
        "destination_chain_name": "Ethereum",
        "user_address": "0x4e8d7b9618c61b183300a29e2db65575beb2898e",
        "transfer_type": "erc20",
        "currency_symbol": "DAI",
        "amount_usd": "10.0173150357",
        "tx_fee_usd": "0.0418849727919359",
        "tx_count": "1",
        "_dlt_load_id": "1711555736.080353",
        "_dlt_id": "symzL66HMjcVRQ",
        "tx_fee_usd__v_text": null
      }
    ]
    ```

---

- [x] Data Soure: Synapse Protocol Explorer

  ```sql
  SELECT * FROM `mainnet-bigq.raw.source_synapseprotocol_explorer_transactions` LIMIT 1000
  ```

  Output

  ```json
  [
    {
      "from_chain_id": "250",
      "from_destination_chain_id": "137",
      "from_address": "0x8D869f609beD9516596D8563E26d4870f5f1AaFB",
      "from_hash": "0x7d05645bde68879ad6d84f2452084bb97866997f80a9965c74bb391faa3b5715",
      "from_value": "400000000",
      "from_formatted_value": "400",
      "from_token_address": "0x04068DA6C83AFCFA0e13ba15A6696662335D5B75",
      "from_token_symbol": "nUSD",
      "from_time": "1676998216",
      "from_event_type": "6",
      "to_chain_id": "137",
      "to_destination_chain_id": null,
      "to_address": "0x8D869f609beD9516596D8563E26d4870f5f1AaFB",
      "to_hash": "0x72a10758001bb467e78dcfed41e6de0c68aa5d656d0f47ed767f6fa772aa6c5c",
      "to_value": "399314201",
      "to_formatted_value": "399.3142",
      "to_token_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
      "to_token_symbol": "nUSD",
      "to_time": "1676998268",
      "to_event_type": "5",
      "kappa": "d6f9fbfe7f35ef9d464430ace18faf0512af3f31e069bfe4b112e64d09753922",
      "pending": "false",
      "swap_success": "true",
      "_dlt_load_id": "1712648737.8738654",
      "_dlt_id": "m9LCzVolIZpwbQ"
    }
  ]
  ```

---

- [x] Data Soure: Symbiosis Protocol Explorer

  ```sql
  SELECT * FROM `mainnet-bigq.raw.source_symbiosis_bridge_explorer_transactions` LIMIT 1000
  ```

  Output

  ```json
  [
    {
      "id": "999356",
      "from_client_id": "symbiosis-app",
      "from_chain_id": "59144",
      "from_tx_hash": "0x13f39539622c88673be86b33d2195a8637699630a5f03db810d1d38b0edc8e93",
      "join_chain_id": "56288",
      "join_tx_hash": "0x1fa02117579763ce60d33a8856e45750544389f0a08c3c4dc52b3461dc70f05b",
      "to_chain_id": "169",
      "to_tx_hash": "0xaa4dd08ca4ec319367f78b485ad0cdd053dc88fa052731023654d1bcf55164bf",
      "event_type": "1",
      "type": "0",
      "hash": "0x13f39539622c88673be86b33d2195a8637699630a5f03db810d1d38b0edc8e93",
      "state": "0",
      "created_at": "2024-01-25 02:35:01",
      "mined_at": "2024-01-25 02:36:09",
      "success_at": "2024-01-25 02:38:49",
      "from_address": "0xA2d311474f29d0f99D1318d286531ceC2140610d",
      "from_sender": "0xA2d311474f29d0f99D1318d286531ceC2140610d",
      "duration": "68",
      "to_address": "0xA2d311474f29d0f99D1318d286531ceC2140610d",
      "to_sender": "0xd99ac0681b904991169a4f398B9043781ADbe0C3",
      "amounts": "[14078474, 13286125]",
      "tokens": "[{\"symbol\": \"USDC\", \"name\": \"USD//C\", \"address\": \"0x176211869ca2b568f2a7d4ee941e073a821ee1ff\", \"decimals\": 6}]",
      "from_route": "[{\"chain_id\": 59144, \"amount\": 14078474, \"token\": {\"symbol\": \"USDC\", \"name\": \"USD//C\", \"address\": \"0x176211869ca2b568f2a7d4ee941e073a821ee1ff\", \"decimals\": 6}}]",
      "to_route": "[{\"chain_id\": 169, \"amount\": 13286125, \"token\": {\"symbol\": \"USDC\", \"name\": \"USD Coin\", \"address\": \"0xb73603c5d87fa094b7314c74ace2e64d165016fb\", \"decimals\": 6}}]",
      "transit_token": "null",
      "from_amount_usd": "14.078474",
      "to_amount_usd": "13.255461",
      "to_tx_id": "999354",
      "retry_active": "false",
      "_dlt_load_id": "1711987486.6091216",
      "_dlt_id": "eg1U75CLsdpyvQ",
      "token_symbol": "USDC",
      "token_name": "USD//C",
      "token_address": "0x176211869ca2b568f2a7d4ee941e073a821ee1ff",
      "token_decimals": "6"
    }
  ]
  ```

---

- [x] Data Soure: Orbiter Finance Explorer

  ```sql
  SELECT * FROM `mainnet-bigq.raw.source_orbiter_explorer__transactions` LIMIT 1
  ```

  Output

  ```json
  [
    {
      "from_hash": "0x5b9564f2cf9f278568a72ae5e69f0f5900dd72384d0176f457f6986f5174d4d6",
      "to_hash": "0x58b338a296601e3b4ca7fdf7296f5698c82bef153fd59f2b7be443a2b5df8c38",
      "from_chain_id": "1",
      "to_chain_id": "10",
      "from_value": "0.0101290000000181",
      "to_value": "0.00842773565",
      "from_amount": "0.0101290000000181",
      "to_amount": "0.00842773565",
      "from_symbol": "ETH",
      "status": "99",
      "from_timestamp": "1704372827000",
      "to_timestamp": "1704372965000.0",
      "source_address": "0x11b09ead8ceee358ee6106e3870a6999a0ee7d22",
      "target_address": "0x11b09ead8ceee358ee6106e3870a6999a0ee7d22",
      "source_maker": "0x8086061cf07c03559fbb4aa58f191f9c4a5df2b2",
      "target_maker": "0x8086061cf07c03559fbb4aa58f191f9c4a5df2b2",
      "source_token": "0x0000000000000000000000000000000000000000",
      "target_token": "0x0000000000000000000000000000000000000000",
      "source_decimal": "18",
      "target_decimal": "18",
      "challenge_status": null
    }
  ]
  ```

---

- [ ] Data Soure: Hop Explorer

  ```sql
  SELECT * FROM `mainnet-bigq.stage.source_hop_explorer__transfers` LIMIT 1
  ```

  Output

  ```json
  [
    {
      "id": "54727",
      "transferid": "54727",
      "transactionhash": "0x1744110cc6fdd60f4e62949379bf719d2ac37a0e49fbf9c451311cb2d72b5624",
      "sourcechainid": "8453",
      "destinationchainid": "10",
      "accountaddress": "0xb7fda9fab3215b27978fbc701844df331216fd8a",
      "amount": "3905552925",
      "amountusd": "3903.0",
      "amountusddisplay": "$3,903.579",
      "amountoutmin": null,
      "deadline": "0",
      "recipientaddress": "0xb7fda9fab3215b27978fbc701844df331216fd8a",
      "bonderfee": "10123",
      "bonderfeeusd": "0.0",
      "bonderfeeusddisplay": "$0.01",
      "bonded": "false",
      "bondtimestamp": "0",
      "bondtimestampiso": null,
      "bondwithintimestamp": "0.0",
      "bondtransactionhash": null,
      "bonderaddress": null,
      "token": "USDC",
      "tokenpriceusd": "0.0",
      "tokenpriceusddisplay": "$0.999",
      "timestamp": "1711688075",
      "preregenesis": "false",
      "receivedhtokens": "false",
      "unbondable": "false",
      "amountreceived": null,
      "amountreceivedformatted": null,
      "origincontractaddress": "0xe7f40bf16ab09f4a6906ac2caa4094ad2da48cc2",
      "integrationpartner": null,
      "integrationpartnercontractaddress": null,
      "accountaddresstruncated": "0xb7fd…fd8a",
      "transactionhashtruncated": "0x174411…2b5624",
      "transferidtruncated": "54727",
      "timestampiso": "2024-03-29T04:54:35.000+00:00",
      "relativetimestamp": "11 minutes ago",
      "sourcechainslug": "base",
      "destinationchainslug": "optimism",
      "sourcechainname": "Base",
      "destinationchainname": "Optimism",
      "sourcechainimageurl": "https://assets.hop.exchange/logos/base.svg",
      "destinationchainimageurl": "https://assets.hop.exchange/logos/optimism.svg",
      "transactionhashexplorerurl": "https://basescan.org/tx/0x1744110cc6fdd60f4e62949379bf719d2ac37a0e49fbf9c451311cb2d72b5624",
      "bondtransactionhashexplorerurl": null,
      "accountaddressexplorerurl": "https://basescan.org/address/0xb7fda9fab3215b27978fbc701844df331216fd8a",
      "recipientaddresstruncated": "0xb7fd…fd8a",
      "recipientaddressexplorerurl": "https://optimistic.etherscan.io/address/0xb7fda9fab3215b27978fbc701844df331216fd8a",
      "bonderaddresstruncated": null,
      "bonderaddressexplorerurl": null,
      "bondtransactionhashtruncated": null,
      "receivestatusunknown": "false",
      "relativebondedtimestamp": null,
      "bondwithintimestamprelative": null,
      "amountformatted": "3905.0",
      "amountdisplay": "3905.5529",
      "bonderfeeformatted": "0.0",
      "bonderfeedisplay": "0.0101",
      "tokenimageurl": "https://assets.hop.exchange/logos/usdc.svg",
      "i": "93",
      "amountoutminformatted": "0.0",
      "timestamprelative": "11 minutes ago",
      "bondtimestamprelative": null,
      "sourcechaincolor": "#0052ff",
      "destinationchaincolor": "#e64b5d",
      "bondstatuscolor": "#ffc55a",
      "converthtokenurl": "https://app.hop.exchange/#/convert/amm?token\u003dUSDC\u0026sourceNetwork\u003doptimism\u0026fromHToken\u003dtrue",
      "hopexplorerurl": "https://explorer.hop.exchange/?transferId\u003d54727",
      "integrationpartnername": null,
      "integrationpartnerimageurl": null,
      "estimatedunixtimeuntilbond": "1711689875",
      "estimatedsecondsuntilbond": "1091",
      "estimatedrelativetimeuntilbond": "0",
      "request_url": "https://explorer-api.hop.exchange/v1/transfers?startDate\u003d2024-03-29\u0026endDate\u003d2024-03-29\u0026page\u003d1"
    }
  ]
  ```

---

- [x] Data Soure: DeBridge Explorer

  ```sql
  SELECT * FROM `mainnet-bigq.raw.source_de_bridge_explorer__transactions` LIMIT 1
  ```

  Output

  ```json
  [
    {
      "creationtimestamp": "1711164442",
      "state": "ClaimedUnlock",
      "externalcallstate": "Completed",
      "orderid_bytesvalue": "0hpjwN0D31KgJ9HdYmcELg/RazU7dC00oE6FIE0NLDQ\u003d",
      "orderid_stringvalue": "0xd21a63c0dd03df52a027d1dd6267042e0fd16b353b742d34a04e85204d0d2c34",
      "giveofferwithmetadata_chainid_bigintegervalue": "42161",
      "giveofferwithmetadata_tokenaddress_stringvalue": "0x0000000000000000000000000000000000000000",
      "giveofferwithmetadata_amount_bigintegervalue": "4993000000000000",
      "giveofferwithmetadata_finalamount_bigintegervalue": "4993000000000000",
      "giveofferwithmetadata_metadata_decimals": "18",
      "giveofferwithmetadata_metadata_name": "Ethereum",
      "giveofferwithmetadata_metadata_symbol": "ETH",
      "giveofferwithmetadata_decimals": "18",
      "giveofferwithmetadata_name": "Ethereum",
      "giveofferwithmetadata_symbol": "ETH",
      "takeofferwithmetadata_chainid_bigintegervalue": "137",
      "takeofferwithmetadata_amount_bigintegervalue": "16257993",
      "takeofferwithmetadata_finalamount_bigintegervalue": "4993000000000000",
      "takeofferwithmetadata_metadata_decimals": "6",
      "takeofferwithmetadata_metadata_name": "(PoS) Tether USD",
      "takeofferwithmetadata_metadata_symbol": "USDT",
      "takeofferwithmetadata_decimals": "6",
      "takeofferwithmetadata_name": "(PoS) Tether USD",
      "takeofferwithmetadata_symbol": "USDT",
      "finalpercentfee_bigintegervalue": "2000000000000.0",
      "fixfee_bigintegervalue": "1000000000000000",
      "fixfee_stringvalue": "1000000000000000",
      "unlockauthoritydst_stringvalue": "0x4846aee6d7c9f176f3f329e01a014c2794e21b92",
      "preswapdata_chainid_bigintegervalue": null,
      "preswapdata_inamount_bigintegervalue": null,
      "preswapdata_tokeninmetadata_name": null,
      "preswapdata_tokeninmetadata_symbol": null,
      "preswapdata_outamount_bigintegervalue": null,
      "preswapdata_tokenoutmetadata_name": null,
      "preswapdata_tokenoutmetadata_symbol": null,
      "ordermetadata_creationprocesstype": "SrcAmountSet",
      "ordermetadata_origin": "Default",
      "ordermetadata_operatingexpensesamount": "45566489065011",
      "ordermetadata_recommendedtakeamount": "16257993"
    }
  ]
  ```

---

- [x] Data Soure: AllBridges Explorer

  ```sql
    1. Tokens
    SELECT * FROM `mainnet-bigq.raw.source_all_bridge_explorer_tokens` LIMIT 1

    2. Transfers
    SELECT * FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers` LIMIT 1
  ```

  Output

  ```json
  [
    {
      "id": "0x441d45676557a74295d217e11f273f6b45a5d2dfd37c21a5a0e25f3bc09e1685",
      "status": "Complete",
      "timestamp": "1702298657000",
      "from_chain_symbol": "POL",
      "to_chain_symbol": "POL",
      "from_amount": "97.918901",
      "stable_fee": "0",
      "from_token_address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
      "to_token_address": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
      "from_address": "0x8f2d1e4630b395eb31090726aa67131e781c243e",
      "to_address": "0x8f2d1e4630b395eb31090726aa67131e781c243e",
      "messaging_type": "None",
      "partner_id": "0",
      "from_gas": "0",
      "to_gas": "0",
      "relayer_fee_in_native": "0",
      "relayer_fee_in_tokens": "0",
      "send_transaction_hash": "0x441d45676557a74295d217e11f273f6b45a5d2dfd37c21a5a0e25f3bc09e1685",
      "receive_transaction_hash": "0x441d45676557a74295d217e11f273f6b45a5d2dfd37c21a5a0e25f3bc09e1685",
      "api_url": "https://explorer-variant-filter.api.allbridgecoreapi.net/transfers?status\u003dComplete\u0026page\u003d18861\u0026limit\u003d20",
      "_dlt_load_id": "1712604517.1451735",
      "_dlt_id": "8Wd9k7IUz5qiKA"
    }
  ]
  ```

---

- [x] Data Soure: Across Explorer: Daily Aggreagted Data

  ```sql
  SELECT * FROM `mainnet-bigq.dune.source_across_aggregator_daily` LIMIT 1000
  ```

  Output

  ```json
  [
    {
      "date": "2024-03-05 00:00:00.000000 UTC",
      "user": "0xadde7028e7ec226777e5dea5d53f6457c21ec7d6",
      "src_chain": "zksync",
      "dst_chain": "base",
      "token_symbol": "USDbC",
      "tx_count": "6",
      "avg_token_price": "1.0000570868055547",
      "value_usd": "31335.027137489255",
      "relay_fee_in_usd": "29.307221359713708",
      "lp_fee_in_usd": "0.0",
      "_dlt_load_id": "1711701865.097749",
      "_dlt_id": "PxTpXupPBGVoVg"
    }
  ]
  ```

---

- [x] Data Soure: Defilamma

  Cleaned data Tables

  1. Bridges by Tokens aggs.

     ```sql
     SELECT * FROM `mainnet-bigq.raw.stg__cln_source_defilamma_bridges_history_tokens` LIMIT 1
     ```

     Output

     ```json
     [
       {
         "name": "Portal by Wormhole",
         "api_url": "https://bridges.llama.fi/bridgedaystats/1701648000/Klaytn?id\u003d9",
         "date": "1701648000",
         "bridge_id": "9",
         "timestamp": "1701648000",
         "chain": "Klaytn",
         "chain_slug": "klaytn",
         "token_address": "0x7ee2ab1443fdb59aecea3add8f81296d68c2d1e3",
         "tx_type": "withdrawal",
         "symbol": "",
         "decimals": "0",
         "usd_value": "0.0",
         "chain_id": "8217"
       }
     ]
     ```

  2. Bridges by Wallets aggs.

     ```sql
     SELECT * FROM `mainnet-bigq.raw.stg__cln_source_defilamma_bridges_history_wallets` LIMIT 1
     ```

     Output

     ```json
     [
       {
         "name": "Connext",
         "api_url": "https://bridges.llama.fi/bridgedaystats/1712534400/Metis?id\u003d59",
         "date": "1712534400",
         "bridge_id": "59",
         "timestamp": "1712534400",
         "chain": "Metis",
         "chain_slug": "metis",
         "wallet_address": "0x24ca98fB6972F5eE05f0dB00595c7f68D9FaFd68",
         "tx_type": "deposit",
         "usd_value": "6930.9563204918777",
         "txs": "52",
         "chain_id": "1088"
       }
     ]
     ```

  **Raw data**

  1. Stables

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_defilamma_stables` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "id": "165",
         "name": "AUDD",
         "symbol": "AUDD",
         "gecko_id": "novatti-australian-digital-dollar",
         "peg_type": "peggedAUD",
         "peg_mechanism": "fiat-backed",
         "circulating": "2192247.0476966",
         "circulating_prev_day": "2544655.2676966",
         "circulating_prev_week": "2323890.3856626",
         "circulating_prev_month": "2257861.8877018997",
         "price": "0.658726",
         "delisted": null,
         "chains": "[\"Stellar\",\"Ethereum\",\"Solana\"]",
         "_dlt_load_id": "1712620815.778221",
         "_dlt_id": "lAKj2JaL8qoBMQ",
         "upload_timestamp": "2024-04-09 00:00:16.000000 UTC"
       }
     ]
     ```

  2. Bridges Metadata

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_defilamma_bridges` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "id": "2",
         "name": "arbitrum",
         "display_name": "Arbitrum Bridge",
         "icon": "chain:arbitrum",
         "volume_prev_day": "15436835.0",
         "volume_prev2_day": "9334394.0",
         "last_hourly_volume": "161865.13389182",
         "current_day_volume": "15436835.143362384",
         "last_daily_volume": "15436835.0",
         "day_before_last_volume": "9334394.0",
         "weekly_volume": "105834092.5",
         "monthly_volume": "476219710.0",
         "chains": "[\"Ethereum\",\"Arbitrum\"]",
         "destination_chain": "Arbitrum",
         "upload_timestamp": "2024-04-09 00:00:20.000000 UTC",
         "_dlt_load_id": "1712620815.778221",
         "_dlt_id": "uachT600X3XdHQ"
       }
     ]
     ```

  3. Bridges Aggreagted by Tokens and Date

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_defilamma_bridges_history_tokens` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "date": "1701648000",
         "status_code": "200",
         "url": "https://bridges.llama.fi/bridgedaystats/1701648000/Klaytn?id\u003d9",
         "key_type": "totalTokensWithdrawn",
         "key": "klaytn:0x7ee2ab1443fdb59aecea3add8f81296d68c2d1e3",
         "usd_value": "0.0",
         "txs": null,
         "upload_timestamp": "2024-04-08 00:00:47.000000 UTC",
         "amount": "100817371",
         "symbol": "",
         "decimals": "0",
         "_dlt_load_id": "1712534444.2075493",
         "_dlt_id": "maQrHhaH01qKRA"
       }
     ]
     ```

  4. Bridges Aggreagted by Wallets and Date

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_defilamma_bridges_history_wallets` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "date": "1712448000",
         "status_code": "200",
         "url": "https://bridges.llama.fi/bridgedaystats/1712448000/Scroll?id\u003d10",
         "key_type": "totalAddressDeposited",
         "key": "scroll:0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
         "usd_value": "29526.2214322",
         "txs": "11",
         "upload_timestamp": "2024-04-08 19:21:50.000000 UTC",
         "amount": null,
         "symbol": null,
         "decimals": null,
         "_dlt_load_id": "1712584301.99512",
         "_dlt_id": "HYn7dHG8MActlg"
       }
     ]
     ```

  5. Chains Metadata

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_defilamma_chains` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "gecko_id": null,
         "tvl": "0.0",
         "token_symbol": null,
         "cmc_id": null,
         "name": "Boba_Avax",
         "chain_id": null,
         "upload_timestamp": "2024-04-09 00:00:22.000000 UTC",
         "_dlt_load_id": "1712620815.778221",
         "_dlt_id": "3CRe08Es2ETm5Q"
       }
     ]
     ```

- [x] Data Soure: Prod mainnet TS file: Tokens, Chains and Routers metadata

  1. Assets

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_monorepo__prod_mainnet_assets` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "name": "USDT",
         "canonical_domain": "6648936",
         "canonical_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
         "canonical_decimals": "6",
         "representation_domain": "1869640809",
         "representation_local": "0x4cbb28fa12264cd8e87c62f4e1d9f5955ce67d20",
         "representation_adopted": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
         "upload_timestamp": "2024-04-08 15:16:54.901975 UTC"
       }
     ]
     ```

  2. Chains <> Domains

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_monorepo__prod_mainnet_supported_domains` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "id": "6648936",
         "chain": "MAINNET",
         "upload_timestamp": "2024-04-08 15:16:49.784385 UTC"
       }
     ]
     ```

  3. Routers

     ```sql
     SELECT * FROM `mainnet-bigq.raw.source_monorepo__prod_mainnet_routes_config` LIMIT 1000
     ```

     Output

     ```json
     [
       {
         "address": "0x5f4E31F4F402E368743bF29954f80f7C4655EA68",
         "name": "Amber",
         "upload_timestamp": "2024-04-08 15:16:42.573527 UTC"
       }
     ]
     ```
