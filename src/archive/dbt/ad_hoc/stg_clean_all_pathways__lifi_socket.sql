  -- Output needed:
  -- Origin - Destination - Token - Amount - Date
  -- Metrics: Connext - pricing, gas costs, aggregated fees | Recommended pricing - pricing, gas costs, aggregated fees, Recommended bridge name
WITH
  lifi_raw AS (
  SELECT
    DISTINCT upload_datetime AS date,
    "lifi" AS aggregator,
    CAST(lr.route_fromchainid AS STRING) AS route_fromchainid,
    CAST(lr.route_tochainid AS STRING) AS route_tochainid,
    lr.route_fromtoken_symbol AS route_fromtoken_symbol,
    lr.route_totoken_symbol AS route_totoken_symbol,
    lr.fee_name,
    CAST(lr.route_fromamountusd AS FLOAT64) AS inputvalueinusd,
    CAST(lr.route_toamountusd AS FLOAT64) outputvalueinusd,
    CAST(lr.fee_amountusd AS FLOAT64) fee_amountusd,
    CAST(lr.route_gascostusd AS FLOAT64) AS totalgasfeesinusd,
    ( CAST(lr.route_toamountusd AS FLOAT64) - CAST(lr.route_gascostusd AS FLOAT64) - CAST(lr.fee_amountusd AS FLOAT64) ) AS receivedvalueinusd,
    -- calulations: in - gas -> received
    ARRAY_TO_STRING(lr.usedbridgenames_array, ",") AS usedbridgenames
  FROM (
    SELECT
      route_id,
      upload_datetime,
      ARRAY_AGG(tooldetails_key) AS usedbridgenames_array,
      route_fromchainid,
      route_tochainid,
      route_fromtoken_symbol,
      route_totoken_symbol,
      route_fromamountusd,
      route_toamountusd,
      route_tags,
      route_gascostusd,
      fee_amountusd,
      fee_name
  FROM `mainnet-bigq.stage.source_lifi__routes`
  WHERE DATE_TRUNC(upload_datetime, DAY) >= "2024-06-26"
    GROUP BY
      route_id,
      upload_datetime,
      route_fromchainid,
      route_tochainid,
      route_fromtoken_symbol,
      route_totoken_symbol,
      route_fromamountusd,
      route_toamountusd,
      route_tags,
      route_gascostusd,
      fee_amountusd,
      fee_name
    ORDER BY
      1 ) lr
    -- LIMIT 10
    ),
  socket_raw AS (
  SELECT
    DISTINCT upload_datetime AS date,
    "socket" AS aggregator,
    CAST(s.fromchainid AS STRING) AS route_fromchainid,
    CAST(s.tochainid AS STRING) AS route_tochainid,
    s.fromasset_symbol AS route_fromtoken_symbol,
    s.toasset_symbol AS route_totoken_symbol,
    s.integratorfee_asset_symbol AS fee_name,
    s.inputvalueinusd,
    s.outputvalueinusd,
    0 AS fee_amountusd,
    s.totalgasfeesinusd,
    s.receivedvalueinusd,
    -- calulations: in - gas -> received: s.outputvalueinusd - s.totalgasfeesinusd AS total_price
    COALESCE( REGEXP_REPLACE( REGEXP_EXTRACT(usedbridgenames, r'\[(.*?)\]'), r"'", '' ), usedbridgenames ) AS usedbridgenames
  FROM
    `mainnet-bigq.raw.source_socket__routes` s
  WHERE DATE_TRUNC(upload_datetime, DAY) >= "2024-06-26"
    ),
  raw AS (
  SELECT
    DISTINCT *
  FROM
    lifi_raw
  UNION ALL
  SELECT
    DISTINCT *
  FROM
    socket_raw 
  )
SELECT
  date,
  aggregator,
  usedbridgenames,
  route_fromchainid AS from_chain,
  route_tochainid AS to_chain,
  route_fromtoken_symbol AS from_token,
  route_totoken_symbol AS to_token,
  fee_name AS fee_type,
  inputvalueinusd AS in_value_usd,
  outputvalueinusd AS out_value_usd,
  fee_amountusd AS fee_usd,
  totalgasfeesinusd AS gas_fee,
  receivedvalueinusd AS final_out_amount,
  RANK() OVER (PARTITION BY date, route_fromchainid, route_tochainid, route_fromtoken_symbol, route_totoken_symbol ORDER BY receivedvalueinusd DESC ) AS max_value_rank_by_output
FROM
  raw