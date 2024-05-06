WITH transfers_mapping AS (
SELECT *
FROM `mainnet-bigq.stage.stg_transfers_raw_usd`
LIMIT 10
),

transfers_amounts AS (
    Select 
    CAST(origin_transacting_amount AS NUMERIC) / POWER(10, origin_asset_decimals) as origin_amount,
    CAST(destination_transacting_amount AS NUMERIC) * POWER(10, 18 - destination_asset_decimals) as normalized_out,
    CAST(destination_transacting_amount AS NUMERIC) / POWER(10, destination_asset_decimals) as destination_amount,
    CAST(bridged_amt AS NUMERIC) / POWER(10, origin_asset_decimals) as bridged_amount,
    * 
    from transfers_mapping
),

--  relayerfees AS (
  router_regexp AS (
  SELECT 
  
    t.*,
    null AS relayerfee_address1,
    null AS relayerfee_amount1,
    null AS relayerfee_address2,
    null AS relayerfee_amount2,
    price AS asset_usd_price,
    usd_destination_amount AS usd_amount
 FROM transfers_amounts t
),

-- adding relay fee data later
router_mapping AS (
  SELECT
    t.*,
    COALESCE(rm.`name`, t.`router`)  AS router_name
  FROM router_regexp AS t
  LEFT JOIN `mainnet-bigq`.`raw`.`dim_connext_routers_name` AS rm ON LOWER(t.`router`) = LOWER(rm.`router`)
)

SELECT * FROM router_mapping

-- domain_name_fix AS (
--     SELECT 
--       CASE
--         WHEN destination_domain_name = '6648936' THEN 'Ethereum Mainnet'
--         WHEN destination_domain_name = '1869640809' THEN 'Optimistic Ethereum'
--         WHEN destination_domain_name = '6450786' THEN 'Binance Smart Chain Mainnet'
--         WHEN destination_domain_name = '6778479' THEN 'xDAI Chain'
--         WHEN destination_domain_name = '1886350457' THEN 'Matic Mainnet'
--         WHEN destination_domain_name = '1634886255' THEN 'Arbitrum One'
--         WHEN destination_domain_name = '1818848877' THEN 'Linea Mainnet'
--         WHEN destination_domain_name = '1835365481' THEN 'Metis Andromeda Mainnet'
--         WHEN destination_domain_name = '1650553709' THEN 'Base Mainnet'
--         WHEN destination_domain_name = '1836016741'THEN 'Mode Mainnet'
--         ELSE destination_domain_name
--       END AS destination_domain_name,
--     t.* EXCEPT (destination_domain_name)

--     FROM router_mapping AS t
-- )

-- SELECT * FROM domain_name_fix

