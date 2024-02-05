WITH 
connext_contracts AS (
  SELECT * FROM `mainnet-bigq.public.connext_contracts`
)

, assets AS (
  SELECT DISTINCT
    da.canonical_id
  , da.decimal
  FROM `mainnet-bigq.public.assets` da
)

, transfer_raw AS (
SELECT 
  t.transfer_id,
  t.canonical_id,
  t.xcall_transaction_hash,
  t.xcall_caller,
  t.`to`,
  t.origin_sender,
  t.bridged_amt,
  t.origin_transacting_asset,
  t.origin_transacting_amount,
  t.destination_transacting_asset,
  t.destination_transacting_amount,
  t.xcall_tx_origin,
  t.execute_tx_origin,
  t.xcall_timestamp AS xcall_timestamp,
  TIMESTAMP_SECONDS(t.execute_timestamp) AS execute_timestamp,
  TIMESTAMP_SECONDS(t.reconcile_timestamp) AS reconcile_timestamp,
  CASE
    WHEN t.origin_domain = '6648936' THEN 'Ethereum'
    WHEN t.origin_domain = '1869640809' THEN 'Optimism'
    WHEN t.origin_domain = '6450786' THEN 'BNB'
    WHEN t.origin_domain = '6778479' THEN 'Gnosis'
    WHEN t.origin_domain = '1886350457' THEN 'Polygon'
    WHEN t.origin_domain = '1634886255' THEN 'Arbitrum One'
    ELSE t.origin_domain
  END AS origin_chain,
  CASE
    WHEN t.destination_domain = '6648936' THEN 'Ethereum'
    WHEN t.destination_domain = '1869640809' THEN 'Optimism'
    WHEN t.destination_domain = '6450786' THEN 'BNB'
    WHEN t.destination_domain = '6778479' THEN 'Gnosis'
    WHEN t.destination_domain = '1886350457' THEN 'Polygon'
    WHEN t.destination_domain = '1634886255' THEN 'Arbitrum One'
    ELSE t.destination_domain
  END AS destination_chain,
  CASE
    WHEN LOWER(t.xcall_caller) != LOWER(t.xcall_tx_origin)
      THEN 'Contract'
    ELSE 'EOA'
  END AS caller_type,
  cc.contract_name,
  cc.contract_author,
  -- START ORIGIN ASSET MAPPING
  CASE
    -- TOKENS WITHOUT xERC20 BELOW
    WHEN origin_transacting_asset IN (
      '0xddafbb505ad214d7b80b1f830fccc89b60fb7a83', 
      '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d', 
      '0x7f5c764cbc14f9669b88837ca1490cca17c31607', 
      '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8', 
      '0x2791bca1f2de4661ed88a30c99a7a9449aa84174', 
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      ) 
    THEN 'USDC'
    WHEN origin_transacting_asset IN (
      '0x44cf74238d840a5febb0eaa089d05b763b73fab8', 
      '0x67e51f46e8e14d4e4cab9df48c59ad8f512486dd', 
      '0x8c556cf37faa0eedac7ae665f1bb0fbd4b2eae36', 
      '0xf96c6d2537e1af1a9503852eb2a4af264272a5b6', 
      '0x5e7d83da751f4c9694b13af351b30ac108f32c38'
      ) 
    THEN 'nextUSDC'
    ---
    WHEN origin_transacting_asset IN (
      '0xdAC17F958D2ee523a2206206994597C13D831ec7', 
      '0xc2132D05D31c914a87C6611C10748AEb04B58e8F', 
      '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9', 
      '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58', 
      '0x4ECaBa5870353805a9F068101A40E0f32ed605C6', 
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      ) 
    THEN 'USDT'
    WHEN origin_transacting_asset IN (
      '0xD609f26B5547d5E31562B29150769Cb7c774B97a', 
      '0xE221C5A2a8348f12dcb2b0e88693522EbAD2690f', 
      '0x4cBB28FA12264cD8E87C62F4E1d9f5955Ce67D20', 
      '0x2fD7E61033b3904c65AA9A9B83DCd344Fa19Ffd2', 
      '0xF4d944883D6FddC56d3534986feF82105CaDbfA1'
      ) 
    THEN 'nextUSDT'
    WHEN origin_transacting_asset IN (
      '0x6B175474E89094C44Da98b954EedeAC495271d0F', 
      '0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3', 
      '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063', 
      '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1', 
      '0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d'
      ) 
    THEN 'DAI'
    WHEN origin_transacting_asset IN (
      '0x86a343BCF17D79C475d300eed35F0145F137D0c9', 
      '0xaDCe87b14d570665222C1172D18a221BF7690d5a', 
      '0xd64Bd028b560bbFc732eA18f282c64B86F3468e0', 
      '0xfDe99b3B3fbB69553D7DaE105EF34Ba4FE971190', 
      '0x0e1D5Bcd2Ac5CF2f71841A9667afC1E995CaAf4F'
      ) 
    THEN 'nextDAI'
    WHEN origin_transacting_asset IN (
      '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 
      '0x2170Ed0880ac9A755fd29B2688956BD959F933F8', 
      '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619', 
      '0x4200000000000000000000000000000000000006', 
      '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
      '0x6A023CCd1ff6F2045C3309768eAd9E68F978f6e1'
      ) 
    THEN 'WETH'
    WHEN origin_transacting_asset IN (
      '0xA9CB51C666D2AF451d87442Be50747B31BB7d805', 
      '0x4b8BaC8Dd1CAA52E32C07755c17eFadeD6A0bbD0', 
      '0xbAD5B3c68F855EaEcE68203312Fd88AD3D365e50', 
      '0x2983bf5c334743Aa6657AD70A55041d720d225dB', 
      '0x538E2dDbfDf476D24cCb1477A518A82C9EA81326'
      ) 
    THEN 'nextWETH'
    WHEN origin_transacting_asset IN (
      '0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44', 
      '0x4a2be2075588bce6a7e072574698a7dbbac39b08', 
      '0xca87472dbfb041c2e5a2672d319ea6184ad9755e'
      ) 
      THEN 'KP3R'
    WHEN origin_transacting_asset IN (
      '0x3f6740b5898c5d3650ec6eace9a649ac791e44d7', 
      '0x7cf93c434260519537184631a347ee8ad0bc68cb', 
      '0xf232d1afbed9df3880143d4fad095f3698c4d1c6'
      ) 
      THEN 'kLP'
    WHEN origin_transacting_asset IN (
      '0x538e2ddbfdf476d24ccb1477a518a82c9ea81326', 
      '0x2983bf5c334743aa6657ad70a55041d720d225db', 
      '0x4b8bac8dd1caa52e32c07755c17efaded6a0bbd0', 
      '0xa9cb51c666d2af451d87442be50747b31bb7d805', 
      '0xbad5b3c68f855eaece68203312fd88ad3d365e50'
      ) 
      THEN 'nextWETH'
    WHEN origin_transacting_asset IN (
      '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', 
      '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619', 
      '0x2170ed0880ac9a755fd29b2688956bd959f933f8', 
      '0x4200000000000000000000000000000000000006', 
      '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 
      '0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1'
      ) 
      THEN 'WETH'
    WHEN origin_transacting_asset IN (
      '0x4cbb28fa12264cd8e87c62f4e1d9f5955ce67d20',
      '0xe221c5a2a8348f12dcb2b0e88693522ebad2690f',
      '0x2fd7e61033b3904c65aa9a9b83dcd344fa19ffd2',
      '0xf4d944883d6fddc56d3534986fef82105cadbfa1',
      '0xd609f26b5547d5e31562b29150769cb7c774b97a'
    )
    THEN 'nextUSDT'
    WHEN origin_transacting_asset IN (
      '0x0100546F2cD4C9D97f798fFC9755E47865FF7Ee6',
      '0x303241e2B3b4aeD0bb0F8623e7442368FED8Faf3'
    )
    THEN 'nextAlETH'
    WHEN origin_transacting_asset IN (
      '0x49000f5e208349D2fA678263418e21365208E498'
    )
    THEN 'nextAlUSD'
    -- xERC20 TOKENS BELOW
    WHEN origin_transacting_asset IN (
      '0xFE67A4450907459c3e1FFf623aA927dD4e28c67a',
      '0x58b9cB810A68a7f3e1E4f8Cb45D1B9B3c79705E8'
    )
    THEN 'NEXT'
    WHEN origin_transacting_asset IN (
      '0x58b9cB810A68a7f3e1E4f8Cb45D1B9B3c79705E8'
    )
    THEN 'xNEXT'
    WHEN origin_transacting_asset IN (
      '0xa411c9Aa00E020e4f88Bc19996d29c5B7ADB4ACf'
    )
    THEN 'XOC'
    WHEN origin_transacting_asset IN (
      '0x44709a920fCcF795fbC57BAA433cc3dd53C44DbE',
      '0x489580eB70a50515296eF31E8179fF3e77E24965'
    )
    THEN 'RADAR'
    WHEN origin_transacting_asset IN (
      '0x202426c15a18a0e0fE3294415E66421891E2EB7C'
    )
    THEN 'xRADAR'
    WHEN origin_transacting_asset IN (
      '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      '0x27b58D226fe8f792730a795764945Cf146815AA7',
      '0xE974B9b31dBFf4369b94a1bAB5e228f35ed44125'
    )
    THEN 'ALCX'
    WHEN origin_transacting_asset IN (
      '0xbd18f9be5675a9658335e6b7e79d9d9b394ac043'
    )
    THEN 'xALCX'
    WHEN origin_transacting_asset IN (
      '0x0D505C03d30e65f6e9b4Ef88855a47a89e4b7676',
      '0xBB1B173cdFBe464caaaCeaB2a9c8C44229d62D14',
      '0xb2588731d8f6F854037936d6ffac4c13d0b6bd62'
    )
    THEN 'ZOOMER'
    WHEN origin_transacting_asset IN (
      '0x772fCe4B8E88BD19e86dC92428d242704aC480a0'
    )
    THEN 'P8'
    WHEN origin_transacting_asset IN (
      '0x2bF2ba13735160624a0fEaE98f6aC8F70885eA61',
      '0xbD80CFA9d93A87D1bb895f810ea348E496611cD4'
    )
    THEN 'FRACTION'
    WHEN origin_transacting_asset IN (
      '0x4602e7CFE18d8b16ED13538603B00073F5c28bc8'
    )
    THEN 'xFRACTION'  
    ELSE origin_transacting_asset
  END AS origin_asset,
  -- END ORIGIN ASSET MAPPING 
 
    -- START DESTINATION ASSET MAPPING
  CASE
    -- TOKENS WITHOUT xERC20 BELOW
    WHEN destination_transacting_asset IN (
      '0xddafbb505ad214d7b80b1f830fccc89b60fb7a83', 
      '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d', 
      '0x7f5c764cbc14f9669b88837ca1490cca17c31607', 
      '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8', 
      '0x2791bca1f2de4661ed88a30c99a7a9449aa84174', 
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      ) 
    THEN 'USDC'
    WHEN destination_transacting_asset IN (
      '0x44cf74238d840a5febb0eaa089d05b763b73fab8', 
      '0x67e51f46e8e14d4e4cab9df48c59ad8f512486dd', 
      '0x8c556cf37faa0eedac7ae665f1bb0fbd4b2eae36', 
      '0xf96c6d2537e1af1a9503852eb2a4af264272a5b6', 
      '0x5e7d83da751f4c9694b13af351b30ac108f32c38'
      ) 
    THEN 'nextUSDC'
    ---
    WHEN destination_transacting_asset IN (
      '0xdAC17F958D2ee523a2206206994597C13D831ec7', 
      '0xc2132D05D31c914a87C6611C10748AEb04B58e8F', 
      '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9', 
      '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58', 
      '0x4ECaBa5870353805a9F068101A40E0f32ed605C6', 
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      ) 
    THEN 'USDT'
    WHEN destination_transacting_asset IN (
      '0xD609f26B5547d5E31562B29150769Cb7c774B97a', 
      '0xE221C5A2a8348f12dcb2b0e88693522EbAD2690f', 
      '0x4cBB28FA12264cD8E87C62F4E1d9f5955Ce67D20', 
      '0x2fD7E61033b3904c65AA9A9B83DCd344Fa19Ffd2', 
      '0xF4d944883D6FddC56d3534986feF82105CaDbfA1'
      ) 
    THEN 'nextUSDT'
    WHEN destination_transacting_asset IN (
      '0x6B175474E89094C44Da98b954EedeAC495271d0F', 
      '0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3', 
      '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063', 
      '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1', 
      '0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d'
      ) 
    THEN 'DAI'
    WHEN destination_transacting_asset IN (
      '0x86a343BCF17D79C475d300eed35F0145F137D0c9', 
      '0xaDCe87b14d570665222C1172D18a221BF7690d5a', 
      '0xd64Bd028b560bbFc732eA18f282c64B86F3468e0', 
      '0xfDe99b3B3fbB69553D7DaE105EF34Ba4FE971190', 
      '0x0e1D5Bcd2Ac5CF2f71841A9667afC1E995CaAf4F'
      ) 
    THEN 'nextDAI'
    WHEN destination_transacting_asset IN (
      '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 
      '0x2170Ed0880ac9A755fd29B2688956BD959F933F8', 
      '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619', 
      '0x4200000000000000000000000000000000000006', 
      '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
      '0x6A023CCd1ff6F2045C3309768eAd9E68F978f6e1'
      ) 
    THEN 'WETH'
    WHEN destination_transacting_asset IN (
      '0xA9CB51C666D2AF451d87442Be50747B31BB7d805', 
      '0x4b8BaC8Dd1CAA52E32C07755c17eFadeD6A0bbD0', 
      '0xbAD5B3c68F855EaEcE68203312Fd88AD3D365e50', 
      '0x2983bf5c334743Aa6657AD70A55041d720d225dB', 
      '0x538E2dDbfDf476D24cCb1477A518A82C9EA81326'
      ) 
    THEN 'nextWETH'
    WHEN destination_transacting_asset IN (
      '0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44', 
      '0x4a2be2075588bce6a7e072574698a7dbbac39b08', 
      '0xca87472dbfb041c2e5a2672d319ea6184ad9755e'
      ) 
      THEN 'KP3R'
    WHEN destination_transacting_asset IN (
      '0x3f6740b5898c5d3650ec6eace9a649ac791e44d7', 
      '0x7cf93c434260519537184631a347ee8ad0bc68cb', 
      '0xf232d1afbed9df3880143d4fad095f3698c4d1c6'
      ) 
      THEN 'kLP'
    WHEN destination_transacting_asset IN (
      '0x538e2ddbfdf476d24ccb1477a518a82c9ea81326', 
      '0x2983bf5c334743aa6657ad70a55041d720d225db', 
      '0x4b8bac8dd1caa52e32c07755c17efaded6a0bbd0', 
      '0xa9cb51c666d2af451d87442be50747b31bb7d805', 
      '0xbad5b3c68f855eaece68203312fd88ad3d365e50'
      ) 
      THEN 'nextWETH'
    WHEN destination_transacting_asset IN (
      '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', 
      '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619', 
      '0x2170ed0880ac9a755fd29b2688956bd959f933f8', 
      '0x4200000000000000000000000000000000000006', 
      '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 
      '0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1'
      ) 
      THEN 'WETH'
    WHEN destination_transacting_asset IN (
      '0x4cbb28fa12264cd8e87c62f4e1d9f5955ce67d20',
      '0xe221c5a2a8348f12dcb2b0e88693522ebad2690f',
      '0x2fd7e61033b3904c65aa9a9b83dcd344fa19ffd2',
      '0xf4d944883d6fddc56d3534986fef82105cadbfa1',
      '0xd609f26b5547d5e31562b29150769cb7c774b97a'
    )
    THEN 'nextUSDT'
    WHEN destination_transacting_asset IN (
      '0x0100546F2cD4C9D97f798fFC9755E47865FF7Ee6',
      '0x303241e2B3b4aeD0bb0F8623e7442368FED8Faf3'
    )
    THEN 'nextAlETH'
    WHEN destination_transacting_asset IN (
      '0x49000f5e208349D2fA678263418e21365208E498'
    )
    THEN 'nextAlUSD'
    -- xERC20 TOKENS BELOW
    WHEN destination_transacting_asset IN (
      '0xFE67A4450907459c3e1FFf623aA927dD4e28c67a',
      '0x58b9cB810A68a7f3e1E4f8Cb45D1B9B3c79705E8'
    )
    THEN 'NEXT'
    WHEN destination_transacting_asset IN (
      '0x58b9cB810A68a7f3e1E4f8Cb45D1B9B3c79705E8'
    )
    THEN 'xNEXT'
    WHEN destination_transacting_asset IN (
      '0xa411c9Aa00E020e4f88Bc19996d29c5B7ADB4ACf'
    )
    THEN 'XOC'
    WHEN destination_transacting_asset IN (
      '0x44709a920fCcF795fbC57BAA433cc3dd53C44DbE',
      '0x489580eB70a50515296eF31E8179fF3e77E24965'
    )
    THEN 'RADAR'
    WHEN destination_transacting_asset IN (
      '0x202426c15a18a0e0fE3294415E66421891E2EB7C'
    )
    THEN 'xRADAR'
    WHEN destination_transacting_asset IN (
      '0xdbdb4d16eda451d0503b854cf79d55697f90c8df',
      '0x27b58D226fe8f792730a795764945Cf146815AA7',
      '0xE974B9b31dBFf4369b94a1bAB5e228f35ed44125'
    )
    THEN 'ALCX'
    WHEN destination_transacting_asset IN (
      '0xbd18f9be5675a9658335e6b7e79d9d9b394ac043'
    )
    THEN 'xALCX'
    WHEN destination_transacting_asset IN (
      '0x0D505C03d30e65f6e9b4Ef88855a47a89e4b7676',
      '0xBB1B173cdFBe464caaaCeaB2a9c8C44229d62D14',
      '0xb2588731d8f6F854037936d6ffac4c13d0b6bd62'
    )
    THEN 'ZOOMER'
    WHEN destination_transacting_asset IN (
      '0x772fCe4B8E88BD19e86dC92428d242704aC480a0'
    )
    THEN 'P8'
    WHEN destination_transacting_asset IN (
      '0x2bF2ba13735160624a0fEaE98f6aC8F70885eA61',
      '0xbD80CFA9d93A87D1bb895f810ea348E496611cD4'
    )
    THEN 'FRACTION'
    WHEN destination_transacting_asset IN (
      '0x4602e7CFE18d8b16ED13538603B00073F5c28bc8'
    )
    THEN 'xFRACTION'  
    ELSE destination_transacting_asset
  END AS destination_asset,
  -- END DESTINATION ASSET MAPPING 
  a.decimal AS token_decimal,
  CAST(t.bridged_amt AS FLOAT64) 
  / pow(10, coalesce(CAST(a.decimal AS INT64), 0)) AS  d_bridged_amt,
  CAST(origin_transacting_amount AS FLOAT64) 
  / pow(10, coalesce(CAST(a.decimal AS INT64), 0)) AS  d_origin_amount,
  CAST(destination_transacting_amount AS FLOAT64) 
  / pow(10, coalesce(CAST(a.decimal AS INT64), 0)) AS  d_destination_amount

FROM `mainnet-bigq.public.transfers` t
LEFT JOIN connext_contracts cc
  ON LOWER(t.xcall_caller) = LOWER(cc.xcall_caller)
LEFT JOIN assets a
  ON (
    t.canonical_id = a.canonical_id
    )

WHERE (
  EXTRACT(MONTH FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = 10
  AND EXTRACT(YEAR FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = 2023)


)

, hr_asset_price AS (
  SELECT 
    ap.canonical_id,
    ap.timestamp - MOD(ap.timestamp,1800) AS timestamp,
    MAX(ap.price) AS price
  FROM `mainnet-bigq.public.asset_prices` ap
  GROUP BY 1,2
)

, transfers_usd_price  AS (
  SELECT
    tr.*,         
    ap.price,
    ROW_NUMBER() OVER
          (
            PARTITION BY tr.transfer_id
            ORDER BY ap.timestamp DESC
          ) AS closet_price_rank
  FROM transfer_raw tr
  LEFT JOIN hr_asset_price ap
    ON (
      tr.canonical_id = ap.canonical_id
      AND ap.timestamp <= (tr.xcall_timestamp - MOD(tr.xcall_timestamp,1800))
      )
)

SELECT 
  tsp.*,
  tsp.d_bridged_amt * CAST(tsp.price AS FLOAT64) AS usd_bridged_amt,
  tsp.d_origin_amount * CAST(tsp.price AS FLOAT64) AS usd_origin_amount,
  tsp.d_destination_amount * CAST(tsp.price AS FLOAT64) AS usd_destination_amount
FROM transfers_usd_price tsp
WHERE closet_price_rank = 1