-- Gather slippage of each transaction

WITH 
  -- Preprocess Relayer Fees
  -- Convert {"token_address": "value"}
  -- to relayer_fee_gas_amount
  -- and relayer_fee_token_amount
  RELAYER_FEES_TMP AS (
    SELECT 
      transfer_id,
      -- Extracting the gas fee (native token) amount
      -- Relayer fee use gas token if there's 0x000... key
      CAST(JSON_EXTRACT_SCALAR(relayer_fees, '$.0x0000000000000000000000000000000000000000') AS NUMERIC) AS relayer_fee_gas_amount,
      -- Create a temporary column to remove the '0x000...' key
      -- This will be further processed in the next step
      REGEXP_REPLACE(TO_JSON_STRING(relayer_fees), r'{"0x0000000000000000000000000000000000000000":"[^"]*",?', '{') AS relayer_fee_tmp
    FROM public.transfers
  ),
  
  RELAYER_FEES AS (
    SELECT 
      transfer_id,
      relayer_fee_gas_amount,
      -- Extracting the token fee amount
      CASE
        -- if there's no relayer fee, set to NULL
        -- these are stuck transfers
        WHEN relayer_fee_tmp = '{}' THEN NULL
        -- if there's no 0x000... key, it's likely
        -- that the relayer fee is in token used
        -- in the transfer
        -- apply regex to remove token address
        -- and convert remaining value to numeric
        -- NOTE: this is a bad approach
        -- since we assume that the dictionary
        -- as expected
        ELSE CAST(REGEXP_REPLACE(REGEXP_REPLACE(
          TO_JSON_STRING(relayer_fee_tmp), 
          r'0x[\dA-Za-z]+', 
          ''
        ), r'["\{\}:\\]', '') AS NUMERIC)
      END AS relayer_fee_token_amount
    FROM RELAYER_FEES_TMP
  ),
  
  -- Join relayer fees table with transfers table
  transfers_formatted AS (
    SELECT
      t.*,
      r.relayer_fee_gas_amount,
      r.relayer_fee_token_amount
    FROM public.transfers t
      JOIN RELAYER_FEES r
      ON t.transfer_id = r.transfer_id
  ),
  
  -- Join with stableswaps to get fees
  transfers_with_fees AS (
    SELECT
      t.transfer_id,
      t.message_status,
      t.xcall_timestamp,
      t.origin_domain,
      t.destination_domain,
      t.xcall_transaction_hash,
      t.execute_transaction_hash,
      t.xcall_gas_price,
      t.xcall_gas_limit,
      t.origin_transacting_asset,
      t.destination_local_asset,
      t.destination_transacting_asset,
      t.origin_transacting_amount,
      t.origin_bridged_amount,
      t.destination_local_amount,
      t.destination_transacting_amount,
      relayer_fees,
      relayer_fee_gas_amount,
      relayer_fee_token_amount,
      x.fee AS source_stableswap_fees,
      e.fee AS destination_stableswap_fees
    FROM transfers_formatted t
    LEFT JOIN public.stableswap_exchanges x
      ON t.xcall_transaction_hash = x.transaction_hash
      AND t.origin_domain = x.domain
    LEFT JOIN public.stableswap_exchanges e
      ON t.execute_transaction_hash = e.transaction_hash
      AND t.destination_domain = e.domain
  ),
  
  -- join with assets table
  -- to get decimal points
  -- we need to format the amount
  -- because fee are in a decimal format
  -- while transfers are BIGINT
  -- we need to convert them to decimal some how
  transfers_with_decimal AS (
    SELECT
      t.*,
      a1.decimal AS origin_asset_decimal,
      a2.decimal AS destination_asset_decimal
    FROM transfers_with_fees t
    LEFT JOIN public.assets a1
      ON 
        t.origin_domain = a1.domain
        AND
        (
          t.origin_transacting_asset = a1.local 
          OR 
          t.origin_transacting_asset = a1.adopted
        ) -- the asset can be local or adopted
    LEFT JOIN public.assets a2
      ON
        t.destination_domain = a2.domain
        AND
        (
          t.destination_transacting_asset = a2.local
          OR
          t.destination_transacting_asset = a2.adopted
        )
  ),
  
  -- format decimal
  -- and get only exact values
  transfers_exact_with_fees AS (
    SELECT
      transfer_id,
      xcall_timestamp,
      origin_domain,
      destination_domain,
      origin_transacting_asset,
      destination_transacting_asset,
      -- origin_transacting_amount,
      CASE
        WHEN 
          (
            origin_transacting_asset = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'  -- BNB Chain native USDC has 18 decimal
            OR
            origin_transacting_asset = '0x55d398326f99059ff775485246999027b3197955'  -- BNB Chain native USDT has 18 decimal
          )
          AND
          origin_domain = '6450786'
          THEN CAST(origin_transacting_amount AS DECIMAL) / POWER(10, 18)
        ELSE
          CAST(origin_transacting_amount AS DECIMAL) / POWER(10, CAST(origin_asset_decimal AS DECIMAL)) 
      END AS exact_origin_transacting_amount,
      -- origin_bridged_amount,
      CAST(origin_bridged_amount AS DECIMAL) / POWER(10, CAST(origin_asset_decimal AS DECIMAL)) AS exact_origin_bridged_amount,
      -- destination_local_amount,
      CAST(destination_local_amount AS DECIMAL) / POWER(10, CAST(origin_asset_decimal AS DECIMAL)) AS exact_destination_local_amount,
      -- destination_transacting_amount,
      CASE
        WHEN 
          (
            destination_transacting_asset = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'  -- BNB Chain native USDC has 18 decimal
            OR
            destination_transacting_asset = '0x55d398326f99059ff775485246999027b3197955'  -- BNB Chain native USDT has 18 decimal
          )
          AND
          destination_domain = '6450786'
          THEN CAST(destination_transacting_amount AS DECIMAL) / POWER(10, 18) -- BNB Chain native USDC has 18 decimal
        ELSE
          CAST(destination_transacting_amount AS DECIMAL) / POWER(10, CAST(destination_asset_decimal AS DECIMAL)) 
      END AS exact_destination_transacting_amount,
      relayer_fee_gas_amount / 1e18 AS exact_relayer_fee_gas_amount,
      CASE
        WHEN 
          (
            origin_transacting_asset = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'  -- BNB Chain native USDC has 18 decimal
            OR
            origin_transacting_asset = '0x55d398326f99059ff775485246999027b3197955'  -- BNB Chain native USDT has 18 decimal
          )
          AND
          origin_domain = '6450786'
          THEN CAST(relayer_fee_token_amount AS DECIMAL) / POWER(10, 18) -- BNB Chain native USDC has 18 decimal
        ELSE
          CAST(relayer_fee_token_amount AS DECIMAL) / POWER(10, CAST(origin_asset_decimal AS DECIMAL)) 
      END AS exact_relayer_fee_token_amount,
    FROM transfers_with_decimal
  ),
  
  -- transaction_hash
  transfers_slippage AS (
    SELECT 
      transfer_id,
      xcall_timestamp,
      origin_domain,
      destination_domain,
      origin_transacting_asset,
      destination_transacting_asset,
      exact_origin_transacting_amount,
      CASE
        WHEN exact_relayer_fee_token_amount IS NULL THEN exact_origin_transacting_amount -- if no relayer fee as token, keep value
        ELSE exact_origin_transacting_amount - exact_relayer_fee_token_amount 
      END AS exact_origin_transacting_amount_without_relayer_fees,
      exact_origin_bridged_amount,
      exact_destination_local_amount,
      exact_destination_transacting_amount,
      exact_relayer_fee_gas_amount,
      exact_relayer_fee_token_amount,
      exact_origin_bridged_amount - exact_origin_transacting_amount AS source_stableswap_slippage,
      exact_destination_transacting_amount - exact_destination_local_amount AS destination_stableswap_slippage,
      (exact_origin_bridged_amount - exact_origin_transacting_amount) + (exact_destination_transacting_amount - exact_destination_local_amount) AS total_stableswap_slippage
    FROM transfers_exact_with_fees
  )

--- main query

SELECT * FROM transfers_slippage
ORDER BY xcall_timestamp DESC