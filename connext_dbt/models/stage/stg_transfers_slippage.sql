-- Gather slippage of each transaction

WITH
-- Preprocess Relayer Fees
-- Convert {"token_address": "value"}
-- to relayer_fee_gas_amount
-- and relayer_fee_token_amount
RELAYER_FEES_TMP AS (
    SELECT
        TRANSFER_ID,
        -- Extracting the gas fee (native token) amount
        -- Relayer fee use gas token if there's 0x000... key
        CAST(
            JSON_EXTRACT_SCALAR(
                RELAYER_FEES, '$.0x0000000000000000000000000000000000000000'
            ) AS NUMERIC
        ) AS RELAYER_FEE_GAS_AMOUNT,
        -- Create a temporary column to remove the '0x000...' key
        -- This will be further processed in the next step
        REGEXP_REPLACE(
            TO_JSON_STRING(RELAYER_FEES),
            r'{"0x0000000000000000000000000000000000000000":"[^"]*",?',
            '{'
        ) AS RELAYER_FEE_TMP
    FROM PUBLIC.TRANSFERS
),

RELAYER_FEES AS (
    SELECT
        TRANSFER_ID,
        RELAYER_FEE_GAS_AMOUNT,
        -- Extracting the token fee amount
        CASE
        -- if there's no relayer fee, set to NULL
        -- these are stuck transfers
            WHEN RELAYER_FEE_TMP = '{}' THEN NULL
            -- if there's no 0x000... key, it's likely
            -- that the relayer fee is in token used
            -- in the transfer
            -- apply regex to remove token address
            -- and convert remaining value to numeric
            -- NOTE: this is a bad approach
            -- since we assume that the dictionary
            -- as expected
            ELSE CAST(REGEXP_REPLACE(REGEXP_REPLACE(
                TO_JSON_STRING(RELAYER_FEE_TMP),
                r'0x[\dA-Za-z]+',
                ''
            ), r'["\{\}:\\]', '') AS NUMERIC)
        END AS RELAYER_FEE_TOKEN_AMOUNT
    FROM RELAYER_FEES_TMP
),

-- Join relayer fees table with transfers table
TRANSFERS_FORMATTED AS (
    SELECT
        T.*,
        R.RELAYER_FEE_GAS_AMOUNT,
        R.RELAYER_FEE_TOKEN_AMOUNT
    FROM PUBLIC.TRANSFERS T
    JOIN RELAYER_FEES R
        ON T.TRANSFER_ID = R.TRANSFER_ID
),

-- Join with stableswaps to get fees
TRANSFERS_WITH_FEES AS (
    SELECT
        T.TRANSFER_ID,
        T.MESSAGE_STATUS,
        T.XCALL_TIMESTAMP,
        T.ORIGIN_DOMAIN,
        T.DESTINATION_DOMAIN,
        T.XCALL_TRANSACTION_HASH,
        T.EXECUTE_TRANSACTION_HASH,
        T.XCALL_GAS_PRICE,
        T.XCALL_GAS_LIMIT,
        T.ORIGIN_TRANSACTING_ASSET,
        T.DESTINATION_LOCAL_ASSET,
        T.DESTINATION_TRANSACTING_ASSET,
        T.ORIGIN_TRANSACTING_AMOUNT,
        T.ORIGIN_BRIDGED_AMOUNT,
        T.DESTINATION_LOCAL_AMOUNT,
        T.DESTINATION_TRANSACTING_AMOUNT,
        RELAYER_FEES,
        RELAYER_FEE_GAS_AMOUNT,
        RELAYER_FEE_TOKEN_AMOUNT,
        X.FEE AS SOURCE_STABLESWAP_FEES,
        E.FEE AS DESTINATION_STABLESWAP_FEES
    FROM TRANSFERS_FORMATTED T
    LEFT JOIN PUBLIC.STABLESWAP_EXCHANGES X
        ON
            T.XCALL_TRANSACTION_HASH = X.TRANSACTION_HASH
            AND T.ORIGIN_DOMAIN = X.DOMAIN
    LEFT JOIN PUBLIC.STABLESWAP_EXCHANGES E
        ON
            T.EXECUTE_TRANSACTION_HASH = E.TRANSACTION_HASH
            AND T.DESTINATION_DOMAIN = E.DOMAIN
),

-- join with assets table
-- to get decimal points
-- we need to format the amount
-- because fee are in a decimal format
-- while transfers are BIGINT
-- we need to convert them to decimal some how
TRANSFERS_WITH_DECIMAL AS (
    SELECT
        T.*,
        A1.DECIMAL AS ORIGIN_ASSET_DECIMAL,
        A2.DECIMAL AS DESTINATION_ASSET_DECIMAL
    FROM TRANSFERS_WITH_FEES T
    LEFT JOIN PUBLIC.ASSETS A1
        ON
            T.ORIGIN_DOMAIN = A1.DOMAIN
            AND
            (
                T.ORIGIN_TRANSACTING_ASSET = A1.LOCAL
                OR
                T.ORIGIN_TRANSACTING_ASSET = A1.ADOPTED
            ) -- the asset can be local or adopted
    LEFT JOIN PUBLIC.ASSETS A2
        ON
            T.DESTINATION_DOMAIN = A2.DOMAIN
            AND
            (
                T.DESTINATION_TRANSACTING_ASSET = A2.LOCAL
                OR
                T.DESTINATION_TRANSACTING_ASSET = A2.ADOPTED
            )
),

-- format decimal
-- and get only exact values
TRANSFERS_EXACT_WITH_FEES AS (
    SELECT
        TRANSFER_ID,
        XCALL_TIMESTAMP,
        ORIGIN_DOMAIN,
        DESTINATION_DOMAIN,
        ORIGIN_TRANSACTING_ASSET,
        DESTINATION_TRANSACTING_ASSET,
        -- origin_transacting_amount,
        CASE
            WHEN
                (
                    -- BNB Chain native USDC has 18 decimal
                    ORIGIN_TRANSACTING_ASSET
                    = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
                    OR
                    -- BNB Chain native USDT has 18 decimal
                    ORIGIN_TRANSACTING_ASSET
                    = '0x55d398326f99059ff775485246999027b3197955'
                )
                AND
                ORIGIN_DOMAIN = '6450786'
                THEN CAST(ORIGIN_TRANSACTING_AMOUNT AS DECIMAL) / POWER(10, 18)
            ELSE
                CAST(ORIGIN_TRANSACTING_AMOUNT AS DECIMAL)
                / POWER(10, CAST(ORIGIN_ASSET_DECIMAL AS DECIMAL))
        END AS EXACT_ORIGIN_TRANSACTING_AMOUNT,
        -- origin_bridged_amount,
        CAST(ORIGIN_BRIDGED_AMOUNT AS DECIMAL)
        / POWER(10, CAST(ORIGIN_ASSET_DECIMAL AS DECIMAL))
            AS EXACT_ORIGIN_BRIDGED_AMOUNT,
        -- destination_local_amount,
        CAST(DESTINATION_LOCAL_AMOUNT AS DECIMAL)
        / POWER(10, CAST(ORIGIN_ASSET_DECIMAL AS DECIMAL))
            AS EXACT_DESTINATION_LOCAL_AMOUNT,
        -- destination_transacting_amount,
        CASE
            WHEN
                (
                    -- BNB Chain native USDC has 18 decimal
                    DESTINATION_TRANSACTING_ASSET
                    = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
                    OR
                    -- BNB Chain native USDT has 18 decimal
                    DESTINATION_TRANSACTING_ASSET
                    = '0x55d398326f99059ff775485246999027b3197955'
                )
                AND
                DESTINATION_DOMAIN = '6450786'
                -- BNB Chain native USDC has 18 decimal
                THEN
                    CAST(DESTINATION_TRANSACTING_AMOUNT AS DECIMAL)
                    / POWER(10, 18)
            ELSE
                CAST(DESTINATION_TRANSACTING_AMOUNT AS DECIMAL)
                / POWER(10, CAST(DESTINATION_ASSET_DECIMAL AS DECIMAL))
        END AS EXACT_DESTINATION_TRANSACTING_AMOUNT,
        RELAYER_FEE_GAS_AMOUNT / 1e18 AS EXACT_RELAYER_FEE_GAS_AMOUNT,
        CASE
            WHEN
                (
                    -- BNB Chain native USDC has 18 decimal
                    ORIGIN_TRANSACTING_ASSET
                    = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
                    OR
                    -- BNB Chain native USDT has 18 decimal
                    ORIGIN_TRANSACTING_ASSET
                    = '0x55d398326f99059ff775485246999027b3197955'
                )
                AND
                ORIGIN_DOMAIN = '6450786'
                -- BNB Chain native USDC has 18 decimal
                THEN CAST(RELAYER_FEE_TOKEN_AMOUNT AS DECIMAL) / POWER(10, 18)
            ELSE
                CAST(RELAYER_FEE_TOKEN_AMOUNT AS DECIMAL)
                / POWER(10, CAST(ORIGIN_ASSET_DECIMAL AS DECIMAL))
        END AS EXACT_RELAYER_FEE_TOKEN_AMOUNT
    FROM TRANSFERS_WITH_DECIMAL
),

-- transaction_hash
TRANSFERS_SLIPPAGE AS (
    SELECT
        TRANSFER_ID,
        XCALL_TIMESTAMP,
        ORIGIN_DOMAIN,
        DESTINATION_DOMAIN,
        ORIGIN_TRANSACTING_ASSET,
        DESTINATION_TRANSACTING_ASSET,
        EXACT_ORIGIN_TRANSACTING_AMOUNT,
        CASE
            -- if no relayer fee as token, keep value
            WHEN
                EXACT_RELAYER_FEE_TOKEN_AMOUNT IS NULL
                THEN EXACT_ORIGIN_TRANSACTING_AMOUNT
            ELSE
                EXACT_ORIGIN_TRANSACTING_AMOUNT - EXACT_RELAYER_FEE_TOKEN_AMOUNT
        END AS EXACT_ORIGIN_TRANSACTING_AMOUNT_WITHOUT_RELAYER_FEES,
        EXACT_ORIGIN_BRIDGED_AMOUNT,
        EXACT_DESTINATION_LOCAL_AMOUNT,
        EXACT_DESTINATION_TRANSACTING_AMOUNT,
        EXACT_RELAYER_FEE_GAS_AMOUNT,
        EXACT_RELAYER_FEE_TOKEN_AMOUNT,
        EXACT_ORIGIN_BRIDGED_AMOUNT
        - EXACT_ORIGIN_TRANSACTING_AMOUNT AS SOURCE_STABLESWAP_SLIPPAGE,
        EXACT_DESTINATION_TRANSACTING_AMOUNT
        - EXACT_DESTINATION_LOCAL_AMOUNT AS DESTINATION_STABLESWAP_SLIPPAGE,
        (EXACT_ORIGIN_BRIDGED_AMOUNT - EXACT_ORIGIN_TRANSACTING_AMOUNT)
        + (
            EXACT_DESTINATION_TRANSACTING_AMOUNT
            - EXACT_DESTINATION_LOCAL_AMOUNT
        ) AS TOTAL_STABLESWAP_SLIPPAGE
    FROM TRANSFERS_EXACT_WITH_FEES
)

--- main query

SELECT * FROM TRANSFERS_SLIPPAGE
ORDER BY XCALL_TIMESTAMP DESC
