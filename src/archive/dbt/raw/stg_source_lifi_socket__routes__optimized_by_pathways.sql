-- Output needed:
-- Origin - Destination - Token - Amount - Date
-- Metrics: Connext - pricing, gas costs, aggregated fees | Recommended pricing - pricing, gas costs, aggregated fees, Recommended bridge name
WITH
lifi_raw AS (
    SELECT DISTINCT
        upload_datetime AS date,
        "lifi" AS aggregator,
        CAST(lr.route_fromchainid AS STRING) AS route_fromchainid,
        CAST(lr.route_tochainid AS STRING) AS route_tochainid,
        lr.route_fromtoken_symbol AS route_fromtoken_symbol,
        lr.route_totoken_symbol AS route_totoken_symbol,
        CAST(lr.route_fromamountusd AS FLOAT64) AS inputvalueinusd,
        CAST(lr.route_toamountusd AS FLOAT64) outputvalueinusd,
        CAST(lr.route_gascostusd AS FLOAT64) AS totalgasfeesinusd,
        (
            CAST(lr.route_toamountusd AS FLOAT64)
            - CAST(lr.route_gascostusd AS FLOAT64)
        ) AS receivedvalueinusd,
        -- calulations: in - gas -> received
        ARRAY_TO_STRING(lr.usedbridgenames_array, ",") AS usedbridgenames,
        ARRAY_LENGTH(lr.usedbridgenames_array) totalusertx
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
            route_gascostusd
        FROM
            `mainnet-bigq.stage.source_lifi__routes`
        -- WHERE route_id = "0x65973977eaa7ce73894607f61ab77577e05e70cadcc0ab8156114dc3a80bb6c0"
        -- WHERE route_id= "0xd77a8745221e7a95405022d4abf278a6932a452fbdb0b83ea562496a229eb96d"
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
            route_gascostusd
        ORDER BY
            1
    ) lr
    -- LIMIT 10
),

socket_raw AS (
    SELECT DISTINCT
        upload_datetime AS date,
        "socket" AS aggregator,
        CAST(s.fromchainid AS STRING) AS route_fromchainid,
        CAST(s.tochainid AS STRING) AS route_tochainid,
        s.fromasset_symbol AS route_fromtoken_symbol,
        s.toasset_symbol AS route_totoken_symbol,
        s.inputvalueinusd,
        s.outputvalueinusd,
        s.totalgasfeesinusd,
        s.receivedvalueinusd,
        -- calulations: in - gas -> received: s.outputvalueinusd - s.totalgasfeesinusd AS total_price
        COALESCE(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(usedbridgenames, r'\[(.*?)\]'), r"'", ''
            ),
            usedbridgenames
        )
            AS usedbridgenames,
        totalusertx
    --   CASE
    --     WHEN ((s.outputvalueinusd - s.totalgasfeesinusd) = s.receivedvalueinusd) THEN "received = out - gas"
    --   ELSE
    --   "received != out - gas"
    -- END
    --   AS amount_logic_flag
    FROM
        `mainnet-bigq.raw.source_socket__routes` s
-- WHERE upload_datetime = "2024-02-28 17:40:54 UTC"
-- LIMIT 10
),

raw AS (
    SELECT * FROM lifi_raw
    UNION ALL
    SELECT * FROM socket_raw
),

rankeddata AS (
    SELECT
        *,
        RANK()
            OVER (
                PARTITION BY
                    date,
                    aggregator,
                    route_fromchainid,
                    route_tochainid,
                    route_fromtoken_symbol,
                    route_totoken_symbol
                ORDER BY receivedvalueinusd DESC
            )
            AS max_value_rank_by_output
    FROM
        raw
),

filtered_first_ranked AS (
    SELECT *
    FROM
        rankeddata
    WHERE
        max_value_rank_by_output = 1
),

connect_data AS (
    SELECT *
    FROM
        rankeddata
    WHERE
        (
            REGEXP_CONTAINS(usedbridgenames, r'connext')
            OR REGEXP_CONTAINS(usedbridgenames, r'amarok')
        )
-- usedbridgenames = 'connext' 
),

final AS (
    SELECT
        r1.*,
        c.outputvalueinusd AS connext_outputvalueinusd,
        c.totalgasfeesinusd AS connext_totalgasfeesinusd,
        c.receivedvalueinusd AS connext_receivedvalueinusd,
        c.max_value_rank_by_output AS connext_max_value_rank_by_output
    FROM
        filtered_first_ranked r1
    LEFT JOIN
        connect_data c
        ON
            r1.date = c.date
            AND r1.route_fromchainid = c.route_fromchainid
            AND r1.route_tochainid = c.route_tochainid
            AND r1.route_fromtoken_symbol = c.route_fromtoken_symbol
            AND r1.route_totoken_symbol = c.route_totoken_symbol
            AND r1.inputvalueinusd = c.inputvalueinusd
)

SELECT * FROM final
-- WHERE date = "2024-02-28 16:59:27 UTC"
