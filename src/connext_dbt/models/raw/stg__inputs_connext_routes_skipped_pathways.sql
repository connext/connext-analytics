WITH
connext_tokens AS (
    SELECT DISTINCT
        ct.token_address,
        ct.token_name,
        ct.is_xerc20
    FROM
        `mainnet-bigq.stage.connext_tokens` ct
)

SELECT DISTINCT
    p.fromchainid,
    ct_from.token_name AS from_token_name,
    p.fromtokenaddress,
    p.tochainid,
    ct_to.token_name AS to_token_name,
    p.totokenaddress
FROM
    `mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways` wp
RIGHT JOIN
    `mainnet-bigq.stage.source_lifi__pathways` p
    ON
        (
            wp.fromchainid = CAST(p.fromchainid AS FLOAT64)
            AND wp.fromtokenaddress = p.fromtokenaddress
            AND wp.tochainid = CAST(p.tochainid AS FLOAT64)
            AND wp.totokenaddress = p.totokenaddress
        )
LEFT JOIN
    connext_tokens ct_from
    ON
        p.fromtokenaddress = ct_from.token_address
LEFT JOIN
    connext_tokens ct_to
    ON
        p.fromtokenaddress = ct_to.token_address
WHERE
    wp.fromchainid IS NULL
