SELECT DISTINCT
    r.action_fromtoken_chainid,
    r.action_fromtoken_symbol,
    r.action_fromtoken_address,

    r.action_totoken_chainid,
    r.action_totoken_symbol,
    r.action_totoken_address,

    r.action_fromaddress,
    r.action_toaddress,

    FORMAT(
        "%.15f",
        POW(10, ROUND(LOG10(CAST(r.action_fromamount AS FLOAT64))))
    ) AS nearest_power_of_ten_amount

FROM `mainnet-bigq.stage.source_lifi__routes` r
RIGHT JOIN `stage.source_lifi__pathways` p
    ON (
        r.action_fromtoken_chainid = CAST(p.fromchainid AS FLOAT64)
        AND r.action_fromtoken_address = p.fromtokenaddress
        AND r.action_totoken_chainid = CAST(p.tochainid AS FLOAT64)
        AND r.action_totoken_address = p.totokenaddress
    )


-- SELECT * FROM `mainnet-bigq.stage.source_lifi__routes` r LIMIT 1
