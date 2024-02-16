# Route Pathways

    - Total possible pathways: 7056!

        ```sql
        SELECT  DISTINCT
        CAST(rp.fromChainId AS FLOAT64) AS fromChainId,
        rp.fromTokenAddress AS fromTokenAddress,
        "0x561d45be49935aad55ab21fa24ccb5d7a3fe4690" AS fromAddress,
        CAST(rp.toChainId AS FLOAT64) AS toChainId,
        rp.toTokenAddress AS toTokenAddress,
        pow(10, row_number) AS amount_fair_usd
        FROM `mainnet-bigq.stage.source_lifi__pathways` rp
        CROSS JOIN
        UNNEST(GENERATE_ARRAY(1,  6)) AS row_number
        ```

    - LIFI

        ```sql
        SELECT DISTINCT
        "lifi" AS aggegator,
        r.route_fromchainid,
        r.route_fromtoken_symbol,
        r.route_fromtoken_address,

        r.route_tochainid,
        r.route_totoken_symbol,
        r.route_totoken_address,

        FORMAT("%.15f",
            POW(10, ROUND(LOG10( CAST(r.route_fromamount AS FLOAT64))))
        ) AS nearest_power_of_ten_amount

        FROM `mainnet-bigq.stage.source_lifi__routes` r
        ```

    - SOCKET
        ```sql
        SELECT DISTINCT
            "socket" AS aggegator,
            s.fromchainid AS route_fromchainid,
            s.fromasset_symbol AS route_fromtoken_symbol,
            s.fromasset_address AS route_fromtoken_address,

            s.tochainid AS route_tochainid,
            s.toasset_symbol AS route_totoken_symbol,
            s.toasset_address AS route_totoken_address,

            FORMAT("%.15f",
                POW(10, ROUND(LOG10( CAST(s.fromamount AS FLOAT64))))
            ) AS nearest_power_of_ten_amount

            FROM `mainnet-bigq.raw.source_socket__routes` s
            )
        ```

Takeaways:

    Out of these working pathways for LIFI: 1058. Socket: 1883 -> 2941 Total working! looks like everything is working fine.
