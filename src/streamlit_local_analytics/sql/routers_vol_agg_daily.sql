-- mainnet-bigq.metrics.router_metrics

WITH stg_daily_transfer_volume AS (
    SELECT
        tf.status,
        DATE_TRUNC(TIMESTAMP_SECONDS(tf.xcall_timestamp), HOUR)
            AS transfer_hour,
        tf.origin_domain AS origin_chain,
        tf.destination_domain AS destination_chain,
        tf.router,
        tf.origin_asset_name AS asset,
        SUM(CAST(tf.origin_transacting_amount AS FLOAT64)) AS volume,
        AVG(tf.asset_usd_price) AS avg_price,
        SUM(tf.usd_amount) AS usd_volume,
        ROW_NUMBER() OVER () AS id
    FROM `mainnet-bigq.y42_connext_y42_dev.transfers_mapped` AS tf
    GROUP BY 1, 2, 3, 4, 5, 6
),


volumehourlymetrics AS (
    SELECT
        status,
        router,
        asset,
        origin_chain,
        destination_chain,
        transfer_hour,
        SUM(usd_volume) AS usd_volume,
        SUM(volume) AS volume,
        MAX(transfer_date) AS last_transfer_date

    FROM stg_daily_transfer_volume
    GROUP BY
        1, 2, 3, 4, 5
),

completedfastoriginvolume AS (
    SELECT
        router,
        asset,
        origin_chain AS chain,
        SUM(usd_volume) AS origin_fast_volume_1_day,
        SUM(usd_volume) AS origin_fast_volume_7_days,
        SUM(usd_volume) AS origin_fast_volume_30_days
    FROM volumemetrics
    WHERE status = 'CompletedFast'
    GROUP BY 1, 2, 3
),

completedfastdestvolume AS (
    SELECT
        router,
        asset,
        destination_chain AS chain,
        SUM(usd_volume_last_1_day) AS destination_fast_volume_1_day,
        SUM(usd_volume_last_7_days) AS destination_fast_volume_7_days,
        SUM(usd_volume_last_30_days) AS destination_fast_volume_30_days
    FROM volumemetrics
    WHERE status = 'CompletedFast'
    GROUP BY 1, 2, 3
),

originvolume AS (
    SELECT
        vm.router,
        vm.asset,
        vm.origin_chain AS chain,
        SUM(usd_volume_last_1_day) AS origin_usd_volume_last_1_day,
        SUM(usd_volume_last_7_days) AS origin_usd_volume_last_7_days,
        SUM(usd_volume_last_30_days) AS origin_usd_volume_last_30_days,
        SUM(volume_last_1_day) AS origin_volume_1_day,
        SUM(volume_last_7_days) AS origin_volume_7_days,
        SUM(volume_last_30_days) AS origin_volume_30_days
    FROM
        volumemetrics AS vm
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
    GROUP BY 1, 2, 3
),

destinationvolume AS (
    SELECT
        vm.router,
        vm.asset,
        vm.destination_chain AS chain,
        SUM(usd_volume_last_1_day) AS destination_usd_volume_last_1_day,
        SUM(usd_volume_last_7_days) AS destination_usd_volume_last_7_days,
        SUM(usd_volume_last_30_days) AS destination_usd_volume_last_30_days,
        SUM(volume_last_1_day) AS destination_volume_1_day,
        SUM(volume_last_7_days) AS destination_volume_7_days,
        SUM(volume_last_30_days) AS destination_volume_30_days,
        MAX(last_transfer_date) AS destination_last_transfer_date
    FROM
        volumemetrics AS vm
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
    GROUP BY 1, 2, 3
),


combinations AS (
    SELECT DISTINCT
        origin_chain AS chain,
        asset,
        router
    FROM volumemetrics
),

metrics AS (
    SELECT
        router,
        asset,
        origin_chain AS chain,
        MAX(vm.last_transfer_date) AS last_txn_date,
        SUM(vm.slow_txns) AS slow_tns
    FROM volumemetrics AS vm
    GROUP BY 1, 2, 3
),

groupedmetrics AS (
    SELECT
        cmbns.chain AS chain_domain,
        cmbns.router AS router_address,
        cmbns.asset AS asset_address,
        ov.origin_usd_volume_last_1_day,
        ov.origin_usd_volume_last_7_days,
        ov.origin_usd_volume_last_30_days,
        ov.origin_volume_1_day,
        ov.origin_volume_7_days,
        ov.origin_volume_30_days,
        cfov.origin_fast_volume_1_day,
        cfov.origin_fast_volume_7_days,
        cfov.origin_fast_volume_30_days,
        dv.destination_usd_volume_last_1_day,
        dv.destination_usd_volume_last_7_days,
        dv.destination_usd_volume_last_30_days,
        dv.destination_volume_1_day,
        dv.destination_volume_7_days,
        dv.destination_volume_30_days,
        cfdv.destination_fast_volume_1_day,
        cfdv.destination_fast_volume_7_days,
        cfdv.destination_fast_volume_30_days,
        last_txn_date,
        slow_tns
    FROM
        combinations AS cmbns
    LEFT JOIN
        metrics AS mtr
        ON
            cmbns.chain = mtr.chain
            AND cmbns.asset = mtr.asset
            AND cmbns.router = mtr.router
    LEFT JOIN
        originvolume AS ov
        ON
            cmbns.chain = ov.chain
            AND cmbns.asset = ov.asset
            AND cmbns.router = ov.router
    LEFT JOIN
        destinationvolume AS dv
        ON
            cmbns.chain = dv.chain
            AND cmbns.asset = dv.asset
            AND cmbns.router = dv.router
    LEFT JOIN
        completedfastoriginvolume AS cfov
        ON
            cmbns.chain = cfov.chain
            AND cmbns.asset = cfov.asset
            AND cmbns.router = cfov.router
    LEFT JOIN
        completedfastdestvolume AS cfdv
        ON
            cmbns.chain = cfdv.chain
            AND cmbns.asset = cfdv.asset
            AND cmbns.router = cfdv.router

),

latestassetprices AS (
    SELECT
        ap.canonical_domain,
        ap.canonical_id,
        ap.price,
        ap.timestamp
    FROM `mainnet-bigq.public.asset_prices` AS ap
    INNER JOIN (
        SELECT
            canonical_domain,
            canonical_id,
            MAX(timestamp) AS max_timestamp
        FROM
            `mainnet-bigq.public.asset_prices`
        GROUP BY
            1, 2
    ) AS latest
        ON
            ap.canonical_domain = latest.canonical_domain
            AND ap.canonical_id = latest.canonical_id
            AND ap.timestamp = latest.max_timestamp
),

routerliquidity AS (
    SELECT
        gv.*,
        lap.*,
        rwb.canonical_domain AS asset_canonical_domain,
        rwb.address,

        rwb.adopted,
        rwb.adopted_decimal,
        rwb.asset_canonical_id,
        rwb.asset_domain AS asset_domain_rwb,
        rwb.asset_usd_price,
        rwb.balance,
        rwb.balance_usd,
        rwb.decimal,
        rwb.domain,
        rwb.fees_earned,
        rwb.id,
        rwb.key,
        rwb.local,
        rwb.locked,
        rwb.locked_usd,
        rwb.removed,
        rwb.removed_usd,
        rwb.router_address AS router_address_rwb,
        rwb.supplied,
        rwb.supplied_usd,
        COALESCE(rwb.domain, gv.chain_domain) AS chain_domain_coalesced,
        COALESCE(rwb.adopted, gv.asset_address) AS asset_address_coalesced,
        COALESCE(rwb.router_address, gv.router_address)
            AS router_address_coalesced,
        COALESCE(
            DATE(gv.last_txn_date), DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ) AS last_txn_date_coalesced

    FROM
        {{ ref('stg_source__cartographer_router_with_balances') }} AS rwb
    --    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb
    FULL OUTER JOIN
        groupedmetrics AS gv
        ON
            rwb.address = gv.router_address
            AND rwb.adopted = gv.asset_address
            AND rwb.domain = gv.chain_domain
    LEFT JOIN
        latestassetprices AS lap
        ON
            rwb.asset_canonical_id = lap.canonical_id
            AND rwb.canonical_domain = lap.canonical_domain
    WHERE
        gv.destination_usd_volume_last_30_days > 0
        OR gv.origin_usd_volume_last_30_days > 0
        OR CAST(rwb.balance AS FLOAT64) > 0
),

connext_tokens AS (
    SELECT DISTINCT
        ct.token_address,
        ct.token_name,
        ct.is_xerc20
    FROM
        `mainnet-bigq.stage.connext_tokens` AS ct
),

routermapping AS (
    SELECT
        pr.*,
        --    cav.domain_name,
        COALESCE(ct.token_name, asset_address_coalesced) AS asset,
        CASE
            WHEN
                router_address_coalesced
                = '0x9584eb0356a380b25d7ed2c14c54de58a25f2581'
                THEN 'Mike Nai'
            WHEN
                router_address_coalesced
                = '0xc4ae07f276768a3b74ae8c47bc108a2af0e40eba'
                THEN 'P2P 2'
            WHEN
                router_address_coalesced
                = '0xeca085906cb531bdf1f87efa85c5be46aa5c9d2c'
                THEN 'Blocktech 2'
            WHEN
                router_address_coalesced
                = '0x22831e4f21ce65b33ef45df0e212b5bebf130e5a'
                THEN 'Blocktech 1'
            WHEN
                router_address_coalesced
                = '0xbe7bc00382a50a711d037eaecad799bb8805dfa8'
                THEN 'Minerva'
            WHEN
                router_address_coalesced
                = '0x63Cda9C42db542bb91a7175E38673cFb00D402b0'
                THEN 'Consensys Mesh'
            WHEN
                router_address_coalesced
                = '0xf26c772c0ff3a6036bddabdaba22cf65eca9f97c'
                THEN 'Connext'
            WHEN
                router_address_coalesced
                = '0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4'
                THEN 'P2P 1'
            WHEN
                router_address_coalesced
                = '0x8cb19ce8eedf740389d428879a876a3b030b9170'
                THEN 'BWare'
            WHEN
                router_address_coalesced
                = '0x0e62f9fa1f9b3e49759dc94494f5bc37a83d1fad'
                THEN 'Bazilik'
            WHEN
                router_address_coalesced
                = '0x58507fed0cb11723dfb6848c92c59cf0bbeb9927'
                THEN 'Hashquark'
            WHEN
                router_address_coalesced
                = '0x7ce49752ffa7055622f444df3c69598748cb2e5f'
                THEN 'Vault Staking'
            WHEN
                router_address_coalesced
                = '0x33b2ad85f7dba818e719fb52095dc768e0ed93ec'
                THEN 'Ethereal'
            WHEN
                router_address_coalesced
                = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
                THEN 'Gnosis'
            WHEN
                router_address_coalesced
                = '0x975574980a5Da77f5C90bC92431835D91B73669e'
                THEN '01 Node'
            WHEN
                router_address_coalesced
                = '0x6892d4D1f73A65B03063B7d78174dC6350Fcc406'
                THEN 'Unagii'
            WHEN
                router_address_coalesced
                = '0x32d63da9f776891843c90787cec54ada23abd4c2'
                THEN 'Ingag'
            WHEN
                router_address_coalesced
                = '0xfaab88015477493cfaa5dfaa533099c590876f21'
                THEN 'Paradox'
            WHEN
                router_address_coalesced
                = '0x49a9e7ec76bc8fdf658d09557305170d9f01d2fa'
                THEN 'Jitin'
            ELSE router_address_coalesced
        END AS router_name,
        CASE
            WHEN pr.chain_domain_coalesced = '6648936' THEN 'Ethereum'
            WHEN pr.chain_domain_coalesced = '1869640809' THEN 'Optimism'
            WHEN pr.chain_domain_coalesced = '6450786' THEN 'BNB'
            WHEN pr.chain_domain_coalesced = '6778479' THEN 'Gnosis'
            WHEN pr.chain_domain_coalesced = '1886350457' THEN 'Polygon'
            WHEN pr.chain_domain_coalesced = '1634886255' THEN 'Arbitrum One'
            WHEN pr.chain_domain_coalesced = '1818848877' THEN 'Linea'
            WHEN pr.chain_domain_coalesced = '31338' THEN 'Local Optimism'
            WHEN pr.chain_domain_coalesced = '31339' THEN 'Local Arbitrum One'
            WHEN pr.chain_domain_coalesced = '1835365481' THEN 'Metis'
            WHEN pr.chain_domain_coalesced = '1650553709' THEN 'Base Mainnet'
            ELSE pr.chain_domain_coalesced
        END AS domain_name
    FROM
        routerliquidity AS pr
    LEFT JOIN
        connext_tokens AS ct
        ON pr.asset_address_coalesced = ct.token_address
),

SELECT * FROM routermapping ORDER BY destination_usd_volume_last_1_day DESC
