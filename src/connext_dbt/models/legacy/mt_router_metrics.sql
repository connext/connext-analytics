-- mainnet-bigq.metrics.router_metrics

WITH VolumeMetrics AS (
    SELECT
        Status,
        Router,
        Asset,
        Origin_Chain,
        Destination_Chain,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN Usd_Volume
                ELSE 0
            END
        ) AS Usd_Volume_Last_1_Day,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    THEN Usd_Volume
                ELSE 0
            END
        ) AS Usd_Volume_Last_7_Days,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    THEN Usd_Volume
                ELSE 0
            END
        ) AS Usd_Volume_Last_30_Days,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    THEN Volume
                ELSE 0
            END
        ) AS Volume_Last_1_Day,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    THEN Volume
                ELSE 0
            END
        ) AS Volume_Last_7_Days,
        SUM(
            CASE
                WHEN
                    DATE(Transfer_Date)
                    >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    THEN Volume
                ELSE 0
            END
        ) AS Volume_Last_30_Days,
        MAX(Transfer_Date) AS Last_Transfer_Date,
        COUNTIF(Status = 'CompletedSlow') AS Slow_Txns
    FROM
        {{ ref('stg_daily_transfer_volume') }}
    GROUP BY
        1, 2, 3, 4, 5
),

CompletedFastOriginVolume AS (
    SELECT
        Router,
        Asset,
        Origin_Chain AS Chain,
        SUM(Usd_Volume_Last_1_Day) AS Origin_Fast_Volume_1_Day,
        SUM(Usd_Volume_Last_7_Days) AS Origin_Fast_Volume_7_Days,
        SUM(Usd_Volume_Last_30_Days) AS Origin_Fast_Volume_30_Days
    FROM VolumeMetrics Vm
    WHERE Status = 'CompletedFast'
    GROUP BY 1, 2, 3
),

CompletedFastDestVolume AS (
    SELECT
        Router,
        Asset,
        Destination_Chain AS Chain,
        SUM(Usd_Volume_Last_1_Day) AS Destination_Fast_Volume_1_Day,
        SUM(Usd_Volume_Last_7_Days) AS Destination_Fast_Volume_7_Days,
        SUM(Usd_Volume_Last_30_Days) AS Destination_Fast_Volume_30_Days
    FROM VolumeMetrics Vm
    WHERE Status = 'CompletedFast'
    GROUP BY 1, 2, 3
),

OriginVolume AS (
    SELECT
        Vm.Router,
        Vm.Asset,
        Vm.Origin_Chain AS Chain,
        SUM(Usd_Volume_Last_1_Day) AS Origin_Usd_Volume_Last_1_Day,
        SUM(Usd_Volume_Last_7_Days) AS Origin_Usd_Volume_Last_7_Days,
        SUM(Usd_Volume_Last_30_Days) AS Origin_Usd_Volume_Last_30_Days,
        SUM(Volume_Last_1_Day) AS Origin_Volume_1_Day,
        SUM(Volume_Last_7_Days) AS Origin_Volume_7_Days,
        SUM(Volume_Last_30_Days) AS Origin_Volume_30_Days
    FROM
        VolumeMetrics Vm
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
    GROUP BY 1, 2, 3
),

DestinationVolume AS (
    SELECT
        Vm.Router,
        Vm.Asset,
        Vm.Destination_Chain AS Chain,
        SUM(Usd_Volume_Last_1_Day) AS Destination_Usd_Volume_Last_1_Day,
        SUM(Usd_Volume_Last_7_Days) AS Destination_Usd_Volume_Last_7_Days,
        SUM(Usd_Volume_Last_30_Days) AS Destination_Usd_Volume_Last_30_Days,
        SUM(Volume_Last_1_Day) AS Destination_Volume_1_Day,
        SUM(Volume_Last_7_Days) AS Destination_Volume_7_Days,
        SUM(Volume_Last_30_Days) AS Destination_Volume_30_Days,
        MAX(Last_Transfer_Date) AS Destination_Last_Transfer_Date
    FROM
        VolumeMetrics Vm
    --JOIN CompletedFastVolume cfv ON vm.router = cfv.router AND vm.asset = cfv.asset AND vm.origin_chain = cfv.origin_chain AND vm.destination_chain = cfv.destination_chain
    GROUP BY 1, 2, 3
),

CombinedVolumes AS (
    SELECT
        COALESCE(Ov.Router, Dv.Router) AS Router,
        COALESCE(Ov.Asset, Dv.Asset) AS Asset,
        COALESCE(Ov.Chain, Dv.Chain) AS Chain,
        Ov.Origin_Usd_Volume_Last_1_Day,
        Ov.Origin_Usd_Volume_Last_7_Days,
        Ov.Origin_Usd_Volume_Last_30_Days,
        --    ov.origin_fast_volume_1_day,
        --    ov.origin_fast_volume_7_days,
        --    ov.origin_fast_volume_30_days,
        Dv.Destination_Usd_Volume_Last_1_Day,
        Dv.Destination_Usd_Volume_Last_7_Days,
        Dv.Destination_Usd_Volume_Last_30_Days
    --    dv.destination_fast_volume_1_day,
    --    dv.destination_fast_volume_7_days,
    --    dv.destination_fast_volume_30_days
    FROM OriginVolume Ov
    FULL OUTER JOIN
        DestinationVolume Dv
        ON Ov.Router = Dv.Router AND Ov.Asset = Dv.Asset AND Ov.Chain = Dv.Chain
),

CombinedView AS (
    SELECT
        Vm.Router AS Router,
        Vm.Asset AS Asset,
        Vm.Origin_Chain AS Chain,
        SUM(Ov.Origin_Usd_Volume_Last_1_Day) AS Ov1d,
        SUM(Ov.Origin_Usd_Volume_Last_7_Days) AS Ov7d,
        SUM(Ov.Origin_Usd_Volume_Last_30_Days) AS Ov30d,
        --  SUM(ov.origin_fast_volume_1_day) AS ofv1d, 
        --  SUM(ov.origin_fast_volume_7_days) AS ofv7d,
        --  SUM(ov.origin_fast_volume_30_days) AS ofv30d,
        SUM(Dv.Destination_Usd_Volume_Last_1_Day) AS Dv1d,
        SUM(Dv.Destination_Usd_Volume_Last_7_Days) AS Dv7d,
        SUM(Dv.Destination_Usd_Volume_Last_30_Days) AS Dv30d,
        --  SUM(dv.destination_fast_volume_1_day) AS dfv1d,
        --  SUM(dv.destination_fast_volume_7_days) AS dfv7d,
        --  SUM(dv.destination_fast_volume_30_days) AS dfv30d,
        MAX(Vm.Last_Transfer_Date) AS Last_Txn_Date,
        SUM(Vm.Slow_Txns) AS Slow_Tns
    FROM
        VolumeMetrics Vm
    FULL OUTER JOIN
        OriginVolume Ov
        ON
            Vm.Router = Ov.Router
            AND Vm.Asset = Ov.Asset
            AND Vm.Origin_Chain = Ov.Chain
    FULL OUTER JOIN
        DestinationVolume Dv
        ON
            Vm.Router = Dv.Router
            AND Vm.Asset = Dv.Asset
            AND Vm.Origin_Chain = Dv.Chain
    GROUP BY
        1, 2, 3
),

Combinations AS (
    SELECT DISTINCT
        Origin_Chain AS Chain,
        Asset,
        Router
    FROM VolumeMetrics
),

Metrics AS (
    SELECT
        Router,
        Asset,
        Origin_Chain AS Chain,
        MAX(Vm.Last_Transfer_Date) AS Last_Txn_Date,
        SUM(Vm.Slow_Txns) AS Slow_Tns
    FROM VolumeMetrics Vm
    GROUP BY 1, 2, 3
),

GroupedMetrics AS (
    SELECT
        Cmbns.Chain AS Chain_Domain,
        Cmbns.Router AS Router_Address,
        Cmbns.Asset AS Asset_Address,
        Ov.Origin_Usd_Volume_Last_1_Day,
        Ov.Origin_Usd_Volume_Last_7_Days,
        Ov.Origin_Usd_Volume_Last_30_Days,
        Ov.Origin_Volume_1_Day,
        Ov.Origin_Volume_7_Days,
        Ov.Origin_Volume_30_Days,
        Cfov.Origin_Fast_Volume_1_Day,
        Cfov.Origin_Fast_Volume_7_Days,
        Cfov.Origin_Fast_Volume_30_Days,
        Dv.Destination_Usd_Volume_Last_1_Day,
        Dv.Destination_Usd_Volume_Last_7_Days,
        Dv.Destination_Usd_Volume_Last_30_Days,
        Dv.Destination_Volume_1_Day,
        Dv.Destination_Volume_7_Days,
        Dv.Destination_Volume_30_Days,
        Cfdv.Destination_Fast_Volume_1_Day,
        Cfdv.Destination_Fast_Volume_7_Days,
        Cfdv.Destination_Fast_Volume_30_Days,
        Last_Txn_Date,
        Slow_Tns
    FROM
        Combinations Cmbns
    LEFT JOIN
        Metrics Mtr
        ON
            Cmbns.Chain = Mtr.Chain
            AND Cmbns.Asset = Mtr.Asset
            AND Cmbns.Router = Mtr.Router
    LEFT JOIN
        OriginVolume Ov
        ON
            Cmbns.Chain = Ov.Chain
            AND Cmbns.Asset = Ov.Asset
            AND Cmbns.Router = Ov.Router
    LEFT JOIN
        DestinationVolume Dv
        ON
            Cmbns.Chain = Dv.Chain
            AND Cmbns.Asset = Dv.Asset
            AND Cmbns.Router = Dv.Router
    LEFT JOIN
        CompletedFastOriginVolume Cfov
        ON
            Cmbns.Chain = Cfov.Chain
            AND Cmbns.Asset = Cfov.Asset
            AND Cmbns.Router = Cfov.Router
    LEFT JOIN
        CompletedFastDestVolume Cfdv
        ON
            Cmbns.Chain = Cfdv.Chain
            AND Cmbns.Asset = Cfdv.Asset
            AND Cmbns.Router = Cfdv.Router

),

LatestAssetPrices AS (
    SELECT
        Ap.Canonical_Domain,
        Ap.Canonical_Id,
        Ap.Price,
        Ap.Timestamp
    FROM `mainnet-bigq.public.asset_prices` Ap
    INNER JOIN (
        SELECT
            Canonical_Domain,
            Canonical_Id,
            MAX(Timestamp) AS Max_Timestamp
        FROM
            `mainnet-bigq.public.asset_prices`
        GROUP BY
            1, 2
    ) AS Latest
        ON
            Ap.Canonical_Domain = Latest.Canonical_Domain
            AND Ap.Canonical_Id = Latest.Canonical_Id
            AND Ap.Timestamp = Latest.Max_Timestamp
),

RouterLiquidity AS (
    SELECT
        COALESCE(Rwb.Domain, Gv.Chain_Domain) AS Chain_Domain_Coalesced,
        COALESCE(Rwb.Adopted, Gv.Asset_Address) AS Asset_Address_Coalesced,
        COALESCE(Rwb.Router_Address, Gv.Router_Address)
            AS Router_Address_Coalesced,
        COALESCE(
            DATE(Gv.Last_Txn_Date), DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ) AS Last_Txn_Date_Coalesced,

        Rwb.Canonical_Domain AS Asset_Canonical_Domain,
        Rwb.Address AS Address,
        Rwb.Adopted AS Adopted,
        Rwb.Adopted_Decimal AS Adopted_Decimal,
        Rwb.Asset_Canonical_Id AS Asset_Canonical_Id,
        Rwb.Asset_Domain AS Asset_Domain_Rwb,
        Rwb.Asset_Usd_Price AS Asset_Usd_Price,
        Rwb.Balance AS Balance,
        Rwb.Balance_Usd AS Balance_Usd,
        Rwb.Decimal AS Decimal,
        Rwb.Domain AS Domain,
        Rwb.Fees_Earned AS Fees_Earned,
        Rwb.Id AS Id,
        Rwb.Key AS Key,
        Rwb.Local AS Local,
        Rwb.Locked AS Locked,
        Rwb.Locked_Usd AS Locked_Usd,
        Rwb.Removed AS Removed,
        Rwb.Removed_Usd AS Removed_Usd,
        Rwb.Router_Address AS Router_Address_Rwb,
        Rwb.Supplied AS Supplied,
        Rwb.Supplied_Usd AS Supplied_Usd,
        Gv.*,
        Lap.*

    FROM
        {{ ref('stg_source__cartographer_router_with_balances') }} Rwb
    --    `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb
    FULL OUTER JOIN
        GroupedMetrics Gv
        ON
            Rwb.Address = Gv.Router_Address
            AND Rwb.Adopted = Gv.Asset_Address
            AND Rwb.Domain = Gv.Chain_Domain
    LEFT JOIN
        LatestAssetPrices Lap
        ON
            Rwb.Asset_Canonical_Id = Lap.Canonical_Id
            AND Rwb.Canonical_Domain = Lap.Canonical_Domain
    WHERE
        Gv.Destination_Usd_Volume_Last_30_Days > 0
        OR Gv.Origin_Usd_Volume_Last_30_Days > 0
        OR CAST(Rwb.Balance AS FLOAT64) > 0
),

Connext_Tokens AS (
    SELECT DISTINCT
        Ct.Token_Address,
        Ct.Token_Name,
        Ct.Is_Xerc20
    FROM
        `mainnet-bigq.stage.connext_tokens` Ct
),

RouterMapping AS (
    SELECT
        COALESCE(Ct.Token_Name, Asset_Address_Coalesced) AS Asset,
        --    cav.domain_name,
        CASE
            WHEN
                Router_Address_Coalesced
                = '0x9584eb0356a380b25d7ed2c14c54de58a25f2581'
                THEN 'Mike Nai'
            WHEN
                Router_Address_Coalesced
                = '0xc4ae07f276768a3b74ae8c47bc108a2af0e40eba'
                THEN 'P2P 2'
            WHEN
                Router_Address_Coalesced
                = '0xeca085906cb531bdf1f87efa85c5be46aa5c9d2c'
                THEN 'Blocktech 2'
            WHEN
                Router_Address_Coalesced
                = '0x22831e4f21ce65b33ef45df0e212b5bebf130e5a'
                THEN 'Blocktech 1'
            WHEN
                Router_Address_Coalesced
                = '0xbe7bc00382a50a711d037eaecad799bb8805dfa8'
                THEN 'Minerva'
            WHEN
                Router_Address_Coalesced
                = '0x63Cda9C42db542bb91a7175E38673cFb00D402b0'
                THEN 'Consensys Mesh'
            WHEN
                Router_Address_Coalesced
                = '0xf26c772c0ff3a6036bddabdaba22cf65eca9f97c'
                THEN 'Connext'
            WHEN
                Router_Address_Coalesced
                = '0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4'
                THEN 'P2P 1'
            WHEN
                Router_Address_Coalesced
                = '0x8cb19ce8eedf740389d428879a876a3b030b9170'
                THEN 'BWare'
            WHEN
                Router_Address_Coalesced
                = '0x0e62f9fa1f9b3e49759dc94494f5bc37a83d1fad'
                THEN 'Bazilik'
            WHEN
                Router_Address_Coalesced
                = '0x58507fed0cb11723dfb6848c92c59cf0bbeb9927'
                THEN 'Hashquark'
            WHEN
                Router_Address_Coalesced
                = '0x7ce49752ffa7055622f444df3c69598748cb2e5f'
                THEN 'Vault Staking'
            WHEN
                Router_Address_Coalesced
                = '0x33b2ad85f7dba818e719fb52095dc768e0ed93ec'
                THEN 'Ethereal'
            WHEN
                Router_Address_Coalesced
                = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
                THEN 'Gnosis'
            WHEN
                Router_Address_Coalesced
                = '0x975574980a5Da77f5C90bC92431835D91B73669e'
                THEN '01 Node'
            WHEN
                Router_Address_Coalesced
                = '0x6892d4D1f73A65B03063B7d78174dC6350Fcc406'
                THEN 'Unagii'
            WHEN
                Router_Address_Coalesced
                = '0x32d63da9f776891843c90787cec54ada23abd4c2'
                THEN 'Ingag'
            WHEN
                Router_Address_Coalesced
                = '0xfaab88015477493cfaa5dfaa533099c590876f21'
                THEN 'Paradox'
            WHEN
                Router_Address_Coalesced
                = '0x49a9e7ec76bc8fdf658d09557305170d9f01d2fa'
                THEN 'Jitin'
            ELSE Router_Address_Coalesced
        END AS Router_Name,
        CASE
            WHEN Pr.Chain_Domain_Coalesced = '6648936' THEN 'Ethereum'
            WHEN Pr.Chain_Domain_Coalesced = '1869640809' THEN 'Optimism'
            WHEN Pr.Chain_Domain_Coalesced = '6450786' THEN 'BNB'
            WHEN Pr.Chain_Domain_Coalesced = '6778479' THEN 'Gnosis'
            WHEN Pr.Chain_Domain_Coalesced = '1886350457' THEN 'Polygon'
            WHEN Pr.Chain_Domain_Coalesced = '1634886255' THEN 'Arbitrum One'
            WHEN Pr.Chain_Domain_Coalesced = '1818848877' THEN 'Linea'
            WHEN Pr.Chain_Domain_Coalesced = '31338' THEN 'Local Optimism'
            WHEN Pr.Chain_Domain_Coalesced = '31339' THEN 'Local Arbitrum One'
            WHEN Pr.Chain_Domain_Coalesced = '1835365481' THEN 'Metis'
            WHEN Pr.Chain_Domain_Coalesced = '1650553709' THEN "Base Mainnet"
            ELSE Pr.Chain_Domain_Coalesced
        END AS Domain_Name,
        Pr.*
    FROM
        RouterLiquidity Pr
    LEFT JOIN Connext_Tokens Ct ON Pr.Asset_Address_Coalesced = Ct.Token_Address
),

ChaihAssetLiquidity AS (
    SELECT
        Rl.Adopted,
        Rl.Adopted_Decimal,
        --    rl.domain,
        Rl.Asset_Canonical_Id,
        Rl.Decimal,
        Rl.Asset_Canonical_Domain,
        SUM(Origin_Usd_Volume_Last_1_Day) AS Origin_Usd_Volume_Last_1_Day,
        SUM(Origin_Usd_Volume_Last_7_Days) AS Origin_Usd_Volume_Last_7_Days,
        SUM(Origin_Usd_Volume_Last_30_Days) AS Origin_Usd_Volume_Last_30_Days,
        SUM(Origin_Volume_1_Day) AS Origin_Volume_1_Day,
        SUM(Origin_Volume_7_Days) AS Origin_Volume_7_Days,
        SUM(Origin_Volume_30_Days) AS Origin_Volume_30_Days,
        SUM(Origin_Fast_Volume_1_Day) AS Origin_Fast_Volume_1_Day,
        SUM(Origin_Fast_Volume_7_Days) AS Origin_Fast_Volume_7_Days,
        SUM(Origin_Fast_Volume_30_Days) AS Origin_Fast_Volume_30_Days,
        SUM(Destination_Usd_Volume_Last_1_Day)
            AS Destination_Usd_Volume_Last_1_Day,
        SUM(Destination_Usd_Volume_Last_7_Days)
            AS Destination_Usd_Volume_Last_7_Days,
        SUM(Destination_Usd_Volume_Last_30_Days)
            AS Destination_Usd_Volume_Last_30_Days,
        SUM(Destination_Volume_1_Day) AS Destination_Volume_1_Day,
        SUM(Destination_Volume_7_Days) AS Destination_Volume_7_Days,
        SUM(Destination_Volume_30_Days) AS Destination_Volume_30_Days,
        SUM(Destination_Fast_Volume_1_Day) AS Destination_Fast_Volume_1_Day,
        SUM(Destination_Fast_Volume_7_Days) AS Destination_Fast_Volume_7_Days,
        SUM(Destination_Fast_Volume_30_Days) AS Destination_Fast_Volume_30_Days,
        MAX(Last_Txn_Date) AS Last_Txn_Date,
        SUM(Slow_Tns) AS Slow_Tns
    FROM
        RouterLiquidity Rl
    GROUP BY
        1, 2, 3, 4, 5
--    ,6
)

--SELECT * FROM ChaihAssetLiquidity
--SELECT * from CompletedFastVolume
SELECT * FROM RouterMapping ORDER BY Destination_Usd_Volume_Last_1_Day DESC
--select * from RouterMapping WHERE RouterMapping.router_address_coalesced = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM RouterLiquidity
--SELECT * from GroupedMetrics
--SELECT * from GroupedMetrics WHERE router_address = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM `mainnet-bigq.y42_connext_y42_dev.source__Cartographer__public_routers_with_balances` rwb WHERE address = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * from Combinations WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--SELECT * FROM DestinationVolume WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--ORDER BY destination_volume_last_1_day desc 
--SELECT * FROM CombinedVolumes
--ORDER BY destination_volume_last_1_day desc 

--SELECT * FROM OriginVolume
--ORDER BY origin_volume_last_1_day desc 
--SELECT DISTINCT asset, router, origin_chain, destination_chain from VolumeMetrics
--SELECT DISTINCT origin_chain from VolumeMetrics
--SELECT * FROM VolumeMetrics WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
--0x4ecaba5870353805a9f068101a40e0f32ed605c6 - USDT
--SELECT * FROM Metrics WHERE router = '0x048a5ecc705c280b2248aeff88fd581abbeb8587'
