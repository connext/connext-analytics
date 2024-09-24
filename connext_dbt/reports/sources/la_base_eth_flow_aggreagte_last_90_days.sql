SELECT
    status,
    origin_domain_name,
    destination_domain_name,
    SUM(usd_amount) AS volume,
    COUNT(transfer_id) AS transfers
FROM `metrics.transfers_mapped`
WHERE
    DATE(xcall_date) >= (CURRENT_DATE - INTERVAL 90 DAY)
    AND (
        origin_domain_name = "Base Mainnet"
        OR destination_domain_name = "Base Mainnet"
    )
    AND destination_asset_name = 'weth'
GROUP BY 1, 2, 3
