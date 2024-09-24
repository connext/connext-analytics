-- For the destination: Base, compare volume against other chains

SELECT
    DATE(xcall_date) AS date,
    destination_domain_name,
    SUM(usd_amount) AS volume
FROM `metrics.transfers_mapped`
WHERE
    DATE(xcall_date) >= (CURRENT_DATE - INTERVAL 90 DAY)
    AND destination_asset_name = 'weth'
    AND status IN ('CompletedSlow', 'CompletedFast')
GROUP BY 1, 2
