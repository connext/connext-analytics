--doc:https://github.com/connext/monorepo/blob/main/packages/adapters/database/db/migrations/20230508151158_refactor_daily_transfers_volume_with_mat.sql

SELECT
    tf.status,
    DATE_TRUNC(tf.xcall_timestamp, DAY) AS transfer_date,
    tf.origin_domain AS origin_chain,
    tf.destination_domain AS destination_chain,
    tf.router,
    tf.origin_asset AS asset,
    SUM(CAST(tf.origin_transacting_amount AS FLOAT64)) AS volume,
    AVG(tf.price) AS avg_price,
    SUM(tf.usd_bridged_amount) AS usd_volume,
    ROW_NUMBER() OVER () AS id
FROM `mainnet-bigq.stage.stg_transfers_raw_usd` tf
GROUP BY
    tf.status,
    DATE_TRUNC(tf.xcall_timestamp, DAY),
    tf.origin_domain,
    tf.destination_domain,
    tf.router,
    tf.origin_asset
