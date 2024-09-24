WITH transfers_mapping AS (
    SELECT *
    FROM `mainnet-bigq.stage.stg_transfers_raw_usd`
)

SELECT *, xcall_timestamp AS date, usd_destination_amount AS usd_amount FROM transfers_mapping