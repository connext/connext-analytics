WITH transfer_raw AS (
SELECT *
FROM `mainnet-bigq.public.transfers` t
WHERE (
  EXTRACT(MONTH FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = {{month}}
  AND EXTRACT(YEAR FROM TIMESTAMP_SECONDS(t.xcall_timestamp)) = {{year}})
)

, hr_asset_price AS (
  SELECT 
    ap.canonical_id,
    ap.timestamp - MOD(ap.timestamp,1800) AS timestamp,
    MAX(ap.price) AS price
  FROM `mainnet-bigq.public.asset_prices` ap
  GROUP BY 1,2
)

, transfers_usd_price  AS (
  SELECT
    tr.*,         
    ap.price,
    ROW_NUMBER() OVER
          (
            PARTITION BY tr.transfer_id
            ORDER BY ap.timestamp DESC
          ) AS closet_price_rank
  FROM transfer_raw tr
  LEFT JOIN hr_asset_price ap
    ON (
      tr.canonical_id = ap.canonical_id
      AND ap.timestamp <= (tr.xcall_timestamp - MOD(tr.xcall_timestamp,1800))
      )
)

SELECT 
  tsp.* EXCEPT(routers)
FROM transfers_usd_price tsp
WHERE closet_price_rank = 1