-- use vatalik adress: 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045

WITH
  chain_transfers AS (
  SELECT
    'zkSync' AS origin,
    'Ethereum Mainnet' AS destination,
    324 AS origin_chain_id,
    1 AS destination_chain_id
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Arbitrum One',
    1,
    42161
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Base Mainnet',
    1,
    8453
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Linea Mainnet',
    1,
    59144
  UNION ALL
  SELECT
    'Arbitrum One',
    'Ethereum Mainnet',
    42161,
    1
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Arbitrum One',
    1,
    42161
  UNION ALL
  SELECT
    'Ethereum Mainnet',
    'Optimism Mainnet',
    1,
    10
    ),
    raw AS (
    SELECT
    usedbridgenames,
    CAST(from_chain AS INT64) AS from_chain,
    CAST(to_chain AS INT64) AS to_chain,
    from_token,
    to_token,
    in_value_usd AS in_amount,
    CASE
        WHEN ABS(in_value_usd - 1000) <= 100 THEN 1000
        WHEN ABS(in_value_usd - 10000) <= 1000 THEN 10000
        WHEN ABS(in_value_usd - 100000) <= 10000 THEN 100000
        ELSE NULL
    END AS amount_category,
    -- fee_type,
    -- in_value_usd,
    -- out_value_usd,
    -- fee_usd,
    -- gas_fee
    AVG(final_out_amount) AS avg_out_amount,
    AVG(max_value_rank_by_output) AS avg_rank_price
    FROM `mainnet-bigq.ad_hoc.stg_clean_top_pathways__lifi_socket` ls
    WHERE final_out_amount > 0
    GROUP BY 1,2,3,4,5,6, 7
    ),

final AS (

    SELECT
    r.usedbridgenames,
    ct.origin AS from_chain,
    r.from_chain AS from_chain_id,
    ct.destination AS to_chain,
    r.to_chain AS to_chain_id,
    r.from_token AS from_token,
    r.to_token AS to_token,

    -- do a case for when amount_category is NULL then 1000 use the raw in_amount else use the amount_category
    CASE
        WHEN r.amount_category IS NULL THEN 1000
        ELSE r.amount_category
    END AS in_amount,
    -- do a case for when amount_category is NULL then 1000 use the raw in_amount else use the amount_category
    CASE
        WHEN r.amount_category IS NULL THEN r.avg_out_amount - (r.in_amount - 1000)
        ELSE r.avg_out_amount - (r.in_amount - r.amount_category)
    END AS out_amount,
    -- other
    r.in_amount AS in_amount_raw,
    r.avg_out_amount AS out_amount_raw,
    r.avg_rank_price,
    r.in_amount - r.amount_category AS amount_diff,
    r.amount_category
    
    FROM raw r
    LEFT JOIN chain_transfers ct 
    ON r.from_chain = ct.origin_chain_id AND r.to_chain = ct.destination_chain_id
    ),

-- create: from_chain	to_chain	token	input_amount	price_rank	best_price
ranking_bridges_raw AS (
    SELECT rb.*, DENSE_RANK() OVER (PARTITION BY rb.from_chain, rb.to_chain, rb.token, rb.input_amount ORDER BY rb.price DESC) AS best_bridge_rank
    FROM (
        SELECT
            f.from_chain,
            f.to_chain,
            f.from_token AS token,
            f.in_amount AS input_amount,
            f.usedbridgenames AS bridge,
            avg(f.out_amount) AS price
        FROM final f
        GROUP BY 1,2,3,4,5
    ) rb
)

SELECT 
    rb.from_chain,
    rb.to_chain,
    rb.token,
    CAST(rb.input_amount AS STRING) AS input_amount,
    rb.price,
    rb.bridge,
    rb.best_bridge_rank
FROM ranking_bridges_raw rb
WHERE best_bridge_rank <= 2