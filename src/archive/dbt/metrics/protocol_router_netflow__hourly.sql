with chain_metadata AS (
  SELECT DISTINCT name, chainid FROM `mainnet-bigq.raw.source_chainlist_network__chains`
  UNION ALL
  SELECT "tron" AS name, 728126428 AS chainid 
)

SELECT *
  -- scm.name AS src_chain_name, dcm.name AS dest_chain_name, rt.src_chain_id, rt.dest_chain_id
FROM `mainnet-bigq.raw.source_router_protocol__transactions` rt
LEFT JOIN chain_metadata scm ON rt.src_chain_id = CAST(scm.chainid AS STRING)
LEFT JOIN chain_metadata dcm ON rt.dest_chain_id = CAST(dcm.chainid AS STRING)
WHERE (scm.chainid IS NULL OR dcm.chainid IS NULL) AND status = "completed"
-- WHERE dest_chain_id= "728126428"
