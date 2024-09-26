SELECT DISTINCT chainid, name
FROM `mainnet-bigq.raw.source_chainlist_network__chains`
WHERE chainid IN (1, 10, 56, 8453, 25327, 42161)

UNION ALL 

SELECT 
25327 AS chainid,
'Everclear' AS name