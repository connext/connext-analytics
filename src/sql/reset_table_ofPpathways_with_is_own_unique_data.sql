-- Step  1: Create a temporary table
CREATE TEMPORARY TABLE temp_table AS
SELECT DISTINCT
  fromChainId,
  fromTokenAddress,
  toChainId,
  toTokenAddress,
  FORMAT("%.15f",
    POW(10, ROUND(LOG10( CAST(fromAmount AS FLOAT64)))) 
  ) AS fromAmount
FROM `stage.source_lifi__pathways`;

-- Step  2: Update the original table
CREATE OR REPLACE TABLE `stage.source_lifi__pathways` AS
SELECT * FROM temp_table;