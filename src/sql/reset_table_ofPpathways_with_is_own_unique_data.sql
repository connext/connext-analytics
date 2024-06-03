-- Step  1: Create a temporary table
CREATE TEMPORARY TABLE temp_table AS
SELECT DISTINCT
    fromchainid,
    fromtokenaddress,
    tochainid,
    totokenaddress,
    FORMAT(
        "%.15f",
        POW(10, ROUND(LOG10(CAST(fromamount AS FLOAT64))))
    ) AS fromamount
FROM `stage.source_lifi__pathways`;

-- Step  2: Update the original table
CREATE OR REPLACE TABLE `stage.source_lifi__pathways` AS
SELECT * FROM temp_table;
