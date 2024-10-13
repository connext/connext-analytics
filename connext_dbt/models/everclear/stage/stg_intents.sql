WITH raw AS (
SELECT * FROM {{ source("everclear_prod_db", "intents") }}


-- TODO:
-- 