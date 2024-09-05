-- Top 100 rows from each table

-- assets:
    -- Takeaway: 
    -- 1. there are 2 assets with addresses
SELECT 'assets' AS table_name, * FROM public.assets LIMIT 100

-- Balances: Nothing in balance
SELECT 'balances' AS table_name, * FROM public.balances LIMIT 100

-- checkpoints: There are check_names: origin/hub_invoice___chain_ids and other cols is check_point: not sure what that is
SELECT 'checkpoints' AS table_name, * FROM public.checkpoints LIMIT 100

-- no data on depositors
SELECT 'depositors' AS table_name, * FROM public.depositors LIMIT 100

-- no data on destination_intents
SELECT 'destination_intents' AS table_name, * FROM public.destination_intents LIMIT 100

-- data by id -> domain | message_id | etc 
SELECT 'hub_intents' AS table_name, * FROM public.hub_intents LIMIT 100

SELECT 'messages' AS table_name, * FROM public.messages LIMIT 100

-- origin_intents, queues, tokens
SELECT 'origin_intents' AS table_name, * FROM public.origin_intents LIMIT 100


-- materialized view: intents
SELECT 'intents' AS table_name, * FROM public.intents LIMIT 100

-- Queue to be used for calculate invoice remains
SELECT 'queues' AS table_name, * FROM public.queues LIMIT 100
--
SELECT 'tokens' AS table_name, * FROM public.tokens LIMIT 100;

-- invoices
SELECT 'invoices' AS table_name, * FROM public.invoices LIMIT 100

-- query views: Intent
SELECT 'public.hub_invoices' AS table_name, * FROM public.hub_invoices LIMIT 100


-- check for user access to Public reader
SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
AND NOT EXISTS (
    SELECT 1
    FROM information_schema.role_table_grants
    WHERE table_schema = schemaname
    AND table_name = tablename
    AND grantee = CURRENT_USER
    AND privilege_type = 'SELECT'
);



-- token Epoch timestamp : changes to token epoch bps every week take a note of cahges as log