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
UNION ALL
SELECT 'queues' AS table_name, * FROM public.queues LIMIT 100
UNION ALL
SELECT 'tokens' AS table_name, * FROM public.tokens LIMIT 100;