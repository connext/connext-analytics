-- logic:
-- for intents that are not in final state. -> merge them.
    -- final state logic -> origin_status IN ('DISPATCHED') AND hub_status IN ('DISPATCHED', 'DISPATCHED_UNSUPPORTED')
-- get new intents based on max timestamp of origin_timestamp in BQ and append them
    -- PULL MAX TIMESTAMP FROM BQ
    -- USING THIS AS FILTER IN WHERE CALUSE ALONG WITH STATUS OF FINAL STATE -> origin_status IN ('DISPATCHED') AND hub_status IN ('DISPATCHED', 'DISPATCHED_UNSUPPORTED'), PULL ALL THOSE TXS AND APPEND TO BQ TABLE

SELECT *
FROM public.intents