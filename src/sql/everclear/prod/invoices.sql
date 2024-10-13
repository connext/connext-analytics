-- logic:
-- for invoices that are not in final state. -> merge them.
-- get new invoices based on max timestamp of origin_timestamp in BQ and append themS
SELECT *
FROM public.invoices