SELECT 
    i.id,
    CAST(inv.hub_invoice_amount AS FLOAT) AS hub_invoice_amount,
     CAST(i.origin_amount AS FLOAT) AS origin_amount,
    (CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.origin_amount AS FLOAT)) AS rewards_for_invoices
FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status IN ('DISPATCHED', 'SETTLED')
ORDER BY 2