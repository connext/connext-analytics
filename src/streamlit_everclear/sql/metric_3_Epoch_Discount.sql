-- Metric 3: Epoch_Discounts- Number of epoch discounts applied to the invoice before settlement
-- LOGIC: based on the epoch logic using the above intents table create this metric-> use hub_invoice and origin_intent columns
-- diff amounts: orgin_intent_amount - settled_amount
-- diff epoch: settlement_timestamp - origin_timestamp
-- Events from Queue -> SettlementEnqueued | DepositEnqueued
-- DepositEnqueued -->SettlementEnqueued or DepositProcessed->SettlementEnqueued

-- 3.1. EPOCH AMOUNT DISCOUNT: same as metric 5.
-- 3.2. EPOCH TIME DISCOUNT:
-- This is calculated based on hub entry and settlement epoch and only for those intents that are not netted ie origin_ttl > 0
-- discount is decided by hub
-- use the hub timestamp -> Settlement_enqueued_timestamp -> its the timestamp where the settlement is finalized


SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    -- each epoch is 30 mins so count avg epoch based on the time
    ROUND(AVG(i.hub_settlement_epoch - i.hub_invoice_entry_epoch), 0) as discount_epoch
FROM public.invoices i
WHERE i.hub_status IN ('DISPATCHED', 'SETTLED')
GROUP BY 1
 ORDER BY 1 DESC;