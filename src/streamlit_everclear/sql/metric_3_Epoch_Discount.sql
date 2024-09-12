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
    -- AVG(i.origin_amount::float - i.settlement_amount::float) as discount_value,
    AVG(hi.settlement_enqueued_timestamp -i.origin_timestamp) as discount_epoch
 FROM public.intents i
 LEFT JOIN public.hub_intents hi ON i.id = hi.id
 -- filter out the netted invoices
 WHERE i.origin_ttl > 0 AND i.settlement_status = 'SETTLED'
 GROUP BY 1