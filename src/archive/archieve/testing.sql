SELECT 
    *
FROM public.intents i
WHERE i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status IN ('DISPATCHED', 'SETTLED')
AND CAST(i.settlement_domain AS INTEGER) = 10 AND i.settlement_asset = LOWER('0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85')