

-- Metric 9: **Total_rebalancing_fee**: Total fee = Protocol fee + Discount
-- CAL; from intents table, get discount and get protocol from the token tables that are then matched to settlement tokens

WITH 
token_decimals AS (
SELECT 
    LOWER(a.adopted) AS token_address,
    CASE 
        WHEN a.adopted = LOWER('0x000000000000000000000000d26e3540A0A368845B234736A0700E0a5A821bBA') THEN 18
        WHEN a.adopted = LOWER('0x0000000000000000000000005f921E4DE609472632CEFc72a3846eCcfbed4ed8') THEN 18
        WHEN a.adopted = LOWER('0x0000000000000000000000007Fa13D6CB44164ea09dF8BCc673A8849092D435b') THEN 18
        WHEN a.adopted  = LOWER('0x000000000000000000000000aBF282c88DeD3e386701a322e76456c062468Ac2') THEN 18
        WHEN a.adopted  = LOWER('0x000000000000000000000000d18C5E22E67947C8f3E112C622036E65a49773ab') THEN 6
        WHEN a.adopted  = LOWER('0x000000000000000000000000def63AA35372780f8F92498a94CD0fA30A9beFbB') THEN 18
        WHEN a.adopted  = LOWER('0x000000000000000000000000294FD6cfb1AB97Ad5EA325207fF1d0E85b9C693f') THEN 6
        WHEN a.adopted  = LOWER('0x000000000000000000000000DFEA0bb49bcdCaE920eb39F48156B857e817840F') THEN 6
        WHEN a.adopted  = LOWER('0x0000000000000000000000008F936120b6c5557e7Cd449c69443FfCb13005751') THEN 18
        WHEN a.adopted  = LOWER('0x0000000000000000000000009064cD072D5cEfe70f854155d1b23171013be5c7') THEN 18
        WHEN a.adopted  = LOWER('0x000000000000000000000000D3D4c6845e297e99e219BD8e3CaC84CA013c0770') THEN 18
        WHEN a.adopted  = LOWER('0x000000000000000000000000d6dF5E67e2DEF6b1c98907d9a09c18b2b7cd32C3') THEN 18
        ELSE NULL
    END AS decimals
FROM public.assets a
)

,raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    -- discount    
    ABS((i.origin_amount::float / POW(10, td.decimals)) - (i.settlement_amount::float / POW(10, td.decimals)))
     AS discount,
    -- [ ] TODO ??? protocol fee: check if the fee_amounts in token table is perct    
    -- jsonb_array_elements_text(to_jsonb(t.fee_amounts))::float -> Fee amount by token -> 0.0001 is 1bps
    ABS(((i.origin_amount::float / POW(10, td.decimals)) / 100 * 0.0001)::float) AS fee_amount
FROM public.intents i
LEFT JOIN token_decimals td ON LOWER(i.origin_output_asset) = td.token_address
-- LEFT JOIN public.tokens t ON i.settlement_asset = t.id
WHERE i.settlement_status = 'SETTLED'
)

SELECT 
    day,
    SUM(discount) AS discount,
    SUM(fee_amount) AS protocol_fee,
    -- rebalancing_fee: Total fee = Protocol fee + Discount
    SUM(fee_amount + discount) AS rebalancing_fee
FROM raw
GROUP BY 1
ORDER BY 1 DESC;


-- missing rewrds in the above query:
-- amounts from the hub invoice amount is thr rewards
-- rewards = origin_amount - hub_invoiced_amount -> accurate for intents that become invoices
-- discounts = hub_invoiced_amount - settlement_amount
-- rebalancing_fee = protocol_fee + discounts