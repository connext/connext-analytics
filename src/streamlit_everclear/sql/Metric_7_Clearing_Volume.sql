-- **Metric 7: Clearing_Volume**: Clearing volume (settlement + netted)
    -- settlement: sum of all settled amount in hub_intent table
    -- netted: double cehck for settlement only
    -- ttl is zero that is a netted order | Others are filled intent order or solver based order

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

SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CASE WHEN i.origin_ttl = 0 THEN 'netted' ELSE 'filled' END as intent_type,
    SUM(i.settlement_amount::float / 10^td.decimals) as volume
  FROM public.intents i
  LEFT JOIN token_decimals td ON LOWER(i.origin_output_asset) = td.token_address
  WHERE i.settlement_status = 'SETTLED'
  GROUP BY 1,2
