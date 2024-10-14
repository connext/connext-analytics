SELECT 
    -- groups
    COALESCE(n.day, s.day) AS day,
    COALESCE(n.from_chain_id, s.from_chain_id) AS from_chain_id,
    COALESCE(n.from_chain_name, s.from_chain_name) AS from_chain_name,
    COALESCE(n.from_asset_address, s.from_asset_address) AS from_asset_address,
    COALESCE(n.from_asset_symbol, s.from_asset_symbol) AS from_asset_symbol,
    COALESCE(n.to_chain_id, s.to_chain_id) AS to_chain_id,
    COALESCE(n.to_chain_name, s.to_chain_name) AS to_chain_name,
    COALESCE(n.to_asset_address, s.to_asset_address) AS to_asset_address,
    COALESCE(n.to_asset_symbol, s.to_asset_symbol) AS to_asset_symbol,
    
    -- metrics
    n.netting_total_intents,
    n.netting_avg_time_in_hrs,
    n.netting_volume_usd,
    n.netting_avg_intent_size_usd,
    n.netting_protocol_revenue_usd,
    s.volume_settled_by_mm_usd,
    s.total_intents_by_mm,
    s.discounts_by_mm_usd,
    s.avg_discounts_by_mm_usd,
    s.rewards_for_invoices_usd,
    s.avg_rewards_by_invoice_usd,
    s.avg_settlement_time_in_hrs_by_mm,
    s.apy,
    s.avg_discount_epoch_by_mm,
    -- add the combinations of metrics here
    -- clearing volume
    COALESCE(n.netting_volume_usd, 0) + COALESCE(s.volume_settled_by_mm_usd, 0) AS total_volume_usd,
    -- intents
    COALESCE(n.netting_total_intents, 0) + COALESCE(s.total_intents_by_mm, 0) AS total_intents,
    -- revenue
    COALESCE(n.netting_protocol_revenue_usd, 0) + COALESCE(s.protocol_revenue_mm_usd, 0) AS total_protocol_revenue_usd,
    -- rebalancing fee
    COALESCE(n.netting_protocol_revenue_usd, 0) + COALESCE(s.protocol_revenue_mm_usd, 0) + COALESCE(s.discounts_by_mm_usd, 0) AS total_rebalancing_fee_usd
FROM  {{ref('mtr_netting_everclear__daily_by_chains_tokens')}} n
FULL OUTER JOIN {{ref('mtr_settled_everclear__daily_by_chains_tokens')}} s 
    ON n.day = s.day
    AND n.from_chain_id = s.from_chain_id
    AND n.to_chain_id = s.to_chain_id
    AND n.from_asset_address = s.from_asset_address
    AND n.to_asset_address = s.to_asset_address