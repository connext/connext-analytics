--TODO
-- Pulling price dta
-- adding usd values for amounts


SELECT
    si.updated_at,
    si.id,
    si.status,
    si.hub_status,
    si.origin_initiator,
    -- from
    si.from_hash,
    si.origin_timestamp,
    si.from_chain_id,
    si.from_chain_name,
    si.from_asset_address,
    si.from_asset_symbol,
    si.from_asset_decimals,
    si.from_asset_amount,
    p.price AS from_asset_price,
    p.price * si.from_asset_amount AS from_asset_amount_usd,
    
    -- to
    si.settlement_timestamp,
    si.to_chain_id,
    si.to_chain_name,
    si.to_asset_address,
    si.to_asset_symbol,
    si.to_asset_decimals,
    si.to_asset_amount,
    p.price AS to_asset_price,
    p.price * si.to_asset_amount AS to_asset_amount_usd,
    -- misc
    si.hub_added_timestamp,
    si.hub_settlement_enqueued_timestamp,
    si.fee_value,
    si.hub_settlement_enqueued_timestamp_epoch,
    si.hub_added_timestamp_epoch,
    p.price * si.hub_settlement_amount AS hub_settlement_amount_usd

    
    
FROM {{ ref("stg_intents") }} si
LEFT JOIN {{ref("cln_everclear_token_price_by_minutes")}} p
ON si.from_asset_symbol = p.symbol
AND DATE_TRUNC(si.origin_timestamp, MINUTE) = p.minute