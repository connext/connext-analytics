SELECT
    siv.updated_at,
    siv.id,
    siv.hub_invoice_id,
    siv.hub_invoice_intent_id,
    siv.origin_timestamp,
    siv.origin_status,
    siv.hub_status,
    siv.hub_invoice_enqueued_timestamp,
    siv.hub_invoice_entry_epoch,
    siv.hub_settlement_epoch,
    siv.origin_initiator,
    siv.from_chain_id,
    siv.from_chain_name,
    siv.from_asset_address,
    siv.from_asset_symbol,
    siv.from_asset_decimals,
    siv.hub_invoice_amount,
    p.price AS from_asset_price,
    siv.hub_invoice_amount * p.price AS hub_invoice_amount_usd

FROM {{ ref("stg_invoices") }} siv
LEFT JOIN {{ref("cln_everclear_token_price_by_minutes")}} p
ON siv.from_asset_symbol = p.symbol
AND DATE_TRUNC(siv.origin_timestamp, MINUTE) = p.minute