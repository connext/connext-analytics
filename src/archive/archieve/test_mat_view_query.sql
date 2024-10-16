SELECT pg_get_viewdef('public.daily_metrics_by_chains_tokens'::regclass, true);


 WITH metadata AS (
         SELECT asset_data.symbol,
            asset_data.decimals AS "decimal",
            asset_data.domainid AS domain_id,
            lower(asset_data.address) AS address,
            lower(concat('0x', lpad(SUBSTRING(asset_data.address FROM 3), 64, '0'::text))) AS adopted_address
           FROM ( VALUES ('Wrapped Ether'::text,'WETH'::text,18,1,'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::text), ('Wrapped Ether'::text,'WETH'::text,18,10,'0x4200000000000000000000000000000000000006'::text), ('Wrapped Ether'::text,'WETH'::text,18,56,'0x2170Ed0880ac9A755fd29B2688956BD959F933F8'::text), ('Wrapped Ether'::text,'WETH'::text,18,8453,'0x4200000000000000000000000000000000000006'::text), ('Wrapped Ether'::text,'WETH'::text,18,42161,'0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'::text), ('USD Coin'::text,'USDC'::text,6,1,'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::text), ('USD Coin'::text,'USDC'::text,6,10,'0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85'::text), ('USD Coin'::text,'USDC'::text,18,56,'0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'::text), ('USD Coin'::text,'USDC'::text,6,8453,'0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::text), ('USD Coin'::text,'USDC'::text,6,42161,'0xaf88d065e77c8cC2239327C5EDb3A432268e5831'::text), ('Tether USD'::text,'USDT'::text,6,1,'0xdAC17F958D2ee523a2206206994597C13D831ec7'::text), ('Tether USD'::text,'USDT'::text,6,10,'0x94b008aA00579c1307B0EF2c499aD98a8ce58e58'::text), ('Tether USD'::text,'USDT'::text,18,56,'0x55d398326f99059fF775485246999027B3197955'::text), ('Tether USD'::text,'USDT'::text,6,42161,'0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7'::text), ('Tether USD'::text,'USDT'::text,6,42161,'0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'::text)) asset_data(assetname, symbol, decimals, domainid, address)
        ), netted_raw AS (
         SELECT date_trunc('day'::text, to_timestamp(i.origin_timestamp::double precision)) AS day,
            i.origin_origin::integer AS from_chain_id,
            i.origin_input_asset AS from_asset_address,
            fm.symbol AS from_asset_symbol,
            i.settlement_domain::integer AS to_chain_id,
            i.settlement_asset AS to_asset_address,
            tm.symbol AS to_asset_symbol,
            sum(i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS netting_volume,
            avg((i.settlement_timestamp::double precision - i.origin_timestamp::double precision) / 3600::double precision) AS netting_avg_time_in_hrs,
            sum(0.0001::double precision * i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS netting_protocol_revenue,
            count(i.id) AS netting_total_intents,
            avg(i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS netting_avg_intent_size
           FROM intents i
             LEFT JOIN invoices inv ON i.id = inv.id
             LEFT JOIN metadata fm ON i.origin_input_asset::text = fm.adopted_address AND i.origin_origin::integer = fm.domain_id
             LEFT JOIN metadata tm ON lower(i.settlement_asset::text) = tm.address AND i.settlement_domain::integer = tm.domain_id
          WHERE inv.id IS NULL AND i.status = 'SETTLED_AND_COMPLETED'::intent_status AND i.hub_status <> 'DISPATCHED_UNSUPPORTED'::intent_status
          GROUP BY (date_trunc('day'::text, to_timestamp(i.origin_timestamp::double precision))), (i.origin_origin::integer), i.origin_input_asset, fm.symbol, (i.settlement_domain::integer), i.settlement_asset, tm.symbol
        ), netted_final AS (
         SELECT netted_raw.day,
            netted_raw.from_chain_id,
            netted_raw.from_asset_address,
            netted_raw.from_asset_symbol,
            netted_raw.to_chain_id,
            netted_raw.to_asset_address,
            netted_raw.to_asset_symbol,
            netted_raw.netting_volume,
            netted_raw.netting_avg_intent_size,
            netted_raw.netting_protocol_revenue,
            netted_raw.netting_total_intents,
            netted_raw.netting_avg_time_in_hrs
           FROM netted_raw
        ), settled_raw AS (
         SELECT date_trunc('day'::text, to_timestamp(i.origin_timestamp::double precision)) AS day,
            i.origin_origin::integer AS from_chain_id,
            i.origin_input_asset AS from_asset_address,
            fm.symbol AS from_asset_symbol,
            i.settlement_domain::integer AS to_chain_id,
            i.settlement_asset AS to_asset_address,
            tm.symbol AS to_asset_symbol,
            avg(inv.hub_invoice_amount::double precision / (10::double precision ^ 18::double precision) - i.settlement_amount::double precision / (10::double precision ^ tm."decimal"::double precision)) AS avg_discounts_by_mm,
            sum(inv.hub_invoice_amount::double precision / (10::double precision ^ 18::double precision) - i.settlement_amount::double precision / (10::double precision ^ tm."decimal"::double precision)) AS discounts_by_mm,
            avg(inv.hub_invoice_amount::double precision / (10::double precision ^ 18::double precision) - i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS avg_rewards_by_invoice,
            sum(inv.hub_invoice_amount::double precision / (10::double precision ^ 18::double precision) - i.origin_amount::double precision / (10::double precision ^ 18::double precision) - 0.0001::double precision * i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS rewards_for_invoices,
            sum(i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS volume_settled_by_mm,
            count(i.id) AS total_intents_by_mm,
            avg((i.hub_settlement_enqueued_timestamp::double precision - i.hub_added_timestamp::double precision) / 3600::double precision) AS avg_time_in_hrs,
            round(avg(inv.hub_settlement_epoch - inv.hub_invoice_entry_epoch), 0) AS avg_discount_epoch,
            sum(0.0001::double precision * i.origin_amount::double precision / (10::double precision ^ 18::double precision)) AS protocol_revenue_mm
           FROM intents i
             JOIN invoices inv ON i.id = inv.id
             LEFT JOIN metadata fm ON i.origin_input_asset::text = fm.adopted_address AND i.origin_origin::integer = fm.domain_id
             LEFT JOIN metadata tm ON lower(i.settlement_asset::text) = tm.address AND i.settlement_domain::integer = tm.domain_id
          WHERE i.status = 'SETTLED_AND_COMPLETED'::intent_status AND (i.hub_status = ANY (ARRAY['DISPATCHED'::intent_status, 'SETTLED'::intent_status]))
          GROUP BY (date_trunc('day'::text, to_timestamp(i.origin_timestamp::double precision))), (i.origin_origin::integer), i.origin_input_asset, fm.symbol, (i.settlement_domain::integer), i.settlement_asset, tm.symbol
        ), settled_final AS (
         SELECT settled_raw.day,
            settled_raw.from_chain_id,
            settled_raw.from_asset_address,
            settled_raw.from_asset_symbol,
            settled_raw.to_chain_id,
            settled_raw.to_asset_address,
            settled_raw.to_asset_symbol,
            settled_raw.volume_settled_by_mm,
            settled_raw.protocol_revenue_mm,
            settled_raw.total_intents_by_mm,
            settled_raw.discounts_by_mm,
            settled_raw.avg_discounts_by_mm,
            settled_raw.rewards_for_invoices,
            settled_raw.avg_rewards_by_invoice,
            settled_raw.avg_time_in_hrs AS avg_settlement_time_in_hrs_by_mm,
            settled_raw.discounts_by_mm / settled_raw.volume_settled_by_mm * 365::double precision * 100::double precision AS apy,
            settled_raw.avg_discount_epoch AS avg_discount_epoch_by_mm
           FROM settled_raw
        )
 SELECT COALESCE(n.day, s.day) AS day,
    COALESCE(n.from_chain_id, s.from_chain_id) AS from_chain_id,
    COALESCE(n.from_asset_address, s.from_asset_address) AS from_asset_address,
    COALESCE(n.from_asset_symbol, s.from_asset_symbol) AS from_asset_symbol,
    COALESCE(n.to_chain_id, s.to_chain_id) AS to_chain_id,
    COALESCE(n.to_asset_address, s.to_asset_address) AS to_asset_address,
    COALESCE(n.to_asset_symbol, s.to_asset_symbol) AS to_asset_symbol,
    n.netting_volume,
    n.netting_avg_intent_size,
    n.netting_protocol_revenue,
    n.netting_total_intents,
    n.netting_avg_time_in_hrs,
    s.volume_settled_by_mm,
    s.total_intents_by_mm,
    s.discounts_by_mm,
    s.avg_discounts_by_mm,
    s.rewards_for_invoices,
    s.avg_rewards_by_invoice,
    s.avg_settlement_time_in_hrs_by_mm,
    s.apy,
    s.avg_discount_epoch_by_mm,
    n.netting_volume + s.volume_settled_by_mm AS total_volume,
    n.netting_total_intents + s.total_intents_by_mm AS total_intents,
    n.netting_protocol_revenue + s.protocol_revenue_mm AS total_protocol_revenue,
    n.netting_protocol_revenue + s.protocol_revenue_mm + s.discounts_by_mm AS total_rebalancing_fee
   FROM netted_final n
     FULL JOIN settled_final s ON n.day = s.day AND n.from_chain_id = s.from_chain_id AND n.to_chain_id = s.to_chain_id AND n.from_asset_address::text = s.from_asset_address::text AND n.to_asset_address::text = s.to_asset_address::text;