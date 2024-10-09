WITH metadata AS (
    SELECT Symbol AS symbol,
        CAST(Decimals AS INTEGER) AS decimal,
        CAST(DomainID AS INTEGER) AS domain_id,
        LOWER(Address) AS address,
        LOWER(
            CONCAT(
                '0x',
                LPAD(
                    SUBSTRING(
                        Address
                        FROM 3
                    ),
                    64,
                    '0'
                )
            )
        ) AS adopted_address
    FROM (
            VALUES (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    1,
                    '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    10,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    56,
                    '0x2170Ed0880ac9A755fd29B2688956BD959F933F8'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    8453,
                    '0x4200000000000000000000000000000000000006'
                ),
                (
                    'Wrapped Ether',
                    'WETH',
                    18,
                    42161,
                    '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    1,
                    '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    10,
                    '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85'
                ),
                (
                    'USD Coin',
                    'USDC',
                    18,
                    56,
                    '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    8453,
                    '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
                ),
                (
                    'USD Coin',
                    'USDC',
                    6,
                    42161,
                    '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    1,
                    '0xdAC17F958D2ee523a2206206994597C13D831ec7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    10,
                    '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58'
                ),
                (
                    'Tether USD',
                    'USDT',
                    18,
                    56,
                    '0x55d398326f99059fF775485246999027B3197955'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7'
                ),
                (
                    'Tether USD',
                    'USDT',
                    6,
                    42161,
                    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'
                )
        ) AS asset_data (AssetName, Symbol, Decimals, DomainID, Address)
),

raw_message_intents AS (
    SELECT
        m.id AS message_id,
        m.message_status,
        m.type,
        unnest(m.intent_ids) AS intent_ids,
        origin_domain,
        destination_domain,
        array_length(m.intent_ids, 1) AS intent_count,
        (m.gas_price::float * m.gas_limit::float) / array_length(m.intent_ids, 1) AS gas_amount_per_intent,
        m.quote::float / array_length(m.intent_ids, 1) AS quote_per_intent
FROM
    public.messages m
)

, semi AS (

    SELECT
        i.id,
        i.status,
        i.hub_status,
        to_timestamp(i.origin_timestamp) AS origin_timestamp,
        to_timestamp(i.hub_added_timestamp) AS hub_added_timestamp,
        to_timestamp(i.hub_settlement_enqueued_timestamp) AS hub_settlement_enqueued_timestamp,
        to_timestamp(i.settlement_timestamp) AS settlement_timestamp,
        CAST(i.origin_origin AS INTEGER) AS from_chain_id,
        i.origin_input_asset AS from_asset_address,
        fm.symbol AS from_asset_symbol,
        CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
        i.settlement_asset AS to_asset_address,
        tm.symbol AS to_asset_symbol,
        i.hub_settlement_domain,
        (i.origin_amount::float / 10 ^ 18) AS origin_amount,
        (inv.hub_invoice_amount::float / 10 ^ 18) AS hub_invoice_amount,
        (i.settlement_amount::float / 10 ^ tm.decimal) AS settlement_amount,
        -- intent status message
        intent_msg.message_status AS intent_message_status,
        intent_msg.type AS intent_message_type,
        intent_msg.intent_ids AS intent_message_intent_id,
        intent_msg.origin_domain AS intent_origin_domain,
        intent_msg.destination_domain AS intent_destination_domain,
        CASE WHEN intent_msg.origin_domain::int = 56 THEN 'BNB' ELSE 'ETH' END AS intent_gas_token_symbol,
        intent_msg.intent_count AS intent_message_intent_count,
        (intent_msg.gas_amount_per_intent::float / 10 ^ 18) AS intent_message_gas_amount_per_intent,
        (intent_msg.quote_per_intent::float / 10 ^ 18)  AS intent_message_quote_per_intent,

        -- settlement statusmessage
        settle_msg.message_status AS settlement_message_status,
        settle_msg.type AS settlement_message_type,
        settle_msg.intent_ids AS settlement_message_intent_id,
        settle_msg.origin_domain AS settlement_origin_domain,
        settle_msg.destination_domain AS settlement_destination_domain,
        CASE WHEN settle_msg.origin_domain::int = 56 THEN 'BNB' ELSE 'ETH' END AS settlement_gas_token_symbol,
        settle_msg.intent_count AS settlement_message_intent_count,
        (settle_msg.gas_amount_per_intent::float / 10 ^ 18) AS settlement_message_gas_amount_per_intent,
        (settle_msg.quote_per_intent::float / 10 ^ 18) AS settlement_message_quote_per_intent

    FROM public.intents i
    LEFT JOIN public.invoices inv ON i.id = inv.id
    LEFT JOIN raw_message_intents intent_msg ON i.id = intent_msg.intent_ids -- intent message
    LEFT JOIN raw_message_intents settle_msg ON i.id = settle_msg.intent_ids -- settlement message
    LEFT JOIN metadata fm ON (
        i.origin_input_asset = fm.adopted_address
        AND CAST(i.origin_origin AS INTEGER) = fm.domain_id
    )
    LEFT JOIN metadata tm ON (
        LOWER(i.settlement_asset) = tm.address
        AND CAST(i.settlement_domain AS INTEGER) = tm.domain_id
    )

    WHERE 
        intent_msg.type = 'INTENT'
        AND settle_msg.type = 'SETTLEMENT'
    )


SELECT
    *
FROM semi s
