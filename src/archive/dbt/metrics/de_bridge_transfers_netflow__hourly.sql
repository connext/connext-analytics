with relevant_usd_prices as (
    select
        symbol as token_name,
        CAST(date as TIMESTAMP) as timestamp,
        average_price as price
    from `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth`
    where symbol in ('WETH', 'DAI', 'USDC', 'USDT')
),

debridge as (
    select
        TIMESTAMP_TRUNC(date, hour) as execute_day,
        UNIX_SECONDS(date) join_time,
        case
            when from_chain_id = 1 then 'Ethereum Mainnet'
            when from_chain_id = 42161 then 'Arbitrum One'
            when from_chain_id = 137 then 'Matic Mainnet'
            when from_chain_id = 10 then 'Optimistic Ethereum'
            when from_chain_id = 8453 then 'Base Mainnet'
            else CAST(from_chain_id as STRING)
        end source_chain,
        UPPER(TRIM(from_actual_symbol)) source_token,
        from_actual_value / POW(10, from_actual_symbol_decimal) source_amount,
        case
            when to_chain_id = 1 then 'Ethereum Mainnet'
            when to_chain_id = 42161 then 'Arbitrum One'
            when to_chain_id = 137 then 'Matic Mainnet'
            when to_chain_id = 10 then 'Optimistic Ethereum'
            when to_chain_id = 8453 then 'Base Mainnet'
            else CAST(to_chain_id as STRING)
        end target_chain,
        UPPER(TRIM(to_symbol)) target_token,
        to_value / POW(10, to_symbol_decimal) target_amount

    from mainnet-bigq.stage.stg_cln_de_bridge_explorer_transactions__dedup
    where
        from_chain_id in (42161, 137, 10, 8453, 1)
        and to_chain_id in (42161, 137, 10, 8453, 1)
        and from_actual_symbol in ('WETH', 'DAI', 'USDC', 'USDT')
        and to_symbol in ('WETH', 'DAI', 'USDC', 'USDT')
        and LOWER(pre_swap_in_token_symbol) = 'nan'
        and LOWER(pre_swap_out_token_symbol) = 'nan'

    order by 1 desc
),

intents_ as (
    select
        d.execute_day,
        d.source_chain,
        d.source_token,
        d.source_amount,
        d.target_chain,
        d.target_token,
        d.target_amount,
        fp_t.price as target_token_price,
        (
            COALESCE(d.target_amount, 0)
            * COALESCE(CAST(fp_t.price as FLOAT64), 0)
        )
            as destination_value_usd
    from debridge d
    left join
        relevant_usd_prices fp_s
        on d.source_token = fp_s.token_name and d.execute_day = fp_s.timestamp
    left join
        relevant_usd_prices fp_t
        on
            d.target_token = fp_t.token_name and d.execute_day = fp_t.timestamp
),

-- SELECT * FROM intents_
-- check for missing price or zero value -> none so far in query
--  WHERE (destination_value_usd IS NULL) OR (destination_value_usd = 0)

inflow as (
    select
        execute_day as date,
        source_token as asset,
        source_chain as chain,
        SUM(destination_value_usd) as inflow
    from intents_
    where destination_value_usd > 0
    group by 1, 2, 3
),

outflow as (
    select
        execute_day as date,
        target_token as asset,
        target_chain as chain,
        SUM(destination_value_usd) as outflow
    from intents_
    where destination_value_usd > 0
    group by 1, 2, 3
),

daily_net_flow as (
    select
        COALESCE(i.date, o.date) as date,
        COALESCE(i.chain, o.chain) as chain,
        COALESCE(i.asset, o.asset) as asset,
        COALESCE(i.inflow, 0) as inflow_usd,
        COALESCE(o.outflow, 0) as outflow_usd,
        COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0) as net_amount_usd,
        ABS(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0))
            as abs_net_amount_usd,
        COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0) total_volume_usd,
        1
        - (
            ABS(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0))
            / (COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0))
        ) as balance_ratio_usd
    from inflow i
    full outer join
        outflow o
        on i.date = o.date and i.chain = o.chain and i.asset = o.asset
    where (COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)) > 0
)

select * from daily_net_flow
order by 1 desc
