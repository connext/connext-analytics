{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

SELECT
    across.bridge,
    CONCAT("across", "_", ROW_NUMBER() OVER ()) AS id,
    across.from_date,
    across.from_tx_hash,
    across.from_chain_id,
    across.from_chain_name,
    across.from_user_address,
    across.from_token_address,
    across.from_token_symbol,
    across.from_amount,
    across.from_amount_usd,
    across.to_date,
    across.to_tx_hash,
    across.to_chain_id,
    across.to_chain_name,
    across.to_user_address,
    across.to_token_address,
    across.to_token_symbol,
    across.to_amount,
    across.to_amount_usd,
    across.relay_symbol,
    across.relay_amount,
    across.relay_amount_usd,
    across.gas_symbol,
    across.gas_amount,
    across.gas_amount_usd

FROM {{ ref('cln_across_txs') }} across

UNION ALL
SELECT
    all_bridge.bridge,
    CONCAT("all_bridge", "_", ROW_NUMBER() OVER ()) AS id,
    all_bridge.from_date,
    all_bridge.from_tx_hash,
    all_bridge.from_chain_id,
    all_bridge.from_chain_name,
    all_bridge.from_user_address,
    all_bridge.from_token_address,
    all_bridge.from_token_symbol,
    all_bridge.from_amount,
    all_bridge.from_amount_usd,
    all_bridge.to_date,
    all_bridge.to_tx_hash,
    all_bridge.to_chain_id,
    all_bridge.to_chain_name,
    all_bridge.to_user_address,
    all_bridge.to_token_address,
    all_bridge.to_token_symbol,
    all_bridge.to_amount,
    all_bridge.to_amount_usd,
    all_bridge.relay_symbol,
    all_bridge.relay_amount,
    all_bridge.relay_amount_usd,
    all_bridge.gas_symbol,
    all_bridge.gas_amount,
    all_bridge.gas_amount_usd

FROM {{ ref('cln_all_bridge_txs') }} all_bridge

UNION ALL
SELECT
    debridge.bridge,
    CONCAT("debridge", "_", ROW_NUMBER() OVER ()) AS id,
    debridge.from_date,
    debridge.from_tx_hash,
    debridge.from_chain_id,
    debridge.from_chain_name,
    debridge.from_user_address,
    debridge.from_token_address,
    debridge.from_token_symbol,
    debridge.from_amount,
    debridge.from_amount_usd,
    debridge.to_date,
    debridge.to_tx_hash,
    debridge.to_chain_id,
    debridge.to_chain_name,
    debridge.to_user_address,
    debridge.to_token_address,
    debridge.to_token_symbol,
    debridge.to_amount,
    debridge.to_amount_usd,
    debridge.relay_symbol,
    debridge.relay_amount,
    debridge.relay_amount_usd,
    debridge.gas_symbol,
    debridge.gas_amount,
    debridge.gas_amount_usd

FROM {{ ref('cln_debridge_txs') }} debridge

UNION ALL

SELECT
    
    hop.bridge,
    CONCAT("hop", "_", ROW_NUMBER() OVER ()) AS id,
    hop.from_date,
    hop.from_tx_hash,
    hop.from_chain_id,
    hop.from_chain_name,
    hop.from_user_address,
    hop.from_token_address,
    hop.from_token_symbol,
    hop.from_amount,
    hop.from_amount_usd,
    hop.to_date,
    hop.to_tx_hash,
    hop.to_chain_id,
    hop.to_chain_name,
    hop.to_user_address,
    hop.to_token_address,
    hop.to_token_symbol,
    hop.to_amount,
    hop.to_amount_usd,
    hop.relay_symbol,
    hop.relay_amount,
    hop.relay_amount_usd,
    hop.gas_symbol,
    hop.gas_amount,
    hop.gas_amount_usd

FROM {{ ref('cln_hop_txs') }} hop

UNION ALL

SELECT
    stargate.bridge,
    CONCAT("stargate", "_", ROW_NUMBER() OVER ()) AS id,
    stargate.from_date,
    stargate.from_tx_hash,
    stargate.from_chain_id,
    stargate.from_chain_name,
    stargate.from_user_address,
    stargate.from_token_address,
    stargate.from_token_symbol,
    stargate.from_amount,
    stargate.from_amount_usd,
    stargate.to_date,
    stargate.to_tx_hash,
    stargate.to_chain_id,
    stargate.to_chain_name,
    stargate.to_user_address,
    stargate.to_token_address,
    stargate.to_token_symbol,
    stargate.to_amount,
    stargate.to_amount_usd,
    stargate.relay_symbol,
    stargate.relay_amount,
    stargate.relay_amount_usd,
    stargate.gas_symbol,
    stargate.gas_amount,
    stargate.gas_amount_usd

FROM {{ ref('cln_stargate_txs') }} stargate

UNION ALL
SELECT
    synapse.bridge,
    CONCAT("synapse", "_", ROW_NUMBER() OVER ()) AS id,
    synapse.from_date,
    synapse.from_tx_hash,
    synapse.from_chain_id,
    synapse.from_chain_name,
    synapse.from_user_address,
    synapse.from_token_address,
    synapse.from_token_symbol,
    synapse.from_amount,
    synapse.from_amount_usd,
    synapse.to_date,
    synapse.to_tx_hash,
    synapse.to_chain_id,
    synapse.to_chain_name,
    synapse.to_user_address,
    synapse.to_token_address,
    synapse.to_token_symbol,
    synapse.to_amount,
    synapse.to_amount_usd,
    synapse.relay_symbol,
    synapse.relay_amount,
    synapse.relay_amount_usd,
    synapse.gas_symbol,
    synapse.gas_amount,
    synapse.gas_amount_usd

FROM {{ ref('cln_synapse_txs') }} synapse