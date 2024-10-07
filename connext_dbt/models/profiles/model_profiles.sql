   {{ config(materialized='table') }}

   {% if var('dbt_profiler_enabled', true) %}
       {% set models_to_profile = [
           'all_combined_bridges_txs',
           'stg_across_txs',
           'stg_all_bridge_txs',
           'stg_debridge_txs',
           'stg_hop_txs',
           'stg_stargate_txs',
           'stg_synapse_txs',
           'cln_across_txs',
           'cln_all_bridge_txs',
           'cln_debridge_txs',
           'cln_hop_txs',
           'cln_stargate_txs',
           'cln_synapse_txs'
       ] %}

       {% for model in models_to_profile %}
           {{ dbt_profiler.get_profile(ref(model)) }}
           {% if not loop.last %}UNION ALL{% endif %}
       {% endfor %}
   {% else %}
       SELECT 1 as dummy_column
   {% endif %}