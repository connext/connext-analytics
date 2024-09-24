{% set prefixes = ['prefix1', 'prefix2'] %}
{% set table_name = 'mainnet-bigq.stage.source_lifi__routes' %}

{% set columns = [] %}
{% for prefix in prefixes %}
 {% set columns_for_prefix = run_query('
    SELECT column_name
    FROM `mainnet-bigq.stage.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = \'' ~ table_name ~ '\' AND column_name LIKE \'' ~ prefix ~ '%\'
 ') %}
 {% do columns.extend(columns_for_prefix) %}
{% endfor %}

SELECT
 {{ columns|join(', ') }}
FROM
 `{{ table_name }}`
