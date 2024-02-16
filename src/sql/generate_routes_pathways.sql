SELECT * 

{% if not reset%}
EXCEPT ({{except_col}})
{% endif %}


FROM `{{table_id}}`

{% if not reset%}
WHERE aggregator = "{{aggregator}}"
{% endif %}