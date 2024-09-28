--     WHERE TIMESTAMP_TRUNC(rf.date, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {{ interval }} DAY))

{{ config(materialized = 'table') }}

WITH
{% set time_buckets = [1, 3, 6, 12, 24] %}
{% for hours in time_buckets %}
flow_{{ hours }}hr AS (
    WITH raw AS (
        SELECT
            TIMESTAMP_SUB(
                TIMESTAMP_TRUNC(rf.date, HOUR),
                INTERVAL MOD(EXTRACT(HOUR FROM rf.date), {{ hours }}) HOUR
            ) AS date,
            rf.from,
            rf.to,
            SUM(rf.amount) AS amount,
            COUNT(rf.transfer_id) AS transfers
        FROM `mainnet-bigq`.`metrics`.`raw_transfer_flows` AS rf
        WHERE TIMESTAMP_TRUNC(rf.date, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
        GROUP BY 1, 2, 3
    ),
    inflow AS (
        SELECT
            r.date,
            r.from AS chain,
            SUM(r.amount) AS inflow
        FROM raw r
        GROUP BY r.date, r.from
    ),
    outflow AS (
        SELECT
            r.date,
            r.to AS chain,
            SUM(r.amount) AS outflow
        FROM raw r
        GROUP BY r.date, r.to
    ),
    net_flow AS (
        SELECT
            i.chain,
            AVG(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) AS avg_net_amount_{{ hours }}hr,
            STDDEV(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) AS stddev_net_amount_{{ hours }}hr,
            MIN(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) AS min_net_amount_{{ hours }}hr,
            MAX(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) AS max_net_amount_{{ hours }}hr,
            AVG(100 - ABS((COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) / NULLIF((COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)), 0)) * 100) AS avg_percent_netted_{{ hours }}hr,
            STDDEV(100 - ABS((COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) / NULLIF((COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)), 0)) * 100) AS stddev_percent_netted_{{ hours }}hr,
            MIN(100 - ABS((COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) / NULLIF((COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)), 0)) * 100) AS min_percent_netted_{{ hours }}hr,
            MAX(100 - ABS((COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0)) / NULLIF((COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)), 0)) * 100) AS max_percent_netted_{{ hours }}hr,
        FROM inflow i
        FULL OUTER JOIN outflow o ON i.date = o.date AND i.chain = o.chain
        GROUP BY  i.chain
    )
    SELECT
        chain,
        avg_net_amount_{{ hours }}hr,
        avg_percent_netted_{{ hours }}hr
    FROM net_flow



) {% if not loop.last %}, {% endif %}
{% endfor %}

-- Combine all CTEs
SELECT
    flow_1hr.chain,
    {% for hours in time_buckets %}
    flow_{{ hours }}hr.avg_net_amount_{{ hours }}hr,
    flow_{{ hours }}hr.avg_percent_netted_{{ hours }}hr
    {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM flow_1hr flow_1hr
{% for hours in time_buckets if hours != 1 %}
LEFT JOIN flow_{{ hours }}hr ON flow_1hr.chain = flow_{{ hours }}hr.chain
{% endfor %}
