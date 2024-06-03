-- TODO
-- Date filter on daily basis
-- Buckets on daily basis: 1hr, 3hr,6hr,9hr,12hr,1D
-- Metric for each bucket: net amount and % of amount netted

WITH
{% set time_buckets = [1, 3, 6, 9, 12, 24] %}
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
                rf.asset,
                SUM(rf.amount) AS amount,
                COUNT(rf.transfer_id) AS transfers
            FROM `mainnet-bigq`.`metrics`.`raw_transfer_flows` AS rf
            GROUP BY 1, 2, 3, 4
        ),

        inflow AS (
            SELECT
                r.date,
                r.from AS chain,
                r.asset,
                SUM(r.amount) AS inflow
            FROM raw r
            GROUP BY r.date, r.from, r.asset
        ),

        outflow AS (
            SELECT
                r.date,
                r.to AS chain,
                r.asset,
                SUM(r.amount) AS outflow
            FROM raw r
            GROUP BY r.date, r.to, r.asset
        ),

        net_flow AS (
            SELECT
                TIMESTAMP_TRUNC(i.date, DAY) AS date,
                i.chain,
                i.asset,
                AVG(COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0))
                    AS avg_net_amount_{{ hours }}hr,
                AVG(
                    100
                    - ABS(
                        (COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0))
                        / NULLIF(
                            (COALESCE(i.inflow, 0) + COALESCE(o.outflow, 0)), 0
                        )
                    )
                    * 100
                ) AS avg_percent_netted_{{ hours }}hr

            FROM inflow i
            FULL OUTER JOIN
                outflow o
                ON i.date = o.date AND i.chain = o.chain AND i.asset = o.asset
            GROUP BY TIMESTAMP_TRUNC(i.date, DAY), i.chain, i.asset
        )

        SELECT
            date,
            chain,
            asset,
            avg_net_amount_{{ hours }}hr,
            stddev_net_amount_{{ hours }}hr,
            min_net_amount_{{ hours }}hr,
            max_net_amount_{{ hours }}hr,
            -- median_net_amount_{{ hours }}hr,
            avg_percent_netted_{{ hours }}hr,
            stddev_percent_netted_{{ hours }}hr,
            min_percent_netted_{{ hours }}hr,
            max_percent_netted_{{ hours }}hr
        -- median_percent_netted_{{ hours }}hr
        FROM net_flow



    ) {% if not loop.last %}, {% endif %}
{% endfor %}

-- Combine all CTEs
SELECT
    flow_1hr.date,
    flow_1hr.chain,
    flow_1hr.asset,
    {% for hours in time_buckets %}
        flow_{{ hours }}hr.avg_net_amount_{{ hours }}hr,
        flow_{{ hours }}hr.stddev_net_amount_{{ hours }}hr,
        flow_{{ hours }}hr.min_net_amount_{{ hours }}hr,
        flow_{{ hours }}hr.max_net_amount_{{ hours }}hr,
        flow_{{ hours }}hr.avg_percent_netted_{{ hours }}hr,
        flow_{{ hours }}hr.stddev_percent_netted_{{ hours }}hr,
        flow_{{ hours }}hr.min_percent_netted_{{ hours }}hr,
        flow_{{ hours }}hr.max_percent_netted_{{ hours }}hr
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM flow_1hr flow_1hr
{% for hours in time_buckets if hours != 1 %}
    LEFT JOIN
        flow_{{ hours }}hr
        ON
            flow_1hr.date = flow_{{ hours }}hr.date
            AND flow_1hr.chain = flow_{{ hours }}hr.chain
            AND flow_1hr.asset = flow_{{ hours }}hr.asset
{% endfor %}
