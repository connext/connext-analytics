-- TODO
-- metrc 6: Net Dollar Retention (NDR)


WITH ii AS (
    SELECT * FROM {{ref('cln_intents_invoices')}}
    ),
    cohort AS (
        SELECT
            ii.origin_initiator,
            DATE_TRUNC(MIN(ii.settlement_timestamp), WEEK) AS cohort_week
        FROM ii
        -- filter current date
        WHERE DATE(ii.settlement_timestamp) < CURRENT_DATE()
        GROUP BY 1
    ),
    user_weekly_spending AS (
    SELECT
        ii.origin_initiator,
        DATE_TRUNC(ii.settlement_timestamp, WEEK) AS week,
        SUM(ii.from_asset_amount_usd) AS weekly_spend
    FROM ii
    GROUP BY 1, 2
    ),
    user_spending_with_cohort AS (
        SELECT
            uws.origin_initiator,
            c.cohort_week,
            uws.week,
            uws.weekly_spend
        FROM user_weekly_spending uws
        JOIN cohort c ON uws.origin_initiator = c.origin_initiator
    ),
    cohort_revenue AS (
        SELECT
            cohort_week,
            SUM(CASE WHEN week = cohort_week THEN weekly_spend ELSE 0 END) AS starting_revenue,
            SUM(weekly_spend) AS total_revenue
        FROM user_spending_with_cohort
        GROUP BY 1
    ),
    cohort_ndr AS (
        SELECT
            cohort_week,
            starting_revenue,
            (total_revenue - starting_revenue) AS expansion_revenue,
            (total_revenue / NULLIF(starting_revenue, 0))  AS ndr_percentage
        FROM cohort_revenue
    ),
    ndr AS (
    SELECT
        cohort_week,
        starting_revenue,
        expansion_revenue,
        ndr_percentage
    FROM cohort_ndr
    ORDER BY 1
    ),
    user_cohort_spend AS (
        -- Calculate the cohort week spend for each user
        SELECT
            origin_initiator,
            cohort_week,
            weekly_spend AS cohort_week_spend
        FROM user_spending_with_cohort 
        WHERE week = cohort_week
    ),
    user_retained_revenue AS (
        SELECT
            uws.origin_initiator,
            uws.cohort_week,
            ucs.cohort_week_spend,
            SUM(
                CASE
                    WHEN uws.week > uws.cohort_week THEN LEAST(uws.weekly_spend, ucs.cohort_week_spend)
                    ELSE 0
                END
            ) AS retained_revenue
        FROM user_spending_with_cohort uws
        JOIN user_cohort_spend ucs ON
            uws.origin_initiator = ucs.origin_initiator AND
            uws.cohort_week = ucs.cohort_week
        GROUP BY 1,2,3
    ),
    cohort_gdr AS (
        SELECT
            urr.cohort_week,
            SUM(ucs.cohort_week_spend) AS total_starting_revenue,
            SUM(urr.retained_revenue) AS total_retained_revenue,
            (SUM(urr.retained_revenue) / NULLIF(SUM(ucs.cohort_week_spend), 0)) * 1.0 AS gdr_percentage
        FROM user_retained_revenue urr
        JOIN user_cohort_spend ucs ON
            urr.origin_initiator = ucs.origin_initiator AND
            urr.cohort_week = ucs.cohort_week
        GROUP BY 1
    ),
    gdr AS (
    SELECT
        cohort_week,
        total_starting_revenue,
        total_retained_revenue,
        gdr_percentage
    FROM cohort_gdr
    ORDER BY 1
    )


SELECT 
    COALESCE(ndr.cohort_week, gdr.cohort_week) AS cohort_week,
    ndr.starting_revenue,
    ndr.expansion_revenue,
    ndr.ndr_percentage,
    gdr.total_starting_revenue,
    gdr.total_retained_revenue,
    gdr.gdr_percentage
FROM ndr
FULL OUTER JOIN gdr 
ON ndr.cohort_week = gdr.cohort_week
