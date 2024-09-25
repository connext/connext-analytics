-- 13. Wallet_retention rate
-- Metric 13: **Wallet_retention_rate**: Measures the frequency and consistency of user activity associated with specific wallet addresses over time
-- user 1: origin_intent -> initiator
-- Other can be added in similar way

-- weekly retention by origin wallet and its start date by first intent id
WITH user_activity AS (
  SELECT 
    initiator AS origin_wallet,
    DATE_TRUNC('week', to_timestamp(timestamp)) AS week
  FROM public.origin_intents
),
cohorts AS (
  SELECT
    origin_wallet,
    MIN(week) AS cohort_week
  FROM user_activity
  GROUP BY origin_wallet
),
user_retention AS (
  SELECT
    c.cohort_week,
    ua.week,
    COUNT(DISTINCT ua.origin_wallet) AS users
  FROM cohorts c
  JOIN user_activity ua ON c.origin_wallet = ua.origin_wallet
  GROUP BY c.cohort_week, ua.week
)
SELECT
  cohort_week,
  week,
  users,
  FLOOR((EXTRACT(EPOCH FROM week) - EXTRACT(EPOCH FROM cohort_week)) / 604800) AS weeks_since_cohort,
  users::FLOAT / FIRST_VALUE(users) OVER (PARTITION BY cohort_week ORDER BY week) AS retention_rate
FROM user_retention
ORDER BY cohort_week, week;