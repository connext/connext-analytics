SELECT * FROM public.daily_metrics_by_date;
SELECT * FROM public.daily_metrics_by_chains_tokens;


-- Replace 'your_cron_job_name' with the actual name of your cron job
SELECT *
FROM cron.job
WHERE jobname = 'daily_metrics_by_date';

SELECT *
FROM cron.job
WHERE jobname = 'daily_metrics_by_chains_tokens';