SELECT
    toStartOfHour(session_start_time) AS hour_bucket,
    tariff_id,
    count() AS sessions,
    round(sum(energy_kwh_total), 2) AS energy_kwh,
    round(sum(revenue_eur_total), 2) AS revenue_eur,
    round(avg(revenue_eur_total), 2) AS avg_revenue_per_session,
    round(sum(revenue_eur_total) / nullIf(sum(energy_kwh_total), 0), 4) AS revenue_per_kwh
FROM fact_sessions
WHERE session_start_time >= now() - INTERVAL 7 DAY
GROUP BY hour_bucket, tariff_id
ORDER BY hour_bucket, tariff_id;