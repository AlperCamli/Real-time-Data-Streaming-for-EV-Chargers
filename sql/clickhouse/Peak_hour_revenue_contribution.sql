WITH revenue_base AS (
    SELECT
        revenue_eur_total,
        toHour(session_start_time) AS hr
    FROM fact_sessions
    WHERE session_start_time >= now() - INTERVAL 7 DAY
)
SELECT
    round(
        100.0 * sumIf(revenue_eur_total, (hr >= 7 AND hr < 9) OR (hr >= 17 AND hr < 20))
        / nullIf(sum(revenue_eur_total), 0),
        2
    ) AS peak_hour_revenue_contribution_pct,
    round(sumIf(revenue_eur_total, (hr >= 7 AND hr < 9) OR (hr >= 17 AND hr < 20)), 2) AS peak_hour_revenue_eur,
    round(sum(revenue_eur_total), 2) AS total_revenue_eur
FROM revenue_base;