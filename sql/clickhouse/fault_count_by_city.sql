SELECT
    country_code,
    city,
    sum(fault_count) AS total_faults,
    sum(distinct_station_count) AS station_days_affected
FROM agg_city_day_faults
WHERE day_date >= today() - 30
GROUP BY country_code, city
ORDER BY total_faults DESC
LIMIT 20;