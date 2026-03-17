WITH brand_stats AS (
    SELECT
        vehicle_brand,
        avg(avg_power_kw) AS mean_power,
        stddevPop(avg_power_kw) AS std_power
    FROM fact_sessions
    WHERE avg_power_kw IS NOT NULL
      AND vehicle_brand IS NOT NULL
      AND session_start_time >= now() - INTERVAL 30 DAY
    GROUP BY vehicle_brand
)
SELECT
    fs.session_id,
    fs.vehicle_brand,
    fs.operator_id,
    fs.station_id,
    fs.session_start_time,
    round(fs.avg_power_kw, 2) AS avg_power_kw,
    round(bs.mean_power, 2) AS brand_mean_power_kw,
    round(bs.std_power, 2) AS brand_std_power_kw,
    round((fs.avg_power_kw - bs.mean_power) / nullIf(bs.std_power, 0), 2) AS z_score
FROM fact_sessions fs
INNER JOIN brand_stats bs
    ON fs.vehicle_brand = bs.vehicle_brand
WHERE fs.avg_power_kw IS NOT NULL
  AND fs.vehicle_brand IS NOT NULL
  AND fs.session_start_time >= now() - INTERVAL 30 DAY
  AND abs((fs.avg_power_kw - bs.mean_power) / nullIf(bs.std_power, 0)) > 2
ORDER BY abs(z_score) DESC
LIMIT 100;