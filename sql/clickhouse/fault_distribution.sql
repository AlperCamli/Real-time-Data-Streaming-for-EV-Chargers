SELECT
    location_country,
    location_city,
    count() AS fault_events
FROM raw_events
WHERE event_type = 'FAULT_ALERT'
  AND event_time >= now() - INTERVAL 30 DAY
GROUP BY location_country, location_city
ORDER BY fault_events DESC
LIMIT 20;