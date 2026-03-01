SELECT country, checkins_count
FROM top_countries_by_checkins
WHERE bucket = 0
LIMIT 10;
