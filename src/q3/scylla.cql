SELECT venue_id, country, latitude, longitude, checkins_count
FROM top_venues_by_checkins
WHERE bucket = 0
LIMIT 20;
