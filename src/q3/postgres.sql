SELECT p.country, p.venue_id, p.latitude, p.longitude, COUNT(c.user_id) as checkins_count
FROM checkins c
JOIN pois p ON c.venue_id = p.venue_id
GROUP BY p.country, p.venue_id, p.latitude, p.longitude
ORDER BY checkins_count DESC
LIMIT 20;
