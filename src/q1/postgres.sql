SELECT p.country, COUNT(*) as checkin_count
FROM checkins c
JOIN pois p ON c.venue_id = p.venue_id
GROUP BY p.country
ORDER BY checkin_count DESC
LIMIT 10;
