CREATE TEMP TABLE friend_venues_tmp AS
WITH stable_friendships AS (
    SELECT user_id, friend_id FROM friendships_before
    INTERSECT
    SELECT user_id, friend_id FROM friendships_after
)
SELECT DISTINCT sf.user_id, sf.friend_id, fc.venue_id
FROM stable_friendships sf
JOIN checkins fc ON sf.friend_id = fc.user_id
LIMIT 1000; -- set value a bit higher to retrieve exactly 100 entries in the end

SELECT DISTINCT tmp.user_id, p.venue_id, p.category, p.country
FROM friend_venues_tmp tmp
JOIN pois p ON tmp.venue_id = p.venue_id
LEFT JOIN checkins user_chk 
    ON user_chk.user_id = tmp.user_id 
    AND user_chk.venue_id = tmp.venue_id
WHERE user_chk.user_id IS NULL
LIMIT 100;
