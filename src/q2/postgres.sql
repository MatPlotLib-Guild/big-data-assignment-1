WITH stable_friendships AS (
    SELECT user_id, friend_id FROM friendships_before
    INTERSECT
    SELECT user_id, friend_id FROM friendships_after
)
SELECT DISTINCT sf.user_id, p.venue_id, p.category, p.country
FROM stable_friendships sf
JOIN checkins friend_chk ON sf.friend_id = friend_chk.user_id
JOIN checkins user_chk ON sf.user_id = user_chk.user_id AND user_chk.venue_id = friend_chk.venue_id
JOIN pois p ON user_chk.venue_id = p.venue_id
LIMIT 100;
