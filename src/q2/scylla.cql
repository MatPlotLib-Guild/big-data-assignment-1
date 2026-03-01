SELECT user_id, friend_id FROM friendships_before;
SELECT user_id, friend_id FROM friendships_after;

SELECT venue_id FROM checkins WHERE user_id = ?;

SELECT * FROM pois WHERE venue_id = ?;
