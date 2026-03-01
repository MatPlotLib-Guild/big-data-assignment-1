SELECT COUNT(*) as venue_count
FROM pois
WHERE to_tsvector('english', category) @@ to_tsquery('english', '{category}');
