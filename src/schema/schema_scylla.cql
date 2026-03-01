CREATE KEYSPACE IF NOT EXISTS foursquaredb 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE foursquaredb;

CREATE TABLE IF NOT EXISTS users (
    user_id bigint PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS pois (
    venue_id text PRIMARY KEY,
    latitude double,
    longitude double,
    category text,
    country text
);

CREATE INDEX IF NOT EXISTS on pois (category);
CREATE INDEX IF NOT EXISTS on pois (country);

CREATE TABLE IF NOT EXISTS checkins (
    user_id bigint,
    venue_id text,
    utc_time text,
    timezone_offset_mins int,
    PRIMARY KEY (user_id, venue_id, utc_time)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS checkins_by_venue AS
    SELECT * FROM checkins
    WHERE venue_id IS NOT NULL 
      AND user_id IS NOT NULL 
      AND utc_time IS NOT NULL
    PRIMARY KEY (venue_id, user_id, utc_time);

CREATE TABLE IF NOT EXISTS friendships_before (
    user_id bigint,
    friend_id bigint,
    PRIMARY KEY (user_id, friend_id)
);

CREATE TABLE IF NOT EXISTS friendships_after (
    user_id bigint,
    friend_id bigint,
    PRIMARY KEY (user_id, friend_id)
);

CREATE TABLE IF NOT EXISTS top_countries_by_checkins (
    bucket int,
    checkins_count bigint,
    country text,
    PRIMARY KEY (bucket, checkins_count, country)
) WITH CLUSTERING ORDER BY (checkins_count DESC);

CREATE TABLE IF NOT EXISTS top_venues_by_checkins (
    bucket int,
    checkins_count bigint,
    venue_id text,
    country text,
    latitude double,
    longitude double,
    PRIMARY KEY (bucket, checkins_count, venue_id)
) WITH CLUSTERING ORDER BY (checkins_count DESC);

CREATE TABLE IF NOT EXISTS category_counts (
    custom_cat text PRIMARY KEY,
    venue_count bigint
);
