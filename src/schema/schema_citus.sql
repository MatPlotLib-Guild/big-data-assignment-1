CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS pois (
    venue_id  VARCHAR(24)      PRIMARY KEY,
    latitude  DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    category  TEXT             NOT NULL,
    country   CHAR(2)          NOT NULL
);

CREATE TABLE IF NOT EXISTS checkins (
    user_id              BIGINT      NOT NULL,
    venue_id             VARCHAR(24) NOT NULL,
    utc_time             TEXT        NOT NULL,
    timezone_offset_mins INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS friendships_before (
    user_id   BIGINT NOT NULL REFERENCES users(user_id),
    friend_id BIGINT NOT NULL REFERENCES users(user_id),
    PRIMARY KEY (user_id, friend_id)
);

CREATE TABLE IF NOT EXISTS friendships_after (
    user_id   BIGINT NOT NULL REFERENCES users(user_id),
    friend_id BIGINT NOT NULL REFERENCES users(user_id),
    PRIMARY KEY (user_id, friend_id)
);

CREATE INDEX IF NOT EXISTS idx_checkins_venue_id   ON checkins (venue_id);
CREATE INDEX IF NOT EXISTS idx_checkins_user_id    ON checkins (user_id);
CREATE INDEX IF NOT EXISTS idx_pois_country        ON pois (country);
CREATE INDEX IF NOT EXISTS idx_friendships_before_friend  ON friendships_before (friend_id);
CREATE INDEX IF NOT EXISTS idx_friendships_after_friend   ON friendships_after  (friend_id);
CREATE INDEX IF NOT EXISTS idx_pois_category_fts ON pois USING GIN (to_tsvector('english', category));

SELECT create_reference_table('users');
SELECT create_reference_table('friendships_before');
SELECT create_reference_table('friendships_after');

SELECT create_distributed_table('pois', 'venue_id');
SELECT create_distributed_table('checkins', 'user_id');

ALTER TABLE checkins ADD CONSTRAINT fk_checkins_user FOREIGN KEY (user_id) REFERENCES users(user_id);
