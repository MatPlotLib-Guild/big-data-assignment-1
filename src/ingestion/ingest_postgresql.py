import time
from pathlib import Path

import psycopg2

DB_CONFIG = dict(host="localhost", port=5432, dbname="foursquaredb", user="postgres", password="postgres")
DATA_DIR = Path("data")


def copy_tsv(cur, path: Path, table: str, columns: str) -> None:
    with open(path) as f:
        cur.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', HEADER TRUE)", f)


def count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    start = time.perf_counter()

    print("Loading users ...")
    with open(DATA_DIR / "my_users.csv") as f:
        next(f)
        cur.copy_expert("COPY users (user_id) FROM STDIN WITH (FORMAT CSV)", f)
    conn.commit()

    print("Loading POIs ...")
    copy_tsv(cur, DATA_DIR / "my_POIs.tsv", "pois", "venue_id, latitude, longitude, category, country")
    conn.commit()

    print("Loading checkins ...")
    copy_tsv(cur, DATA_DIR / "my_checkins_anonymized.tsv", "checkins", "user_id, venue_id, utc_time, timezone_offset_mins")
    conn.commit()

    print("Loading friendships (before) ...")
    copy_tsv(cur, DATA_DIR / "my_friendship_before.tsv", "friendships_before", "user_id, friend_id")
    conn.commit()

    print("Loading friendships (after) ...")
    copy_tsv(cur, DATA_DIR / "my_friendship_after.tsv", "friendships_after", "user_id, friend_id")
    conn.commit()

    elapsed = time.perf_counter() - start
    print(f"\nTotal ingestion time: {elapsed:.1f}s  ({elapsed / 60:.1f} min)")

    print(f"  {count(cur, 'users'):,} rows")
    print(f"  {count(cur, 'pois'):,} rows")
    print(f"  {count(cur, 'checkins'):,} rows")
    print(f"  {count(cur, 'friendships_before'):,} rows")
    print(f"  {count(cur, 'friendships_after'):,} rows")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
