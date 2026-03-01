import time
import csv
from pathlib import Path

import polars as pl
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

DB_CONFIG = ["127.0.0.1"]
DATA_DIR = Path("data")


def ingest_file(session, path, query, batch_size=2000):
    with open(path, "r", encoding="utf-8") as f:
        delimiter = "," if path.suffix == ".csv" else "\t"
        reader = csv.reader(f, delimiter=delimiter)

        next(reader, None)

        args_list = []
        count = 0

        for row in reader:
            if not row:
                continue

            if path.name == "my_users.csv":
                args_list.append((int(row[0]),))
            elif path.name == "my_POIs.tsv":
                args_list.append((row[0], float(row[1]), float(row[2]), row[3], row[4]))
            elif path.name == "my_checkins_anonymized.tsv":
                args_list.append((int(row[0]), row[1], row[2], int(row[3])))
            elif "friendship" in path.name:
                args_list.append((int(row[0]), int(row[1])))

            count += 1
            if len(args_list) >= batch_size:
                execute_concurrent_with_args(session, query, args_list, concurrency=100)
                args_list = []

        if args_list:
            execute_concurrent_with_args(session, query, args_list, concurrency=100)

    return count


def _compute_venue_counts() -> pl.DataFrame:
    checkins = pl.scan_csv(DATA_DIR / "my_checkins_anonymized.tsv", separator="\t").select("venue_id")
    venue_counts = checkins.group_by("venue_id").agg(pl.len().alias("checkins_count")).sort("checkins_count", descending=True)
    return venue_counts.collect()


def _compute_top_countries(venue_counts: pl.DataFrame) -> pl.DataFrame:
    pois_country = pl.scan_csv(DATA_DIR / "my_POIs.tsv", separator="\t").select(["venue_id", "country"])
    joined = venue_counts.lazy().join(pois_country, on="venue_id", how="left")
    top_countries = joined.group_by("country").agg(pl.sum("checkins_count").alias("checkins_count")).sort("checkins_count", descending=True).head(10).collect()
    return top_countries


def _compute_top_venues(venue_counts: pl.DataFrame) -> pl.DataFrame:
    top_venues = venue_counts.lazy().head(20)
    pois_details = pl.scan_csv(DATA_DIR / "my_POIs.tsv", separator="\t").select(["venue_id", "country", "latitude", "longitude"])
    enriched = top_venues.join(pois_details, on="venue_id", how="left").collect()
    return enriched


def _compute_category_counts() -> pl.DataFrame:
    pois = pl.scan_csv(DATA_DIR / "my_POIs.tsv", separator="\t").select("category")
    custom_cat = (
        pl.when(pl.col("category").str.contains("(?i)restaurant"))
        .then(pl.lit("Restaurant"))
        .when(pl.col("category").str.contains("(?i)club"))
        .then(pl.lit("Club"))
        .when(pl.col("category").str.contains("(?i)museum"))
        .then(pl.lit("Museum"))
        .when(pl.col("category").str.contains("(?i)shop"))
        .then(pl.lit("Shop"))
        .otherwise(pl.lit("Others"))
        .alias("custom_cat")
    )
    counts = pois.with_columns(custom_cat).group_by("custom_cat").agg(pl.len().alias("venue_count")).sort("venue_count", descending=True).collect()
    return counts


def build_aggregates(session) -> None:
    print("Building aggregates (counts/top-N) from local TSVs...")

    venue_counts = _compute_venue_counts()
    top_countries = _compute_top_countries(venue_counts)
    top_venues = _compute_top_venues(venue_counts)
    category_counts = _compute_category_counts()

    session.execute("TRUNCATE top_countries_by_checkins")
    session.execute("TRUNCATE top_venues_by_checkins")
    session.execute("TRUNCATE category_counts")

    ps_country = session.prepare("INSERT INTO top_countries_by_checkins (bucket, checkins_count, country) VALUES (?, ?, ?)")
    ps_venue = session.prepare("INSERT INTO top_venues_by_checkins (bucket, checkins_count, venue_id, country, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)")
    ps_cat = session.prepare("INSERT INTO category_counts (custom_cat, venue_count) VALUES (?, ?)")

    execute_concurrent_with_args(
        session,
        ps_country,
        [(0, int(r["checkins_count"]), r["country"]) for r in top_countries.to_dicts()],
        concurrency=50,
    )
    execute_concurrent_with_args(
        session,
        ps_venue,
        [
            (
                0,
                int(r["checkins_count"]),
                r["venue_id"],
                r["country"],
                float(r["latitude"]) if r["latitude"] is not None else None,
                float(r["longitude"]) if r["longitude"] is not None else None,
            )
            for r in top_venues.to_dicts()
        ],
        concurrency=50,
    )
    execute_concurrent_with_args(
        session,
        ps_cat,
        [(r["custom_cat"], int(r["venue_count"])) for r in category_counts.to_dicts()],
        concurrency=20,
    )


def main():
    print("Connecting to ScyllaDB...")
    cluster = Cluster(DB_CONFIG)
    session = cluster.connect("foursquaredb")

    insert_users = session.prepare("INSERT INTO users (user_id) VALUES (?)")
    insert_pois = session.prepare("INSERT INTO pois (venue_id, latitude, longitude, category, country) VALUES (?, ?, ?, ?, ?)")
    insert_checkins = session.prepare("INSERT INTO checkins (user_id, venue_id, utc_time, timezone_offset_mins) VALUES (?, ?, ?, ?)")
    insert_friendships_before = session.prepare("INSERT INTO friendships_before (user_id, friend_id) VALUES (?, ?)")
    insert_friendships_after = session.prepare("INSERT INTO friendships_after (user_id, friend_id) VALUES (?, ?)")

    start = time.perf_counter()

    print("Loading users...")
    c_users = ingest_file(session, DATA_DIR / "my_users.csv", insert_users)

    print("Loading POIs...")
    c_pois = ingest_file(session, DATA_DIR / "my_POIs.tsv", insert_pois)

    print("Loading checkins...")
    c_checkins = ingest_file(session, DATA_DIR / "my_checkins_anonymized.tsv", insert_checkins)

    print("Loading friendships before...")
    c_fbefore = ingest_file(session, DATA_DIR / "my_friendship_before.tsv", insert_friendships_before)

    print("Loading friendships after...")
    c_fafter = ingest_file(session, DATA_DIR / "my_friendship_after.tsv", insert_friendships_after)

    build_aggregates(session)

    elapsed = time.perf_counter() - start
    print(f"\nTotal ingestion time: {elapsed:.1f}s  ({elapsed / 60:.1f} min)")

    print(f"  {c_users:,} rows in users")
    print(f"  {c_pois:,} rows in pois")
    print(f"  {c_checkins:,} rows in checkins")
    print(f"  {c_fbefore:,} rows in friendships_before")
    print(f"  {c_fafter:,} rows in friendships_after")

    cluster.shutdown()


if __name__ == "__main__":
    main()
