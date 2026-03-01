import time
import csv
from pathlib import Path
from pymongo import MongoClient, InsertOne

DB_CONFIG = "mongodb://localhost:27017/?directConnection=true"
DATA_DIR = Path("data")


def ingest_file(collection, path, doc_mapper, batch_size=2000):
    with open(path, "r", encoding="utf-8") as f:
        delimiter = "," if path.suffix == ".csv" else "\t"
        reader = csv.reader(f, delimiter=delimiter)
        next(reader, None)

        batch = []
        count = 0

        for row in reader:
            if not row:
                continue

            doc = doc_mapper(row)
            batch.append(InsertOne(doc))
            count += 1

            if len(batch) >= batch_size:
                collection.bulk_write(batch, ordered=False)
                batch = []

        if batch:
            collection.bulk_write(batch, ordered=False)

    return count


def main():
    print("Connecting to MongoDB...")
    client = MongoClient(DB_CONFIG)
    db = client["foursquaredb"]

    c_users = db["users"]
    c_pois = db["pois"]
    c_checkins = db["checkins"]
    c_friendships_before = db["friendships_before"]
    c_friendships_after = db["friendships_after"]

    # db.users.drop()
    # db.pois.drop()
    # db.checkins.drop()
    # db.friendships_before.drop()
    # db.friendships_after.drop()

    print("Creating indexes...")
    c_checkins.create_index("venue_id")
    c_checkins.create_index("user_id")
    c_pois.create_index("country")
    c_pois.create_index([("category", "text")])
    c_friendships_before.create_index("user_id")
    c_friendships_after.create_index("user_id")
    print("Indexes created.")

    start = time.perf_counter()

    print("Loading users...")
    n_users = ingest_file(c_users, DATA_DIR / "my_users.csv", lambda r: {"_id": int(r[0])})

    print("Loading POIs...")
    n_pois = ingest_file(c_pois, DATA_DIR / "my_POIs.tsv", lambda r: {"_id": r[0], "latitude": float(r[1]), "longitude": float(r[2]), "category": r[3], "country": r[4]})

    print("Loading checkins...")
    n_checkins = ingest_file(
        c_checkins, DATA_DIR / "my_checkins_anonymized.tsv", lambda r: {"user_id": int(r[0]), "venue_id": r[1], "utc_time": r[2], "timezone_offset_mins": int(r[3])}
    )

    print("Loading friendships before...")
    n_fbefore = ingest_file(c_friendships_before, DATA_DIR / "my_friendship_before.tsv", lambda r: {"user_id": int(r[0]), "friend_id": int(r[1])})

    print("Loading friendships after...")
    n_fafter = ingest_file(c_friendships_after, DATA_DIR / "my_friendship_after.tsv", lambda r: {"user_id": int(r[0]), "friend_id": int(r[1])})

    elapsed = time.perf_counter() - start
    print(f"\nTotal ingestion time (including indexes): {elapsed:.1f}s  ({elapsed / 60:.1f} min)")

    print(f"  {n_users:,} rows in users")
    print(f"  {n_pois:,} rows in pois")
    print(f"  {n_checkins:,} rows in checkins")
    print(f"  {n_fbefore:,} rows in friendships_before")
    print(f"  {n_fafter:,} rows in friendships_after")

    client.close()


if __name__ == "__main__":
    main()
