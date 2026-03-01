import polars as pl
from cassandra.cluster import Cluster


def connect():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("foursquaredb")
    return (cluster, session)


def close(conn):
    cluster, _ = conn
    cluster.shutdown()


def run_q1(conn) -> pl.DataFrame:
    _, session = conn
    rows = session.execute("SELECT country, checkins_count FROM top_countries_by_checkins WHERE bucket = 0 LIMIT 10")
    return pl.DataFrame([{"country": r.country, "checkin_count": r.checkins_count} for r in rows])


def run_q2(conn) -> pl.DataFrame:
    _, session = conn

    before = set((r.user_id, r.friend_id) for r in session.execute("SELECT user_id, friend_id FROM friendships_before"))
    after = set((r.user_id, r.friend_id) for r in session.execute("SELECT user_id, friend_id FROM friendships_after"))
    stable = list(before.intersection(after))

    ps_checkins = session.prepare("SELECT venue_id FROM checkins WHERE user_id = ?")
    ps_poi = session.prepare("SELECT * FROM pois WHERE venue_id = ?")

    results: list[dict] = []
    seen = set()

    for user_id, friend_id in stable:
        friend_venues = set(r.venue_id for r in session.execute(ps_checkins, (friend_id,)))
        if not friend_venues:
            continue
        user_venues = set(r.venue_id for r in session.execute(ps_checkins, (user_id,)))
        shared = friend_venues.intersection(user_venues)

        for venue_id in shared:
            if len(results) >= 100:
                break
            if (user_id, venue_id) in seen:
                continue
            seen.add((user_id, venue_id))

            pois = list(session.execute(ps_poi, (venue_id,)))
            for poi in pois:
                results.append({"user_id": user_id, "venue_id": poi.venue_id, "category": poi.category, "country": poi.country})
                if len(results) >= 100:
                    break
        if len(results) >= 100:
            break

    return pl.DataFrame(results)


def run_q3(conn) -> pl.DataFrame:
    _, session = conn
    rows = session.execute("SELECT venue_id, country, latitude, longitude, checkins_count FROM top_venues_by_checkins WHERE bucket = 0 LIMIT 20")
    return pl.DataFrame(
        [
            {
                "venue_id": r.venue_id,
                "country": r.country,
                "latitude": r.latitude,
                "longitude": r.longitude,
                "checkins_count": r.checkins_count,
            }
            for r in rows
        ]
    )


def run_q4(conn, category: str = "Restaurant") -> pl.DataFrame:
    _, session = conn
    rows = session.execute("SELECT custom_cat, venue_count FROM category_counts")
    df = pl.DataFrame([{"custom_cat": r.custom_cat, "venue_count": r.venue_count} for r in rows])
    return df.filter(pl.col("custom_cat") == category)
