import polars as pl
from pymongo import MongoClient


def connect():
    client = MongoClient("mongodb://localhost:27017/?directConnection=true")
    return client


def close(client):
    client.close()


def run_q1(client) -> pl.DataFrame:
    db = client["foursquaredb"]
    pipeline = [
        {"$group": {"_id": "$venue_id", "checkins_count": {"$sum": 1}}},
        {"$lookup": {"from": "pois", "localField": "_id", "foreignField": "_id", "as": "poi"}},
        {"$unwind": "$poi"},
        {"$group": {"_id": "$poi.country", "checkin_count": {"$sum": "$checkins_count"}}},
        {"$sort": {"checkin_count": -1}},
        {"$limit": 10},
        {"$project": {"country": "$_id", "checkin_count": 1, "_id": 0}},
    ]
    results = list(db.checkins.aggregate(pipeline, allowDiskUse=True))
    return pl.DataFrame(results)


def run_q2(client) -> pl.DataFrame:
    db = client["foursquaredb"]
    before = set((f["user_id"], f["friend_id"]) for f in db.friendships_before.find({}, {"_id": 0}))
    after = set((f["user_id"], f["friend_id"]) for f in db.friendships_after.find({}, {"_id": 0}))
    stable = list(before.intersection(after))

    results = []
    seen = set()
    for user_id, friend_id in stable:
        friend_venues = set(db.checkins.distinct("venue_id", {"user_id": friend_id}))
        if not friend_venues:
            continue
        user_venues = set(db.checkins.distinct("venue_id", {"user_id": user_id}))
        shared_venues = friend_venues.intersection(user_venues)

        if shared_venues:
            pois = list(db.pois.find({"_id": {"$in": list(shared_venues)}}))
            for poi in pois:
                venue_id = poi["_id"]
                if (user_id, venue_id) in seen:
                    continue
                seen.add((user_id, venue_id))

                results.append({"user_id": user_id, "venue_id": venue_id, "category": poi.get("category", ""), "country": poi.get("country", "")})
                if len(results) >= 100:
                    break
        if len(results) >= 100:
            break

    return pl.DataFrame(results)


def run_q3(client) -> pl.DataFrame:
    db = client["foursquaredb"]
    pipeline = [
        {"$group": {"_id": "$venue_id", "checkins_count": {"$sum": 1}}},
        {"$sort": {"checkins_count": -1}},
        {"$limit": 20},
        {"$lookup": {"from": "pois", "localField": "_id", "foreignField": "_id", "as": "poi"}},
        {"$unwind": "$poi"},
        {"$project": {"venue_id": "$_id", "country": "$poi.country", "latitude": "$poi.latitude", "longitude": "$poi.longitude", "checkins_count": 1, "_id": 0}},
    ]
    results = list(db.checkins.aggregate(pipeline, allowDiskUse=True))
    return pl.DataFrame(results)


def run_q4(client, category: str = "Restaurant") -> pl.DataFrame:
    db = client["foursquaredb"]

    if category == "Others":
        total = db.pois.count_documents({})
        sum_found = 0
        for cat in ["Restaurant", "Club", "Museum", "Shop"]:
            sum_found += db.pois.count_documents({"$text": {"$search": cat}})
        count = total - sum_found
    else:
        count = db.pois.count_documents({"$text": {"$search": category}})

    return pl.DataFrame([{"venue_count": count, "custom_cat": category}])
