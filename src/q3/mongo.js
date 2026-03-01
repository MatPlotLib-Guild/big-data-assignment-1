db.checkins.aggregate([
  {
    $group: {
      _id: "$venue_id",
      checkins_count: { $sum: 1 }
    }
  },
  { $sort: { checkins_count: -1 } },
  { $limit: 20 },
  {
    $lookup: {
      from: "pois",
      localField: "_id",
      foreignField: "_id",
      as: "poi"
    }
  },
  { $unwind: "$poi" },
  {
    $project: {
      venue_id: "$_id",
      country: "$poi.country",
      latitude: "$poi.latitude",
      longitude: "$poi.longitude",
      checkins_count: 1,
      _id: 0
    }
  }
], { allowDiskUse: true })
