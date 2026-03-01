db.checkins.aggregate([
  {
    $group: { _id: "$venue_id", checkins_count: { $sum: 1 } }
  },
  {
    $lookup: { from: "pois", localField: "_id", foreignField: "_id", as: "poi" }
  },
  { $unwind: "$poi" },
  {
    $group: {
      _id: "$poi.country",
      checkin_count: { $sum: "$checkins_count" }
    }
  },
  { $sort: { checkin_count: -1 } },
  { $limit: 10 }
], { allowDiskUse: true })
