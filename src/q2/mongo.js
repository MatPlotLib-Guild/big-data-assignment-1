db.checkins.distinct("venue_id", { user_id: FRIEND_ID })
db.checkins.distinct("venue_id", { user_id: USER_ID })
db.pois.find({ _id: { $in: [ "venue1", "venue2" ] } })
