db.pois.countDocuments({ $text: { $search: "{category}" } })
