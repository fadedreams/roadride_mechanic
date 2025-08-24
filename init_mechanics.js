use repairdb
db.mechanics.drop()
db.mechanics.insertMany([
    {
        "_id": "mechanic1",
        "name": "Berlin Auto Repair",
        "location": {
            "longitude": 13.388860,
            "latitude": 52.517037
        }
    },
    {
        "_id": "mechanic2",
        "name": "City Garage",
        "location": {
            "longitude": 13.397634,
            "latitude": 52.529407
        }
    },
    {
        "_id": "mechanic3",
        "name": "Fast Fix Mechanics",
        "location": {
            "longitude": 13.428555,
            "latitude": 52.523219
        }
    }
])
