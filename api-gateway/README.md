to test websocket:

```
```
```
curl -v -X POST http://localhost:8081/repairs -H "Content-Type: application/json" -d '{"userID":"test-user1","repairType":"flat_tire","totalPrice":50.0,"location":{"longitude":13.400000,"latitude":52.520000}}

wscat -c "ws://localhost:8081/ws?userID=test-user1"

curl -v -X PUT http://localhost:8081/repairs/68abfd0ca1eea024f45681f8 -H "Content-Type: application/json" -d '{"status":"in_progress"}'
```
```

