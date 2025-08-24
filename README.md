```
```
api-gateway 8081
repair-service 8080

curl -X POST http://localhost:8081/repairs -H "Content-Type: application/json" -d '{"userID":"test-user","repairType":"flat_tire","totalPrice":50.0}'
curl -X POST http://localhost:8081/repairs/estimate -H "Content-Type: application/json" -d '{"repairType":"brake_repair","userID":"test-user"}'
curl -X GET "http://localhost:8081/repairs/cost/68ab0fc1b7a8d776c5130d17?userID=test-user" -H "Content-Type: application/json"
curl -X GET http://localhost:8081/repairs/68ab0fc1b7a8d776c5130d18 -H "Content-Type: application/json"
curl -X PUT http://localhost:8081/repairs/68ab0fc1b7a8d776c5130d18 -H "Content-Type: application/json" -d '{"status":"in_progress"}'
```
