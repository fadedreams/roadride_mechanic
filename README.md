```
```
api-gateway 8081
repair-service 8080

# POST /repairs
curl -v -X POST http://localhost:8081/repairs -H "Content-Type: application/json" -d '{"userID":"test-user2","repairType":"flat_tire","totalPrice":50.0,"userLocation":{"longitude":13.400000,"latitude":52.520000}}'

# GET /repairs/cost/{costID} (use costID from POST /repairs)
curl -v -X GET "http://localhost:8081/repairs/cost/<costID>?userID=test-user" -H "Content-Type: application/json"

# GET /repairs/{repairID} (use repairID from POST /repairs)
curl -v -X GET http://localhost:8081/repairs/<repairID> -H "Content-Type: application/json"

# PUT /repairs/{repairID}
curl -v -X PUT http://localhost:8081/repairs/<repairID> -H "Content-Type: application/json" -d '{"status":"completed"}'

docker exec -it roadride_mechanic-mongodb-1 mongosh -u admin -p admin
```
```

###consul
curl http://localhost:8500/v1/catalog/services
curl http://localhost:8500/v1/status/leader
curl http://localhost:8080/health
curl http://localhost:8500/v1/health/service/api-gateway
curl http://localhost:8500/v1/health/service/repair-service
