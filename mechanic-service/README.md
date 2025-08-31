curl http://localhost:8082/health
curl "http://localhost:8082/repairs/nearby?mechanicID=m1"
curl "http://localhost:8082/repairs/nearby?mechanicID=mechanic1"

curl "http://localhost:8082/repairs/nearby?mechanicID=mechanic1"
[{"id":"68ac8565145b4cb8ee5b60ef","userID":"test-user2","status":"pending","repairCost":{"id":"68ac8565145b4cb8ee5b60ee","userID":"test-user2","repairType":"flat_tire","totalPrice":50,"userLocation":{"latitude":52.52,"longitude":13.4},"mechanics":null},"assignedTo":""}]
m@debian:~/goprj/roadride_mechanic$ 

curl -X POST http://localhost:8082/repairs/<repairID>/assign -H "Content-Type: application/json" -d '{"mechanicID":"mechanic1"}'
