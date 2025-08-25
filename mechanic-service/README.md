curl http://localhost:8082/health
curl http://localhost:8082/repairs/nearb
curl -X POST http://localhost:8082/repairs/r1/assign -H "Content-Type: application/json" -d '{"mechanicID":"m1"}'
