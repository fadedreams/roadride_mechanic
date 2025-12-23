## setup
```
minikube start --memory=6000
eval $(minikube docker-env)

kubectl apply -f k8s/consul.yaml
kubectl apply -f k8s/jaeger.yaml

```

## strimzi
```
follow https://strimzi.io/quickstarts/
afterwards
kubectl -n kafka get kafka my-cluster -o yaml > my-cluster-backup.yaml
kubectl -n kafka edit kafka my-cluster

listeners:
  - name: plain
    port: 9092
    type: internal
    tls: false
  - name: external
    port: 9094
    type: nodeport
    tls: false

kubectl -n kafka get svc | grep -E "(external|nodeport)"
my-cluster-kafka-external-bootstrap   NodePort    10.99.233.143    <none>        9094:31213/TCP                                 4m32s

kubectl -n kafka port-forward svc/my-cluster-kafka-external-bootstrap 9094:9094 &
```

## schema-registry
```
kubectl apply -f schema-registry.yaml -n kafka
kubectl port-forward svc/schema-registry 8081:8081 -n kafka &
```

## elk
```
 https://artifacthub.io/packages/helm/elk8-dev/elk8-dev 
```

## mongodb
```
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install mongodb bitnami/mongodb -f k8s/mongodb-values.yaml
```


## setup
```
docker build -t api-gateway:latest ./api-gateway
docker build -t repair-service:latest ./repair-service
docker build -t mechanic-service:latest ./mechanic-service

kubectl apply -f k8s/api-gateway-deployment.yaml
kubectl apply -f k8s/api-gateway-service.yaml
kubectl rollout restart deployment api-gateway
kubectl logs -f -l app=api-gateway

kubectl apply -f k8s/repair-service-deployment.yaml
kubectl apply -f k8s/repair-service-service.yaml
kubectl rollout restart deployment api-gateway
kubectl logs -f -l app=repair-service

kubectl apply -f k8s/mechanic-service-deployment.yaml
kubectl apply -f k8s/mechanic-service-service.yaml
kubectl rollout restart deployment mechanic-service
kubectl logs -f -l app=mechanic-service

```
## testing stuff

```
api-gateway 8085
mechanic-service 8086
repair-service 8087

curl http://localhost:8085/health
curl http://localhost:8086/health
curl http://localhost:8087/health

# POST /repairs
curl -v -X POST http://localhost:8085/repairs -H "Content-Type: application/json" -d '{"userID":"test-user2","repairType":"flat_tire","totalPrice":50.0,"userLocation":{"longitude":13.400000,"latitude":52.520000}}'

# GET /repairs/cost/{costID} (use costID from POST /repairs)
curl -v -X GET "http://localhost:8085/repairs/cost/<costID>?userID=test-user" -H "Content-Type: application/json"

# GET /repairs/{repairID} (use repairID from POST /repairs)
curl -v -X GET http://localhost:8085/repairs/<repairID> -H "Content-Type: application/json"

# PUT /repairs/{repairID}
curl -v -X PUT http://localhost:8085/repairs/<repairID> -H "Content-Type: application/json" -d '{"status":"completed"}'


#testing grpc
grpcurl -plaintext localhost:50051 repair.RepairService/StreamAllRepairs


#consul
curl http://localhost:8500/v1/catalog/services
curl http://localhost:8500/v1/status/leader
curl http://localhost:8080/health
curl http://localhost:8500/v1/health/service/api-gateway
curl http://localhost:8500/v1/health/service/repair-service

```
