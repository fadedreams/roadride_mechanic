sudo tee -a /etc/hosts <<EOF
127.0.0.1 api.local.dev
127.0.0.1 consul.local.dev
127.0.0.1 jaeger.local.dev
EOF

ping -c 1 api.local.dev
ping -c 1 consul.local.dev
ping -c 1 jaeger.local.dev


minikube start
eval $(minikube docker-env)
docker build -t api-gateway:latest ./api-gateway
kubectl logs -f deployment/api-gateway


kubectl apply -f k8s/local-stack/mongodb.yaml
kubectl apply -f k8s/local-stack/mongo-init-job.yaml   # <-- run this once
kubectl apply -f k8s/local-stack/mongo-test.yaml
kubectl logs job/mongo-test

docker build -t api-gateway:latest ./api-gateway
kubectl rollout restart deployment api-gateway
kubectl logs -f deployment/api-gateway

kubectl apply -f k8s/local-stack/consul.yaml
kubectl apply -f k8s/local-stack/jaeger.yaml

# Wait until they’re healthy
kubectl wait --for=condition=ready pod -l app=consul --timeout=120s
kubectl wait --for=condition=ready pod -l app=jaeger --timeout=120s
kubectl wait --for=condition=ready pod -l app=mongodb --timeout=120s

# Now api-gateway stuff
kubectl apply -f k8s/api-gateway/configmap.yaml
kubectl apply -f k8s/api-gateway/secret.yaml     # optional
kubectl apply -f k8s/api-gateway/deployment.yaml
kubectl apply -f k8s/api-gateway/service.yaml
kubectl apply -f k8s/api-gateway/ingress.yaml    # or local-stack/ingress.yaml

# Should return "OK"
curl -H "Host: api.local.dev" http://localhost/health

# Or port-forward temporarily
kubectl port-forward svc/api-gateway 8085:80
curl http://localhost:8085/health

# 1. Create a repair (replace with real repair-service when ready, or just hit /health for now)
curl -X POST http://localhost/repairs -H "Host: api.local.dev" -H "Content-Type: application/json" -d '{"userID":"test-user1","repairType":"flat_tire","totalPrice":50}'

# 2. Connect WS
wscat -c "ws://localhost/ws?userID=test-user1" -H "Host: api.local.dev"
# or use https://piehost.com/websocket-tester if you’re lazy
