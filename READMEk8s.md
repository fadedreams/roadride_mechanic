sudo tee -a /etc/hosts <<EOF
127.0.0.1 api.local.dev
127.0.0.1 consul.local.dev
127.0.0.1 jaeger.local.dev
EOF

ping -c 1 api.local.dev
ping -c 1 consul.local.dev
ping -c 1 jaeger.local.dev


eval $(minikube docker-env)
kubectl apply -f k8s/local-stack/mongodb.yaml
