

2. **go.mod** (dependencies for Kafka and Schema Registry):
<xaiArtifact artifact_id="9412ae5a-a341-493d-8131-464021ffc361" artifact_version_id="6f4ee97a-fa2c-4365-b8a7-3e324ce3e051" title="go.mod" contentType="text/x-go">
```go
module kafka-app

go 1.21

require (
    github.com/confluentinc/confluent-kafka-go/v2 v2.5.0
    github.com/gorilla/mux v1.8.1
)
```

3. **Dockerfile** (builds the Go app):
<xaiArtifact artifact_id="ab5785ae-938d-4750-b088-1ac1e4e03486" artifact_version_id="846d192f-e190-4ecf-938a-884a8354aa3e" title="Dockerfile" contentType="text/dockerfile">
```dockerfile
FROM golang:1.21 AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-app

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/kafka-app .
EXPOSE 8087
CMD ["./kafka-app"]
```

4. **deployment.yaml** (Kubernetes Deployment and Service):
<xaiArtifact artifact_id="3198f206-781d-4c1f-92a9-da56e6318763" artifact_version_id="ab660b34-67f9-45ab-ad3e-c6b2af71f6af" title="deployment.yaml" contentType="text/yaml">
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-app
  namespace: roadride
  labels:
    app: kafka-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-app
  template:
    metadata:
      labels:
        app: kafka-app
    spec:
      containers:
      - name: kafka-app
        image: kafka-app:latest
        ports:
        - containerPort: 8087
          name: http
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: KAFKA_BOOTSTRAP_SERVERS
        - name: SCHEMA_REGISTRY_URL
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: SCHEMA_REGISTRY_URL
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /health
            port: 8087
          initialDelaySeconds: 15
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8087
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-app
  namespace: roadride
  labels:
    app: kafka-app
spec:
  type: ClusterIP
  ports:
  - port: 8087
    targetPort: 8087
    protocol: TCP
    name: http
  selector:
    app: kafka-app
```

#### Step 2: Build and Deploy in Minikube
Run these in `~/roadride_mechanic/k8s/kafka-app`:

```bash
# Ensure Minikube Docker env
eval $(minikube docker-env)

# Build Docker image
docker build -t kafka-app:latest .

# Deploy to Minikube
kubectl apply -f deployment.yaml

# Wait for readiness
kubectl wait --for=condition=Ready pod -l app=kafka-app -n roadride --timeout=120s

# Verify
kubectl get all -n roadride | grep kafka-app
# Should show: pod/kafka-app-... 1/1 Running, service/kafka-app ClusterIP, deployment.apps/kafka-app 1/1
```

#### Step 3: Test the Application
1. **Health Check**:
   ```bash
   kubectl port-forward svc/kafka-app 8087:8087 -n roadride &
   sleep 5
   curl http://localhost:8087/health
   # Expected: {"status": "healthy", "timestamp": "2025-09-12T..."}
   ```

2. **Produce an Event**:
   ```bash
   curl http://localhost:8087/produce-repair-event
   # Expected: {"status": "produced", "schema_id": 1, "topic": "repair_events"}
   ```

3. **Check Consumer Logs** (the app auto-consumes and logs deserialized events):
   ```bash
   kubectl logs -l app=kafka-app -n roadride -f
   # Look for: "Consumed event: {ID:evt-202509121518..., UserID:user1, ...}"
   ```

#### Step 4: Corrected `kcat` Commands for Manual Testing
The original `kcat` command used `-H 'schema-id=1 magic=0'`, which is incorrect—Avro serialization embeds the schema ID in the payload (magic byte `0` + 4-byte ID). Here's the fixed version (assumes `kcat` with Schema Registry support; install via `sudo apt update && sudo apt install kafkacat` if missing):

```bash
# Ensure topic exists (auto-create should work, but let's be explicit)
kubectl exec -it kafka-0 -n roadride -- kafka-topics --bootstrap-server localhost:9092 --create --topic repair_events --partitions 1 --replication-factor 1

# Port-forward Kafka and Schema Registry
kubectl port-forward svc/kafka-external 30092:30092 -n roadride &
kubectl port-forward svc/schema-registry-svc 8081:8081 -n roadride &

# Produce Avro message (uses -s avro to auto-serialize with SR)
echo '{"id":"evt-test-123","user_id":"user1","status":"pending","repair_type":"flat_tire","total_price":50.0,"user_location":{"longitude":13.4,"latitude":52.52},"mechanics":[{"id":"m1","name":"John","location":{"longitude":13.5,"latitude":52.5},"distance":1.2}]}' | \
kcat -b localhost:30092 -t repair_events -P -s avro -r http://localhost:8081 -k test-key

# Consume (deserialize with SR for readability)
kcat -b localhost:30092 -t repair_events -C -o beginning -c 1 -s avro -r http://localhost:8081
# Expected: test-key {"id":"evt-test-123",...}

# Stop forwards
fg; kill %1; fg; kill %2
```

- **Fixes**:
  - Removed `-H 'schema-id=1 magic=0'` (manual headers break Avro wire format).
  - Added `-s avro -r http://localhost:8081` to let `kcat` handle serialization/deserialization via Schema Registry.
  - Added `-k test-key` for a sample key (optional; adjust as needed).
- **If `kcat` lacks Avro**: Use a Go consumer or Python (`confluent_kafka` with `AvroConsumer`) for testing.

#### Notes and Troubleshooting
- **Verify ConfigMap**: Ensure `./app-configmap.yaml` has:
  ```yaml
  data:
    KAFKA_BOOTSTRAP_SERVERS: "kafka-headless.roadride.svc.cluster.local:9092"
    SCHEMA_REGISTRY_URL: "http://schema-registry-svc.roadride.svc.cluster.local:8081"
  ```
  Apply: `kubectl apply -f app-configmap.yaml`.
- **Logs**: Check `kubectl logs -l app=kafka-app -n roadride` for errors (e.g., Kafka connection or serialization issues).
- **Topic issues**: If `repair_events` isn't created, re-run the `kafka-topics` command or check Kafka logs: `kubectl logs pod/kafka-0 -n roadride | grep -i repair_events`.
- **External access**: For Minikube, add `type: NodePort` and `nodePort: 30887` to the Service in `deployment.yaml`, then `curl http://$(minikube ip):30887/health`.

This app produces and consumes Avro events, exposes a health check, and integrates seamlessly with your setup. Test with `curl` or `kcat`, and let me know if you need consumer tweaks or additional endpoints!
