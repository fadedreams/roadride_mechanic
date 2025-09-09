#!/bin/bash

# Exit on any error
set -e

# Define variables
SCHEMA_REGISTRY_URL="http://localhost:8081"
KSQLDB_URL="http://localhost:8088"
KAFKA_CONNECT_URL="http://localhost:8083"
KAFKA_CONTAINER="roadride_mechanic-kafka-1"
KSQLDB_CONTAINER="roadride_mechanic-ksqldb-server-1"
KAFKA_CONNECT_CONTAINER="roadride_mechanic-kafka-connect-1"
ELASTICSEARCH_URL="http://localhost:9200"

# Function to wait for a service to be healthy
wait_for_service() {
  local service_name=$1
  local healthcheck_url=$2
  local max_attempts=$3
  local interval=$4
  local attempt=1

  echo "Waiting for $service_name to be healthy..."
  until curl -s -f "$healthcheck_url" > /dev/null; do
    if [ $attempt -ge $max_attempts ]; then
      echo "Error: $service_name did not become healthy after $max_attempts attempts"
      exit 1
    fi
    echo "Attempt $attempt/$max_attempts: $service_name not ready, waiting $interval seconds..."
    sleep $interval
    ((attempt++))
  done
  echo "$service_name is healthy!"
}

# Wait for services to be ready
wait_for_service "Kafka" "http://localhost:8080/actuator/health" 30 5
wait_for_service "Schema Registry" "$SCHEMA_REGISTRY_URL/subjects" 30 5
wait_for_service "ksqlDB" "$KSQLDB_URL/info" 30 5
wait_for_service "Kafka Connect" "$KAFKA_CONNECT_URL/connectors" 30 5
wait_for_service "Elasticsearch" "$ELASTICSEARCH_URL/_cluster/health" 30 5

# Step 1: Register Avro schema
echo "Registering Avro schema for repair-events..."
SCHEMA=$(jq -c -r 'tojson | tojson' repair_event.avsc)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\":$SCHEMA}" \
  $SCHEMA_REGISTRY_URL/subjects/repair-events-value/versions

# Verify schema registration
echo "Verifying schema registration..."
curl -s $SCHEMA_REGISTRY_URL/subjects/repair-events-value/versions/1 | jq .
if [ $? -ne 0 ]; then
  echo "Error: Schema registration failed"
  exit 1
fi

# Step 2: Create ksqlDB stream and table
echo "Creating ksqlDB stream and table..."
cat <<EOF > create_stream_table.ksql
CREATE STREAM REPAIR_EVENTS (
  id STRING,
  user_id STRING,
  status STRING,
  repair_type STRING,
  total_price DOUBLE,
  user_location STRUCT<longitude DOUBLE, latitude DOUBLE>,
  mechanics ARRAY<STRUCT<id STRING, name STRING, location STRUCT<longitude DOUBLE, latitude DOUBLE>, distance DOUBLE>>
) WITH (
  KAFKA_TOPIC='repair-events',
  VALUE_FORMAT='AVRO',
  PARTITIONS=1
);

CREATE TABLE REPAIR_STATUS_COUNTS AS
  SELECT status, COUNT(*) AS event_count
  FROM REPAIR_EVENTS
  GROUP BY status
  EMIT CHANGES;
EOF

# Execute ksqlDB commands
docker exec -i $KSQLDB_CONTAINER ksql http://ksqldb-server:8088 < create_stream_table.ksql

# Verify stream creation
echo "Verifying REPAIR_EVENTS stream..."
docker exec -i $KSQLDB_CONTAINER ksql http://ksqldb-server:8088 <<EOF
DESCRIBE REPAIR_EVENTS;
EOF

# Step 3: Configure Kafka Connect Elasticsearch Sink
echo "Configuring Kafka Connect Elasticsearch Sink..."
curl -X POST -H "Content-Type: application/json" --data '{
  "name": "repair-status-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "REPAIR_STATUS_COUNTS",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "_doc",
    "key.ignore": "true",
    "schema.ignore": "true",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}' $KAFKA_CONNECT_URL/connectors

# Verify connector status
echo "Verifying Kafka Connect Elasticsearch Sink status..."
sleep 5 # Give connector time to start
curl -s $KAFKA_CONNECT_URL/connectors/repair-status-elasticsearch-sink/status | jq .
if [ $? -ne 0 ]; then
  echo "Error: Kafka Connect Elasticsearch Sink configuration failed"
  exit 1
fi

# Step 4: Insert sample data(optional)
echo "Inserting sample data into repair-events topic..."
cat <<EOF > sample_data.json
{"id":"event1","user_id":"user123","status":"PENDING","repair_type":"ENGINE","total_price":500.0,"user_location":{"longitude":40.7128,"latitude":-74.0060},"mechanics":[{"id":"mech1","name":"John Doe","location":{"longitude":40.7110,"latitude":-74.0050},"distance":1.2}]}
{"id":"event2","user_id":"user456","status":"COMPLETED","repair_type":"BRAKES","total_price":300.0,"user_location":{"longitude":34.0522,"latitude":-118.2437},"mechanics":[{"id":"mech2","name":"Jane Smith","location":{"longitude":34.0510,"latitude":-118.2400},"distance":0.8}]}
{"id":"event3","user_id":"user123","status":"PENDING","repair_type":"TIRES","total_price":200.0,"user_location":{"longitude":40.7128,"latitude":-74.0060},"mechanics":[{"id":"mech3","name":"Bob Wilson","location":{"longitude":40.7100,"latitude":-74.0070},"distance":1.5}]}
EOF

# Produce sample data
while IFS= read -r line; do
  echo "$line" | docker exec -i $KAFKA_CONNECT_CONTAINER kafka-avro-console-producer \
    --broker-list kafka:9094 \
    --topic repair-events \
    --property schema.registry.url=http://schema-registry:8081 \
    --property value.schema="$(cat repair_event.avsc)"
done < sample_data.json

# Step 5: Verify data in Elasticsearch
echo "Verifying data in Elasticsearch..."
sleep 5 # Allow time for data to propagate
curl -s -X GET "$ELASTICSEARCH_URL/repair_status_counts/_search?pretty" | jq .
if [ $? -ne 0 ]; then
  echo "Error: Failed to verify data in Elasticsearch"
  exit 1
fi

# Step 6: Verify topics in Kafka
echo "Listing Kafka topics..."
docker exec -i $KAFKA_CONTAINER kafka-topics.sh --bootstrap-server kafka:9094 --list

echo "Pipeline setup completed successfully!"
