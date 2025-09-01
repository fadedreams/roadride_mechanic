#!/bin/sh

CONSUL_ADDRESS=${CONSUL_ADDRESS:-consul:8500}
SERVICE_NAME=${SERVICE_NAME:-kafka}
SERVICE_ID=${SERVICE_ID:-kafka-9094}
SERVICE_PORT=${SERVICE_PORT:-9094}
SERVICE_ADDRESS=${SERVICE_ADDRESS:-kafka}

echo "Registering $SERVICE_NAME with Consul at $CONSUL_ADDRESS"

curl -X PUT "http://$CONSUL_ADDRESS/v1/agent/service/register" \
  -H "Content-Type: application/json" \
  -d '{
    "ID": "'"$SERVICE_ID"'",
    "Name": "'"$SERVICE_NAME"'",
    "Address": "'"$SERVICE_ADDRESS"'",
    "Port": '"$SERVICE_PORT"',
    "Check": {
      "TCP": "'"$SERVICE_ADDRESS:$SERVICE_PORT"'",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

if [ $? -eq 0 ]; then
  echo "Successfully registered $SERVICE_NAME with Consul"
else
  echo "Failed to register $SERVICE_NAME with Consul"
  exit 1
fi

# Keep the container running
tail -f /dev/null
