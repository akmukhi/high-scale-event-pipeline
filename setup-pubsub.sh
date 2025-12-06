#!/bin/bash
# Setup script to create Pub/Sub topic and subscription

set -e

PROJECT_ID=${GCP_PROJECT_ID:-test-project}
TOPIC_NAME=${PUBSUB_TOPIC:-events}
SUBSCRIPTION_NAME=${PUBSUB_SUBSCRIPTION:-events-subscription}
PUBSUB_EMULATOR_HOST=${PUBSUB_EMULATOR_HOST:-localhost:8085}

echo "Setting up Pub/Sub topic and subscription..."
echo "Project ID: $PROJECT_ID"
echo "Topic: $TOPIC_NAME"
echo "Subscription: $SUBSCRIPTION_NAME"
echo "Emulator: $PUBSUB_EMULATOR_HOST"

# Set emulator host
export PUBSUB_EMULATOR_HOST=$PUBSUB_EMULATOR_HOST

# Wait for emulator to be ready
echo "Waiting for Pub/Sub emulator to be ready..."
until curl -f http://$PUBSUB_EMULATOR_HOST > /dev/null 2>&1; do
  echo "Waiting for emulator..."
  sleep 2
done

# Create topic
echo "Creating topic: $TOPIC_NAME"
python3 << EOF
from google.cloud import pubsub_v1
import os

project_id = "$PROJECT_ID"
topic_name = "$TOPIC_NAME"
emulator_host = "$PUBSUB_EMULATOR_HOST"

os.environ["PUBSUB_EMULATOR_HOST"] = emulator_host

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

try:
    publisher.get_topic(request={"topic": topic_path})
    print(f"Topic {topic_name} already exists")
except Exception as e:
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Created topic {topic_name}")
    except Exception as create_error:
        print(f"Error creating topic: {create_error}")
EOF

# Create subscription
echo "Creating subscription: $SUBSCRIPTION_NAME"
python3 << EOF
from google.cloud import pubsub_v1
import os

project_id = "$PROJECT_ID"
topic_name = "$TOPIC_NAME"
subscription_name = "$SUBSCRIPTION_NAME"
emulator_host = "$PUBSUB_EMULATOR_HOST"

os.environ["PUBSUB_EMULATOR_HOST"] = emulator_host

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, subscription_name)

try:
    subscriber.get_subscription(request={"subscription": subscription_path})
    print(f"Subscription {subscription_name} already exists")
except Exception as e:
    try:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path
            }
        )
        print(f"Created subscription {subscription_name}")
    except Exception as create_error:
        print(f"Error creating subscription: {create_error}")
EOF

echo "Pub/Sub setup complete!"

