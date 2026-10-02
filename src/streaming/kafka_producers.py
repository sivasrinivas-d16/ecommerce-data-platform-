from kafka import KafkaProducer
import json


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# Test event
event = {
    "event_id": "EVT000005",
    "customer_id": "C00005",
    "event_type": "PRODUCT_VIEWED",
    "event_timestamp": "2026-10-01T13:40:30",
    "source": "WEB"
}


# Send event to Kafka
future = producer.send(
    "ecommerce-events",
    value=event
)

# Wait for Kafka acknowledgement
record_metadata = future.get(timeout=10)

print("Event sent successfully")
print("Topic:", record_metadata.topic)
print("Partition:", record_metadata.partition)
print("Offset:", record_metadata.offset)


producer.flush()
producer.close()