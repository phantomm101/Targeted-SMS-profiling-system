from kafka import KafkaProducer
import json

# Localhost because you're testing from the host
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

test_sms = {
    "phone": "+1234567890",
    "body": "Transaction: OMR 25.50 at SuperMarket on 2024-06-10",
    "timestamp": "2024-06-10T10:30:00"
}

producer.send('sms-messages', test_sms)
producer.flush()
print("✅ Test message sent to Kafka")
