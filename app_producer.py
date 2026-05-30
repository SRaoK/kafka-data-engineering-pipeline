import json
import time
from kafka import KafkaProducer

# 1. Initialize the direct Kafka Producer client
# We point to localhost:9092 because this script runs directly on your Mac host network.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Microservice initialized. Starting to push direct application events...")

# 2. Simulate user activity events inside a microservice loop
mock_orders = [
    {"order_id": "ORD-1001", "user": "Charlie", "item": "Laptop", "price": 1200.00},
    {"order_id": "ORD-1002", "user": "Diana", "item": "Headphones", "price": 150.00},
    {"order_id": "ORD-1003", "user": "Evan", "item": "Wireless Mouse", "price": 25.00}
]

for order in mock_orders:
    # Add a timestamp to simulate a live system
    order["timestamp"] = int(time.time())
    
    print(f"Direct Pushing: {order['order_id']} to Kafka...")
    
    # 3. PUSH payload directly into a clean topic named 'app.orders'
    producer.send('app.orders', value=order)
    
    time.sleep(1.5) # Pause to simulate human user spacing

# Ensure all messages are flushed over the socket network connection
producer.flush()
print("✅ All application events pushed successfully.")
