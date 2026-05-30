import json
import time
import random
from kafka import KafkaProducer

# 1. Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'infra.logs'
print(f"🚀 Log Shipper daemon started. Streaming telemetry to topic: {TOPIC_NAME}")

# Simulated log templates
ip_addresses = ["192.168.1.50", "10.0.0.12", "172.16.254.1"]
endpoints = ["/api/v1/login", "/api/v1/checkout", "/index.html", "/images/logo.png"]
status_codes = [200, 200, 200, 404, 500]

try:
    while True:
        # Simulate a live web server appending lines to an internal log file
        log_payload = {
            "timestamp": int(time.time()),
            "host": "web-server-prod-01",
            "client_ip": random.choice(ip_addresses),
            "request_path": random.choice(endpoints),
            "http_status": random.choice(status_codes),
            "response_time_ms": random.randint(10, 350)
        }
        
        print(f"📡 Shifting Log Event -> Status: {log_payload['http_status']} | Path: {log_payload['request_path']}")
        
        # PUSH data straight into Kafka over the network socket connection
        producer.send(TOPIC_NAME, value=log_payload)
        producer.flush()
        
        # Logs stream rapidly; pause briefly to simulate random traffic spikes
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("\n👋 Log Shipper daemon stopped.")
