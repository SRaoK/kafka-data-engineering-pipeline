import json
import time
import os
from kafka import KafkaProducer

# 1. Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

FILE_PATH = "landing_zone/users.csv"
print(f"👀 Monitoring file: {FILE_PATH} for changes...")

# Keep track of where we are in the file
last_position = 0

# If file already exists, start at the end so we only catch NEW entries
if os.path.exists(FILE_PATH):
    last_position = os.path.getsize(FILE_PATH)

try:
    while True:
        if os.path.exists(FILE_PATH):
            with open(FILE_PATH, 'r') as f:
                # Seek to our last read position
                f.seek(last_position)
                lines = f.readlines()
                # Update position tracker
                last_position = f.tell()
                
                # Stream out any new lines found
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith("id,name"): # Ignore header lines
                        parts = line.split(",")
                        if len(parts) == 3:
                            payload = {
                                "id": parts[0],
                                "name": parts[1],
                                "country": parts[2],
                                "ingested_at": int(time.time())
                            }
                            print(f"📂 File change detected! Streaming row: {payload}")
                            producer.send('file.users', value=payload)
                            producer.flush()
                            
        time.sleep(1) # Check the file every second
except KeyboardInterrupt:
    print("\n👋 File watcher stopped.")
