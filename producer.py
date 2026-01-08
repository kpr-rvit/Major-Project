from kafka import KafkaProducer
import pandas as pd
import json
import time
import sys

# Kafka producer
KAFKA_BROKER = "localhost:29092"

print(f"🔄 Connecting to Kafka at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=10000  # Wait up to 10 seconds for connection
    )
    print("✅ Producer connected successfully")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    sys.exit(1)

# Read CSV data
print("📖 Reading stream_ready.csv...")
try:
    df = pd.read_csv("stream_ready.csv")
    print(f"✅ Loaded {len(df)} rows from CSV")
except FileNotFoundError:
    print("❌ Error: stream_ready.csv not found")
    sys.exit(1)

# Send data row by row
print(f"\n📤 Starting to send data to 'sensor-stream' topic...\n")
for idx, row in df.iterrows():
    # Map CSV columns to Siddhi attribute names
    feature_map = {
        "temperature_2m (°C)": "temperature_2m",
        "relative_humidity_2m (%)": "relative_humidity_2m",
        "wind_speed_10m (km/h)": "wind_speed_10m",
        "soil_temperature_0_to_7cm (°C)": "soil_temperature_0_to_7cm"
    }

    # Start payload with sensorId
    payload = {"sensorId": "sensor-stream"}

    # Map CSV columns to Siddhi keys
    for col in df.columns:
        if col in feature_map:
            payload[feature_map[col]] = row[col]

    #payload = row.to_dict()
    
    # Send to Kafka
    try:
        producer.send("sensor-stream", payload)
        print(f"✅ [{idx + 1}/{len(df)}] Sent: {payload}")
    except Exception as e:
        print(f"❌ Error sending message {idx + 1}: {e}")
        continue
    
    # Flush every 10 messages instead of every message
    if idx % 10 == 0:
        producer.flush(timeout=5)
    
    time.sleep(0.1)  # Small delay between messages

# Final flush at the end
producer.flush(timeout=10)

producer.close()
print(f"\n✅ All {len(df)} messages sent successfully!")
print("🎉 Producer finished")