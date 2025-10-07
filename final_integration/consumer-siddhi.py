# consumer-siddhi.py
from kafka import KafkaConsumer, KafkaProducer
import json
import joblib
import numpy as np
from collections import deque
import time
import sys

# Load model
print("🔄 Loading ML model...")
model = joblib.load("xgb_multi_model.pkl")
print("✅ Model loaded successfully")

FEATURES = [
    "temperature_2m (°C)",
    "relative_humidity_2m (%)",
    "wind_speed_10m (km/h)",
    "soil_temperature_0_to_7cm (°C)"
]

look_back, horizon = 72, 6
buffer = deque(maxlen=look_back)

# Kafka connection settings
KAFKA_BROKER = "kafka:9092"
MAX_RETRIES = 10
RETRY_DELAY = 5

# Wait for Kafka to be ready
print(f"🔄 Connecting to Kafka at {KAFKA_BROKER}...")
for attempt in range(MAX_RETRIES):
    try:
        # Test connection with producer
        test_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        test_producer.close()
        print("✅ Kafka connection established")
        break
    except Exception as e:
        if attempt < MAX_RETRIES - 1:
            print(f"⏳ Waiting for Kafka... (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
        else:
            print(f"❌ Failed to connect to Kafka after {MAX_RETRIES} attempts")
            sys.exit(1)

# Kafka Consumer - reads sensor stream
consumer = KafkaConsumer(
    "sensor-stream",
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ml-consumer-group"
)

# Kafka Producer - to send forecast to Siddhi
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ ML Consumer started. Waiting for messages on 'sensor-stream' topic...")

try:
    for record in consumer:
        msg = record.value
        row = [msg.get(f, 0) for f in FEATURES]
        buffer.append(row)

        if len(buffer) == look_back:
            X_input = np.array(buffer).flatten().reshape(1, -1)
            y_pred = model.predict(X_input).reshape(horizon, len(FEATURES))
            temp_first_hour = float(y_pred[0, FEATURES.index("temperature_2m (°C)")])

            print(f"🔮 Forecast - First hour temperature: {temp_first_hour:.2f}°C")

            # Send prediction to Siddhi via Kafka
            forecast_msg = {
                "sensorId": "ml-forecast",
                "temperature": temp_first_hour
            }
            producer.send("ml-forecast", forecast_msg)
            producer.flush()
            print(f"📤 Sent forecast to ml-forecast topic: {forecast_msg}")

except KeyboardInterrupt:
    print("\n⏹️ Shutting down consumer...")
finally:
    consumer.close()
    producer.close()
    print("✅ Consumer closed")




















# from kafka import KafkaConsumer
# import json, joblib
# import numpy as np
# import requests
# from collections import deque

# # Load ML model
# model = joblib.load("xgb_multi_model.pkl")

# FEATURES = [
#     "temperature_2m (°C)",
#     "relative_humidity_2m (%)",
#     "wind_speed_10m (km/h)",
#     "soil_temperature_0_to_7cm (°C)"
# ]

# look_back, horizon = 72, 6
# buffer = deque(maxlen=look_back)

# # Kafka consumer (reads full feature rows)
# consumer = KafkaConsumer(
#     "sensor-stream",
#     bootstrap_servers="localhost:29092",
#     value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#     auto_offset_reset="earliest",
#     enable_auto_commit=True,
#     group_id="ml-consumer-group"
# )

# # Siddhi HTTP endpoint
# SIDDHI_URL = "http://localhost:8006/SensorStream"

# print("✅ Consumer started. Waiting for messages...")

# for record in consumer:
#     msg = record.value
#     row = [msg.get(f, 0) for f in FEATURES]
#     buffer.append(row)

#     if len(buffer) == look_back:
#         # ML prediction
#         X_input = np.array(buffer).flatten().reshape(1, -1)
#         y_pred = model.predict(X_input).reshape(horizon, len(FEATURES))

#         # First-hour temperature
#         temp_first_hour = float(y_pred[0, FEATURES.index("temperature_2m (°C)")])
#         print(f"🔮 Forecast - First hour temperature: {temp_first_hour}°C")

#         # Send to Siddhi via HTTP
#         try:
#             r = requests.post(
#                 "http://localhost:8006/SensorStream",
#                 json={"sensorId": "ml-forecast", "temperature": temp_first_hour}
#             )
#             print(f"📨 Sent to Siddhi (status {r.status_code})")
#         except Exception as e:
#             print("❌ Error sending to Siddhi:", e)