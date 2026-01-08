# consumer-siddhi.py
from kafka import KafkaConsumer, KafkaProducer
import json
import joblib
import numpy as np
from collections import deque
import time
import sys
from tensorflow.keras.models import load_model

# ---------------- Load stacked pipeline ----------------
print("🔄 Loading stacked pipeline and models...")
pipeline = joblib.load("stacked_pipeline.pkl")

meta_model = pipeline["meta_model"]
xgb_model  = pipeline["xgb_model"]
scaler     = pipeline["scaler"]   # MinMaxScaler
FEATURES   = pipeline.get("features", [
    "temperature_2m (°C)",
    "relative_humidity_2m (%)",
    "wind_speed_10m (km/h)",
    "soil_temperature_0_to_7cm (°C)"
])
look_back  = int(pipeline.get("look_back", 72))
horizon    = int(pipeline.get("horizon", 6))

# Load Keras models (saved separately)
lstm_model = load_model("best_lstm.keras")
gru_model  = load_model("best_gru.keras")

print("✅ stacked pipeline, scaler and Keras models loaded")
"""

FEATURES = [
    "temperature_2m (°C)",
    "relative_humidity_2m (%)",
    "wind_speed_10m (km/h)",
    "soil_temperature_0_to_7cm (°C)"
]

look_back, horizon = 72, 6"""
buffer = deque(maxlen=look_back)

# Kafka connection settings
KAFKA_BROKER = "localhost:29092"
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
feature_map = {
    "temperature_2m (°C)": "temperature_2m",
    "relative_humidity_2m (%)": "relative_humidity_2m",
    "wind_speed_10m (km/h)": "wind_speed_10m",
    "soil_temperature_0_to_7cm (°C)": "soil_temperature_0_to_7cm"
}

# last_sent should use Siddhi names
last_sent = {feature_map[f]: None for f in FEATURES}

try:
    for record in consumer:
        msg = record.value
        row = [msg.get(feature_map[f], 0) for f in FEATURES]
        buffer.append(row)

        if len(buffer) == look_back:
            # Prepare raw data array from buffer (shape: look_back x n_features)
            raw_arr = np.array(buffer)  # shape: (look_back, n_features)

            # 1) scale exactly like training
            # scaler expects 2D array of shape (n_samples, n_features). We transform the full sequence rows.
            scaled_seq = scaler.transform(raw_arr)  # shape: (look_back, n_features)

            # 2) prepare inputs for each base model
            X_seq = scaled_seq.reshape(1, look_back, len(FEATURES))   # for LSTM/GRU (1, look_back, n_features)
            X_flat = X_seq.reshape(1, -1)                             # for XGBoost (1, look_back*n_features)

            # 3) base model predictions
            y_lstm = lstm_model.predict(X_seq).reshape(1, -1)   # (1, horizon*n_features) expected
            y_gru  = gru_model.predict(X_seq).reshape(1, -1)
            y_xgb  = xgb_model.predict(X_flat)                  # ensure xgb_model in pipeline matches training

            # If y_xgb is (1, horizon*n_features) or shape (1, k) adapt similarly:
            if y_xgb.ndim == 1:
                y_xgb = y_xgb.reshape(1, -1)

            # 4) stack and meta predict
            X_meta = np.concatenate([y_lstm, y_gru, y_xgb], axis=1)  # (1, 3*horizon*n_features) depending on training
            y_pred_flat = meta_model.predict(X_meta)                 # (1, horizon*n_features)

            # 5) reshape to (horizon, n_features) in scaled units
            y_pred_scaled = np.array(y_pred_flat).reshape(horizon, len(FEATURES))

            # 6) inverse scale to original units
            y_pred = scaler.inverse_transform(y_pred_scaled)  # now in real-world units
            """feature_map = {
            "temperature_2m (°C)": "temperature_2m",
            "relative_humidity_2m (%)": "relative_humidity_2m",
            "wind_speed_10m (km/h)": "wind_speed_10m",
            "soil_temperature_0_to_7cm (°C)": "soil_temperature_0_to_7cm"}"""

            # 7) take first hour temperature
                        # Create a forecast message for all features
            forecast_msg = {"sensorId": "ml-forecast"}

            

            for i, feature in enumerate(FEATURES):
                # Sanitize feature names for Siddhi (remove spaces, parentheses, or /)
                key = feature_map[feature]  # map to Siddhi attribute name
                val = float(y_pred[0, i])
    
                # Send only if changed
                if last_sent[key] != val:
                    forecast_msg[key] = val
                    last_sent[key] = val
            
            

            if len(forecast_msg) > 1:  # Only send if at least one feature has changed
                producer.send("ml-forecast", forecast_msg)
                producer.flush()
                print(f"📤 Sent forecast to ml-forecast topic: {forecast_msg}")



except KeyboardInterrupt:
    print("\n⏹️ Shutting down consumer...")
finally:
    consumer.close()
    producer.close()
    print("✅ Consumer closed")
