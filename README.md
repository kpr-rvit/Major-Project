This project implements Smart Agriculture Decision Support System (DSS) using Machine Learning (ML) and Complex Event Processing (CEP). The system predicts future environmental conditions, generates crop recommendations, and produces real-time alerts based on streaming agricultural data.

Historical agricultural and weather data are used to train machine learning models. A stacked forecasting model combining LSTM, GRU, and XGBoost is used to predict environmental parameters such as temperature and humidity. A Random Forest classifier is implemented to recommend suitable crops based on user-provided weather and soil parameters.

Real-time sensor data are simulated from historical records and streamed using Apache Kafka. These streams are processed by the WSO2 Siddhi CEP engine to detect predefined patterns and anomalies, triggering alerts such as temperature alerts, humidity alerts, and composite alerts.

A web-based backend and frontend are developed to display real-time sensor data, forecasted values, crop recommendations, and CEP-generated alerts through an interactive dashboard. The project demonstrates end-to-end integration of ML-based prediction, CEP-based event detection, and web-based visualization for smart agriculture decision support.
