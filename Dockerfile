FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Files will be mounted as volumes, not copied
# This allows for easier development and updates

CMD ["python", "-u", "consumer-siddhi.py"]