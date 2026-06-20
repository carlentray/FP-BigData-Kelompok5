FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python modules
COPY server.py .
COPY vehicle_simulator.py .
COPY vehicle_server.py .

# Expose both ports (transaction: 8765, vehicle: 8766)
EXPOSE 8765 8766

# Default environment
ENV CSV_PATH=/data/dfTransjakarta1_4MRows.csv
ENV WS_HOST=0.0.0.0
ENV SPEED_MULTIPLIER=60
ENV BATCH_INTERVAL_MS=100
ENV BUS_FREQUENCY_MIN=15
ENV TICK_INTERVAL_S=1.0
ENV AUTO_START=false

# CMD di-override oleh docker-compose per service
CMD ["python", "server.py"]
