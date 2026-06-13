import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# 1. Konek ke Apache Kafka di Docker
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("SUCCESS: Connected to Kafka Broker!")
except Exception as e:
    print(f"ERROR: Failed to connect to Kafka. Make sure Docker is UP! Error: {e}")
    exit()

# 2. PATH menuju file CSV
CSV_PATH = 'ingestion/dataset/dfTransjakarta180krows.csv'

print("START: Streaming Transjakarta data to Kafka...")
print("Tekan Ctrl+C untuk berhenti.\n")

try:
    with open(CSV_PATH, mode='r', encoding='utf-8') as file:
        # Menggunakan DictReader supaya tiap baris otomatis jadi format dictionary/JSON
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Tambahkan timestamp waktu sekarang biar berasa real-time streaming
            row['streaming_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Kirim data satu baris CSV ke topik Kafka
            # Kita arahkan ke topik 'topic-transjakarta'
            producer.send('topic-transjakarta', value=row)
            
            # Print di terminal untuk memantau data yang terkirim
            # Menggunakan .get() aman agar tidak crash jika nama kolom agak berbeda
            print(f"[KAFKA STREAM] Sent row ID: {row.get('transID', 'No-ID')} | Halte: {row.get('tapInStopsName', 'Unknown')}")
            
            time.sleep(1)

except FileNotFoundError:
    print(f"ERROR: File not found at {CSV_PATH}.")
except KeyboardInterrupt:
    print("\nSTOP: Feeder stopped by user.")
finally:
    producer.flush()