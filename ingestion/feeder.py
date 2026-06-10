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
    print("✅ Berhasil konek ke Kafka Broker!")
except Exception as e:
    print(f"❌ Gagal konek ke Kafka. Pastikan Docker sudah UP! Error: {e}")
    exit()

# 2. PATH menuju file CSV
CSV_PATH = 'ingestion/dataset/dfTransjakarta180krows.csv'

print("🚀 Mulai mengalirkan data ASLI Transjakarta (180k Rows) ke Kafka...")
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
    print(f"❌ File tidak ditemukan di {CSV_PATH}.")
except KeyboardInterrupt:
    print("\n🛑 Feeder dihentikan oleh pengguna.")
finally:
    producer.flush()