import asyncio
import json
import sys
import os
from kafka import KafkaProducer
import websockets
from datetime import datetime

# 1. Koneksi ke Kafka Broker
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("SUCCESS: Connected to Kafka Broker!")
except Exception as e:
    print(f"ERROR: Failed to connect to Kafka. Make sure Docker is UP! Error: {e}")
    sys.exit(1)

# 2. Ambil URI WebSocket
# Prioritas: 1) Argumen terminal, 2) Default remote IP, 3) Fallback localhost
DEFAULT_URI = "ws://70.153.136.193:8765"
FALLBACK_LOCAL_URI = "ws://localhost:8765"

ws_uri = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URI

async def stream_transactions():
    global ws_uri
    print(f"START: Connecting to WebSocket server at {ws_uri}...")
    
    try:
        async with websockets.connect(ws_uri, max_size=50*1024*1024) as ws:
            # 1. Terima welcome message
            welcome = json.loads(await ws.recv())
            server_name = welcome.get("server", "Unknown")
            print(f"SUCCESS: Connected to {server_name}")
            
            # 2. Kirim sinyal start ke server
            print("Sending 'start' command to WebSocket...")
            await ws.send("start")
            
            # 3. Loop dengar data transaksi secara real-time
            while True:
                try:
                    # Ambil data dari WebSocket
                    message = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(message)
                    
                    if data.get("type") == "transaction_batch":
                        transactions = data.get("transactions", [])
                        count = data.get("count", 0)
                        sim_time = data.get("sim_time", "")
                        
                        print(f"\n--- [Sim Time: {sim_time}] Received batch of {count} transactions ---")
                        
                        # Kirim setiap data transaksi ke Kafka topik 'topic-transjakarta'
                        for t in transactions:
                            # Pastikan memiliki streaming_timestamp
                            if 'streaming_timestamp' not in t:
                                t['streaming_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            producer.send('topic-transjakarta', value=t)
                            
                        # Flush data ke Kafka agar terkirim segera
                        producer.flush()
                        print(f"SUCCESS: Sent {count} transaction records to Kafka topic 'topic-transjakarta'")
                        
                    elif data.get("type") == "control":
                        evt = data.get("event", "")
                        print(f"[CONTROL EVENT] {evt} | Sent count: {data.get('sent_count', 0)}")
                        
                except asyncio.TimeoutError:
                    print("WARNING: Timeout waiting for transaction batch from server, checking status...")
                    await ws.send("status")
                    
    except Exception as e:
        print(f"ERROR: Connection closed or failed: {e}")
        # Jika gagal connect ke remote, coba tawarkan fallback ke lokal
        if ws_uri == DEFAULT_URI:
            print(f"Attempting fallback to local simulator at {FALLBACK_LOCAL_URI}...")
            ws_uri = FALLBACK_LOCAL_URI
            await stream_transactions()

if __name__ == "__main__":
    try:
        asyncio.run(stream_transactions())
    except KeyboardInterrupt:
        print("\nSTOP: Transaction feeder stopped by user.")