import asyncio
import json
import sys
import os
from kafka import KafkaProducer
import websockets

# 1. Koneksi ke Kafka Broker
# Menggunakan localhost:9092 untuk di luar Docker host
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
FALLBACK_LOCAL_URI = "ws://localhost:8766"

ws_uri = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URI

async def stream_telemetry():
    global ws_uri
    print(f"START: Connecting to WebSocket server at {ws_uri}...")
    
    try:
        async with websockets.connect(ws_uri, max_size=50*1024*1024) as ws:
            # 1. Terima welcome message
            welcome = json.loads(await ws.recv())
            server_name = welcome.get("server", "Unknown")
            total_trips = welcome.get("total_trips", 0)
            print(f"SUCCESS: Connected to {server_name} | Total Trips: {total_trips}")
            
            # 2. Kirim sinyal start ke server
            print("Sending 'start' command to WebSocket...")
            await ws.send("start")
            
            # 3. Loop dengar data telemetry secara real-time
            while True:
                try:
                    # Ambil data dari WebSocket
                    message = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(message)
                    
                    if data.get("type") == "vehicle_telemetry_batch":
                        vehicles = data.get("vehicles", [])
                        count = data.get("count", 0)
                        sim_time = data.get("sim_time", "")
                        
                        print(f"\n--- [Sim Time: {sim_time}] Received batch of {count} vehicles ---")
                        
                        # Kirim setiap data posisi kendaraan ke Kafka topik 'topic-telemetry'
                        for v in vehicles:
                            producer.send('topic-telemetry', value=v)
                            
                        # Flush data ke Kafka agar terkirim segera
                        producer.flush()
                        print(f"SUCCESS: Sent {count} vehicle records to Kafka topic 'topic-telemetry'")
                        
                    elif data.get("type") == "control":
                        evt = data.get("event", "")
                        print(f"[CONTROL EVENT] {evt} | Active vehicles: {data.get('active_vehicles', 0)}")
                        
                except asyncio.TimeoutError:
                    print("WARNING: Timeout waiting for telemetry batch from server, checking status...")
                    await ws.send("status")
                    
    except Exception as e:
        print(f"ERROR: Connection closed or failed: {e}")
        # Jika gagal connect ke remote, coba tawarkan fallback ke lokal
        if ws_uri == DEFAULT_URI:
            print(f"Attempting fallback to local simulator at {FALLBACK_LOCAL_URI}...")
            ws_uri = FALLBACK_LOCAL_URI
            await stream_telemetry()

if __name__ == "__main__":
    try:
        asyncio.run(stream_telemetry())
    except KeyboardInterrupt:
        print("\nSTOP: Telemetry feeder stopped by user.")
