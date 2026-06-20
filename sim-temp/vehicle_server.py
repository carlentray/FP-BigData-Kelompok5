"""
Vehicle Telemetry WebSocket Server — GTFS Edition
====================================================
Server WebSocket untuk streaming GPS telemetri armada bus Transjakarta.
Menggunakan data GTFS sebagai ground truth untuk rute, jadwal, & kecepatan.

Endpoint: ws://host:8766 (default)

Environment Variables:
    GTFS_DIR           : Path ke folder GTFS (default: file_gtfs)
    WS_HOST            : Host WebSocket (default: 0.0.0.0)
    WS_PORT            : Port WebSocket (default: 8766)
    SPEED_MULTIPLIER   : Kecepatan simulasi (default: 60)
    TICK_INTERVAL_S    : Interval update posisi (default: 5.0)
    SERVICE_FILTER     : Service IDs, comma-separated (default: SH)

Usage:
    python vehicle_server.py
    python vehicle_server.py --speed 60 --port 8766
    python vehicle_server.py --tick 5 --service SH,HK
"""

import asyncio
import json
import os
import sys
import signal
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import websockets
except ImportError:
    print("ERROR: 'websockets' not found. Install: pip install websockets")
    sys.exit(1)

from vehicle_simulator import GTFSManager, VehicleSimulator

# ========== LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("vehicle-ws")

# Max telemetri per WebSocket message
MAX_VEHICLES_PER_MSG = 200


# ========== GLOBAL STATE ==========
simulator: Optional[VehicleSimulator] = None
gtfs_manager: Optional[GTFSManager] = None
clients: set = set()


async def broadcast(message: str):
    """Kirim message ke semua client yang terhubung."""
    if not clients:
        return
    disconnected = set()
    for ws in clients:
        try:
            await ws.send(message)
        except websockets.exceptions.ConnectionClosed:
            disconnected.add(ws)
    clients.difference_update(disconnected)


async def on_telemetry(telemetry_list: list[dict], sim_time: datetime):
    """
    Callback dari VehicleSimulator.
    Chunk telemetri untuk mencegah WebSocket frame overflow.
    """
    if not clients or not telemetry_list:
        return

    sim_time_str = sim_time.strftime("%Y-%m-%d %H:%M:%S")

    for i in range(0, len(telemetry_list), MAX_VEHICLES_PER_MSG):
        chunk = telemetry_list[i:i + MAX_VEHICLES_PER_MSG]
        message = json.dumps({
            "type": "vehicle_telemetry_batch",
            "count": len(chunk),
            "total_active": len(telemetry_list),
            "sim_time": sim_time_str,
            "streaming_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "vehicles": chunk,
        })
        await broadcast(message)


async def handle_client(websocket):
    """Handle koneksi WebSocket client baru."""
    client_addr = websocket.remote_address
    logger.info(f"Client connected: {client_addr}")
    clients.add(websocket)

    # Welcome message
    welcome = {
        "type": "control",
        "event": "connected",
        "server": "Transjakarta Vehicle Telemetry Simulator (GTFS)",
        "total_trips": len(gtfs_manager.trips) if gtfs_manager else 0,
        "speed_multiplier": simulator.speed_multiplier if simulator else 0,
        "tick_interval_s": simulator.tick_interval_s if simulator else 0,
        "commands": [
            "start    — Mulai simulasi pergerakan bus",
            "stop     — Hentikan simulasi",
            "pause    — Pause simulasi",
            "resume   — Lanjutkan simulasi",
            "status   — Cek status simulasi & jumlah bus aktif",
            "speed:N  — Ubah kecepatan (misal speed:120)",
        ],
        "is_running": simulator.is_running if simulator else False,
    }
    await websocket.send(json.dumps(welcome))

    try:
        async for message in websocket:
            cmd = message.strip().lower()
            logger.info(f"Command from {client_addr}: {cmd}")

            if cmd == "start":
                if simulator.is_running:
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "error",
                        "message": "Simulation already running. Use 'stop' first."
                    }))
                else:
                    await simulator.start()
                    await broadcast(json.dumps({
                        "type": "control",
                        "event": "simulation_started",
                        **simulator.get_status(),
                    }))

            elif cmd == "stop":
                if simulator.is_running:
                    await simulator.stop()
                    await broadcast(json.dumps({
                        "type": "control",
                        "event": "simulation_stopped",
                        **simulator.get_status(),
                    }))
                else:
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "info",
                        "message": "No simulation running."
                    }))

            elif cmd == "pause":
                if simulator.is_running and not simulator.is_paused:
                    await simulator.pause()
                    await broadcast(json.dumps({
                        "type": "control",
                        "event": "simulation_paused",
                    }))

            elif cmd == "resume":
                if simulator.is_running and simulator.is_paused:
                    await simulator.resume()
                    await broadcast(json.dumps({
                        "type": "control",
                        "event": "simulation_resumed",
                    }))

            elif cmd == "status":
                status = simulator.get_status()
                status["type"] = "control"
                status["event"] = "status"
                await websocket.send(json.dumps(status))

            elif cmd.startswith("speed:"):
                try:
                    new_speed = float(cmd.split(":")[1])
                    if new_speed <= 0:
                        raise ValueError("Speed must be positive")
                    simulator.speed_multiplier = new_speed
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "speed_changed",
                        "speed_multiplier": new_speed,
                    }))
                    logger.info(f"Speed changed to {new_speed}x")
                except (ValueError, IndexError) as e:
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "error",
                        "message": f"Invalid speed: {e}"
                    }))

            else:
                await websocket.send(json.dumps({
                    "type": "control",
                    "event": "error",
                    "message": f"Unknown command: {cmd}. "
                               f"Available: start, stop, pause, resume, status, speed:N"
                }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        logger.info(f"Client disconnected: {client_addr}")


async def main_server(args):
    """Inisialisasi dan jalankan Vehicle WebSocket Server."""
    global simulator, gtfs_manager

    # Parse config
    gtfs_dir = args.gtfs or os.environ.get("GTFS_DIR", "file_gtfs")
    host = args.host or os.environ.get("WS_HOST", "0.0.0.0")
    port = int(args.port or os.environ.get("WS_PORT", "8766"))
    speed = float(args.speed or os.environ.get("SPEED_MULTIPLIER", "60"))
    tick = float(args.tick or os.environ.get("TICK_INTERVAL_S", "5.0"))
    service_str = args.service or os.environ.get("SERVICE_FILTER", "SH")
    service_filter = [s.strip() for s in service_str.split(",")]

    # Resolve GTFS path
    gtfs_path = Path(gtfs_dir)
    if not gtfs_path.is_absolute():
        gtfs_path = Path(__file__).parent / gtfs_dir
    if not gtfs_path.exists():
        logger.error(f"GTFS directory not found: {gtfs_path}")
        sys.exit(1)

    # Load GTFS data
    gtfs_manager = GTFSManager(str(gtfs_path))
    gtfs_manager.load()

    # Create simulator
    simulator = VehicleSimulator(
        gtfs_manager=gtfs_manager,
        speed_multiplier=speed,
        tick_interval_s=tick,
        on_telemetry=on_telemetry,
        service_filter=service_filter,
    )

    logger.info("=" * 60)
    logger.info("  TRANSJAKARTA VEHICLE TELEMETRY (GTFS)")
    logger.info("=" * 60)
    logger.info(f"  WebSocket   : ws://{host}:{port}")
    logger.info(f"  GTFS Dir    : {gtfs_path}")
    logger.info(f"  Trips       : {len(gtfs_manager.trips)}")
    logger.info(f"  Speed       : {speed}x")
    logger.info(f"  Tick        : {tick}s")
    logger.info(f"  Service     : {service_filter}")
    logger.info("=" * 60)

    async with websockets.serve(handle_client, host, port,
                                max_size=50 * 1024 * 1024):
        logger.info(f"Server listening on ws://{host}:{port}")
        logger.info("Waiting for clients... Send 'start' to begin.")

        stop_event = asyncio.Event()

        def shutdown_handler():
            logger.info("Shutdown signal received")
            stop_event.set()

        loop = asyncio.get_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, shutdown_handler)
            loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
        except NotImplementedError:
            pass  # Windows

        await stop_event.wait()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transjakarta Vehicle Telemetry WebSocket Server (GTFS)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vehicle_server.py                                # Default
  python vehicle_server.py --speed 120 --port 8766        # 120x speed
  python vehicle_server.py --tick 5 --service SH,HK       # Weekday trips
        """
    )
    parser.add_argument("--gtfs", type=str, default=None,
                        help="Path to GTFS directory (default: file_gtfs)")
    parser.add_argument("--host", type=str, default=None,
                        help="WebSocket host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None,
                        help="WebSocket port (default: 8766)")
    parser.add_argument("--speed", type=float, default=None,
                        help="Speed multiplier (default: 60)")
    parser.add_argument("--tick", type=float, default=None,
                        help="Tick interval in seconds (default: 5.0)")
    parser.add_argument("--service", type=str, default=None,
                        help="Service IDs, comma-separated (default: SH)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main_server(args))
    except KeyboardInterrupt:
        logger.info("Server shutdown by user (Ctrl+C)")
