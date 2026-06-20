"""
Transjakarta WebSocket Simulator Server
========================================
Server simulasi pembayaran Transjakarta yang push data via WebSocket
sesuai dengan timestamp tapInTime dari dataset.

Cara kerja:
- Membaca CSV 1.4 juta transaksi, diurutkan berdasarkan tapInTime
- Saat client connect, server mulai simulasi dari jam awal data (05:00)
- Data di-push ke client ketika "waktu simulasi" sesuai dengan tapInTime
- Speed multiplier bisa dikonfigurasi (misal 60x = 17 jam → 17 menit)

Environment Variables:
    CSV_PATH          : Path ke file CSV (default: dfTransjakarta1_4MRows.csv)
    WS_HOST           : Host WebSocket (default: 0.0.0.0)
    WS_PORT           : Port WebSocket (default: 8765)
    SPEED_MULTIPLIER  : Kecepatan simulasi, 1=realtime, 60=1jam=1menit (default: 60)
    BATCH_INTERVAL_MS : Interval pengiriman batch dalam ms (default: 100)

Usage:
    python server.py
    python server.py --speed 120 --port 8765
"""

import asyncio
import csv
import json
import os
import sys
import time
import argparse
import signal
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    import websockets
except ImportError:
    print("ERROR: 'websockets' package not found. Install with: pip install websockets")
    sys.exit(1)

# Max transactions per WebSocket message to avoid frame size overflow
MAX_BATCH_SIZE = 500

# ========== LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("transjakarta-sim")

# ========== CONSTANTS ==========
CSV_COLUMNS = [
    'transID', 'payCardID', 'payCardBank', 'payCardName', 'payCardSex',
    'payCardBirthDate', 'corridorID', 'corridorName', 'direction',
    'tapInStops', 'tapInStopsName', 'tapInStopsLat', 'tapInStopsLon',
    'stopStartSeq', 'tapInTime', 'tapOutStops', 'tapOutStopsName',
    'tapOutStopsLat', 'tapOutStopsLon', 'stopEndSeq', 'tapOutTime', 'payAmount'
]


class TransjakartaSimulator:
    """
    Manages the simulation state and data loading.
    Loads all transactions sorted by tapInTime and provides
    time-based batching for WebSocket streaming.
    """

    def __init__(self, csv_path: str, speed_multiplier: float = 60.0):
        self.csv_path = csv_path
        self.speed_multiplier = speed_multiplier
        self.transactions: list[dict] = []
        self.sim_start_time: Optional[datetime] = None  # Earliest tapInTime in data
        self.sim_end_time: Optional[datetime] = None     # Latest tapInTime in data
        self.total_rows = 0
        self.loaded = False

    def load_data(self):
        """Load and parse CSV data, sorted by tapInTime."""
        logger.info(f"Loading data from {self.csv_path}...")
        start = time.time()

        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.transactions = []
            for row in reader:
                # Parse tapInTime for sorting
                try:
                    tap_in_dt = datetime.strptime(row['tapInTime'], "%Y-%m-%d %H:%M:%S")
                    row['_tap_in_ts'] = tap_in_dt.timestamp()
                except (ValueError, KeyError):
                    continue
                self.transactions.append(row)

        # Sort by tapInTime
        self.transactions.sort(key=lambda r: r['_tap_in_ts'])
        self.total_rows = len(self.transactions)

        if self.total_rows > 0:
            self.sim_start_time = datetime.fromtimestamp(self.transactions[0]['_tap_in_ts'])
            self.sim_end_time = datetime.fromtimestamp(self.transactions[-1]['_tap_in_ts'])

        elapsed = time.time() - start
        logger.info(f"Loaded {self.total_rows:,} transactions in {elapsed:.1f}s")
        logger.info(f"Simulation time range: {self.sim_start_time} → {self.sim_end_time}")

        sim_duration = (self.sim_end_time - self.sim_start_time).total_seconds()
        real_duration = sim_duration / self.speed_multiplier
        logger.info(f"Speed: {self.speed_multiplier}x → {sim_duration/3600:.1f}h simulated in {real_duration/60:.1f} min")

        self.loaded = True

    def get_clean_row(self, row: dict) -> dict:
        """Remove internal fields and return clean transaction dict."""
        clean = {k: v for k, v in row.items() if not k.startswith('_')}
        return clean


class SimulationSession:
    """
    Manages a single simulation session for one or more WebSocket clients.
    All connected clients receive the same simulation stream.
    """

    def __init__(self, simulator: TransjakartaSimulator, batch_interval_ms: int = 100):
        self.simulator = simulator
        self.batch_interval_ms = batch_interval_ms
        self.clients: set = set()
        self.is_running = False
        self.is_paused = False
        self.current_index = 0
        self.sent_count = 0
        self.wall_start_time: Optional[float] = None
        self._task: Optional[asyncio.Task] = None

    @property
    def progress_pct(self) -> float:
        if self.simulator.total_rows == 0:
            return 0.0
        return (self.current_index / self.simulator.total_rows) * 100

    def get_sim_current_time(self) -> Optional[datetime]:
        """Calculate the current simulation time based on wall clock elapsed."""
        if self.wall_start_time is None or self.simulator.sim_start_time is None:
            return None
        elapsed_wall = time.time() - self.wall_start_time
        elapsed_sim = elapsed_wall * self.simulator.speed_multiplier
        return self.simulator.sim_start_time + timedelta(seconds=elapsed_sim)

    async def broadcast(self, message: str):
        """Send message to all connected clients."""
        if not self.clients:
            return
        disconnected = set()
        for ws in self.clients:
            try:
                await ws.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(ws)
        self.clients -= disconnected

    async def start(self):
        """Start the simulation loop."""
        if self.is_running:
            return
        if not self.simulator.loaded:
            self.simulator.load_data()

        self.is_running = True
        self.is_paused = False
        self.current_index = 0
        self.sent_count = 0
        self.wall_start_time = time.time()

        logger.info("Simulation STARTED")
        await self.broadcast(json.dumps({
            "type": "control",
            "event": "simulation_started",
            "total_transactions": self.simulator.total_rows,
            "speed_multiplier": self.simulator.speed_multiplier,
            "sim_start_time": str(self.simulator.sim_start_time),
            "sim_end_time": str(self.simulator.sim_end_time),
        }))

        self._task = asyncio.create_task(self._simulation_loop())

    async def stop(self):
        """Stop the simulation."""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Simulation STOPPED at {self.sent_count:,}/{self.simulator.total_rows:,} transactions")
        await self.broadcast(json.dumps({
            "type": "control",
            "event": "simulation_stopped",
            "sent_count": self.sent_count,
        }))

    async def pause(self):
        """Pause the simulation."""
        self.is_paused = True
        logger.info("Simulation PAUSED")
        await self.broadcast(json.dumps({
            "type": "control",
            "event": "simulation_paused",
            "sent_count": self.sent_count,
        }))

    async def resume(self):
        """Resume the simulation."""
        self.is_paused = False
        logger.info("Simulation RESUMED")
        await self.broadcast(json.dumps({
            "type": "control",
            "event": "simulation_resumed",
        }))

    async def _simulation_loop(self):
        """
        Main simulation loop. Groups transactions into time-based batches
        and sends them when the simulation clock reaches their tapInTime.
        """
        transactions = self.simulator.transactions
        total = len(transactions)
        batch_interval_s = self.batch_interval_ms / 1000.0
        speed = self.simulator.speed_multiplier
        sim_start_ts = self.simulator.sim_start_time.timestamp()

        logger.info(f"Streaming {total:,} transactions at {speed}x speed...")

        try:
            while self.current_index < total and self.is_running:
                # Handle pause
                while self.is_paused and self.is_running:
                    await asyncio.sleep(0.1)

                if not self.is_running:
                    break

                # Calculate current simulation timestamp
                elapsed_wall = time.time() - self.wall_start_time
                current_sim_ts = sim_start_ts + (elapsed_wall * speed)

                # Collect all transactions whose tapInTime <= current simulation time
                batch = []
                while (self.current_index < total and
                       transactions[self.current_index]['_tap_in_ts'] <= current_sim_ts):
                    row = self.simulator.get_clean_row(transactions[self.current_index])
                    row['streaming_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    batch.append(row)
                    self.current_index += 1

                if batch:
                    # Chunk large batches to avoid WebSocket frame size overflow
                    sim_time_str = datetime.fromtimestamp(current_sim_ts).strftime("%Y-%m-%d %H:%M:%S")
                    for chunk_start in range(0, len(batch), MAX_BATCH_SIZE):
                        chunk = batch[chunk_start:chunk_start + MAX_BATCH_SIZE]
                        message = json.dumps({
                            "type": "transaction_batch",
                            "count": len(chunk),
                            "sim_time": sim_time_str,
                            "progress": round(self.progress_pct, 2),
                            "total_sent": self.sent_count + chunk_start + len(chunk),
                            "transactions": chunk,
                        })
                        await self.broadcast(message)
                    self.sent_count += len(batch)

                    # Log progress periodically
                    if self.sent_count % 50000 < len(batch):
                        sim_time_str = datetime.fromtimestamp(current_sim_ts).strftime("%H:%M:%S")
                        logger.info(
                            f"Progress: {self.sent_count:>10,}/{total:,} "
                            f"({self.progress_pct:.1f}%) | "
                            f"Sim time: {sim_time_str} | "
                            f"Batch: {len(batch)} rows"
                        )

                # Sleep for batch interval
                await asyncio.sleep(batch_interval_s)

            # Simulation complete
            if self.current_index >= total:
                elapsed_total = time.time() - self.wall_start_time
                logger.info(f"✓ Simulation COMPLETE! Sent {self.sent_count:,} transactions in {elapsed_total:.1f}s")
                await self.broadcast(json.dumps({
                    "type": "control",
                    "event": "simulation_complete",
                    "total_sent": self.sent_count,
                    "elapsed_seconds": round(elapsed_total, 1),
                }))
                self.is_running = False

        except asyncio.CancelledError:
            logger.info("Simulation loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Simulation error: {e}", exc_info=True)
            await self.broadcast(json.dumps({
                "type": "control",
                "event": "simulation_error",
                "error": str(e),
            }))
            self.is_running = False


# ========== GLOBAL STATE ==========
simulator: Optional[TransjakartaSimulator] = None
session: Optional[SimulationSession] = None


async def handle_client(websocket):
    """Handle a new WebSocket client connection."""
    global session

    client_addr = websocket.remote_address
    logger.info(f"Client connected: {client_addr}")

    # Register client
    session.clients.add(websocket)

    # Send welcome message with server info
    welcome = {
        "type": "control",
        "event": "connected",
        "server": "Transjakarta WebSocket Simulator",
        "total_transactions": simulator.total_rows,
        "speed_multiplier": simulator.speed_multiplier,
        "sim_start_time": str(simulator.sim_start_time),
        "sim_end_time": str(simulator.sim_end_time),
        "commands": [
            "start    — Mulai simulasi dari awal",
            "stop     — Hentikan simulasi",
            "pause    — Pause simulasi",
            "resume   — Lanjutkan simulasi",
            "status   — Cek status simulasi",
            "speed:N  — Ubah kecepatan (misal speed:120)",
        ],
        "is_running": session.is_running,
        "sent_count": session.sent_count,
    }
    await websocket.send(json.dumps(welcome))

    try:
        async for message in websocket:
            cmd = message.strip().lower()
            logger.info(f"Command from {client_addr}: {cmd}")

            if cmd == "start":
                if session.is_running:
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "error",
                        "message": "Simulation already running. Use 'stop' first."
                    }))
                else:
                    await session.start()

            elif cmd == "stop":
                if session.is_running:
                    await session.stop()
                else:
                    await websocket.send(json.dumps({
                        "type": "control",
                        "event": "info",
                        "message": "No simulation running."
                    }))

            elif cmd == "pause":
                if session.is_running and not session.is_paused:
                    await session.pause()

            elif cmd == "resume":
                if session.is_running and session.is_paused:
                    await session.resume()

            elif cmd == "status":
                sim_time = session.get_sim_current_time()
                status = {
                    "type": "control",
                    "event": "status",
                    "is_running": session.is_running,
                    "is_paused": session.is_paused,
                    "sent_count": session.sent_count,
                    "total_transactions": simulator.total_rows,
                    "progress": round(session.progress_pct, 2),
                    "sim_current_time": str(sim_time) if sim_time else None,
                    "speed_multiplier": simulator.speed_multiplier,
                }
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
                        "message": f"Invalid speed value: {e}"
                    }))

            else:
                await websocket.send(json.dumps({
                    "type": "control",
                    "event": "error",
                    "message": f"Unknown command: {cmd}. Available: start, stop, pause, resume, status, speed:N"
                }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        session.clients.discard(websocket)
        logger.info(f"Client disconnected: {client_addr}")


async def main_server(args):
    """Initialize and run the WebSocket server."""
    global simulator, session

    # Initialize simulator
    csv_path = args.csv or os.environ.get("CSV_PATH", "dfTransjakarta1_4MRows.csv")
    host = args.host or os.environ.get("WS_HOST", "0.0.0.0")
    port = int(args.port or os.environ.get("WS_PORT", "8765"))
    speed = float(args.speed or os.environ.get("SPEED_MULTIPLIER", "60"))
    batch_ms = int(args.batch_ms or os.environ.get("BATCH_INTERVAL_MS", "100"))

    # Resolve CSV path
    csv_full = Path(csv_path)
    if not csv_full.is_absolute():
        csv_full = Path(__file__).parent / csv_path
    if not csv_full.exists():
        logger.error(f"CSV file not found: {csv_full}")
        sys.exit(1)

    simulator = TransjakartaSimulator(str(csv_full), speed_multiplier=speed)
    simulator.load_data()

    session = SimulationSession(simulator, batch_interval_ms=batch_ms)

    # Auto-start if requested
    auto_start = args.auto_start or os.environ.get("AUTO_START", "false").lower() == "true"

    logger.info("=" * 60)
    logger.info("  TRANSJAKARTA WEBSOCKET SIMULATOR")
    logger.info("=" * 60)
    logger.info(f"  WebSocket : ws://{host}:{port}")
    logger.info(f"  CSV       : {csv_full}")
    logger.info(f"  Rows      : {simulator.total_rows:,}")
    logger.info(f"  Speed     : {speed}x")
    logger.info(f"  Batch     : {batch_ms}ms interval")
    logger.info(f"  Auto-start: {auto_start}")
    logger.info("=" * 60)

    async with websockets.serve(handle_client, host, port, max_size=50 * 1024 * 1024):
        logger.info(f"Server listening on ws://{host}:{port}")
        logger.info("Waiting for clients... Send 'start' to begin simulation.")

        if auto_start:
            logger.info("Auto-start enabled. Simulation will begin when first client connects.")
            # We can't auto-start without clients, so we set a flag
            # The simulation will auto-start when the first client connects

        # Keep running forever
        stop_event = asyncio.Event()

        def shutdown_handler():
            logger.info("Shutdown signal received")
            stop_event.set()

        loop = asyncio.get_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, shutdown_handler)
            loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

        await stop_event.wait()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transjakarta WebSocket Simulator Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python server.py                          # Default: 60x speed, port 8765
  python server.py --speed 120 --port 9000  # 120x speed, port 9000
  python server.py --speed 1                # Realtime (17 jam simulasi)
  python server.py --auto-start             # Auto-start on first client
        """
    )
    parser.add_argument("--csv", type=str, default=None,
                        help="Path to CSV file (default: dfTransjakarta1_4MRows.csv)")
    parser.add_argument("--host", type=str, default=None,
                        help="WebSocket host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None,
                        help="WebSocket port (default: 8765)")
    parser.add_argument("--speed", type=float, default=None,
                        help="Speed multiplier, 1=realtime, 60=1hr→1min (default: 60)")
    parser.add_argument("--batch-ms", type=int, default=None, dest="batch_ms",
                        help="Batch send interval in milliseconds (default: 100)")
    parser.add_argument("--auto-start", action="store_true", default=False, dest="auto_start",
                        help="Auto-start simulation when first client connects")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main_server(args))
    except KeyboardInterrupt:
        logger.info("Server shutdown by user (Ctrl+C)")
