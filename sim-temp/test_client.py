"""
Test client untuk WebSocket Transjakarta Simulator.
Konek ke server, kirim 'start', dan cetak transaksi yang diterima.

Usage:
    python test_client.py
    python test_client.py --url ws://localhost:8765
"""

import asyncio
import json
import sys
import argparse

try:
    import websockets
except ImportError:
    print("ERROR: Install websockets: pip install websockets")
    sys.exit(1)


async def run_client(url: str, max_messages: int = 100):
    """Connect to the simulator server and display transactions."""
    print(f"Connecting to {url}...")

    async with websockets.connect(url, max_size=50 * 1024 * 1024) as ws:
        # Receive welcome message
        welcome = json.loads(await ws.recv())
        print(f"\n{'='*60}")
        print(f"  Connected to: {welcome.get('server', 'Unknown')}")
        print(f"  Total transactions: {welcome.get('total_transactions', '?'):,}")
        print(f"  Speed: {welcome.get('speed_multiplier', '?')}x")
        print(f"  Sim range: {welcome.get('sim_start_time')} → {welcome.get('sim_end_time')}")
        print(f"{'='*60}")
        print(f"\nAvailable commands:")
        for cmd in welcome.get('commands', []):
            print(f"  {cmd}")

        # Start simulation
        print(f"\nSending 'start' command...")
        await ws.send("start")

        msg_count = 0
        total_txn = 0

        while msg_count < max_messages:
            try:
                data = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            except asyncio.TimeoutError:
                print("No data received for 30s, checking status...")
                await ws.send("status")
                continue

            msg_type = data.get("type")

            if msg_type == "control":
                event = data.get("event")
                print(f"\n[CONTROL] {event}: {json.dumps(data, indent=2)}")

                if event == "simulation_complete":
                    print(f"\n✓ Simulation complete! Total sent: {data.get('total_sent', 0):,}")
                    break

            elif msg_type == "transaction_batch":
                count = data.get("count", 0)
                total_txn += count
                sim_time = data.get("sim_time", "?")
                progress = data.get("progress", 0)

                # Print first transaction of each batch as sample
                txns = data.get("transactions", [])
                if txns:
                    t = txns[0]
                    print(
                        f"[{sim_time}] Batch: {count:>5} txns | "
                        f"Total: {total_txn:>8,} | "
                        f"Progress: {progress:>5.1f}% | "
                        f"Sample: {t.get('transID', '?')} → "
                        f"{t.get('tapInStopsName', '?')} → {t.get('tapOutStopsName', '?')}"
                    )

            msg_count += 1

        print(f"\nClient received {total_txn:,} transactions in {msg_count} messages")

        # Send stop command before disconnecting
        await ws.send("stop")
        print("Sent 'stop' command. Disconnecting.")


def main():
    parser = argparse.ArgumentParser(description="Test client for Transjakarta Simulator")
    parser.add_argument("--url", default="ws://localhost:8765", help="WebSocket URL")
    parser.add_argument("--max", type=int, default=200, dest="max_messages",
                        help="Max messages to receive before stopping (default: 200)")
    args = parser.parse_args()

    asyncio.run(run_client(args.url, args.max_messages))


if __name__ == "__main__":
    main()
