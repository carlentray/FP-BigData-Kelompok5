import asyncio, json, websockets

async def test():
    async with websockets.connect("ws://localhost:8766", max_size=50*1024*1024) as ws:
        welcome = json.loads(await ws.recv())
        srv = welcome.get("server", "?")
        trips = welcome.get("total_trips", "?")
        print(f"Connected: {srv} | Trips: {trips}")

        await ws.send("start")

        for i in range(12):
            data = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
            if data["type"] == "control":
                evt = data.get("event", "")
                active = data.get("active_vehicles", "")
                sched = data.get("schedule_total", "")
                print(f"[CONTROL] {evt} | active={active} schedule={sched}")
            elif data["type"] == "vehicle_telemetry_batch":
                cnt = data["count"]
                total = data["total_active"]
                sim = data["sim_time"]
                v = data["vehicles"][0]
                bid = v["bus_id"]
                lat = v["lat"]
                lon = v["lon"]
                spd = v["speed_kmh"]
                rid = v["route_id"]
                rname = v["route_name"]
                prog = v.get("progress", 0)
                cstop = v.get("current_stop", "")
                nstop = v.get("next_stop", "")
                moving = v.get("is_moving", "?")
                print(f"[{sim}] Active: {total:>4} | {bid} route={rid} ({rname[:25]}) "
                      f"@ ({lat}, {lon}) {spd}km/h moving={moving} "
                      f"progress={prog:.1%} stop={cstop[:20]}")

        # Drain any in-flight telemetry, wait for status control message
        await ws.send("status")
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if msg.get("type") == "control" and msg.get("event") == "status":
                active = msg.get("active_vehicles", "?")
                spawned = msg.get("total_spawned", "?")
                despawned = msg.get("total_despawned", "?")
                remaining = msg.get("schedule_remaining", "?")
                sim_time = msg.get("sim_time", "?")
                speed = msg.get("speed_multiplier", "?")
                tick = msg.get("tick_interval_s", "?")
                total_trips = msg.get("total_trips", "?")
                print(f"\n{'='*60}")
                print(f"  STATUS")
                print(f"  Sim time   : {sim_time}")
                print(f"  Speed      : {speed}x | Tick: {tick}s")
                print(f"  Trips      : {total_trips}")
                print(f"  Active bus : {active}")
                print(f"  Spawned    : {spawned}")
                print(f"  Despawned  : {despawned}")
                print(f"  Remaining  : {remaining} departures left")
                print(f"{'='*60}")
                break

        await ws.send("stop")
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if msg.get("type") == "control":
                print(f"Stopped: {msg.get('event', '')}")
                break
        print("Done!")

asyncio.run(test())
