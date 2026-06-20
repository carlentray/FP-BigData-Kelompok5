"""
Vehicle Spatial Simulator — GTFS Ground Truth Engine
======================================================
Mensimulasikan pergerakan armada bus Transjakarta secara real-time
menggunakan data GTFS resmi sebagai sumber kebenaran.

Data Sources (dari folder file_gtfs/):
    routes.txt       → Definisi koridor (nama, tipe, warna)
    trips.txt        → Variasi trip per rute (shape_id, direction)
    shapes.txt       → GPS waypoints jalur fisik bus (ratusan titik per trip)
    stop_times.txt   → Waktu tiba/berangkat per halte per trip
    stops.txt        → Koordinat halte
    frequencies.txt  → Headway keberangkatan bus
    calendar.txt     → Service days (SH/HK/HL/dll)

Architecture:
    Section 1 — Spatial Math       : Haversine (pre-computation only)
    Section 2 — Data Structures    : Immutable pre-computed route data
    Section 3 — GTFSManager        : Load + pre-compute semua data GTFS
    Section 4 — VehicleSimulator   : Async simulation engine

Pre-computation Strategy:
    Semua kalkulasi berat (Haversine distance) dilakukan SEKALI saat
    GTFSManager.load(). Hasilnya disimpan sebagai array jarak kumulatif.
    Saat simulasi berjalan, tick update hanya melakukan:
    1. Lookup indeks waypoint (binary search pada cumulative distance)
    2. Linear interpolation koordinat (operasi aritmatika sederhana)
    Tidak ada trigonometri saat runtime → sangat efisien untuk ratusan bus.
"""

import math
import time
import random
import bisect
import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Callable

logger = logging.getLogger("vehicle-sim")


# ============================================================================
# SECTION 1: SPATIAL MATH — Haversine (hanya dipakai saat pre-computation)
# ============================================================================

EARTH_RADIUS_KM = 6371.0


def haversine_distance(lat1: float, lon1: float,
                       lat2: float, lon2: float) -> float:
    """
    Menghitung jarak great-circle antara dua titik di permukaan Bumi
    menggunakan formula Haversine.

    Formula:
        a = sin²(Δφ/2) + cos(φ₁) · cos(φ₂) · sin²(Δλ/2)
        c = 2 · atan2(√a, √(1−a))
        d = R · c

    CATATAN: Fungsi ini HANYA dipanggil saat pre-computation (load time).
    Tidak pernah dipanggil di simulation loop untuk efisiensi.

    Args:
        lat1, lon1: Koordinat titik awal (derajat)
        lat2, lon2: Koordinat titik tujuan (derajat)

    Returns:
        Jarak dalam kilometer
    """
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_KM * c


def lerp(start: float, end: float, t: float) -> float:
    """Linear Interpolation: result = start + (end - start) × t"""
    return start + (end - start) * t


# ============================================================================
# SECTION 2: DATA STRUCTURES — Pre-computed, Immutable Route Data
# ============================================================================

@dataclass(frozen=True)
class ShapePoint:
    """Satu titik GPS pada shape polyline."""
    lat: float
    lon: float
    cumulative_dist_km: float  # Jarak kumulatif dari titik awal shape


@dataclass(frozen=True)
class TripStop:
    """
    Satu halte pada trip, sudah di-project ke shape polyline.

    cumulative_dist_km = posisi halte di sepanjang shape (bukan lat/lon).
    Ini memungkinkan pengecekan "bus sudah sampai?" cukup dengan
    membandingkan distance_traveled >= cumulative_dist_km.
    """
    stop_id: str
    stop_name: str
    lat: float
    lon: float
    arrival_secs: int          # Detik sejak midnight (dari stop_times.txt)
    departure_secs: int        # Detik sejak midnight
    dwell_secs: int            # departure - arrival (waktu tunggu di halte)
    cumulative_dist_km: float  # Posisi halte di shape polyline
    sequence: int


@dataclass(frozen=True)
class SegmentSpeed:
    """
    Kecepatan bus pada segmen antara dua halte berurutan.
    Dihitung dari: v = Δs / Δt (jarak shape ÷ waktu stop_times).
    """
    from_stop_seq: int
    to_stop_seq: int
    distance_km: float
    duration_secs: int
    speed_kmh: float  # km/h, sudah di-clamp ke range realistis


@dataclass
class TripData:
    """
    Data lengkap satu trip yang sudah di-pre-compute.
    Semua yang dibutuhkan simulator tersedia di sini — tidak perlu
    akses ke file GTFS lagi saat runtime.
    """
    trip_id: str
    route_id: str
    route_name: str
    route_desc: str
    direction_id: int
    service_id: str

    # Shape polyline (GPS waypoints + cumulative distance)
    shape_points: list[ShapePoint]
    total_distance_km: float

    # Halte pada trip (sudah di-project ke shape)
    stops: list[TripStop]

    # Kecepatan per segmen antar halte
    segment_speeds: list[SegmentSpeed]

    # Cumulative distance array (untuk binary search saat runtime)
    # Ini adalah array terpisah dari shape_points untuk efisiensi bisect
    _cum_dists: list[float] = field(default=None, repr=False)

    def __post_init__(self):
        if self._cum_dists is None:
            self._cum_dists = [sp.cumulative_dist_km for sp in self.shape_points]

    def get_position_at_distance(self, dist_km: float) -> tuple[float, float]:
        """
        Dapatkan koordinat GPS pada jarak tertentu di shape polyline.

        Menggunakan binary search (bisect) untuk menemukan dua waypoints
        yang mengapit jarak tersebut, lalu interpolasi linier.

        Complexity: O(log N) dimana N = jumlah shape_points (~200-400).
        Tidak ada trigonometri — hanya aritmatika sederhana.

        Args:
            dist_km: Jarak tempuh dari titik awal shape (km)

        Returns:
            Tuple (lat, lon) posisi terinterpolasi
        """
        if dist_km <= 0:
            return (self.shape_points[0].lat, self.shape_points[0].lon)
        if dist_km >= self.total_distance_km:
            return (self.shape_points[-1].lat, self.shape_points[-1].lon)

        # Binary search: cari indeks waypoint tepat setelah dist_km
        idx = bisect.bisect_right(self._cum_dists, dist_km)
        idx = max(1, min(idx, len(self.shape_points) - 1))

        # Dua waypoint yang mengapit
        p0 = self.shape_points[idx - 1]
        p1 = self.shape_points[idx]

        # Progress antara p0 dan p1
        seg_len = p1.cumulative_dist_km - p0.cumulative_dist_km
        if seg_len < 1e-9:
            return (p0.lat, p0.lon)

        t = (dist_km - p0.cumulative_dist_km) / seg_len
        t = max(0.0, min(1.0, t))

        return (lerp(p0.lat, p1.lat, t), lerp(p0.lon, p1.lon, t))

    def get_speed_at_distance(self, dist_km: float) -> float:
        """
        Dapatkan kecepatan bus berdasarkan posisi di shape.

        Menggunakan segmental speed: kecepatan berbeda di setiap segmen
        antar halte (sesuai revisi user — bukan kecepatan rata-rata global).

        Args:
            dist_km: Jarak tempuh dari titik awal shape (km)

        Returns:
            Kecepatan dalam km/h
        """
        if not self.segment_speeds:
            return 25.0  # Fallback

        # Cari di segmen mana bus berada
        for seg in self.segment_speeds:
            to_stop = self.stops[seg.to_stop_seq] if seg.to_stop_seq < len(self.stops) else None
            if to_stop and dist_km < to_stop.cumulative_dist_km:
                return seg.speed_kmh

        # Sudah melewati semua segmen → pakai kecepatan segmen terakhir
        return self.segment_speeds[-1].speed_kmh

    def get_current_stop_index(self, dist_km: float) -> int:
        """
        Cari halte terakhir yang sudah dilewati berdasarkan jarak tempuh.

        Returns:
            Index halte terakhir yang distance-nya <= dist_km
        """
        # Binary search pada cumulative distance halte
        stop_dists = [s.cumulative_dist_km for s in self.stops]
        idx = bisect.bisect_right(stop_dists, dist_km)
        return max(0, idx - 1)


@dataclass
class SpawnEntry:
    """Satu entri jadwal keberangkatan bus."""
    departure_time: datetime
    trip: TripData
    bus_number: int


@dataclass
class VehicleState:
    """
    State satu armada bus yang sedang aktif.

    Posisi bus ditentukan oleh satu nilai: distance_traveled_km.
    Koordinat GPS dihitung dari distance → lookup shape → lerp.
    """
    bus_id: str
    trip_id: str
    route_id: str
    route_name: str
    direction_id: int

    # Posisi: satu-satunya state yang terus berubah
    distance_traveled_km: float = 0.0

    # Cache posisi GPS (dihitung dari distance_traveled)
    current_lat: float = 0.0
    current_lon: float = 0.0

    # Kecepatan saat ini (dari segmental speed)
    speed_kmh: float = 0.0

    # Status
    is_moving: bool = True
    is_active: bool = True

    # Dwell tracking
    current_stop_idx: int = 0     # Index halte terakhir yang dilewati
    next_stop_idx: int = 1        # Index halte berikutnya
    dwell_remaining_s: float = 0  # Sisa dwell time (detik simulasi)
    is_at_stop: bool = False

    # Reference ke trip data (immutable)
    trip: Optional[TripData] = field(default=None, repr=False)

    def to_telemetry(self, sim_time: datetime) -> dict:
        """Konversi state ke format JSON telemetri untuk WebSocket."""
        current_stop_name = ""
        next_stop_name = ""
        if self.trip:
            if self.current_stop_idx < len(self.trip.stops):
                current_stop_name = self.trip.stops[self.current_stop_idx].stop_name
            if self.next_stop_idx < len(self.trip.stops):
                next_stop_name = self.trip.stops[self.next_stop_idx].stop_name

        progress = 0.0
        if self.trip and self.trip.total_distance_km > 0:
            progress = min(1.0, self.distance_traveled_km / self.trip.total_distance_km)

        return {
            "type": "vehicle_telemetry",
            "bus_id": self.bus_id,
            "trip_id": self.trip_id,
            "route_id": self.route_id,
            "route_name": self.route_name,
            "direction": self.direction_id,
            "lat": round(self.current_lat, 6),
            "lon": round(self.current_lon, 6),
            "speed_kmh": round(self.speed_kmh, 1),
            "is_moving": self.is_moving,
            "current_stop": current_stop_name,
            "next_stop": next_stop_name,
            "progress": round(progress, 3),
            "distance_km": round(self.distance_traveled_km, 2),
            "sim_time": sim_time.strftime("%Y-%m-%d %H:%M:%S"),
            "streaming_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


# ============================================================================
# SECTION 3: GTFS MANAGER — Load & Pre-compute
# ============================================================================

# Batas kecepatan realistis untuk clamp
MIN_SPEED_KMH = 8.0    # Minimum (macet parah / jalan sempit)
MAX_SPEED_KMH = 80.0   # Maximum (jalan tol)
DEFAULT_SPEED_KMH = 25.0  # Fallback jika data tidak cukup
DEFAULT_DWELL_SECS = 30   # Default dwell time jika stop_times tidak punya


class GTFSManager:
    """
    Load dan pre-compute semua data GTFS menjadi struktur data
    yang efisien untuk simulasi.

    Pre-computation yang dilakukan (hanya sekali saat load):
    1. Hitung jarak kumulatif setiap titik di shapes.txt (Haversine)
    2. Project halte ke shape polyline (snap ke cumulative distance)
    3. Hitung kecepatan segmental antar halte (v = Δs/Δt)
    4. Bangun spawn schedule dari frequencies.txt

    Setelah load(), semua data tersedia di self.trips: dict[str, TripData]
    """

    def __init__(self, gtfs_dir: str):
        self.gtfs_dir = gtfs_dir
        self.trips: dict[str, TripData] = {}
        self.service_days: dict[str, dict] = {}

    def load(self) -> dict[str, TripData]:
        """
        Load semua file GTFS dan bangun TripData untuk setiap trip.
        """
        import pandas as pd
        from pathlib import Path

        gtfs = Path(self.gtfs_dir)
        start = time.time()
        logger.info(f"Loading GTFS from {gtfs}...")

        # ---- 1. Load raw data ----
        routes_df = pd.read_csv(gtfs / "routes.txt")
        trips_df = pd.read_csv(gtfs / "trips.txt")
        shapes_df = pd.read_csv(gtfs / "shapes.txt")
        stop_times_df = pd.read_csv(gtfs / "stop_times.txt")
        stops_df = pd.read_csv(gtfs / "stops.txt")
        frequencies_df = pd.read_csv(gtfs / "frequencies.txt")
        calendar_df = pd.read_csv(gtfs / "calendar.txt")

        logger.info(
            f"  Raw: {len(routes_df)} routes, {len(trips_df)} trips, "
            f"{len(shapes_df):,} shape points, {len(stop_times_df):,} stop times, "
            f"{len(stops_df):,} stops, {len(frequencies_df)} freq entries"
        )

        # ---- 2. Build lookup tables ----
        route_map = {}
        for _, r in routes_df.iterrows():
            route_map[r['route_id']] = {
                'name': r['route_long_name'],
                'desc': r.get('route_desc', ''),
            }

        # Calendar → service days
        for _, c in calendar_df.iterrows():
            self.service_days[c['service_id']] = {
                'monday': c['monday'], 'tuesday': c['tuesday'],
                'wednesday': c['wednesday'], 'thursday': c['thursday'],
                'friday': c['friday'], 'saturday': c['saturday'],
                'sunday': c['sunday'],
            }

        # Stops lookup
        stop_map = {}
        for _, s in stops_df.iterrows():
            stop_map[s['stop_id']] = {
                'name': str(s['stop_name']),
                'lat': float(s['stop_lat']),
                'lon': float(s['stop_lon']),
            }

        # ---- 3. Pre-compute shapes (cumulative distances) ----
        logger.info("  Pre-computing shape cumulative distances...")
        shape_data = self._precompute_shapes(shapes_df)

        # ---- 4. Build TripData for each trip ----
        logger.info("  Building trip data...")
        trips_built = 0
        trips_skipped = 0

        # Group stop_times by trip_id
        st_grouped = stop_times_df.groupby('trip_id')

        for _, trip_row in trips_df.iterrows():
            trip_id = trip_row['trip_id']
            route_id = trip_row['route_id']
            shape_id = trip_row['shape_id']
            direction_id = int(trip_row.get('direction_id', 0))
            service_id = trip_row.get('service_id', 'SH')

            # Skip jika shape tidak ada
            if shape_id not in shape_data:
                trips_skipped += 1
                continue

            # Skip jika stop_times tidak ada
            if trip_id not in st_grouped.groups:
                trips_skipped += 1
                continue

            shape_points = shape_data[shape_id]
            total_dist = shape_points[-1].cumulative_dist_km if shape_points else 0

            if total_dist < 0.1:  # Skip rute terlalu pendek
                trips_skipped += 1
                continue

            route_info = route_map.get(route_id, {'name': route_id, 'desc': ''})

            # Build stops for this trip
            trip_st = st_grouped.get_group(trip_id).sort_values('stop_sequence')
            trip_stops = self._build_trip_stops(trip_st, stop_map, shape_points)

            if len(trip_stops) < 2:
                trips_skipped += 1
                continue

            # Compute segmental speeds
            segment_speeds = self._compute_segment_speeds(trip_stops)

            trip_data = TripData(
                trip_id=trip_id,
                route_id=route_id,
                route_name=route_info['name'],
                route_desc=route_info['desc'],
                direction_id=direction_id,
                service_id=service_id,
                shape_points=shape_points,
                total_distance_km=total_dist,
                stops=trip_stops,
                segment_speeds=segment_speeds,
            )

            self.trips[trip_id] = trip_data
            trips_built += 1

        elapsed = time.time() - start
        total_shape_pts = sum(len(t.shape_points) for t in self.trips.values())
        total_stops = sum(len(t.stops) for t in self.trips.values())

        logger.info(
            f"  GTFS loaded in {elapsed:.1f}s | "
            f"{trips_built} trips built ({trips_skipped} skipped) | "
            f"{total_shape_pts:,} shape points | {total_stops:,} stop entries"
        )

        return self.trips

    def _precompute_shapes(self, shapes_df) -> dict[str, list[ShapePoint]]:
        """
        Pre-compute jarak kumulatif untuk setiap shape.

        Untuk setiap shape_id:
        1. Sort by shape_pt_sequence
        2. Hitung Haversine distance antar titik berurutan
        3. Simpan sebagai cumulative_dist_km

        Ini adalah operasi O(N) dimana N = total shape points (~245K).
        Dilakukan SEKALI saat load, tidak pernah di runtime.
        """
        result = {}

        for shape_id, group in shapes_df.groupby('shape_id'):
            pts = group.sort_values('shape_pt_sequence')
            lats = pts['shape_pt_lat'].values
            lons = pts['shape_pt_lon'].values

            shape_points = []
            cum_dist = 0.0

            for i in range(len(lats)):
                if i > 0:
                    d = haversine_distance(lats[i-1], lons[i-1], lats[i], lons[i])
                    cum_dist += d

                shape_points.append(ShapePoint(
                    lat=float(lats[i]),
                    lon=float(lons[i]),
                    cumulative_dist_km=cum_dist,
                ))

            result[shape_id] = shape_points

        return result

    def _build_trip_stops(
        self, trip_st, stop_map: dict,
        shape_points: list[ShapePoint]
    ) -> list[TripStop]:
        """
        Build TripStop list untuk satu trip.

        Kunci: setiap halte di-PROJECT ke shape polyline.
        Artinya, kita cari titik pada shape yang paling dekat dengan
        koordinat halte, lalu catat jarak kumulatifnya.

        Ini menyelesaikan masalah bahwa stops.txt dan shapes.txt
        kadang memiliki selisih koordinat beberapa meter.
        """
        # Pre-build arrays for efficient snapping
        shape_lats = [sp.lat for sp in shape_points]
        shape_lons = [sp.lon for sp in shape_points]
        shape_cum = [sp.cumulative_dist_km for sp in shape_points]

        trip_stops = []
        last_shape_idx = 0  # Index-based forward search (crucial for loops)

        for _, row in trip_st.iterrows():
            sid = row['stop_id']
            seq = int(row['stop_sequence'])

            stop_info = stop_map.get(sid)
            if not stop_info:
                continue

            # Parse arrival/departure times
            arr_secs = self._time_to_seconds(row.get('arrival_time', '05:00:00'))
            dep_secs = self._time_to_seconds(row.get('departure_time', '05:00:10'))
            dwell = max(0, dep_secs - arr_secs)
            if dwell == 0:
                dwell = DEFAULT_DWELL_SECS

            # Project stop ke shape: sequential forward scan by INDEX
            # Krusial untuk loop routes — cari titik shape terdekat
            # mulai dari indeks terakhir yang cocok, BUKAN dari awal
            stop_lat = stop_info['lat']
            stop_lon = stop_info['lon']

            best_dist_sq = float('inf')
            best_idx = last_shape_idx

            for i in range(last_shape_idx, len(shape_lats)):
                dlat = stop_lat - shape_lats[i]
                dlon = stop_lon - shape_lons[i]
                dist_sq = dlat * dlat + dlon * dlon

                if dist_sq < best_dist_sq:
                    best_dist_sq = dist_sq
                    best_idx = i
                elif dist_sq > best_dist_sq * 4 and i > best_idx + 10:
                    # Early exit: sudah menjauh signifikan dari titik terbaik
                    break

            cum_dist = shape_cum[best_idx]
            last_shape_idx = best_idx  # Next stop searches from here

            trip_stops.append(TripStop(
                stop_id=sid,
                stop_name=stop_info['name'],
                lat=stop_lat,
                lon=stop_lon,
                arrival_secs=arr_secs,
                departure_secs=dep_secs,
                dwell_secs=dwell,
                cumulative_dist_km=cum_dist,
                sequence=seq,
            ))

        return trip_stops

    def _snap_to_shape(
        self, stop_lat: float, stop_lon: float,
        shape_lats: list, shape_lons: list, shape_cum: list,
        search_from_dist: float = -1.0
    ) -> float:
        """
        Project satu halte ke shape polyline.

        Cari titik shape terdekat ke koordinat halte, return
        cumulative distance pada titik tersebut.

        Menggunakan squared Euclidean (approx) untuk speed.
        Untuk jarak pendek di Jakarta, ini cukup akurat.

        search_from_dist: HANYA search titik SETELAH jarak ini.
        Strict forward-only search, krusial untuk loop routes
        dimana return-leg stops bisa dekat dengan outbound-leg points.
        """
        best_dist_sq = float('inf')
        best_cum = 0.0
        found = False

        for i in range(len(shape_lats)):
            # Strict forward-only: skip titik sebelum search_from_dist
            if shape_cum[i] < search_from_dist:
                continue

            dlat = stop_lat - shape_lats[i]
            dlon = stop_lon - shape_lons[i]
            dist_sq = dlat * dlat + dlon * dlon

            if dist_sq < best_dist_sq:
                best_dist_sq = dist_sq
                best_cum = shape_cum[i]
                found = True

        # Fallback: jika tidak ditemukan (seharusnya tidak terjadi)
        if not found and shape_cum:
            best_cum = shape_cum[-1]

        return best_cum

    def _compute_segment_speeds(self, stops: list[TripStop]) -> list[SegmentSpeed]:
        """
        Hitung kecepatan per segmen antar halte berurutan.

        v = Δs / Δt
        dimana:
            Δs = cumulative_dist(stop[i+1]) - cumulative_dist(stop[i])
            Δt = arrival(stop[i+1]) - departure(stop[i])

        Kecepatan di-clamp ke [MIN_SPEED_KMH, MAX_SPEED_KMH]
        untuk menghindari nilai tidak realistis.
        """
        segments = []

        for i in range(len(stops) - 1):
            s0 = stops[i]
            s1 = stops[i + 1]

            dist_km = s1.cumulative_dist_km - s0.cumulative_dist_km
            dt_secs = s1.arrival_secs - s0.departure_secs

            if dt_secs <= 0 or dist_km <= 0:
                speed = DEFAULT_SPEED_KMH
            else:
                dt_hours = dt_secs / 3600.0
                speed = dist_km / dt_hours

            # Clamp ke range realistis
            speed = max(MIN_SPEED_KMH, min(MAX_SPEED_KMH, speed))

            segments.append(SegmentSpeed(
                from_stop_seq=i,
                to_stop_seq=i + 1,
                distance_km=dist_km,
                duration_secs=max(1, dt_secs),
                speed_kmh=speed,
            ))

        return segments

    @staticmethod
    def _time_to_seconds(time_str) -> int:
        """Convert HH:MM:SS ke detik sejak midnight."""
        if not time_str or (isinstance(time_str, float) and math.isnan(time_str)):
            return 5 * 3600  # Default 05:00
        parts = str(time_str).split(':')
        if len(parts) != 3:
            return 5 * 3600
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 3600 + m * 60 + s


# ============================================================================
# SECTION 4: VEHICLE SIMULATOR ENGINE
# ============================================================================

# Tick interval (detik real-time antar update posisi)
# 5 detik = cukup smooth untuk telemetri, ringan untuk CPU
DEFAULT_TICK_INTERVAL_S = 5.0


class VehicleSimulator:
    """
    Engine simulasi pergerakan armada bus Transjakarta.

    Concurrency Model:
        - Satu asyncio task untuk main simulation loop
        - Setiap tick (default 5 detik real-time):
          1. Advance distance_traveled setiap bus aktif
          2. Lookup posisi GPS via binary search + lerp (O(log N) per bus)
          3. Cek apakah bus sampai di halte → trigger dwell
          4. Kumpulkan telemetri → kirim via callback
        - Tidak ada Haversine / trigonometri saat runtime
        - asyncio.sleep() di antara tick menyerahkan kontrol ke event loop

    Args:
        gtfs_manager: GTFSManager yang sudah di-load
        speed_multiplier: Pengali kecepatan simulasi (1=realtime, 60=1jam→1menit)
        tick_interval_s: Interval tick real-time (default 5 detik)
        on_telemetry: Async callback untuk mengirim telemetri
        service_filter: Service IDs yang aktif (default: ['SH'] = semua hari)
    """

    def __init__(
        self,
        gtfs_manager: GTFSManager,
        speed_multiplier: float = 60.0,
        tick_interval_s: float = DEFAULT_TICK_INTERVAL_S,
        on_telemetry: Optional[Callable] = None,
        service_filter: Optional[list[str]] = None,
    ):
        self.gtfs_manager = gtfs_manager
        self.speed_multiplier = speed_multiplier
        self.tick_interval_s = tick_interval_s
        self.on_telemetry = on_telemetry
        self.service_filter = service_filter or ['SH']

        # State
        self.vehicles: dict[str, VehicleState] = {}
        self.spawn_schedule: list[SpawnEntry] = []
        self.spawn_index: int = 0

        # Simulation clock
        self.sim_start_time: Optional[datetime] = None
        self.sim_end_time: Optional[datetime] = None
        self.wall_start_time: Optional[float] = None
        self._last_tick_wall: Optional[float] = None

        # Counters
        self.total_spawned: int = 0
        self.total_despawned: int = 0

        # Control
        self.is_running: bool = False
        self.is_paused: bool = False
        self._task: Optional[asyncio.Task] = None

        # RNG
        self._rng = random.Random(42)

    @property
    def active_vehicle_count(self) -> int:
        return sum(1 for v in self.vehicles.values() if v.is_active)

    def get_sim_time(self) -> Optional[datetime]:
        """Hitung waktu simulasi saat ini berdasarkan wall clock."""
        if self.wall_start_time is None or self.sim_start_time is None:
            return None
        elapsed_wall = time.time() - self.wall_start_time
        elapsed_sim = elapsed_wall * self.speed_multiplier
        return self.sim_start_time + timedelta(seconds=elapsed_sim)

    def build_spawn_schedule(self):
        """
        Bangun jadwal keberangkatan bus dari frequencies.txt.

        Untuk setiap trip yang memiliki entry di frequencies:
        - Keberangkatan pertama = start_time
        - Keberangkatan selanjutnya setiap headway_secs
        - Sampai end_time

        Filter berdasarkan service_id (SH=setiap hari, HK=hari kerja, dll).
        """
        import pandas as pd
        from pathlib import Path

        gtfs = Path(self.gtfs_manager.gtfs_dir)
        freq_df = pd.read_csv(gtfs / "frequencies.txt")

        self.spawn_schedule = []
        base_date = datetime(2023, 4, 1)  # Tanggal referensi simulasi

        trips_with_freq = set()

        for _, row in freq_df.iterrows():
            trip_id = row['trip_id']
            if trip_id not in self.gtfs_manager.trips:
                continue

            trip = self.gtfs_manager.trips[trip_id]

            # Filter service
            if trip.service_id not in self.service_filter:
                continue

            start_secs = GTFSManager._time_to_seconds(row['start_time'])
            end_secs = GTFSManager._time_to_seconds(row['end_time'])
            headway = int(row['headway_secs'])

            if headway <= 0:
                continue

            # Handle overnight (end < start → end is next day)
            if end_secs <= start_secs:
                end_secs += 24 * 3600

            bus_num = 1
            t = start_secs
            while t < end_secs:
                dep_time = base_date + timedelta(seconds=t)
                self.spawn_schedule.append(SpawnEntry(
                    departure_time=dep_time,
                    trip=trip,
                    bus_number=bus_num,
                ))
                t += headway
                bus_num += 1

            trips_with_freq.add(trip_id)

        # Sort by departure time
        self.spawn_schedule.sort(key=lambda e: e.departure_time)
        self.spawn_index = 0

        # Determine simulation time range
        if self.spawn_schedule:
            self.sim_start_time = self.spawn_schedule[0].departure_time
            self.sim_end_time = self.spawn_schedule[-1].departure_time + timedelta(hours=2)

        logger.info(
            f"Spawn schedule: {len(self.spawn_schedule):,} departures "
            f"across {len(trips_with_freq)} trips | "
            f"Service filter: {self.service_filter}"
        )

    async def start(self):
        """Mulai simulasi."""
        if self.is_running:
            return

        self.build_spawn_schedule()
        self.vehicles.clear()
        self.total_spawned = 0
        self.total_despawned = 0
        self.wall_start_time = time.time()
        self._last_tick_wall = self.wall_start_time
        self.is_running = True
        self.is_paused = False

        logger.info(
            f"Vehicle simulation STARTED | "
            f"Speed: {self.speed_multiplier}x | "
            f"Tick: {self.tick_interval_s}s | "
            f"Schedule: {len(self.spawn_schedule):,} departures"
        )

        self._task = asyncio.create_task(self._simulation_loop())

    async def stop(self):
        """Hentikan simulasi."""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"Vehicle simulation STOPPED | "
            f"Spawned: {self.total_spawned} | Active: {self.active_vehicle_count}"
        )

    async def pause(self):
        self.is_paused = True
        logger.info("Vehicle simulation PAUSED")

    async def resume(self):
        self.is_paused = False
        self._last_tick_wall = time.time()
        logger.info("Vehicle simulation RESUMED")

    async def _simulation_loop(self):
        """
        Main loop simulasi.

        Setiap tick:
        1. Hitung dt_sim (delta waktu simulasi)
        2. Spawn bus baru (jika waktunya)
        3. Update setiap bus aktif:
           a. Jika di halte → kurangi dwell time
           b. Jika bergerak → advance distance, lookup position
           c. Cek apakah sampai halte → trigger dwell
           d. Cek apakah selesai rute → despawn
        4. Kirim telemetri
        5. Cleanup bus yang sudah despawn
        """
        try:
            while self.is_running:
                while self.is_paused and self.is_running:
                    await asyncio.sleep(0.1)
                if not self.is_running:
                    break

                now_wall = time.time()
                dt_wall = now_wall - self._last_tick_wall
                dt_sim = dt_wall * self.speed_multiplier
                self._last_tick_wall = now_wall

                sim_time = self.get_sim_time()
                if sim_time is None:
                    await asyncio.sleep(self.tick_interval_s)
                    continue

                # 1. Spawn
                self._spawn_due_vehicles(sim_time)

                # 2. Update semua bus
                for vehicle in list(self.vehicles.values()):
                    if vehicle.is_active:
                        self._update_vehicle(vehicle, dt_sim)

                # 3. Telemetri
                telemetry = [
                    v.to_telemetry(sim_time)
                    for v in self.vehicles.values()
                    if v.is_active
                ]

                if telemetry and self.on_telemetry:
                    await self.on_telemetry(telemetry, sim_time)

                # 4. Cleanup
                despawned = [bid for bid, v in self.vehicles.items() if not v.is_active]
                for bid in despawned:
                    del self.vehicles[bid]
                    self.total_despawned += 1

                # Log berkala
                if self.total_spawned > 0 and self.total_spawned % 200 == 0:
                    logger.info(
                        f"Vehicles | Active: {self.active_vehicle_count} | "
                        f"Spawned: {self.total_spawned} | "
                        f"Despawned: {self.total_despawned} | "
                        f"Sim: {sim_time.strftime('%H:%M:%S')}"
                    )

                # Cek selesai
                if (self.spawn_index >= len(self.spawn_schedule) and
                        self.active_vehicle_count == 0):
                    logger.info("✓ Vehicle simulation COMPLETE")
                    self.is_running = False
                    break

                await asyncio.sleep(self.tick_interval_s)

        except asyncio.CancelledError:
            logger.info("Vehicle simulation loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Vehicle simulation error: {e}", exc_info=True)
            self.is_running = False

    def _spawn_due_vehicles(self, sim_time: datetime):
        """Spawn bus yang jadwal keberangkatannya sudah lewat."""
        while (self.spawn_index < len(self.spawn_schedule) and
               self.spawn_schedule[self.spawn_index].departure_time <= sim_time):

            entry = self.spawn_schedule[self.spawn_index]
            self.spawn_index += 1

            trip = entry.trip
            bus_id = f"TJ-{trip.route_id}-{entry.bus_number:03d}"

            if bus_id in self.vehicles:
                continue

            if not trip.shape_points or not trip.stops:
                continue

            # Posisi awal = titik pertama shape
            first_pt = trip.shape_points[0]

            # Kecepatan awal = segmen pertama
            initial_speed = (trip.segment_speeds[0].speed_kmh
                             if trip.segment_speeds else DEFAULT_SPEED_KMH)

            vehicle = VehicleState(
                bus_id=bus_id,
                trip_id=trip.trip_id,
                route_id=trip.route_id,
                route_name=trip.route_name,
                direction_id=trip.direction_id,
                distance_traveled_km=0.0,
                current_lat=first_pt.lat,
                current_lon=first_pt.lon,
                speed_kmh=initial_speed,
                is_moving=True,
                is_active=True,
                current_stop_idx=0,
                next_stop_idx=1,
                dwell_remaining_s=0.0,
                is_at_stop=False,
                trip=trip,
            )

            self.vehicles[bus_id] = vehicle
            self.total_spawned += 1

    def _update_vehicle(self, v: VehicleState, dt_sim_s: float):
        """
        Update satu bus untuk satu tick.

        Menggunakan loop untuk mengkonsumsi dt_sim secara inkremental:
        - Jika bus sedang dwell → kurangi dwell time dari sisa dt
        - Jika bus bergerak → hitung jarak, cek apakah melewati halte
        - Jika melewati halte → snap, mulai dwell, konsumsi sisa dt
        - Loop sampai dt habis atau bus dwell/despawn

        Ini menyelesaikan masalah skip-stop pada speed multiplier tinggi
        (misal 600x: dt_sim = 3000s per tick, bisa lewati banyak halte).
        """
        if not v.trip or not v.is_active:
            return

        trip = v.trip
        remaining_dt = dt_sim_s

        # Loop untuk mengkonsumsi remaining_dt secara inkremental
        # Max iterations sebagai safety net
        for _ in range(len(trip.stops) + 5):
            if remaining_dt <= 0 or not v.is_active:
                break

            # --- Dwell time di halte ---
            if v.is_at_stop:
                v.is_moving = False
                v.speed_kmh = 0.0

                if v.dwell_remaining_s > remaining_dt:
                    # Masih dwell, habiskan sisa dt
                    v.dwell_remaining_s -= remaining_dt
                    remaining_dt = 0
                    break
                else:
                    # Selesai dwell, lanjut bergerak
                    remaining_dt -= v.dwell_remaining_s
                    v.dwell_remaining_s = 0
                    v.is_at_stop = False
                    v.is_moving = True
                    v.current_stop_idx = v.next_stop_idx
                    v.next_stop_idx += 1

                    # Update kecepatan ke segmen berikutnya
                    seg_idx = v.current_stop_idx
                    if seg_idx < len(trip.segment_speeds):
                        v.speed_kmh = trip.segment_speeds[seg_idx].speed_kmh
                    elif trip.segment_speeds:
                        v.speed_kmh = trip.segment_speeds[-1].speed_kmh
                    else:
                        v.speed_kmh = DEFAULT_SPEED_KMH

                    # Cek apakah ini halte terakhir
                    if v.current_stop_idx >= len(trip.stops) - 1:
                        v.is_active = False
                        v.is_moving = False
                        return
                    continue  # Proses sisa remaining_dt

            # --- Pergerakan ---
            speed_kmh = v.speed_kmh
            if speed_kmh <= 0:
                speed_kmh = DEFAULT_SPEED_KMH

            # Cek apakah ada halte berikutnya yang akan dilewati
            if v.next_stop_idx < len(trip.stops):
                next_stop = trip.stops[v.next_stop_idx]
                dist_to_stop = next_stop.cumulative_dist_km - v.distance_traveled_km

                if dist_to_stop <= 0:
                    # Sudah melewati halte (edge case), snap langsung
                    v.distance_traveled_km = next_stop.cumulative_dist_km
                    v.current_lat = next_stop.lat
                    v.current_lon = next_stop.lon
                    v.is_at_stop = True
                    v.is_moving = False
                    v.speed_kmh = 0.0
                    v.dwell_remaining_s = float(next_stop.dwell_secs)
                    continue  # Proses dwell dengan sisa remaining_dt

                # Waktu yang dibutuhkan untuk sampai halte berikutnya
                time_to_stop_s = dist_to_stop / (speed_kmh / 3600.0)

                if time_to_stop_s <= remaining_dt:
                    # Sampai di halte dalam tick ini
                    v.distance_traveled_km = next_stop.cumulative_dist_km
                    v.current_lat = next_stop.lat
                    v.current_lon = next_stop.lon
                    v.is_at_stop = True
                    v.is_moving = False
                    v.speed_kmh = 0.0
                    v.dwell_remaining_s = float(next_stop.dwell_secs)
                    remaining_dt -= time_to_stop_s
                    continue  # Proses dwell dengan sisa remaining_dt
                else:
                    # Tidak sampai halte, bergerak sepanjang remaining_dt
                    distance_delta = speed_kmh * (remaining_dt / 3600.0)
                    v.distance_traveled_km += distance_delta
                    remaining_dt = 0
            else:
                # Tidak ada halte berikutnya, bergerak sampai akhir rute
                distance_delta = speed_kmh * (remaining_dt / 3600.0)
                v.distance_traveled_km += distance_delta
                remaining_dt = 0

            # Cek apakah selesai rute
            if v.distance_traveled_km >= trip.total_distance_km:
                last_pt = trip.shape_points[-1]
                v.current_lat = last_pt.lat
                v.current_lon = last_pt.lon
                v.is_active = False
                v.is_moving = False
                return

            # Lookup posisi baru (binary search + lerp)
            lat, lon = trip.get_position_at_distance(v.distance_traveled_km)
            v.current_lat = lat
            v.current_lon = lon

            # Update kecepatan berdasarkan segmen saat ini
            v.speed_kmh = trip.get_speed_at_distance(v.distance_traveled_km)

    def get_status(self) -> dict:
        """Dapatkan status simulasi saat ini."""
        sim_time = self.get_sim_time()
        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "speed_multiplier": self.speed_multiplier,
            "tick_interval_s": self.tick_interval_s,
            "sim_time": sim_time.strftime("%Y-%m-%d %H:%M:%S") if sim_time else None,
            "active_vehicles": self.active_vehicle_count,
            "total_spawned": self.total_spawned,
            "total_despawned": self.total_despawned,
            "total_trips": len(self.gtfs_manager.trips),
            "schedule_total": len(self.spawn_schedule),
            "schedule_remaining": len(self.spawn_schedule) - self.spawn_index,
            "service_filter": self.service_filter,
        }
