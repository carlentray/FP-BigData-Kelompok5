"""
Script untuk memperbesar dataset Transjakarta dari ~160K menjadi ~1.4 juta transaksi.

Strategi:
- Proporsi halte (tapIn/tapOut) dan koridor dipertahankan sesuai data asli
- Distribusi jam dibuat random dengan pola realistis (jam sibuk pagi & sore)
- TransID dan PayCardID dibuat unik untuk setiap baris baru
- Tanggal tetap 2023-04-01 (1 hari)

Output: dfTransjakarta1_4MRows.csv
"""

import pandas as pd
import numpy as np
import string
import time
from datetime import datetime, timedelta

# ========== CONFIG ==========
INPUT_FILE = "dfTransjakarta180kRows.csv"
OUTPUT_FILE = "dfTransjakarta1_4MRows.csv"
TARGET_ROWS = 1_400_000  # Target ~1.4 juta transaksi
SEED = 42
BASE_DATE = "2023-04-01"

# Distribusi jam realistis Transjakarta (bobot per jam, 05:00-23:00)
# Pola: puncak pagi (06-09), sepi siang, puncak sore (16-19), sepi malam
HOUR_WEIGHTS = {
    5:  0.04,   # early morning
    6:  0.10,   # morning rush starts
    7:  0.12,   # morning peak
    8:  0.10,   # morning rush
    9:  0.07,   # late morning
    10: 0.04,   # mid-day
    11: 0.03,   # mid-day
    12: 0.04,   # lunch
    13: 0.03,   # afternoon
    14: 0.03,   # afternoon
    15: 0.05,   # pre-evening
    16: 0.08,   # evening rush starts
    17: 0.10,   # evening peak
    18: 0.07,   # evening rush
    19: 0.05,   # late evening
    20: 0.03,   # night
    21: 0.02,   # late night
}

def generate_trans_id(n, rng):
    """Generate n unique transaction IDs like 'NDEJ618V8M14IY'."""
    chars = string.ascii_uppercase + string.digits
    ids = set()
    batch_size = n + n // 10  # Generate extra to account for duplicates
    while len(ids) < n:
        batch = [''.join(rng.choice(list(chars), size=14)) for _ in range(batch_size)]
        ids.update(batch)
    return list(ids)[:n]

def generate_card_id(n, rng):
    """Generate n card IDs (16-digit numbers)."""
    # Generate as strings to preserve leading digits
    return [str(rng.integers(1000000000000000, 9999999999999999)) for _ in range(n)]

def generate_random_times(n, hour_weights, rng, base_date="2023-04-01"):
    """
    Generate n random tap-in times distributed across hours according to weights.
    Returns sorted list of datetime strings.
    """
    hours = list(hour_weights.keys())
    weights = np.array(list(hour_weights.values()))
    weights = weights / weights.sum()  # normalize

    # Assign hours based on weights
    assigned_hours = rng.choice(hours, size=n, p=weights)

    # Generate random minute and second within each hour
    minutes = rng.integers(0, 60, size=n)
    seconds = rng.integers(0, 60, size=n)

    # Build datetime strings
    base = datetime.strptime(base_date, "%Y-%m-%d")
    times = []
    for h, m, s in zip(assigned_hours, minutes, seconds):
        dt = base.replace(hour=int(h), minute=int(m), second=int(s))
        times.append(dt)

    return times

def main():
    start_time = time.time()

    print(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    original_count = len(df)
    print(f"  Jumlah baris asli: {original_count:,}")

    rng = np.random.default_rng(SEED)

    # Jumlah baris baru yang perlu dibuat
    new_rows_needed = TARGET_ROWS - original_count
    print(f"  Target: {TARGET_ROWS:,} baris")
    print(f"  Perlu dibuat: {new_rows_needed:,} baris baru")

    # ========== STEP 1: Sample routes proportionally ==========
    print("\n[1/5] Sampling rute berdasarkan proporsi asli...")

    # Kolom yang mendefinisikan "rute" (halte, koridor, dll) - BUKAN waktu
    route_cols = [
        'payCardBank', 'payCardSex', 'payCardBirthDate',
        'corridorID', 'corridorName', 'direction',
        'tapInStops', 'tapInStopsName', 'tapInStopsLat', 'tapInStopsLon', 'stopStartSeq',
        'tapOutStops', 'tapOutStopsName', 'tapOutStopsLat', 'tapOutStopsLon', 'stopEndSeq',
        'payAmount'
    ]

    # Sample rows from original data (with replacement) to maintain stop proportion
    sampled_indices = rng.choice(len(df), size=new_rows_needed, replace=True)
    new_df = df.iloc[sampled_indices].copy().reset_index(drop=True)

    # ========== STEP 2: Generate unique IDs ==========
    print("[2/5] Membuat TransID unik...")
    new_df['transID'] = generate_trans_id(new_rows_needed, rng)

    print("[3/5] Membuat PayCardID unik...")
    # Mix new card IDs with some from original data for realism
    # ~30% reuse existing cards, ~70% new cards
    n_reuse = int(new_rows_needed * 0.3)
    n_new = new_rows_needed - n_reuse

    existing_cards = df['payCardID'].dropna().unique()
    reused_cards = rng.choice(existing_cards, size=n_reuse, replace=True)
    new_cards = generate_card_id(n_new, rng)

    all_cards = np.concatenate([reused_cards.astype(str), new_cards])
    rng.shuffle(all_cards)
    new_df['payCardID'] = all_cards[:new_rows_needed]

    # Generate new names for new cards using existing name pool
    existing_names = df['payCardName'].dropna().unique()
    new_df['payCardName'] = rng.choice(existing_names, size=new_rows_needed, replace=True)

    # ========== STEP 3: Randomize times ==========
    print("[4/5] Mengacak distribusi jam (dengan pola realistis)...")

    tap_in_times = generate_random_times(new_rows_needed, HOUR_WEIGHTS, rng, BASE_DATE)

    # Calculate tap-out time offset from original data
    orig_tap_in = pd.to_datetime(df['tapInTime'])
    orig_tap_out = pd.to_datetime(df['tapOutTime'])
    orig_durations = (orig_tap_out - orig_tap_in).dt.total_seconds()

    # Use durations from the sampled rows
    sampled_durations = orig_durations.iloc[sampled_indices].values

    # Handle NaN durations (replace with random 10-60 min)
    nan_mask = np.isnan(sampled_durations)
    sampled_durations[nan_mask] = rng.integers(600, 3600, size=nan_mask.sum())

    # Build tap-out times
    tap_out_times = []
    for tap_in, duration in zip(tap_in_times, sampled_durations):
        tap_out = tap_in + timedelta(seconds=max(0, duration))
        # Cap at 23:59:59
        if tap_out.hour >= 23 and tap_out.minute >= 59:
            tap_out = tap_out.replace(hour=23, minute=59, second=59)
        tap_out_times.append(tap_out)

    new_df['tapInTime'] = [t.strftime("%Y-%m-%d %H:%M:%S") for t in tap_in_times]
    new_df['tapOutTime'] = [t.strftime("%Y-%m-%d %H:%M:%S") for t in tap_out_times]

    # ========== STEP 4: Combine & Sort ==========
    print("[5/5] Menggabungkan dan menulis output...")

    # Also randomize times for original data
    orig_tap_in_new = generate_random_times(original_count, HOUR_WEIGHTS, rng, BASE_DATE)
    orig_durations_clean = orig_durations.values.copy()
    nan_mask_orig = np.isnan(orig_durations_clean)
    orig_durations_clean[nan_mask_orig] = rng.integers(600, 3600, size=nan_mask_orig.sum())

    orig_tap_out_new = []
    for tap_in, duration in zip(orig_tap_in_new, orig_durations_clean):
        tap_out = tap_in + timedelta(seconds=max(0, duration))
        if tap_out.hour >= 23 and tap_out.minute >= 59:
            tap_out = tap_out.replace(hour=23, minute=59, second=59)
        orig_tap_out_new.append(tap_out)

    df['tapInTime'] = [t.strftime("%Y-%m-%d %H:%M:%S") for t in orig_tap_in_new]
    df['tapOutTime'] = [t.strftime("%Y-%m-%d %H:%M:%S") for t in orig_tap_out_new]

    # Combine original (with new times) + new data
    combined = pd.concat([df, new_df], ignore_index=True)

    # Sort by tapInTime
    combined = combined.sort_values('tapInTime').reset_index(drop=True)

    # Ensure column order matches original
    combined = combined[df.columns]

    print(f"\n  Total baris output: {len(combined):,}")

    # Verify stop proportions are maintained
    orig_top_stops = df['tapInStops'].value_counts(normalize=True).head(10)
    new_top_stops = combined['tapInStops'].value_counts(normalize=True).head(10)
    print("\n  Verifikasi proporsi halte (top 10):")
    print(f"  {'Halte':<12} {'Asli':>8} {'Baru':>8}")
    for stop in orig_top_stops.index:
        orig_pct = orig_top_stops.get(stop, 0) * 100
        new_pct = new_top_stops.get(stop, 0) * 100
        print(f"  {stop:<12} {orig_pct:>7.2f}% {new_pct:>7.2f}%")

    # Show new hour distribution
    print("\n  Distribusi jam baru:")
    hour_dist = pd.to_datetime(combined['tapInTime']).dt.hour.value_counts().sort_index()
    for h, count in hour_dist.items():
        pct = count / len(combined) * 100
        bar = '█' * int(pct * 2)
        print(f"  {h:02d}:00  {count:>8,}  ({pct:>5.1f}%)  {bar}")

    # Save to CSV
    combined.to_csv(OUTPUT_FILE, index=False)

    elapsed = time.time() - start_time
    print(f"\n✓ Selesai! File disimpan: {OUTPUT_FILE}")
    print(f"  Waktu: {elapsed:.1f} detik")
    print(f"  Ukuran: {len(combined):,} baris")

if __name__ == "__main__":
    main()
