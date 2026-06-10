# ShelterEye — Transjakarta Real-Time Streaming Analytics & Lakehouse Pipeline

Sistem pipeline data berskala besar (*Big Data Ecosystem*) untuk menyimulasikan, mengolah, dan memvisualisasikan data transaksi penumpang Transjakarta secara *real-time* menggunakan arsitektur modern Data Lakehouse.

---

## 📌 Informasi Dasar Sistem & Setup
Berikut adalah parameter utama sistem yang wajib diketahui oleh seluruh anggota kelompok sebelum memulai pengembangan:

1. **Database:** Menggunakan **PostgreSQL Database** sebagai *serving layer* terstruktur.
2. **Broker Data (Streaming):** Menggunakan **Apache Kafka** untuk mengalirkan data secara *real-time*.
3. **Nama Topik Kafka:** Seluruh data dialirkan melalui satu pintu bernama `topic-transjakarta`.
4. **Alamat Koneksi & Port:**
   * **Kafka:** `localhost:9092`
   * **PostgreSQL:** `localhost:5432`
5. **Skema Pengujian:** Cukup **SATU ORANG SAJA** yang menjalankan script `feeder.py` saat uji coba bersama agar antrean data di Kafka tidak duplikat atau bentrok.
6. **Lokasi File Dataset:** File `dfTransjakarta180kRows.csv` wajib diletakkan di dalam folder `ingestion/dataset/` dengan penulisan huruf kapital yang presisi.

---

## 👥 Pembagian Tugas & Target Pengembangan

| Peran / Anggota | Komponen Kerja | Target Utama |
| :--- | :--- | :--- |
| **Anggota 1** | `ingestion/feeder.py` | Bertindak sebagai penyuplai data, mengawal jalannya broker Kafka, serta membantu administrasi infrastruktur / laporan. |
| **Anggota 2** | `processing/passenger_logic.py` | Membuat kodingan PySpark Streaming untuk menghitung agregasi total volume/kepadatan penumpang per halte secara *real-time*. |
| **Anggota 3** | `processing/spatial_eta_logic.py` | Membuat kodingan PySpark Streaming untuk analisis spasial dan estimasi waktu kedatangan armada (*ETA*) berdasarkan pergerakan koordinat halte. |
| **Anggota 4** | `storage_dashboard/` (Backend) | Menyiapkan skema tabel di PostgreSQL dan mengonfigurasi jalur penerimaan data bersih (*sink*) yang dikirim oleh Apache Spark. |
| **Anggota 5** | `storage_dashboard/` (Frontend) | Membangun visualisasi grafik tren kepadatan penumpang dan plot peta spasial halte menggunakan framework Streamlit. |

---

## 📂 Struktur Repositori (Clean Directory Structure)
Pastikan folder proyek bersih tanpa ada penomoran di depannya:

```text
FP-BigData-Kelompok5/
├── ingestion/
│   ├── dataset/
│   │   └── dfTransjakarta180kRows.csv  <-- Masukkan manual di sini (di-gitignore)
│   └── feeder.py                       <-- Script penyuplai data ke Kafka
├── processing/
│   ├── passenger_logic.py              <-- Tempat ngoding PySpark Kepadatan Penumpang
│   └── spatial_eta_logic.py            <-- Tempat ngoding PySpark Estimasi Waktu (ETA)
├── storage_dashboard/                  <-- Tempat script visualisasi UI (Streamlit, dll)
└── docker-compose.yml                  <-- Konfigurasi infrastruktur utama
```

## 🚀 Panduan Memulai (Quick Start)

### 1. Sinkronisasi Awal & Unduh Dataset
1. Jalankan perintah `git pull origin main` di terminal VS Code masing-masing.
2. Unduh file dataset asli **`dfTransjakarta180kRows.csv`** (180k rows, ~44 MB) melalui tautan Google Drive yang dibagikan di grup WhatsApp.
3. Pindahkan file tersebut ke dalam folder `ingestion/dataset/`. (*Catatan: File ini otomatis diabaikan oleh Git karena sudah didaftarkan di `.gitignore` agar repositori tetap ringan*).

### 2. Menyalakan Infrastruktur
Pastikan aplikasi **Docker Desktop** sudah menyala di laptop Anda, kemudian buka terminal utama di *root directory* proyek (`FP-BigData-Kelompok5`) dan ketik:
```bash
docker-compose up -d
```
Tunggu hingga status seluruh container (Zookeeper, Kafka, Spark, Postgres) berubah menjadi Started atau Running.

### 3. Simulasi Ingestion (Aliran Data)
Untuk mulai mengalirkan data mentah Transjakarta ke dalam broker Kafka, jalankan script feeder:
```bash
python ingestion/feeder.py
```
Tekan Ctrl + C pada terminal untuk menghentikan simulasi aliran data.

### 4. Mematikan Sistem
Jika sesi pengujian atau coding sudah selesai, matikan seluruh infrastruktur agar laptop tidak berat dengan perintah:
```bash
docker-compose down
```
