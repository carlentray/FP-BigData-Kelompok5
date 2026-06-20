# Panduan Demo & Kunci Jawaban Penilaian Big Data

Berikut adalah panduan demonstrasi dan penjelasan komponen kode berdasarkan Rubrik Penilaian Final Project Big Data ITS 2025/2026.

---

## 1. Evaluasi & Keselarasan Rubrik (Self-Assessment)

### CPMK-1: Identifikasi Masalah & Relevansi Big Data (Bobot 15%)
* **Kesesuaian Kode:** Masalah penumpukan halte diselesaikan dengan kerangka 5V:
  * **Volume & Velocity:** Ditangani dengan streaming data puluhan ribu transaksi per detik.
  * **Variety:** Data koordinat spasial bus dan data tap-in transaksi terstruktur.
  * **Veracity:** Penanganan anomali data late-arrival dan data rusak via watermark.
  * **Value:** Dashboard penyeimbang armada bus secara real-time.

### CPMK-2: Desain Infrastruktur Big Data (Bobot 20%)
* **Kesesuaian Kode:** Infrastruktur lengkap yang terdiri dari:
  * **Ingestion:** WebSocket API + Kafka Broker ([feeder.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/ingestion/feeder.py))
  * **Storage:** PostgreSQL DB sebagai datastore serving.
  * **Processing:** Apache Spark Engine ([passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py))
  * **Serving:** Streamlit UI ([app.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/storage_dashboard/app.py))

### CPMK-3: Implementasi Data Lakehouse (Bobot 20%)
* **Kesesuaian Kode:** Penerapan Medallion Architecture secara eksplisit:
  * **Bronze Layer:** Data mentah disimpan dalam format **Parquet** di direktori local disk `storage/lakehouse/bronze/` yang dipartisi berdasarkan tanggal (`date=YYYY-MM-DD`).
  * **Silver Layer:** Skema parsing data terstruktur dan windowing watermark (penanganan data telat) di Spark.
  * **Gold Layer:** Hasil join prediktif AI dan data ETA spasial di tabel `passenger_density`, `bus_eta`, dan `system_recommendations` di PostgreSQL.
  * **Time Travel/Versioning:** Replay data mentah didukung oleh Kafka Offset log retention (hingga 7 hari) dan partisi file historis Parquet di Bronze Layer.

### CPMK-4: Teknik Analisis & Kualitas Output (Bobot 25%)
* **Kesesuaian Kode:** Penerapan dua metode analisis lanjutan:
  * **Machine Learning / Forecasting:** Join prediktif deret waktu dengan profil historis halte 20 menit ke depan.
  * **Geospatial & Clustering Analisis:** Perhitungan jarak euclidean koordinat spasial bus ke halte tujuan untuk memproyeksikan estimasi waktu kedatangan (ETA).

### CPMK 1-4: Keunikan & Inovasi Solusi (Bobot 10%)
* **Kesesuaian Kode:** Integrasi sensor spasial GPS bus dengan transaksi tap-in penumpang untuk menghasilkan rekomendasi headway bus secara otomatis.
* **Perbandingan Solusi Existing:**
  | Dimensi | Solusi Existing (Static Apps / Google Maps) | Solusi Project Kami (ShelterEye) |
  | :--- | :--- | :--- |
  | **Kepadatan Halte** | Tidak mendeteksi kerumunan halte secara live. | Real-time monitoring via tap-in transaksi. |
  | **Prediksi Masa Depan** | Hanya memperkirakan durasi lalu lintas jalan raya. | AI Forecasting tingkat penumpukan halte 20 menit ke depan. |
  | **Headway Control** | Jadwal bus statis dan rentan penumpukan bus. | Pemicu otomatis pengiriman bus cadangan dari pool. |

### CPMK 2-4: Implementasi & Demo Sistem (Bobot 10%)
* **Kesesuaian Kode:** Pipeline data aktif end-to-end teruji lancar dengan mekanisme penanganan data kosong dan data error via DB fallback connection.
* **Logging & Monitoring:** Spark Streaming UI (port 4040) untuk memantau visualisasi grafik throughput input rate, watermark delay, dan logging terminal.

---

## 2. Alur Panduan Demo Presentasi (Step-by-Step)

### Langkah 1: Tunjukkan Data Lakehouse Bronze Layer (Parquet)
1. Tunjukkan folder `storage/lakehouse/bronze/` di file explorer Anda.
2. Perlihatkan sub-folder `transactions/` dan `telemetry/` yang di dalamnya terbagi menjadi partisi tanggal (contoh: `date=2026-06-20/`).
3. Jelaskan bahwa file `.parquet` di dalamnya adalah data stream mentah (*raw data*) yang disimpan permanen sebelum diproses.

### Langkah 2: Tunjukkan Sistem Dashboard Berjalan Aktif
1. Buka browser pada URL Ngrok atau `http://localhost:8501`.
2. Tunjukkan **Peta Spasial** yang menampilkan pergerakan bus biru secara dinamis dan indikator halte.
3. Tunjukkan tab **"Log Transaksi Penumpang Real-Time (Tap-In)"** untuk membuktikan bahwa data riil terus mengalir masuk tanpa henti.

### Langkah 3: Demonstrasikan Fitur Analisis Spasial & ETA Bus
1. Klik salah satu marker halte hijau di peta.
2. Tunjukkan tabel **"Armada Bus Mendatang"** di dalam popup halte yang menampilkan ID Bus terdekat, ETA (menit), dan Okupansi bus yang akan tiba.

### Langkah 4: Demonstrasikan AI Prediction & Peringatan Kritis (Demo Skenario)
1. Tunjukkan halte **Dukuh Atas 1** dan **Kampung Melayu** yang berwarna **MERAH** di peta.
2. Tunjukkan kolom **"Probabilitas Overload"** di popup peta yang bernilai >100% (hasil prediksi 20 menit ke depan).
3. Tunjukkan box kuning **"Rekomendasi Sistem & Peringatan"** di sebelah kanan peta yang menampilkan perintah otomatis: *"Segera luncurkan Bus Cadangan dari Pool Manggarai!"*

---

## 3. Komponen Kode Kunci Yang Harus Ditunjukkan ke Penguji

### A. Lakehouse Bronze Layer (CPMK-3)
Tunjukkan baris kode di akhir file [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py):
* **Parquet Bronze Streaming (Line 505-516):** Kode yang mengonversi data stream mentah, menambahkan kolom tanggal (`to_date`), melakukan partisi (`partitionBy`), dan menulisnya ke direktori lokal dalam format Parquet.

### B. Medallion Layer & AI Forecasting (CPMK-3 & CPMK-4)
Tunjukkan file [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py):
* **Parsing & Bronze Layer (Line 193-201):** Kode membaca string Kafka JSON dan melakukan parsing skema data mentah terstruktur.
* **Windowing Silver Layer (Line 205-215):** Penerapan watermarking late-data dan group-by time window.
* **AI Projection Gold Layer (Line 220-245):** Logika peramalan penumpukan 20 menit ke depan dengan mencocokkan profil waktu historis.

### C. Analisis Spasial & ETA Telemetry (CPMK-4)
Tunjukkan file [spatial_eta_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/spatial_eta_logic.py):
* **Euclidean Distance & ETA Projection (Line 135-155):** Rumus matematika untuk menghitung jarak spasial GPS koordinat bus ke koordinat halte dan membaginya dengan sisa waktu kedatangan (ETA).
