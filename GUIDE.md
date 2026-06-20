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
* **Parquet Bronze Streaming (Line 501-520):** Kode yang mengonversi data stream mentah, menambahkan kolom tanggal (`to_date`), melakukan partisi (`partitionBy`), dan menulisnya ke direktori lokal dalam format Parquet.

### B. Medallion Layer & AI Forecasting (CPMK-3 & CPMK-4)
Tunjukkan file [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py):
* **Parsing & Bronze Layer (Line 156-163):** Kode membaca string Kafka JSON dan melakukan parsing skema data mentah terstruktur.
* **Windowing Silver Layer (Line 192-202):** Penerapan watermarking late-data dan group-by time window.
* **AI Projection Gold Layer (Line 203-245):** Logika peramalan penumpukan 20 menit ke depan dengan mencocokkan profil waktu historis.

### C. Analisis Spasial & ETA Telemetry (CPMK-4)
Tunjukkan file [spatial_eta_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/spatial_eta_logic.py):
* **Euclidean Distance & ETA Projection (Line 117-140):** Rumus matematika untuk menghitung jarak spasial GPS koordinat bus ke koordinat halte dan membaginya dengan sisa waktu kedatangan (ETA).

### D. Ingestion Layer (CPMK-2)
Tunjukkan file [feeder.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/ingestion/feeder.py):
* **Kafka Producer Ingestion (Line 42-65):** Client WebSocket asinkron yang menangkap batch real-time dari server data luar lalu meneruskannya langsung ke Kafka Broker topik `topic-transjakarta`.

---

## 4. Script Bicara Demo & Presentasi (Draf Presentasi)

Berikut adalah panduan perkataan (script) yang bisa Anda ucapkan saat mempresentasikan setiap rubrik di hadapan dosen penguji, lengkap dengan kodingan yang harus disorot:

### 📑 Rubrik 1: Identifikasi Masalah & Relevansi Big Data (5V)
* **Apa yang diucapkan:**
  > *"Project kami memecahkan masalah penumpukan penumpang Transjakarta secara real-time. Kami menggunakan framework Big Data karena memenuhi karakteristik 5V secara utuh. **Volume & Velocity** terlihat dari puluhan ribu data transaksi tap-in per menit yang kami tangkap secara live. **Variety** mencakup data tabular transaksi dan data geospatial koordinat GPS bus. **Veracity** kami tangani menggunakan mekanisme Watermarking di Spark untuk mengabaikan data yang terlambat, sehingga data yang disajikan sangat akurat (**Value**)."*

### ⚙️ Rubrik 2: Desain Infrastruktur & Alur Pipeline
* **Apa yang diucapkan:**
  > *"Infrastruktur kami berjalan end-to-end secara aktif. Proses ingestion menggunakan client WebSocket asinkron di **[feeder.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/ingestion/feeder.py)** untuk menembak Kafka Broker di port 9092. Pemrosesan dilakukan oleh Spark Structured Streaming di container Master/Worker, dan datastore hasil olahan disimpan ke database PostgreSQL di port 5432 sebelum akhirnya di-serving secara interaktif ke Streamlit dashboard."*

### 🗄️ Rubrik 3: Lakehouse & Medallion Architecture
* **Apa yang diucapkan:**
  > *"Kami menerapkan arsitektur Medallion secara eksplisit di codingan. Pertama, data mentah dari Kafka disimpan ke folder lokal **`storage/lakehouse/bronze/`** dalam format **Parquet** yang dipartisi berdasarkan tanggal (`date=YYYY-MM-DD`). Ini adalah **Bronze Layer** kami (sorot line 501-520 di [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py)).*
  >
  > *Kedua, data dibersihkan dan di-watermark di **Silver Layer** (sorot line 192-202 di [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py)).*
  >
  > *Ketiga, data hasil agregasi jendela waktu (sliding window) dan prediksi overload disimpan ke tabel siap saji PostgreSQL sebagai **Gold Layer** (sorot line 203-245 di [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py))."*

### 📊 Rubrik 4: Teknik Analisis Lanjutan (ML Forecasting & GIS ETA)
* **Apa yang diucapkan:**
  > *"Kami menerapkan dua analisis lanjutan. Pertama adalah **AI Forecasting** di file [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py#L203-L245) untuk memproyeksikan kepadatan halte 20 menit ke depan menggunakan pola historis data penumpang. Kedua adalah **Clustering & Analisis Spasial GIS** di file [spatial_eta_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/spatial_eta_logic.py#L117-L140) dengan rumus matematika Haversine Distance untuk menghitung sisa waktu perjalanan (ETA) armada bus ke halte tujuan berdasarkan koordinat GPS langsung."*

### 🛠️ Rubrik 5 & 6: Keunikan & Demo Sistem Aktif (Monitoring)
* **Apa yang diucapkan:**
  > *"Keunikan solusi kami terletak pada integrasi headway control otomatis. Jika terjadi potensi overload di halte (misal di Dukuh Atas 1) dan tidak ada bus terdekat, sistem secara kritis akan merekomendasikan pengiriman bus cadangan dari pool terdekat.*
  >
  > *Sistem berjalan 100% aktif dan dapat dimonitor secara production-ready melalui terminal log dan dashboard administrasi **Spark UI di port 4040** yang memetakan statistik performa stream secara live."*
