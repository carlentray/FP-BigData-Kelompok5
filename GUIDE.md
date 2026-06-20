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
* **Kesesuaian Kode:** Penerapan Medallion Architecture:
  * **Bronze:** Data JSON mentah di Kafka topic `topic-transjakarta` dan tabel `passenger_transactions`.
  * **Silver:** Skema parsing data terstruktur dan windowing watermark di Spark.
  * **Gold:** Hasil join prediktif AI dan data ETA spasial di tabel `passenger_density` dan `bus_eta`.

### CPMK-4: Teknik Analisis & Kualitas Output (Bobot 25%)
* **Kesesuaian Kode:** Penerapan dua metode analisis lanjutan:
  * **Machine Learning / Forecasting:** Join prediktif deret waktu dengan profil historis halte 20 menit ke depan.
  * **Geospatial & Clustering Analisis:** Perhitungan jarak euclidean koordinat spasial bus ke halte tujuan untuk memproyeksikan estimasi waktu kedatangan (ETA).

### CPMK 1-4: Keunikan & Inovasi Solusi (Bobot 10%)
* **Kesesuaian Kode:** Integrasi sensor spasial GPS bus dengan transaksi tap-in penumpang untuk menghasilkan rekomendasi headway bus secara otomatis.

### CPMK 2-4: Implementasi & Demo Sistem (Bobot 10%)
* **Kesesuaian Kode:** Pipeline data aktif end-to-end teruji lancar dengan mekanisme penanganan data kosong dan data error via DB fallback connection.

---

## 2. Alur Panduan Demo Presentasi (Step-by-Step)

### Langkah 1: Tunjukkan Sistem Sedang Berjalan Aktif
1. Buka browser pada URL Ngrok atau `http://localhost:8501`.
2. Tunjukkan **Peta Spasial** yang menampilkan pergerakan bus biru secara dinamis dan indikator halte.
3. Tunjukkan tab **"Log Transaksi Penumpang Real-Time (Tap-In)"** untuk membuktikan bahwa data riil terus mengalir masuk tanpa henti.

### Langkah 2: Demonstrasikan Fitur Analisis Spasial & ETA Bus
1. Klik salah satu marker halte hijau di peta.
2. Tunjukkan tabel **"Armada Bus Mendatang"** di dalam popup halte yang menampilkan ID Bus terdekat, ETA (menit), dan Okupansi bus yang akan tiba.
3. Tunjukkan grafik batang **"Okupansi & Posisi Armada Bus Aktif"** di bagian bawah.

### Langkah 3: Demonstrasikan AI Prediction & Peringatan Kritis (Demo Skenario)
1. Tunjukkan halte **Dukuh Atas 1** dan **Kampung Melayu** yang berwarna **MERAH** di peta.
2. Tunjukkan kolom **"Probabilitas Overload"** di popup peta yang bernilai >100% (hasil prediksi 20 menit ke depan).
3. Tunjukkan box kuning **"Rekomendasi Sistem & Peringatan"** di sebelah kanan peta yang menampilkan perintah otomatis: *"Segera luncurkan Bus Cadangan dari Pool Manggarai!"*

---

## 3. Komponen Kode Kunci Yang Harus Ditunjukkan ke Penguji

### A. Medallion Layer & AI Forecasting (CPMK-3 & CPMK-4)
Tunjukkan file [passenger_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/passenger_logic.py):
* **Parsing & Bronze Layer (Line 193-201):** Kode membaca string Kafka JSON dan melakukan parsing skema data mentah terstruktur.
* **Windowing Silver Layer (Line 205-215):** Penerapan watermarking late-data dan group-by time window.
* **AI Projection Gold Layer (Line 220-245):** Logika peramalan penumpukan 20 menit ke depan dengan mencocokkan profil waktu historis.

### B. Analisis Spasial & ETA Telemetry (CPMK-4)
Tunjukkan file [spatial_eta_logic.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/processing/spatial_eta_logic.py):
* **Euclidean Distance & ETA Projection (Line 135-155):** Rumus matematika untuk menghitung jarak spasial GPS koordinat bus ke koordinat halte dan membaginya dengan sisa waktu kedatangan (ETA).

### C. Ingestion Layer (CPMK-2)
Tunjukkan file [feeder.py](file:///c:/Users/LOQ/Downloads/FP-BigData-Kelompok5-main/FP-BigData-Kelompok5-main/ingestion/feeder.py):
* **Kafka Producer Ingestion (Line 42-65):** Client WebSocket asinkron yang menangkap batch real-time dari server data luar lalu meneruskannya langsung ke Kafka Broker topik `topic-transjakarta`.
