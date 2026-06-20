import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, expr, hour, minute, floor, lit, coalesce, concat


# 1. Konfigurasi Awal & Host Resolution
import socket

def resolve_host(docker_host, local_host):
    try:
        socket.gethostbyname(docker_host)
        return docker_host
    except socket.gaierror:
        return local_host

KAFKA_HOST = resolve_host("kafka", "localhost")
KAFKA_PORT = 29092 if KAFKA_HOST == "kafka" else 9092
POSTGRES_HOST = resolve_host("postgres-db", "localhost")

print(f"DEBUG: Resolved hostnames -> Kafka: {KAFKA_HOST}:{KAFKA_PORT}, Postgres: {POSTGRES_HOST}:5432")

# Kita muat package Spark SQL Kafka dan PostgreSQL JDBC Driver
spark = SparkSession.builder \
    .appName("ShelterEye-PassengerDensityProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session berhasil diinisialisasi dengan package Kafka & Postgres JDBC!")

# Helper untuk mendapatkan koneksi PostgreSQL JDBC bypass classloader
jdbc_driver = None

def get_postgres_connection(spark, db_url, conn_props):
    global jdbc_driver
    jvm = spark._jvm
    if jdbc_driver is None:
        print("Mendaftarkan driver PostgreSQL JDBC...")
        try:
            jdbc_driver = jvm.org.postgresql.Driver()
        except Exception as je:
            print(f"Pendaftaran direct gagal ({je}), mencoba ClassLoader...")
            class_loader = jvm.Thread.currentThread().getContextClassLoader()
            driver_class = class_loader.loadClass("org.postgresql.Driver")
            jdbc_driver = driver_class.newInstance()
    return jdbc_driver.connect(db_url, conn_props)

# 2. Setup Database PostgreSQL (Inisialisasi Tabel)
def init_postgresql_tables():
    db_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye"
    jvm = spark._jvm
    conn_props = jvm.java.util.Properties()
    conn_props.setProperty("user", "admin")
    conn_props.setProperty("password", "sheltereyepassword")
    conn_props.setProperty("driver", "org.postgresql.Driver")
    
    print("Menghubungkan ke PostgreSQL untuk inisialisasi skema tabel...")
    conn = get_postgres_connection(spark, db_url, conn_props)
    stmt = conn.createStatement()
    
    # 1. Tabel Kepadatan Penumpang
    stmt.execute("""
        CREATE TABLE IF NOT EXISTS passenger_density (
            halte_name VARCHAR(100) PRIMARY KEY,
            passenger_count INT,
            predicted_overload_pct DOUBLE PRECISION,
            predicted_status VARCHAR(100),
            last_updated TIMESTAMP
        )
    """)
    
    # 2. Tabel ETA Bus (Diisi oleh Anggota 3)
    stmt.execute("""
        CREATE TABLE IF NOT EXISTS bus_eta (
            halte_name VARCHAR(100),
            bus_id VARCHAR(50),
            eta_minutes DOUBLE PRECISION,
            bus_lat DOUBLE PRECISION,
            bus_lon DOUBLE PRECISION,
            bus_capacity INT,
            current_passenger_count INT,
            occupancy_pct DOUBLE PRECISION,
            is_empty BOOLEAN,
            last_updated TIMESTAMP,
            PRIMARY KEY (halte_name, bus_id)
        )
    """)
    
    # 3. Tabel Rekomendasi Sistem (Fitur 5)
    stmt.execute("""
        CREATE TABLE IF NOT EXISTS system_recommendations (
            halte_name VARCHAR(100) PRIMARY KEY,
            status VARCHAR(50),
            recommendation_text TEXT,
            created_at TIMESTAMP
        )
    """)
    
    # 4. Tabel Log Transaksi Mentah
    stmt.execute("""
        CREATE TABLE IF NOT EXISTS passenger_transactions (
            trans_id VARCHAR(100) PRIMARY KEY,
            card_id VARCHAR(50),
            bank VARCHAR(50),
            name VARCHAR(100),
            halte_name VARCHAR(100),
            tap_in_time TIMESTAMP,
            created_at TIMESTAMP
        )
    """)
    
    # Masukkan data dummy awal ke bus_eta jika kosong untuk keperluan demo/tes
    rs = stmt.executeQuery("SELECT COUNT(*) FROM bus_eta")
    rs.next()
    count = rs.getInt(1)
    if count == 0:
        print("Menambahkan data dummy awal ke tabel bus_eta...")
        stmt.execute("INSERT INTO bus_eta (halte_name, bus_id, eta_minutes, bus_lat, bus_lon, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated) VALUES ('Dukuh Atas 1', 'BUS-01', 20.0, -6.2001, 106.8000, 80, 0, 0.0, true, NOW())")
        stmt.execute("INSERT INTO bus_eta (halte_name, bus_id, eta_minutes, bus_lat, bus_lon, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated) VALUES ('Karet Sudirman', 'BUS-02', 8.0, -6.2120, 106.8200, 80, 0, 0.0, true, NOW())")
        stmt.execute("INSERT INTO bus_eta (halte_name, bus_id, eta_minutes, bus_lat, bus_lon, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated) VALUES ('Monas', 'BUS-03', 18.0, -6.1754, 106.8271, 80, 0, 0.0, true, NOW())")
    
    stmt.close()
    conn.close()
    print("Inisialisasi tabel di PostgreSQL selesai!")

try:
    init_postgresql_tables()
except Exception as e:
    print(f"Peringatan: Gagal melakukan inisialisasi tabel ke Postgres. Pastikan Docker sudah menyala! Error: {e}")

# 3. Muat Data Historis Profil Penumpang (Untuk Fitur 4: AI Forecasting)
HISTORICAL_PATH = "processing/historical_patterns.json"
if os.path.exists(HISTORICAL_PATH):
    print(f"Memuat data historis untuk AI Forecasting dari {HISTORICAL_PATH}...")
    historical_df = spark.read.json(HISTORICAL_PATH)
else:
    print(f"Peringatan: File data historis tidak ditemukan di {HISTORICAL_PATH}. Membuat profil default...")
    # Default schema
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("halte_name", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute_slot", IntegerType(), True),
        StructField("historical_count", IntegerType(), True)
    ])
    historical_df = spark.createDataFrame([], schema)

# Caching data historis karena ukurannya kecil dan sering di-join
historical_df.cache()

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - BRONZE INGESTION LAYER] ###
# ==============================================================================
# 4. Membaca Stream Transaksi dari Kafka (Data Mentah)
print("Menghubungkan ke Kafka topic 'topic-transjakarta'...")
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
    .option("subscribe", "topic-transjakarta") \
    .option("startingOffsets", "latest") \
    .load()

# Skema data JSON dari Kafka (sesuai kolom dataset Transjakarta)
transaction_schema = """
    transID STRING,
    payCardID STRING,
    payCardBank STRING,
    payCardName STRING,
    payCardSex STRING,
    payCardBirthDate STRING,
    corridorID STRING,
    corridorName STRING,
    direction STRING,
    tapInTime STRING,
    tapInStopsID STRING,
    tapInStopsName STRING,
    tapInStopsLat DOUBLE,
    tapInStopsLon DOUBLE,
    tapOutTime STRING,
    tapOutStopsID STRING,
    tapOutStopsName STRING,
    tapOutStopsLat DOUBLE,
    tapOutStopsLon DOUBLE,
    payAmount STRING,
    payCardType STRING,
    transType STRING,
    streaming_timestamp STRING
"""

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - SILVER TRANSFORMATION LAYER (CLEANED & WATERMARKED)] ###
# ==============================================================================
# Parsing kolom value dari Kafka (JSON)
parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), transaction_schema).alias("data")) \
    .select("data.*")

# Konversi string timestamp ke format Timestamp Spark & atur watermark
processed_stream_df = parsed_stream_df \
    .withColumn("timestamp", to_timestamp(col("streaming_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("timestamp", "10 minutes")

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - GOLD SERVING LAYER (AGGREGATIONS & AI FORECASTING)] ###
# ==============================================================================
# 5. Agregasi Jendela Waktu (Real-Time Passenger Volume)
# Menghitung jumlah tap-in per halte dalam sliding window 10 menit, slide tiap 1 menit
passenger_aggregation_df = processed_stream_df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "1 minute"),
        col("tapInStopsName").alias("halte_name")
    ) \
    .count() \
    .select(
        col("halte_name"),
        col("count").alias("passenger_count"),
        col("window.end").alias("last_updated")
    )

# 6. Integrasi AI Passenger Forecasting (Fitur 4)
# Kita hitung waktu proyeksi 20 menit ke depan
forecasting_prepared_df = passenger_aggregation_df \
    .withColumn("projected_time", col("last_updated") + expr("INTERVAL 20 MINUTES")) \
    .withColumn("proj_hour", hour(col("projected_time"))) \
    .withColumn("proj_minute_slot", floor(minute(col("projected_time")) / 10))

# Lakukan join dengan data historis
# Jika join kosong (tidak ada kecocokan jam/halte), kita berikan historical_count default = 50
joined_forecast_df = forecasting_prepared_df.join(
    historical_df,
    (forecasting_prepared_df.halte_name == historical_df.halte_name) &
    (forecasting_prepared_df.proj_hour == historical_df.hour) &
    (forecasting_prepared_df.proj_minute_slot == historical_df.minute_slot),
    "left"
).select(
    forecasting_prepared_df.halte_name,
    forecasting_prepared_df.passenger_count,
    forecasting_prepared_df.last_updated,
    coalesce(historical_df.historical_count, lit(50)).alias("historical_prediction")
)

# Hitung overload percentage berdasarkan batas kapasitas halte (misal kapasitas halte = 200)
# Skenario Overstimulated Demo: Memaksa Dukuh Atas 1 dan Kampung Melayu memiliki status overload tinggi
final_streaming_df = joined_forecast_df \
    .withColumn("predicted_overload_pct", 
        expr("CASE WHEN halte_name = 'Dukuh Atas 1' THEN 135.0 "
             "WHEN halte_name = 'Kampung Melayu' THEN 145.0 "
             "ELSE (coalesce(historical_prediction, 50.0) / 200.0) * 100.0 END")
    ) \
    .withColumn("predicted_status", 
        expr("CASE WHEN predicted_overload_pct >= 100.0 THEN 'Potensi Overload ' || CAST(CAST(predicted_overload_pct AS INT) AS STRING) || '%' ELSE 'Normal' END")
    )

# 7. Fungsi foreachBatch untuk Menulis ke PostgreSQL & Menjalankan Rekomendasi
def write_to_postgres_and_recommend(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    print(f"\n--- [Batch ID: {batch_id}] Memproses data kepadatan penumpang & sistem rekomendasi ---")
    
    # 1. Ambil data ETA bus terdekat dari PostgreSQL (yang di-update oleh Anggota 3)
    try:
        bus_eta_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye") \
            .option("dbtable", """(
                SELECT halte_name, MIN(eta_minutes) as eta_minutes 
                FROM bus_eta 
                WHERE is_empty = true 
                GROUP BY halte_name
             ) as empty_buses""") \
            .option("user", "admin") \
            .option("password", "sheltereyepassword") \
            .option("driver", "org.postgresql.Driver") \
            .load()
    except Exception as e:
        print(f"Peringatan: Gagal membaca tabel bus_eta dari Postgres. Menggunakan default ETA. Error: {e}")
        # Jika gagal (misal tabel kosong atau belum ada), kita buat dataframe kosong
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("halte_name", StringType(), True),
            StructField("eta_minutes", DoubleType(), True)
        ])
        bus_eta_df = spark.createDataFrame([], schema)

    # 2. Lakukan Left Join antara hasil agregasi batch saat ini dengan ETA Bus
    joined_batch_df = batch_df.join(bus_eta_df, on="halte_name", how="left")
    
    # 3. Hitung Rekomendasi Headway (Fitur 5)
    # Logika: Jika (passenger_count > 200) DAN (eta_minutes > 15 atau tidak ada bus yang datang), status = CRITICAL_SEND_BACKUP
    # Skenario Overstimulated Demo: Secara otomatis memicu status kritis untuk Dukuh Atas 1 dan Kampung Melayu
    final_recommendations_df = joined_batch_df \
        .withColumn("eta", coalesce(col("eta_minutes"), lit(999.0))) \
        .withColumn("rec_status", 
            expr("CASE WHEN (passenger_count > 200 AND eta > 15.0) "
                 "OR (halte_name IN ('Dukuh Atas 1', 'Kampung Melayu')) "
                 "THEN 'CRITICAL_SEND_BACKUP' ELSE 'NORMAL' END")
        ) \
        .withColumn("recommendation_text", 
            expr("""CASE WHEN rec_status = 'CRITICAL_SEND_BACKUP' 
                 THEN 'REKOMENDASI SISTEM: Segera luncurkan Bus Cadangan dari Pool Manggarai menuju Halte ' || halte_name || '!'
                 ELSE 'Kondisi halte aman terkendali.' END""")
        )

    # Deduplicate: ambil baris dengan last_updated terbaru untuk tiap halte_name di batch ini
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy("halte_name").orderBy(col("last_updated").desc())
    deduplicated_df = final_recommendations_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Pisahkan data untuk disimpan ke tabel masing-masing
    # A. Data Kepadatan & Prediksi Penumpang
    density_to_write = deduplicated_df.select(
        "halte_name",
        "passenger_count",
        "predicted_overload_pct",
        "predicted_status",
        "last_updated"
    )
    
    # B. Data Rekomendasi
    rec_to_write = deduplicated_df.select(
        "halte_name",
        col("rec_status").alias("status"),
        "recommendation_text",
        col("last_updated").alias("created_at")
    )
    
    # Tampilkan di console untuk monitoring
    print("Live Data Kepadatan & Prediksi:")
    density_to_write.show(truncate=False)
    
    print("Live Rekomendasi Sistem:")
    rec_to_write.show(truncate=False)

    # Tulis ke PostgreSQL menggunakan UPSERT manual (ON CONFLICT DO UPDATE) via temp table
    # Langkah A: Tulis ke tabel temporary
    db_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye"
    write_props = {
        "user": "admin",
        "password": "sheltereyepassword",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Tulis kepadatan ke tabel temp
        density_to_write.write \
            .jdbc(url=db_url, table="temp_passenger_density", mode="overwrite", properties=write_props)
            
        # Jalankan query UPSERT ke tabel utama passenger_density
        upsert_density_sql = """
            INSERT INTO passenger_density (halte_name, passenger_count, predicted_overload_pct, predicted_status, last_updated)
            SELECT halte_name, passenger_count, predicted_overload_pct, predicted_status, last_updated 
            FROM temp_passenger_density
            ON CONFLICT (halte_name) 
            DO UPDATE SET 
                passenger_count = EXCLUDED.passenger_count,
                predicted_overload_pct = EXCLUDED.predicted_overload_pct,
                predicted_status = EXCLUDED.predicted_status,
                last_updated = EXCLUDED.last_updated
        """
        
        # Tulis rekomendasi ke tabel temp
        rec_to_write.write \
            .jdbc(url=db_url, table="temp_system_recommendations", mode="overwrite", properties=write_props)
            
        # Jalankan query UPSERT ke tabel utama system_recommendations
        upsert_rec_sql = """
            INSERT INTO system_recommendations (halte_name, status, recommendation_text, created_at)
            SELECT halte_name, status, recommendation_text, created_at 
            FROM temp_system_recommendations
            ON CONFLICT (halte_name) 
            DO UPDATE SET 
                status = EXCLUDED.status,
                recommendation_text = EXCLUDED.recommendation_text,
                created_at = EXCLUDED.created_at
        """
        
        # Eksekusi SQL UPSERT menggunakan JVM JDBC connection
        jvm = spark._jvm
        conn_props = jvm.java.util.Properties()
        conn_props.setProperty("user", "admin")
        conn_props.setProperty("password", "sheltereyepassword")
        conn_props.setProperty("driver", "org.postgresql.Driver")
        
        conn = get_postgres_connection(spark, db_url, conn_props)
        stmt = conn.createStatement()
        stmt.execute(upsert_density_sql)
        stmt.execute(upsert_rec_sql)
        
        # Skenario Overstimulated Demo: Memaksa Dukuh Atas 1 dan Kampung Melayu selalu terdaftar sebagai overload & kritis
        stmt.execute("""
            INSERT INTO passenger_density (halte_name, passenger_count, predicted_overload_pct, predicted_status, last_updated)
            VALUES ('Dukuh Atas 1', 270, 135.0, 'Potensi Overload 135%', NOW())
            ON CONFLICT (halte_name) DO UPDATE 
            SET passenger_count = EXCLUDED.passenger_count, 
                predicted_overload_pct = EXCLUDED.predicted_overload_pct, 
                predicted_status = EXCLUDED.predicted_status, 
                last_updated = EXCLUDED.last_updated
        """)
        stmt.execute("""
            INSERT INTO system_recommendations (halte_name, status, recommendation_text, created_at)
            VALUES ('Dukuh Atas 1', 'CRITICAL_SEND_BACKUP', 'REKOMENDASI SISTEM: Segera luncurkan Bus Cadangan dari Pool Manggarai menuju Halte Dukuh Atas 1!', NOW())
            ON CONFLICT (halte_name) DO UPDATE 
            SET status = EXCLUDED.status, 
                recommendation_text = EXCLUDED.recommendation_text, 
                created_at = EXCLUDED.created_at
        """)
        stmt.execute("""
            INSERT INTO passenger_density (halte_name, passenger_count, predicted_overload_pct, predicted_status, last_updated)
            VALUES ('Kampung Melayu', 290, 145.0, 'Potensi Overload 145%', NOW())
            ON CONFLICT (halte_name) DO UPDATE 
            SET passenger_count = EXCLUDED.passenger_count, 
                predicted_overload_pct = EXCLUDED.predicted_overload_pct, 
                predicted_status = EXCLUDED.predicted_status, 
                last_updated = EXCLUDED.last_updated
        """)
        stmt.execute("""
            INSERT INTO system_recommendations (halte_name, status, recommendation_text, created_at)
            VALUES ('Kampung Melayu', 'CRITICAL_SEND_BACKUP', 'REKOMENDASI SISTEM: Segera luncurkan Bus Cadangan dari Pool Manggarai menuju Halte Kampung Melayu!', NOW())
            ON CONFLICT (halte_name) DO UPDATE 
            SET status = EXCLUDED.status, 
                recommendation_text = EXCLUDED.recommendation_text, 
                created_at = EXCLUDED.created_at
        """)
        
        stmt.close()
        conn.close()
        
        print("Data sukses disimpan ke PostgreSQL database (UPSERT berhasil)!")
    except Exception as ex:
        print(f"Gagal menulis data ke database PostgreSQL. Pastikan Docker Postgres aktif! Error: {ex}")

# Fungsi helper untuk menulis raw transactions ke PostgreSQL (ON CONFLICT DO NOTHING)
def write_raw_transactions_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    # Ambil kolom yang diperlukan untuk log transaksi mentah
    raw_tx_to_write = batch_df.select(
        col("transID").alias("trans_id"),
        col("payCardID").alias("card_id"),
        col("payCardBank").alias("bank"),
        col("payCardName").alias("name"),
        col("tapInStopsName").alias("halte_name"),
        to_timestamp(col("tapInTime"), "yyyy-MM-dd HH:mm:ss").alias("tap_in_time"),
        col("timestamp").alias("created_at")
    ).distinct()
    
    db_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye"
    write_props = {
        "user": "admin",
        "password": "sheltereyepassword",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Tulis ke tabel temp_transactions
        raw_tx_to_write.write \
            .jdbc(url=db_url, table="temp_passenger_transactions", mode="overwrite", properties=write_props)
            
        # SQL UPSERT ke tabel utama
        upsert_tx_sql = """
            INSERT INTO passenger_transactions (trans_id, card_id, bank, name, halte_name, tap_in_time, created_at)
            SELECT trans_id, card_id, bank, name, halte_name, tap_in_time, created_at 
            FROM temp_passenger_transactions
            ON CONFLICT (trans_id) 
            DO NOTHING
        """
        
        jvm = spark._jvm
        conn_props = jvm.java.util.Properties()
        conn_props.setProperty("user", "admin")
        conn_props.setProperty("password", "sheltereyepassword")
        conn_props.setProperty("driver", "org.postgresql.Driver")
        
        conn = get_postgres_connection(spark, db_url, conn_props)
        stmt = conn.createStatement()
        stmt.execute(upsert_tx_sql)
        stmt.close()
        conn.close()
    except Exception as ex:
        print(f"Gagal menulis data transaksi mentah ke database. Error: {ex}")

# 8. Memulai Streaming Query
print("Memulai streaming pemrosesan data real-time...")
checkpoint_dir = "processing/checkpoints/passenger_density"
checkpoint_raw = "processing/checkpoints/raw_transactions"

# Pastikan folder checkpoint bersih atau dibuat
os.makedirs(checkpoint_dir, exist_ok=True)
os.makedirs(checkpoint_raw, exist_ok=True)

# 1. Write to Serving Database (Gold Layer)
query_density = final_streaming_df.writeStream \
    .foreachBatch(write_to_postgres_and_recommend) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query_raw = processed_stream_df.writeStream \
    .foreachBatch(write_raw_transactions_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_raw) \
    .start()

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - BRONZE STORAGE LAYER (PARQUET ON LOCAL DISK)] ###
# ==============================================================================
# 2. Write Raw Data to Lakehouse Bronze Layer (Format Parquet, Partisi berdasarkan Tanggal)
from pyspark.sql.functions import to_date
bronze_df = processed_stream_df.withColumn("date", to_date(col("timestamp")))
checkpoint_bronze = "processing/checkpoints/bronze_transactions"
os.makedirs(checkpoint_bronze, exist_ok=True)

query_bronze = bronze_df.writeStream \
    .format("parquet") \
    .partitionBy("date") \
    .option("path", "storage/lakehouse/bronze/transactions") \
    .option("checkpointLocation", checkpoint_bronze) \
    .outputMode("append") \
    .start()

# Standby menangkap data secara kontinu
spark.streams.awaitAnyTermination()
