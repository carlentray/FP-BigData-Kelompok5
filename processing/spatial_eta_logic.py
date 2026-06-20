import os
import sys
import json
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, radians, sin, cos, sqrt, asin, when, coalesce

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

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("ShelterEye-SpatialEtaProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session untuk ETA Spasial berhasil dibuat!")

# Helper untuk mendapatkan koneksi PostgreSQL JDBC bypass classloader
jdbc_driver = None

def get_postgres_connection(spark, db_url, conn_props):
    global jdbc_driver
    jvm = spark._jvm
    if jdbc_driver is None:
        print("Mendaftarkan driver PostgreSQL JDBC...")
        try:
            jdbc_driver = jvm.org.postgresql.Driver()
        except Exception as e:
            print(f"Pendaftaran direct gagal ({e}), mencoba ClassLoader...")
            class_loader = jvm.Thread.currentThread().getContextClassLoader()
            driver_class = class_loader.loadClass("org.postgresql.Driver")
            jdbc_driver = driver_class.newInstance()
    return jdbc_driver.connect(db_url, conn_props)

# 2. Koordinat Halte Target (Ground Truth)
# Kita definisikan koordinat 7 halte utama untuk perhitungan rumus Haversine
HALTE_COORDINATES = {
    "Dukuh Atas 1": {"lat": -6.2001, "lon": 106.8000},
    "Karet Sudirman": {"lat": -6.2120, "lon": 106.8200},
    "Monas": {"lat": -6.1754, "lon": 106.8271},
    "Harmoni Central": {"lat": -6.1601, "lon": 106.8164},
    "Manggarai": {"lat": -6.2084, "lon": 106.8483},
    "Kampung Melayu": {"lat": -6.2244, "lon": 106.8576},
    "Senen": {"lat": -6.1744, "lon": 106.8431}
}

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - BRONZE INGESTION LAYER] ###
# ==============================================================================
# 3. Membaca Stream dari Kafka topic-telemetry (Data Mentah)
print("Menghubungkan ke Kafka topic 'topic-telemetry'...")
telemetry_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
    .option("subscribe", "topic-telemetry") \
    .option("startingOffsets", "latest") \
    .load()

# Skema JSON untuk telemetry bus
telemetry_schema = """
    type STRING,
    bus_id STRING,
    trip_id STRING,
    route_id STRING,
    route_name STRING,
    direction INT,
    lat DOUBLE,
    lon DOUBLE,
    speed_kmh DOUBLE,
    is_moving BOOLEAN,
    current_stop STRING,
    next_stop STRING,
    progress DOUBLE,
    distance_km DOUBLE,
    sim_time STRING,
    streaming_timestamp STRING
"""

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - SILVER TRANSFORMATION LAYER (CLEANED & STRUCTURIZED)] ###
# ==============================================================================
parsed_telemetry_df = telemetry_stream_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), telemetry_schema).alias("data")) \
    .select("data.*")

# 4. Fungsi Kalkulasi Jarak Spasial dengan Rumus Haversine
# d = 2 * R * asin(sqrt(sin^2(dlat/2) + cos(lat1) * cos(lat2) * sin^2(dlon/2)))
R = 6371.0 # Radius bumi dalam km

def calculate_haversine_dist(lat_col, lon_col, target_lat, target_lon):
    lat1 = radians(lat_col)
    lon1 = radians(lon_col)
    lat2 = radians(lit(target_lat))
    lon2 = radians(lit(target_lon))
    
    dlat = lat1 - lat2
    dlon = lon1 - lon2
    
    a = sin(dlat / 2.0)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2.0)**2
    # batasi nilai a agar tidak diluar range [-1.0, 1.0] akibat pembulatan float
    a_bounded = when(a > 1.0, 1.0).otherwise(when(a < 0.0, 0.0).otherwise(a))
    c = 2.0 * asin(sqrt(a_bounded))
    return R * c

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - GOLD PROCESSING LAYER (GEOSPATIAL JOIN & ETA CALCULATION)] ###
# ==============================================================================
# Tambahkan kolom jarak dan ETA untuk setiap halte target ke dalam dataframe
# Untuk mencocokkan mana halte yang merupakan "next_stop", kita gunakan ekspresi dinamis
processed_telemetry_df = parsed_telemetry_df \
    .withColumn("timestamp", to_timestamp(col("streaming_timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Kita bangun ekspresi jarak dan ETA dinamis berdasarkan next_stop bus
# default kecepatan jika bus diam/lambat adalah 15 km/jam untuk menghindari pembagian dengan nol
effective_speed = when(col("speed_kmh") < 5.0, lit(15.0)).otherwise(col("speed_kmh"))

# Ekspresi jarak berdasarkan halte next_stop
distance_expr = lit(999.0)
for stop_name, coords in HALTE_COORDINATES.items():
    dist = calculate_haversine_dist(col("lat"), col("lon"), coords["lat"], coords["lon"])
    distance_expr = when(col("next_stop") == stop_name, dist).otherwise(distance_expr)

# Tambahkan metrik jarak, ETA, kapasitas bus, dan jumlah penumpang live di dalam bus
eta_prepared_df = (processed_telemetry_df
    .withColumn("distance_to_next_stop", distance_expr)
    .withColumn("eta_minutes", (col("distance_to_next_stop") / effective_speed) * 60.0)
    .withColumn("bus_capacity", lit(80))
    # Estimasi kapasitas terisi: berdasarkan progress rute bus
    .withColumn("current_passenger_count", (coalesce(col("progress"), lit(0.2)) * 60).cast("int"))
    .withColumn("occupancy_pct", (col("current_passenger_count") / col("bus_capacity")) * 100.0))

# Filter hanya data yang mengarah ke salah satu halte target kita
filtered_eta_df = eta_prepared_df \
    .filter(col("next_stop").isin(list(HALTE_COORDINATES.keys())))

# 5. Fungsi foreachBatch untuk Menulis ETA Bus ke PostgreSQL (UPSERT)
def write_eta_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    print(f"\n--- [ETA Batch: {batch_id}] Menghitung ETA Spasial dari GPS Bus ---")
    
    # Deduplicate: ambil posisi bus terbaru untuk tiap halte & bus_id
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy("next_stop", "bus_id").orderBy(col("timestamp").desc())
    latest_bus_df = batch_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
        
    # Format data untuk disinkronkan ke PostgreSQL tabel bus_eta
    # Kolom: halte_name, bus_id, eta_minutes, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated
    bus_eta_to_write = latest_bus_df.select(
        col("next_stop").alias("halte_name"),
        col("bus_id"),
        col("eta_minutes"),
        col("lat").alias("bus_lat"),
        col("lon").alias("bus_lon"),
        col("bus_capacity"),
        col("current_passenger_count"),
        col("occupancy_pct"),
        (col("current_passenger_count") == 0).alias("is_empty"),
        col("timestamp").alias("last_updated")
    )
    
    print("Live Posisi & ETA Armada Bus:")
    bus_eta_to_write.show(truncate=False)
    
    # Tulis ke PostgreSQL
    db_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye"
    write_props = {
        "user": "admin",
        "password": "sheltereyepassword",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Tulis ke tabel temp
        bus_eta_to_write.write \
            .jdbc(url=db_url, table="temp_bus_eta", mode="overwrite", properties=write_props)
            
        # SQL UPSERT ke tabel utama bus_eta
        # Mendukung kapasitas bus dan jumlah penumpang live di bus
        upsert_eta_sql = """
            INSERT INTO bus_eta (halte_name, bus_id, eta_minutes, bus_lat, bus_lon, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated)
            SELECT halte_name, bus_id, eta_minutes, bus_lat, bus_lon, bus_capacity, current_passenger_count, occupancy_pct, is_empty, last_updated 
            FROM temp_bus_eta
            ON CONFLICT (halte_name, bus_id) 
            DO UPDATE SET 
                eta_minutes = EXCLUDED.eta_minutes,
                bus_lat = EXCLUDED.bus_lat,
                bus_lon = EXCLUDED.bus_lon,
                bus_capacity = EXCLUDED.bus_capacity,
                current_passenger_count = EXCLUDED.current_passenger_count,
                occupancy_pct = EXCLUDED.occupancy_pct,
                is_empty = EXCLUDED.is_empty,
                last_updated = EXCLUDED.last_updated
        """
        
        # Eksekusi SQL UPSERT via JDBC
        jvm = spark._jvm
        conn_props = jvm.java.util.Properties()
        conn_props.setProperty("user", "admin")
        conn_props.setProperty("password", "sheltereyepassword")
        conn_props.setProperty("driver", "org.postgresql.Driver")
        
        conn = get_postgres_connection(spark, db_url, conn_props)
        stmt = conn.createStatement()
        stmt.execute(upsert_eta_sql)
        stmt.close()
        conn.close()
        
        print("Data ETA sukses disimpan ke PostgreSQL database (UPSERT berhasil)!")
    except Exception as ex:
        print(f"Gagal menulis data ETA ke database PostgreSQL. Error: {ex}")
        traceback.print_exc()

# 6. Memulai Streaming Query
print("Memulai streaming pemrosesan ETA Spasial...")
checkpoint_dir = "processing/checkpoints/bus_eta"
os.makedirs(checkpoint_dir, exist_ok=True)

# A. Tulis ke Database postgres (Serving/Gold Layer)
query = filtered_eta_df.writeStream \
    .foreachBatch(write_eta_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# ==============================================================================
# ### [MEDALLION ARCHITECTURE - BRONZE STORAGE LAYER (PARQUET ON LOCAL DISK)] ###
# ==============================================================================
# B. Tulis Raw Telemetry ke Lakehouse Bronze Layer (Format Parquet, Partisi berdasarkan Tanggal)
from pyspark.sql.functions import to_date
bronze_telemetry_df = processed_telemetry_df.withColumn("date", to_date(col("timestamp")))
checkpoint_bronze_telemetry = "processing/checkpoints/bronze_telemetry"
os.makedirs(checkpoint_bronze_telemetry, exist_ok=True)

query_bronze_telemetry = bronze_telemetry_df.writeStream \
    .format("parquet") \
    .partitionBy("date") \
    .option("path", "storage/lakehouse/bronze/telemetry") \
    .option("checkpointLocation", checkpoint_bronze_telemetry) \
    .outputMode("append") \
    .start()

# Standby menangkap data secara kontinu
spark.streams.awaitAnyTermination()
