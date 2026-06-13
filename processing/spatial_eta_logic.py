import os
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit,
    radians, sin, cos, asin, sqrt, pow as spark_pow, concat_ws, row_number
)
from pyspark.sql.window import Window


# 1. Konfigurasi Awal & Host Resolution
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

spark = SparkSession.builder \
    .appName("ShelterEye-SpatialETAProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session berhasil diinisialisasi (Spatial & ETA Processor)!")

DB_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/db_sheltereye"
DB_PROPS = {
    "user": "admin",
    "password": "sheltereyepassword",
    "driver": "org.postgresql.Driver"
}


# 1b. Inisialisasi Tabel bus_eta (jaga-jaga kalau passenger_logic.py
#     belum dijalankan duluan oleh Anggota 2)
def init_bus_eta_table():
    try:
        jvm = spark._jvm
        conn_props = jvm.java.util.Properties()
        for k, v in DB_PROPS.items():
            conn_props.setProperty(k, v)
        driver = jvm.org.postgresql.Driver()
        conn = driver.connect(DB_URL, conn_props)
        stmt = conn.createStatement()
        stmt.execute("""
            CREATE TABLE IF NOT EXISTS bus_eta (
                halte_name   VARCHAR(100),
                bus_id       VARCHAR(50),
                eta_minutes  DOUBLE PRECISION,
                is_empty     BOOLEAN,
                last_updated TIMESTAMP,
                PRIMARY KEY (halte_name, bus_id)
            )
        """)
        stmt.close()
        conn.close()
        print("Tabel bus_eta siap!")
    except Exception as e:
        print(f"Peringatan init tabel: {e}")

init_bus_eta_table()


# 2. Master Data Koordinat Halte Tujuan (static, di-cache)
HALTE_COORDINATES = [
    ("Dukuh Atas 1",     -6.2001, 106.8000),
    ("Karet Sudirman",   -6.2049, 106.8093),
    ("Monas",            -6.1754, 106.8272),
    ("Harmoni Central",  -6.1654, 106.8163),
    ("Manggarai",        -6.2127, 106.8500),
    ("Kampung Melayu",   -6.2249, 106.8597),
    ("Senen",            -6.1755, 106.8417),
]

halte_df = spark.createDataFrame(
    HALTE_COORDINATES,
    ["target_halte_name", "halte_lat", "halte_lon"]
)
halte_df.cache()


# 3. Membaca Stream dari Kafka
print("Menghubungkan ke Kafka topic 'topic-transjakarta'...")
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
    .option("subscribe", "topic-transjakarta") \
    .option("startingOffsets", "latest") \
    .load()

transaction_schema = """
    transID STRING, payCardID STRING, payCardBank STRING,
    payCardName STRING, payCardSex STRING, payCardBirthDate STRING,
    corridorID STRING, corridorName STRING, direction STRING,
    tapInTime STRING, tapInStops STRING, tapInStopsName STRING,
    tapInStopsLat STRING, tapInStopsLon STRING,
    stopStartSeq STRING, tapOutTime STRING,
    tapOutStops STRING, tapOutStopsName STRING,
    tapOutStopsLat STRING, tapOutStopsLon STRING,
    stopEndSeq STRING, payAmount STRING,
    streaming_timestamp STRING
"""

parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), transaction_schema).alias("data")) \
    .select("data.*")

bus_position_df = parsed_stream_df \
    .withColumn("event_time", to_timestamp(col("streaming_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("event_time", "10 minutes") \
    .withColumn("bus_lat", col("tapOutStopsLat").cast("double")) \
    .withColumn("bus_lon", col("tapOutStopsLon").cast("double")) \
    .filter(
        col("corridorID").isNotNull() &
        col("bus_lat").isNotNull() &
        col("bus_lon").isNotNull()
    ) \
    .select(
        concat_ws("-", col("corridorID"), col("direction")).alias("bus_id"),
        col("bus_lat"),
        col("bus_lon"),
        col("event_time")
    )


# 4. Haversine: hitung jarak & ETA semua kombinasi bus-halte
#    CATATAN: row_number() TIDAK boleh di streaming DF.
#    Pemilihan halte terdekat per bus dilakukan di dalam foreachBatch.
EARTH_RADIUS_KM = 6371.0
AVG_BUS_SPEED_KMH = 20.0

cross_df = bus_position_df.crossJoin(halte_df)

eta_raw_df = cross_df \
    .withColumn("dlat", radians(col("halte_lat") - col("bus_lat"))) \
    .withColumn("dlon", radians(col("halte_lon") - col("bus_lon"))) \
    .withColumn(
        "haversine_a",
        spark_pow(sin(col("dlat") / 2), 2) +
        cos(radians(col("bus_lat"))) * cos(radians(col("halte_lat"))) *
        spark_pow(sin(col("dlon") / 2), 2)
    ) \
    .withColumn("distance_km", lit(2 * EARTH_RADIUS_KM) * asin(sqrt(col("haversine_a")))) \
    .withColumn("eta_minutes", (col("distance_km") / lit(AVG_BUS_SPEED_KMH)) * 60.0) \
    .select(
        col("target_halte_name").alias("halte_name"),
        col("bus_id"),
        col("eta_minutes"),
        col("event_time").alias("last_updated")
    )


# 5. foreachBatch: filter halte terdekat + UPSERT ke PostgreSQL
#    row_number() aman di sini karena batch_df sudah static DataFrame
def write_eta_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n--- [Batch ID: {batch_id}] Memproses Spatial & ETA Logic ---")

    # Step A: ambil halte TERDEKAT per bus (row_number OK di static DF)
    nearest_window = Window.partitionBy("bus_id").orderBy(col("eta_minutes").asc())
    nearest_df = batch_df \
        .withColumn("rn", row_number().over(nearest_window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Step B: dedup per (halte_name, bus_id) -> ambil event terbaru di batch
    dedup_window = Window.partitionBy("halte_name", "bus_id").orderBy(col("last_updated").desc())
    deduplicated_df = nearest_df \
        .withColumn("rn", row_number().over(dedup_window)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .withColumn("is_empty", lit(True))

    print("Live Data ETA Bus per Halte:")
    deduplicated_df.show(truncate=False)

    try:
        # Tulis ke tabel temporary dulu
        deduplicated_df.select(
            "halte_name", "bus_id", "eta_minutes", "is_empty", "last_updated"
        ).write.jdbc(url=DB_URL, table="temp_bus_eta", mode="overwrite", properties=DB_PROPS)

        upsert_sql = """
            INSERT INTO bus_eta (halte_name, bus_id, eta_minutes, is_empty, last_updated)
            SELECT halte_name, bus_id, eta_minutes, is_empty, last_updated
            FROM temp_bus_eta
            ON CONFLICT (halte_name, bus_id)
            DO UPDATE SET
                eta_minutes  = EXCLUDED.eta_minutes,
                is_empty     = EXCLUDED.is_empty,
                last_updated = EXCLUDED.last_updated
        """

        jvm = spark._jvm
        conn_props = jvm.java.util.Properties()
        for k, v in DB_PROPS.items():
            conn_props.setProperty(k, v)

        driver = jvm.org.postgresql.Driver()
        conn = driver.connect(DB_URL, conn_props)
        stmt = conn.createStatement()
        stmt.execute(upsert_sql)
        stmt.close()
        conn.close()

        print("Data ETA bus sukses di-UPSERT ke tabel bus_eta!")
    except Exception as ex:
        print(f"Gagal menulis ke PostgreSQL: {ex}")


# 6. Jalankan Streaming Query
print("Memulai streaming Spatial & ETA Logic...")
checkpoint_dir = "processing/checkpoints/spatial_eta"
os.makedirs(checkpoint_dir, exist_ok=True)

query = eta_raw_df.writeStream \
    .foreachBatch(write_eta_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()