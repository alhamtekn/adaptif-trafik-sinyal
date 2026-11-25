"""Spark Structured Streaming: kavşak düzeyinde pencere göstergeleri.

Akış 1: TOPIC_RAW -> pencere bazlı göstergeler -> TOPIC_METRICS (karar servisi) + ClickHouse tls_metrics
Akış 2: TOPIC_APPLIED -> ClickHouse control_applied (uçtan uca gecikme kayıtları)

Bu iş yalnızca ANALİZ yapar; sinyal kararı vermez.
"""
import os
import time

import clickhouse_connect
from pyspark.sql import SparkSession, functions as F, types as T

E = os.environ
BOOT = E["KAFKA_BOOTSTRAP"]

spark = (SparkSession.builder.appName("tls-metrics")
         .config("spark.sql.shuffle.partitions", "4")      # varsayılan 200 her paketi yavaşlatır
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

_ch = None


def ch():
    global _ch
    if _ch is None:
        for attempt in range(30):
            try:
                _ch = clickhouse_connect.get_client(
                    host=E["CLICKHOUSE_HOST"], port=int(E["CLICKHOUSE_PORT"]),
                    username=E["CLICKHOUSE_USER"], password=E["CLICKHOUSE_PASSWORD"],
                    database=E["CLICKHOUSE_DB"])
                break
            except Exception as exc:  # noqa: BLE001
                print(f"ClickHouse bekleniyor ({attempt}): {exc}", flush=True)
                time.sleep(2)
    return _ch


raw_schema = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("scenario", T.StringType()),
    T.StructField("mode", T.StringType()), T.StructField("tls_id", T.StringType()),
    T.StructField("sim_time", T.DoubleType()), T.StructField("produced_at", T.LongType()),
    T.StructField("queue", T.IntegerType()), T.StructField("queue_main", T.IntegerType()),
    T.StructField("queue_side", T.IntegerType()), T.StructField("vehicle_count", T.IntegerType()),
    T.StructField("waiting_time", T.DoubleType()), T.StructField("mean_speed", T.DoubleType()),
    T.StructField("occupancy", T.DoubleType()), T.StructField("active_program", T.StringType()),
])


def kafka_stream(topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOT)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())


raw = (kafka_stream(E["TOPIC_RAW"])
       .select(F.from_json(F.col("value").cast("string"), raw_schema).alias("d")).select("d.*")
       .withColumn("event_ts", (F.col("produced_at") / 1000).cast("timestamp")))

metrics = (raw.withWatermark("event_ts", E["WATERMARK"])
           .groupBy(F.window("event_ts", E["WINDOW"], E["SLIDE"]), "run_id", "scenario", "mode", "tls_id")
           .agg(F.count(F.lit(1)).alias("n_records"),
                F.avg("queue").alias("avg_queue"),
                F.max("queue").alias("max_queue"),
                F.avg("queue_main").alias("avg_queue_main"),
                F.avg("queue_side").alias("avg_queue_side"),
                (F.sum("waiting_time") / F.greatest(F.sum("vehicle_count"), F.lit(1))).alias("avg_wait"),
                F.avg("mean_speed").alias("avg_speed"),
                F.avg("occupancy").alias("occupancy"),
                F.max("sim_time").alias("sim_time"),
                F.max("produced_at").alias("last_produced_at"),
                F.last("active_program").alias("active_program"))
           .select("run_id", "scenario", "mode", "tls_id",
                   F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                   "n_records", "avg_queue", "max_queue", "avg_queue_main", "avg_queue_side",
                   "avg_wait", "avg_speed", "occupancy", "sim_time", "last_produced_at", "active_program"))

METRIC_COLS = ["run_id", "scenario", "mode", "tls_id", "window_start", "window_end", "n_records",
               "avg_queue", "max_queue", "avg_queue_main", "avg_queue_side", "avg_wait", "avg_speed",
               "occupancy", "sim_time", "last_produced_at", "active_program"]


def sink_metrics(df, batch_id):
    df = df.cache()
    rows = [tuple(r) for r in df.select(*METRIC_COLS).collect()]
    if rows:
        # Karar servisi pencereleri zaman sırasıyla alsın diye sıralı ve tek görevle yazılır
        (df.orderBy("window_end").coalesce(1)
           .select(F.col("tls_id").alias("key"), F.to_json(F.struct(*METRIC_COLS)).alias("value"))
           .write.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("topic", E["TOPIC_METRICS"]).save())
        ch().insert("tls_metrics", rows, column_names=METRIC_COLS)
        print(f"batch {batch_id}: {len(rows)} pencere", flush=True)
    df.unpersist()


applied_schema = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("tls_id", T.StringType()),
    T.StructField("program", T.StringType()), T.StructField("reason", T.StringType()),
    T.StructField("source_produced_at", T.LongType()), T.StructField("decided_at", T.LongType()),
    T.StructField("applied_at", T.LongType()), T.StructField("applied_sim_time", T.DoubleType()),
])
APPLIED_COLS = ["run_id", "tls_id", "program", "reason", "source_produced_at", "decided_at",
                "applied_at", "applied_sim_time"]

applied = (kafka_stream(E["TOPIC_APPLIED"])
           .select(F.from_json(F.col("value").cast("string"), applied_schema).alias("d")).select("d.*"))


def sink_applied(df, batch_id):
    rows = [tuple(r) for r in df.select(*APPLIED_COLS).collect()]
    if rows:
        ch().insert("control_applied", rows, column_names=APPLIED_COLS)


chk = E.get("CHECKPOINT_DIR", "/tmp/chk")
(metrics.writeStream.outputMode("update").foreachBatch(sink_metrics)
 .option("checkpointLocation", f"{chk}/metrics").trigger(processingTime=E["TRIGGER"]).start())
(applied.writeStream.outputMode("append").foreachBatch(sink_applied)
 .option("checkpointLocation", f"{chk}/applied").trigger(processingTime=E["TRIGGER"]).start())

print("Spark akışları başladı", flush=True)
spark.streams.awaitAnyTermination()
