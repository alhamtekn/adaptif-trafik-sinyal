-- ClickHouse bu dosyayı yalnızca veri klasörü BOŞKEN çalıştırır.
-- Şemayı değiştirdiyseniz: docker compose down -v  (tüm ClickHouse verisini siler)
CREATE DATABASE IF NOT EXISTS traffic;

-- Spark 'update' modunda aynı pencereyi birden fazla kez yazar; ReplacingMergeTree en son satırı tutar.
-- Sorgularda FINAL kullanın: SELECT ... FROM traffic.tls_metrics FINAL
CREATE TABLE IF NOT EXISTS traffic.tls_metrics
(
    run_id           String,
    scenario         LowCardinality(String),
    mode             LowCardinality(String),
    tls_id           LowCardinality(String),
    window_start     DateTime64(3, 'UTC'),
    window_end       DateTime64(3, 'UTC'),
    n_records        UInt32,
    avg_queue        Float32,
    max_queue        UInt16,
    avg_queue_main   Float32,
    avg_queue_side   Float32,
    avg_wait         Float32,
    avg_speed        Float32,
    occupancy        Float32,
    sim_time         Float64,
    last_produced_at Int64,
    active_program   LowCardinality(String),
    inserted_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (run_id, tls_id, window_start);

CREATE TABLE IF NOT EXISTS traffic.decisions
(
    run_id       String,
    tls_id       LowCardinality(String),
    program      LowCardinality(String),
    prev_program LowCardinality(String),
    reason       LowCardinality(String),
    avg_queue    Float32,
    decided_at   Int64,
    sim_time     Float64,
    decided_ts   DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(decided_at)
)
ENGINE = MergeTree
ORDER BY (run_id, tls_id, decided_at);

CREATE TABLE IF NOT EXISTS traffic.control_applied
(
    run_id             String,
    tls_id             LowCardinality(String),
    program            LowCardinality(String),
    reason             LowCardinality(String),
    source_produced_at Int64,
    decided_at         Int64,
    applied_at         Int64,
    applied_sim_time   Float64,
    applied_ts         DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(applied_at),
    latency_ms         Int64 MATERIALIZED applied_at - source_produced_at,
    decision_ms        Int64 MATERIALIZED decided_at - source_produced_at
)
ENGINE = MergeTree
ORDER BY (run_id, applied_at);
