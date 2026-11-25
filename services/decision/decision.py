"""Karar servisi: eşik tabanlı adaptif sinyal kontrolü.

TOPIC_METRICS'ten pencere göstergelerini okur, her (run_id, kavşak) için durum tutar:
  - yoğunluk düzeyi (low/medium/high), histerezisli eşiklerle
  - yüksek düzeyin ardışık pencere sayısı
  - son plan değişikliğinin simülasyon zamanı (minimum kontrol aralığı için)
Gerekirse TOPIC_COMMANDS'a plan değişikliği yazar ve kararı ClickHouse'a kaydeder.
fixed modda komut göndermez.
"""
import json
import os
import time
from dataclasses import dataclass

import clickhouse_connect
import yaml
from confluent_kafka import Consumer, Producer

E = os.environ
with open(E.get("CONFIG_PATH", "/config/config.yaml"), encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
CTRL = CFG["control"]
TH = CTRL["thresholds"]
PROG = CFG["programs"]


@dataclass
class TlsState:
    level: str = "low"
    high_streak: int = 0
    last_window_end: str = ""
    last_switch_sim: float = -1e9
    program: str = PROG["normal"]
    pending: dict | None = None        # henüz kapanmamış pencerenin son güncellemesi


def level_of(q: float, prev: str) -> str:
    """Histerezis: üst düzeye çıkmak için *_enter, düzeyde kalmak için daha düşük *_exit eşiği."""
    if q >= TH["high_enter"] or (prev == "high" and q >= TH["high_exit"]):
        return "high"
    if q >= TH["medium_enter"] or (prev in ("high", "medium") and q >= TH["medium_exit"]):
        return "medium"
    return "low"


def target_program(level: str, m: dict) -> str:
    if level == "high":
        return PROG["main_priority"] if m["avg_queue_main"] >= m["avg_queue_side"] else PROG["side_priority"]
    return CTRL["plan_for_level"][level]


def now_ms() -> int:
    return int(time.time() * 1000)


def connect_ch():
    for attempt in range(30):
        try:
            return clickhouse_connect.get_client(
                host=E["CLICKHOUSE_HOST"], port=int(E["CLICKHOUSE_PORT"]),
                username=E["CLICKHOUSE_USER"], password=E["CLICKHOUSE_PASSWORD"],
                database=E["CLICKHOUSE_DB"])
        except Exception as exc:  # noqa: BLE001
            print(f"ClickHouse bekleniyor ({attempt}): {exc}", flush=True)
            time.sleep(2)
    raise SystemExit("ClickHouse'a bağlanılamadı")


def main() -> None:
    ch = connect_ch()
    producer = Producer({"bootstrap.servers": E["KAFKA_BOOTSTRAP"]})
    consumer = Consumer({"bootstrap.servers": E["KAFKA_BOOTSTRAP"], "group.id": "decision-service",
                         "auto.offset.reset": "latest"})
    consumer.subscribe([E["TOPIC_METRICS"]])
    states: dict[tuple[str, str], TlsState] = {}
    n_msgs, last_report = 0, time.time()
    print(f"karar servisi hazır. eşikler={TH} ardışık={CTRL['n_consecutive']} "
          f"min_aralık={CTRL['min_control_interval_s']}s", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"kafka hata: {msg.error()}", flush=True)
            continue
        m = json.loads(msg.value())
        n_msgs += 1
        if time.time() - last_report > 30:
            levels = {k[1]: (v.level, v.program) for k, v in states.items() if k[0] == m["run_id"]}
            print(f"[{m['run_id']}] {n_msgs} gösterge mesajı alındı, durum={levels}", flush=True)
            last_report = time.time()
        key = (m["run_id"], m["tls_id"])
        st = states.setdefault(key, TlsState())

        # Spark 'update' modu, henüz kapanmamış (az veri içeren) pencereyi de gönderir.
        # Bir pencereyi ancak daha yeni bir pencere başladığında (yani veri zamanı o pencerenin
        # sonunu geçtiğinde) TAMAMLANMIŞ sayıp değerlendiriyoruz. Ek gecikme: en fazla bir SLIDE.
        if m["window_end"] < st.last_window_end:
            continue                       # eski pencerenin geç güncellemesi
        if m["window_end"] == st.last_window_end:
            st.pending = m                 # açık pencerenin güncel hali
            continue
        done, st.pending, st.last_window_end = st.pending, m, m["window_end"]
        if done is None:
            continue
        m = done                           # bundan sonrası tamamlanmış pencere üzerinden

        st.level = level_of(m["avg_queue"], st.level)
        st.high_streak = st.high_streak + 1 if st.level == "high" else 0

        if m["mode"] != "adaptive":
            continue

        want = target_program(st.level, m)
        streak_ok = st.level != "high" or st.high_streak >= CTRL["n_consecutive"]
        interval_ok = m["sim_time"] - st.last_switch_sim >= CTRL["min_control_interval_s"]
        if want == st.program or not (streak_ok and interval_ok):
            continue

        cmd = {"run_id": m["run_id"], "tls_id": m["tls_id"], "program": want, "reason": st.level,
               "decided_at": now_ms(), "source_produced_at": m["last_produced_at"],
               "sim_time": m["sim_time"], "avg_queue": m["avg_queue"]}
        producer.produce(E["TOPIC_COMMANDS"], key=m["tls_id"], value=json.dumps(cmd))
        producer.poll(0)
        ch.insert("decisions",
                  [(cmd["run_id"], cmd["tls_id"], want, st.program, st.level, float(m["avg_queue"]),
                    cmd["decided_at"], float(m["sim_time"]))],
                  column_names=["run_id", "tls_id", "program", "prev_program", "reason", "avg_queue",
                                "decided_at", "sim_time"])
        print(f"{m['run_id']} {m['tls_id']}: {st.program} -> {want} "
              f"(düzey={st.level}, kuyruk={m['avg_queue']:.1f}, t={m['sim_time']:.0f})", flush=True)
        st.program, st.last_switch_sim = want, m["sim_time"]


if __name__ == "__main__":
    main()
