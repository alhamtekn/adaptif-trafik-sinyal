"""SUMO <-> Kafka köprüsü.

- SUMO'yu TraCI ile adım adım ilerletir (SIM_SPEEDUP ile gerçek zamana göre hızlandırılmış).
- Her adımda kavşak başına ölçümleri TOPIC_RAW'a yazar.
- TOPIC_COMMANDS'tan gelen plan değişikliklerini uygular ve TOPIC_APPLIED'a kaydeder.
- Senaryo olaylarını (şerit kapatma vb.) zamanı gelince uygular.

TraCI'ye bağlanan TEK servis budur; kontrol kararı burada verilmez.
"""
import json
import os
import signal
import sys
import time

import traci
import yaml
from confluent_kafka import Consumer, Producer

ENV = os.environ
CONFIG_PATH = ENV.get("CONFIG_PATH", "/config/config.yaml")
SUMO_DIR = ENV.get("SUMO_DIR", "/sumo")
RESULTS_DIR = ENV.get("RESULTS_DIR", "/results")

SCENARIO = ENV["SCENARIO"]
MODE = ENV["CONTROL_MODE"]                      # fixed | adaptive
SEED = int(ENV["SEED"])
SIM_END = int(ENV.get("SIM_END", "3600"))
SPEEDUP = float(ENV.get("SIM_SPEEDUP", "5"))
RUN_ID = ENV.get("RUN_ID") or f"{SCENARIO}_{MODE}_seed{SEED}_{int(time.time())}"

TOPIC_RAW = ENV["TOPIC_RAW"]
TOPIC_COMMANDS = ENV["TOPIC_COMMANDS"]
TOPIC_APPLIED = ENV["TOPIC_APPLIED"]


def now_ms() -> int:
    return int(time.time() * 1000)


def log(msg: str) -> None:
    print(f"[bridge {RUN_ID}] {msg}", flush=True)


def main() -> int:
    if MODE not in ("fixed", "adaptive"):
        log(f"CONTROL_MODE fixed veya adaptive olmalı, gelen: {MODE}")
        return 2
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    scen = cfg["scenarios"][SCENARIO]
    tls_map = cfg["tls"]                                   # kısa ad -> sumo id
    sumo_to_short = {v: k for k, v in tls_map.items()}
    main_edges = set(cfg["sumo"]["main_edges"])
    step_len = float(cfg["sumo"]["step_length"])
    normal_prog = cfg["programs"]["normal"]
    events = sorted(scen.get("events", []), key=lambda e: e["time"])

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out = os.path.join(RESULTS_DIR, RUN_ID)
    sumo_cmd = [
        "sumo", "-c", os.path.join(SUMO_DIR, scen["sumocfg"]),
        "--seed", str(SEED), "--end", str(SIM_END), "--step-length", str(step_len),
        "--tripinfo-output", f"{out}_tripinfo.xml",
        "--summary-output", f"{out}_summary.xml",
        "--queue-output", f"{out}_queue.xml",
    ]
    with open(f"{out}_meta.json", "w", encoding="utf-8") as f:
        json.dump({"run_id": RUN_ID, "scenario": SCENARIO, "mode": MODE, "seed": SEED,
                   "sim_end": SIM_END, "speedup": SPEEDUP, "sumo_cmd": sumo_cmd,
                   "config": cfg, "started_at": now_ms()}, f, ensure_ascii=False, indent=2)

    producer = Producer({"bootstrap.servers": ENV["KAFKA_BOOTSTRAP"], "linger.ms": 20})
    consumer = Consumer({"bootstrap.servers": ENV["KAFKA_BOOTSTRAP"],
                         "group.id": f"sumo-bridge-{RUN_ID}",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe([TOPIC_COMMANDS])

    log(f"SUMO başlıyor: {' '.join(sumo_cmd)}")
    traci.start(sumo_cmd)
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    lanes, main_lanes = {}, {}
    for short, tid in tls_map.items():
        ls = sorted(set(traci.trafficlight.getControlledLanes(tid)))
        lanes[tid] = ls
        main_lanes[tid] = {l for l in ls if traci.lane.getEdgeID(l) in main_edges}
        traci.trafficlight.setProgram(tid, normal_prog)    # iki modda da aynı başlangıç planı
    log(f"kavşaklar: {tls_map}, mod={MODE}, hız x{SPEEDUP}")

    step_wall = step_len / SPEEDUP
    next_t = time.perf_counter()
    sent = applied = 0
    ev_i = 0

    while not stop["flag"] and traci.simulation.getTime() < SIM_END:
        traci.simulationStep()
        sim_t = traci.simulation.getTime()

        # Senaryo olayları
        while ev_i < len(events) and events[ev_i]["time"] <= sim_t:
            ev = events[ev_i]
            if ev["action"] == "close_lane":
                traci.lane.setDisallowed(ev["lane"], ["passenger"])
            elif ev["action"] == "open_lane":
                traci.lane.setDisallowed(ev["lane"], [])
            log(f"olay t={sim_t:.0f}: {ev}")
            ev_i += 1

        # Ölçümler
        ts = now_ms()
        for tid, ls in lanes.items():
            q_main = q_side = n = 0
            wait = speed = occ = 0.0
            for l in ls:
                h = traci.lane.getLastStepHaltingNumber(l)
                if l in main_lanes[tid]:
                    q_main += h
                else:
                    q_side += h
                n += traci.lane.getLastStepVehicleNumber(l)
                wait += traci.lane.getWaitingTime(l)
                speed += traci.lane.getLastStepMeanSpeed(l)
                occ += traci.lane.getLastStepOccupancy(l)
            msg = {
                "run_id": RUN_ID, "scenario": SCENARIO, "mode": MODE,
                "tls_id": sumo_to_short[tid], "sim_time": sim_t, "produced_at": ts,
                "queue": q_main + q_side, "queue_main": q_main, "queue_side": q_side,
                "vehicle_count": n, "waiting_time": wait,
                "mean_speed": speed / len(ls), "occupancy": occ / len(ls),
                "active_program": traci.trafficlight.getProgram(tid),
            }
            producer.produce(TOPIC_RAW, key=msg["tls_id"], value=json.dumps(msg))
            sent += 1
        producer.poll(0)

        # Komutlar (bloklamadan)
        while True:
            m = consumer.poll(0)
            if m is None:
                break
            if m.error():
                log(f"kafka hata: {m.error()}")
                break
            cmd = json.loads(m.value())
            if cmd.get("run_id") != RUN_ID or MODE != "adaptive":
                continue
            tid = tls_map.get(cmd["tls_id"])
            if tid is None:
                continue
            traci.trafficlight.setProgram(tid, cmd["program"])
            ack = {**cmd, "applied_at": now_ms(), "applied_sim_time": sim_t}
            producer.produce(TOPIC_APPLIED, key=cmd["tls_id"], value=json.dumps(ack))
            applied += 1
            log(f"t={sim_t:.0f} {cmd['tls_id']} -> {cmd['program']} "
                f"(gecikme {ack['applied_at'] - cmd['source_produced_at']} ms)")

        if int(sim_t) % 300 == 0:
            log(f"t={sim_t:.0f} gönderilen={sent} uygulanan={applied} araç={traci.vehicle.getIDCount()}")

        next_t += step_wall
        time.sleep(max(0.0, next_t - time.perf_counter()))

    traci.close()
    producer.flush(10)
    consumer.close()
    log(f"bitti. gönderilen={sent} uygulanan komut={applied}. çıktılar: {out}_*.xml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
