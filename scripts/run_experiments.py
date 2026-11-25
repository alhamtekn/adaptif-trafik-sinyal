"""Tüm deneyleri sırayla koşturur: senaryo x kontrol modu x tohum.

Önce altyapıyı başlatın:   docker compose up -d --build kafka clickhouse spark decision-service grafana
Sonra (bilgisayarınızda):  python scripts/run_experiments.py
Örnek alt küme:            python scripts/run_experiments.py --scenarios surge --seeds 1 --sim-end 600

Her koşunun SUMO çıktıları results/<run_id>_*.xml olarak kaydedilir.
"""
import argparse
import itertools
import subprocess
import sys
import time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", nargs="+", default=["normal", "surge", "side_surge", "lane_closure"])
    ap.add_argument("--modes", nargs="+", default=["fixed", "adaptive"])
    ap.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3])
    ap.add_argument("--sim-end", type=int, default=3600)
    ap.add_argument("--speedup", type=float, default=None, help=".env'deki SIM_SPEEDUP'ı ezer")
    a = ap.parse_args()

    runs = list(itertools.product(a.scenarios, a.modes, a.seeds))
    print(f"{len(runs)} koşu planlandı")
    for i, (scen, mode, seed) in enumerate(runs, 1):
        run_id = f"{scen}_{mode}_seed{seed}"
        cmd = ["docker", "compose", "run", "--rm", "--no-deps",
               "-e", f"SCENARIO={scen}", "-e", f"CONTROL_MODE={mode}", "-e", f"SEED={seed}",
               "-e", f"RUN_ID={run_id}", "-e", f"SIM_END={a.sim_end}"]
        if a.speedup:
            cmd += ["-e", f"SIM_SPEEDUP={a.speedup}"]
        cmd.append("sumo-bridge")
        print(f"[{i}/{len(runs)}] {run_id}", flush=True)
        t0 = time.time()
        r = subprocess.run(cmd)
        if r.returncode != 0:
            print(f"HATA: {run_id} kod {r.returncode}", file=sys.stderr)
            return r.returncode
        print(f"  {time.time() - t0:.0f} sn sürdü")
        time.sleep(5)   # son pencerelerin işlenmesi için
    return 0


if __name__ == "__main__":
    sys.exit(main())
