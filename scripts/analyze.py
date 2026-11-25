"""SUMO çıktılarından sabit zamanlı / adaptif karşılaştırma tablosu üretir.

Kullanım:  python scripts/analyze.py   (gereken: pip install sumolib pandas matplotlib)
Çıktılar:  results/summary_runs.csv, results/comparison.csv, results/fig_*.png

Trafik metrikleri SUMO'nun kendi tripinfo çıktısından hesaplanır (Spark'tan değil).
"""
import glob
import json
import os

import pandas as pd
from sumolib.xml import parse

RES = os.path.join(os.path.dirname(__file__), "..", "results")
COLS = ["duration", "waitingTime", "timeLoss", "waitingCount", "routeLength"]
# Düşük olması iyi olan metrikler (iyileşme % = (fixed - adaptive) / fixed)
LOWER_BETTER = ["travel_time", "waiting_time", "time_loss", "stops"]


def run_metrics(prefix: str) -> dict:
    meta = json.load(open(prefix + "_meta.json", encoding="utf-8"))
    # parse_fast özniteliklerin dosyadaki sırasına bağlıdır ve sessizce boş döner; parse sıradan bağımsız
    t = pd.DataFrame([[getattr(e, c) for c in COLS] for e in parse(prefix + "_tripinfo.xml", "tripinfo")],
                     columns=COLS).astype(float)
    return {
        "run_id": meta["run_id"], "scenario": meta["scenario"], "mode": meta["mode"], "seed": meta["seed"],
        "trips": len(t),
        "throughput_veh_h": len(t) / meta["sim_end"] * 3600,
        "travel_time": t.duration.mean(),
        "waiting_time": t.waitingTime.mean(),
        "time_loss": t.timeLoss.mean(),
        "stops": t.waitingCount.mean(),
        "avg_speed": (t.routeLength / t.duration).mean(),
    }


def main() -> None:
    prefixes = [p[: -len("_meta.json")] for p in glob.glob(os.path.join(RES, "*_meta.json"))]
    prefixes = [p for p in prefixes if os.path.exists(p + "_tripinfo.xml")]
    if not prefixes:
        print("results/ içinde tamamlanmış koşu yok")
        return
    runs = pd.DataFrame([run_metrics(p) for p in prefixes]).sort_values(["scenario", "mode", "seed"])
    if (runs.trips == 0).any():
        print("UYARI: araç tamamlamamış koşular var:", list(runs.run_id[runs.trips == 0]))
    runs.to_csv(os.path.join(RES, "summary_runs.csv"), index=False)

    metrics = ["throughput_veh_h", "travel_time", "waiting_time", "time_loss", "stops", "avg_speed"]
    agg = runs.groupby(["scenario", "mode"])[metrics].agg(["mean", "std"])
    print(agg.round(2).to_string())

    rows = []
    for scen in runs.scenario.unique():
        try:
            f = agg.loc[(scen, "fixed")]
            a = agg.loc[(scen, "adaptive")]
        except KeyError:
            continue
        for m in metrics:
            fv, av = f[(m, "mean")], a[(m, "mean")]
            imp = (fv - av) / fv * 100 if m in LOWER_BETTER else (av - fv) / fv * 100
            rows.append({"scenario": scen, "metric": m, "fixed": fv, "adaptive": av, "improvement_pct": imp})
    comp = pd.DataFrame(rows)
    comp.to_csv(os.path.join(RES, "comparison.csv"), index=False)
    print("\nİyileşme (%; pozitif = adaptif daha iyi)")
    print(comp.pivot(index="metric", columns="scenario", values="improvement_pct").round(1).to_string())

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return
    for m in ["waiting_time", "travel_time", "time_loss", "stops"]:
        mean = agg[(m, "mean")].unstack("mode")
        std = agg[(m, "std")].unstack("mode")
        ax = mean.plot.bar(yerr=std, capsize=3, figsize=(7, 4), rot=0)
        ax.set_ylabel(m)
        ax.set_title(f"{m}: sabit zamanlı vs adaptif (3 tohum ortalaması ± std)")
        plt.tight_layout()
        plt.savefig(os.path.join(RES, f"fig_{m}.png"), dpi=150)
        plt.close()


if __name__ == "__main__":
    main()
