"""Ağdaki her sinyal için üç program üretir: normal, main_priority, side_priority.

- normal: netconvert/netgenerate'in ürettiği varsayılan program (süreler aynen korunur)
- main_priority: ana arter yönüne yeşil veren fazı shift_s kadar uzatır, diğer büyük yeşil fazı o kadar kısaltır
- side_priority: tersi

Çevrim süresi her üç programda aynı kalır; bu, kavşaklar arası koordinasyonu korur.
Ana yön fazı, config.yaml'daki sumo.main_edges listesinden otomatik bulunur.

Çalıştırma (konteyner içinde): python /scripts/gen_tls_programs.py
"""
import os
import sys
from xml.sax.saxutils import quoteattr

import sumolib
import yaml

CONFIG = os.environ.get("CONFIG_PATH", "/config/config.yaml")
SUMO_DIR = os.environ.get("SUMO_DIR", "/sumo")


def main() -> int:
    with open(CONFIG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    net_path = os.path.join(SUMO_DIR, cfg["sumo"]["net_file"])
    out_path = os.path.join(SUMO_DIR, cfg["sumo"]["programs_file"])
    main_edges = set(cfg["sumo"]["main_edges"])
    names = cfg["programs"]
    shift, min_green = int(names["shift_s"]), int(names["min_green_s"])

    net = sumolib.net.readNet(net_path, withPrograms=True)
    wanted = set(cfg["tls"].values())
    found = {t.getID(): t for t in net.getTrafficLights()}
    missing = wanted - set(found)
    if missing:
        print(f"HATA: config.yaml'daki şu sinyaller ağda yok: {sorted(missing)}", file=sys.stderr)
        print(f"Ağdaki sinyaller: {sorted(found)}", file=sys.stderr)
        return 1

    lines = ["<additional>"]
    for tls_id in sorted(wanted):
        tls = found[tls_id]
        base = next(iter(tls.getPrograms().values()))
        phases = [(int(round(p.duration)), p.state) for p in base.getPhases()]

        # link index -> gelen edge id
        link_edge = {idx: in_lane.getEdge().getID() for in_lane, _out, idx in tls.getConnections()}

        def main_score(state: str) -> int:
            return sum(1 for i, c in enumerate(state) if c in "Gg" and link_edge.get(i) in main_edges)

        majors = [i for i, (d, s) in enumerate(phases) if "y" not in s and any(c in "Gg" for c in s) and d >= 2 * min_green]
        if len(majors) < 2:
            print(f"UYARI: {tls_id} için iki büyük yeşil faz bulunamadı; öncelikli planlar = normal", file=sys.stderr)
            main_i = side_i = None
        else:
            main_i = max(majors, key=lambda i: main_score(phases[i][1]))
            side_i = max((i for i in majors if i != main_i), key=lambda i: phases[i][0])

        def variant(boost_i, cut_i):
            ph = list(phases)
            if boost_i is None:
                return ph
            delta = min(shift, ph[cut_i][0] - min_green)
            ph[boost_i] = (ph[boost_i][0] + delta, ph[boost_i][1])
            ph[cut_i] = (ph[cut_i][0] - delta, ph[cut_i][1])
            return ph

        programs = {
            names["normal"]: phases,
            names["main_priority"]: variant(main_i, side_i),
            names["side_priority"]: variant(side_i, main_i),
        }
        for pid, ph in programs.items():
            lines.append(f'    <tlLogic id={quoteattr(tls_id)} type="static" programID="{pid}" offset="0">')
            for d, s in ph:
                lines.append(f'        <phase duration="{d}" state="{s}"/>')
            lines.append("    </tlLogic>")
        cyc = sum(d for d, _ in phases)
        print(f"{tls_id}: çevrim={cyc}s, ana faz={main_i}, yan faz={side_i}, bağlantı={len(phases[0][1])}")
    lines.append("</additional>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"yazıldı: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
