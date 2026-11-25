#!/bin/sh
# Geçici test ağı: 250 m aralıklı, 3 ışıklı kavşaktan oluşan düz bir koridor.
# Gerçek konumu seçince build_net_from_osm.sh kullanın ve config/config.yaml'ı güncelleyin.
# Çalıştırma: docker compose run --rm --no-deps sumo-bridge sh /sumo/build_placeholder_net.sh
set -e
cd "$(dirname "$0")"
netgenerate --grid --grid.x-number 3 --grid.y-number 1 --grid.x-length 250 \
  --grid.attach-length 200 --default.lanenumber 2 --default.speed 13.89 \
  --tls.guess true --seed 42 -o net/corridor.net.xml
python /scripts/gen_tls_programs.py
