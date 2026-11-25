#!/bin/sh
# Gerçek konum: net/map.osm dosyasını openstreetmap.org -> Export ile indirip buraya koyun.
# Çalıştırma: docker compose run --rm --no-deps sumo-bridge sh /sumo/build_net_from_osm.sh
# Sonra: netedit ile kontrol edin, config/config.yaml içindeki tls / main_edges / senaryo edge'lerini güncelleyin.
set -e
cd "$(dirname "$0")"
netconvert --osm-files net/map.osm -o net/corridor.net.xml \
  --geometry.remove --ramps.guess --junctions.join \
  --tls.guess-signals --tls.discard-simple --tls.join --tls.default-type static \
  --keep-edges.by-vclass passenger --remove-edges.isolated --output.street-names
python /scripts/gen_tls_programs.py
