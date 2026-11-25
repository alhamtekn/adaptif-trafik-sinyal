# Gerçek Zamanlı Büyük Veri Tabanlı Adaptif Trafik Sinyal Kontrol Sistemi

SUMO ile simüle edilen, birbirine bağlı 3 sinyalize kavşaktan gelen veriyi Kafka ve Spark Structured Streaming ile gerçek zamanlı işleyen bir sistem. Kavşaklardaki yoğunluk eşikleri aştığında sinyal planını otomatik olarak değiştirir. Sabit zamanlı kontrol ile adaptif kontrol aynı koşullarda karşılaştırılır.

## Mimari

```
sumo-bridge ──► [traffic-raw] ──► spark ──► ClickHouse ◄── Grafana
     ▲                              │
     │                              └──► [tls-metrics] ──► decision-service
     └──────────── [signal-commands] ◄──────────────────────────┘
                   [control-applied] ──► spark ──► ClickHouse (gecikme kayıtları)
```

| Servis | Görev |
|---|---|
| `sumo-bridge` | SUMO'yu TraCI ile adım adım çalıştırır, ölçümleri Kafka'ya yazar, gelen plan komutlarını uygular. TraCI'ye bağlanan tek servis budur. |
| `spark` | Pencere bazlı kavşak göstergelerini hesaplar: kuyruk, bekleme, hız, doluluk. Kontrol kararı vermez. |
| `decision-service` | Eşik, histerezis, ardışık pencere ve minimum kontrol aralığı kurallarına göre plan seçer. |
| `clickhouse` | Göstergeleri, kararları ve gecikme kayıtlarını saklar. |
| `grafana` | Canlı dashboard. |
| `kafka` | Servisler arası mesaj akışı (KRaft modu, tek düğüm). |

## Gereksinimler

- Docker Desktop (Windows'ta WSL 2 altyapısıyla). Bellek ayarı için `docs/RAM.md` dosyasına bakın.
- En az 8 GB RAM'in Docker'a ayrılabilmesi önerilir. Servislerin toplam bellek üst sınırı yaklaşık 5 GB.
- Deney analizi için Python 3.11+ ve `pip install sumolib pandas matplotlib`.

## Hızlı başlangıç

```bash
cp .env.example .env
docker compose up -d --build
```

- Grafana: http://localhost:3000 (kullanıcı/şifre: `admin` / `admin`)
- ClickHouse sorgu ekranı: http://localhost:8123/play (kullanıcı/şifre: `traffic` / `traffic`)
- Bilgisayardan Kafka'ya bağlanmak için: `localhost:29092`

`docker compose up` komutu `.env` dosyasındaki senaryoyu bir kez koşturur. Bridge koşu bitince kapanır, diğer servisler çalışmaya devam eder.

Tek bir koşuyu elle başlatmak için:

```bash
docker compose run --rm -e SCENARIO=surge -e CONTROL_MODE=adaptive -e SEED=1 -e RUN_ID=surge_adaptive_seed1 sumo-bridge
```

## Deneyler

```bash
docker compose up -d --build kafka clickhouse spark decision-service grafana
python scripts/run_experiments.py              # 4 senaryo x 2 mod x 3 tohum = 24 koşu
python scripts/analyze.py                      # results/comparison.csv ve fig_*.png
```

Her koşunun SUMO çıktıları (`tripinfo`, `summary`, `queue`) ve ayarları `results/<run_id>_*` dosyalarına kaydedilir. Trafik metrikleri Spark'tan değil, SUMO'nun kendi çıktılarından hesaplanır.

| Senaryo | Açıklama |
|---|---|
| `normal` | Temel talep |
| `surge` | Ana yönde 1200–2400 sn arası ani talep artışı |
| `side_surge` | Orta kavşakta yan yoldan yüksek katılım |
| `lane_closure` | Orta kesitte 900–2700 sn arası bir şerit kapanması |

## Yol ağı

Repodaki `sumo/net/corridor.net.xml` **geçici bir test ağıdır** (`sumo/build_placeholder_net.sh`). Gerçek konuma geçmek için:

1. openstreetmap.org → Export ile bölgeyi indirip `sumo/net/map.osm` olarak kaydedin.
2. `docker compose run --rm --no-deps sumo-bridge sh /sumo/build_net_from_osm.sh`
3. Ağı `netedit` ile kontrol edin. Ardından `config/config.yaml` içinde şunları güncelleyin: `tls` eşlemesi, `main_edges`, senaryo olaylarındaki şerit ID'leri. Ayrıca `sumo/scenarios/*.rou.xml` içindeki edge ID'lerini değiştirin.
4. Sinyal programlarını yeniden üretin: `docker compose run --rm --no-deps sumo-bridge python /scripts/gen_tls_programs.py`

## Yapılandırma

- `.env`: servis adresleri, topic adları, Spark pencere süreleri, koşu parametreleri.
- `config/config.yaml`: kavşak eşlemesi, eşikler, histerezis, ardışık pencere sayısı, minimum kontrol aralığı, senaryolar.

Kodda gömülü (hard-coded) ayar yoktur.

## Klasör yapısı

```
config/            config.yaml
sumo/              ağ, sinyal programları, senaryolar, ağ üretim scriptleri
services/          sumo_bridge/, spark/, decision/
infra/             clickhouse/ (şema, düşük bellek ayarı), grafana/ (provisioning, dashboard)
scripts/           run_experiments.py, analyze.py, gen_tls_programs.py
results/           koşu çıktıları (git'e yalnızca özet CSV ve grafikler girer)
docs/              plan, mimari, RAM ayarları
```

## Sorun giderme

| Belirti | Çözüm |
|---|---|
| `.sh` dosyası "no such file" hatası veriyor | Dosya CRLF satır sonlarıyla kaydedilmiş. `.gitattributes` LF zorunlu kılar; dosyayı yeniden checkout edin. |
| ClickHouse şeması güncellenmiyor | Init SQL yalnızca boş veri klasöründe çalışır. `docker compose down -v` komutu tüm veriyi silerek şemayı yeniden oluşturur. |
| Spark "falling behind" uyarısı | Başlangıçta normaldir. Sürekli görünüyorsa `.env` dosyasında `TRIGGER` değerini artırın. |
| Grafana panelleri boş | Önce üstteki "Koşu" seçicisinden bir koşu seçin, sonra zaman aralığını kontrol edin. |

## Kullanılan üçüncü taraf yazılımlar

Eclipse SUMO, OpenStreetMap verisi (ODbL), Apache Kafka, Apache Spark, ClickHouse, Grafana ve grafana-clickhouse-datasource eklentisi, Docker Compose. Python kütüphaneleri: traci, sumolib, confluent-kafka, clickhouse-connect, PyYAML, pandas, matplotlib.
