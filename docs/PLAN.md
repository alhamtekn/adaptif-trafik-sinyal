# Proje planı (25 Kasım – 25 Aralık 2026)

Son teslim **25 Aralık Cuma 17:00**. Hedefimiz **24 Aralık akşamı** teslim etmek; son gün internet, GitHub veya bilgisayar sorunu çıkabilir. Sunumlar 28 Aralık – 8 Ocak arasında.

Kişiler: **A** = Ali, **O** = Onur.
Sorumluluk alanları:
- **A:** simülasyon ve kontrol (SUMO ağı, senaryolar, sumo-bridge, decision-service)
- **O:** veri hattı ve görselleştirme (Kafka, Spark, ClickHouse, Grafana, Compose)
- **İkimiz birlikte:** deneyler ve rapor

Sunumda her ikimiz de tüm sisteme hakim olmalıyız. Bu yüzden her PR'ı diğer kişi inceleyip onaylıyor.

---

## Çalışma kuralları

- **Commit dengesi:** Belgede sıfırlanma sebebi olarak geçiyor. Her birimiz haftada **en az 4 farklı günde** commit atacağız. Haftanın sonunda tek dev commit atmak yok.
- **Dallar:** `main` her zaman çalışır durumda olsun. Her iş kendi dalında yapılsın (`feature/osm-net`, `feature/grafana-latency`) ve PR ile birleştirilsin. Kendi PR'ını kendin birleştirmek yok.
- **Commit mesajları:** Ne yapıldığını yazsın. "fix" veya "update" gibi boş mesajlar yok. Örnek: `decision: histerezis eşiklerini config'e taşı`.
- **Senkronizasyon:** Pazartesi ve Perşembe 20–30 dakika görüşme: ne bitti, ne takıldı, bu hafta kim ne yapacak.
- **README'de isim veya öğrenci numarası olmayacak.**

---

## Hafta 1: 25 Kasım – 1 Aralık · Kurulum, repo, iskeleti anlamak

| Gün | A | O |
|---|---|---|
| **25 Çar (bugün)** | GitHub reposunu aç, O'yu collaborator olarak ekle, **ilk push** (aşağıdaki "İlk gün" bölümü) | Docker Desktop + `.wslconfig` (docs/RAM.md), repoyu klonla |
| 26 Per | `docker compose up` ile sistemi kendi bilgisayarında çalıştır, Grafana'da canlı veri gör | Aynısı; ClickHouse eklentisi ve dashboard'un açıldığını doğrula |
| 27 Cum | Gerçek konum için 2–3 aday bölge (osmWebWizard ile bak) | Kafka/Spark kavramları: `metrics_job.py`'yi satır satır oku, notlarını al |
| 28–29 Hft sonu | Konumu seç, `map.osm` indir, `build_net_from_osm.sh` dene | Spark akış kodunu kendi commitlerinle iyileştir (log, işlenen kayıt/sn ölçümü) |
| 30 Pzt | **Senkron:** konum kararı | Grafana dashboard'u gözden geçir, eksik panelleri listele |
| 1 Sal | netedit'te 3 kavşağı temizle, sinyal ID'lerini çıkar | Kafka UI (isteğe bağlı) veya CLI ile topic izleme notları |

**Hafta 1 çıktısı:** İki bilgisayarda da sistem çalışıyor. Gerçek konum seçilmiş ve SUMO ağı oluşturulmuş.

---

## Hafta 2: 2 – 8 Aralık · Gerçek ağa geçiş

| | A | O |
|---|---|---|
| Ağ | `config.yaml` güncellemesi: `tls`, `main_edges`. `gen_tls_programs.py` ile planları üret ve sumo-gui'de kontrol et | Gerçek ağla gelen verinin Spark ve ClickHouse'ta doğru göründüğünü doğrula |
| Talep | Gerçek ağın edge'leriyle `base.rou.xml` + 3 senaryo dosyası. Senaryolar gerçekten tıkanma yaratıyor mu? | Gecikme paneli: p50/p95, gecikmenin bileşenleri (pencere bekleme, Spark, karar, iletim) |
| Kontrol | Normal senaryoyu koşup kuyruk dağılımına göre **eşikleri yeniden belirle** | "İşlenen kayıt/saniye" ve "veri akışının sürekliliği" metrikleri: boşluk veya kopma tespiti |
| Kalite | Bridge: senaryo olaylarını gerçek ağda test et (şerit kapatma) | `docker compose down -v && up` ile sıfırdan kurulum testi |

**Hafta 2 çıktısı:** Gerçek ağda adaptif döngü çalışıyor. Grafana'da belgede istenen tüm paneller var.

---

## Hafta 3: 9 – 15 Aralık · Kontrol mantığını ayarlama ve deneme deneyleri

| | A | O |
|---|---|---|
| Adil karşılaştırma | **Sabit zamanlı planı makul hale getir.** Yeşil süreleri talebe orantılı olsun (Webster yaklaşımı). Bunu yapmazsak iyileşme oranı şişer ve sunumda sorgulanır. | Koşular arası temizlik: Spark checkpoint ve karar servisi durumu. Bir koşunun bir sonrakini etkilemediğini kanıtla. |
| Ayar | `shift_s`, `n_consecutive`, `min_control_interval_s` için birkaç deneme. Plan çok sık değişiyor mu? | `analyze.py` grafiklerini rapor kalitesine getir (eksen adları, birimler, hata çubukları) |
| Deneme | Her senaryodan 1 tohum, fixed + adaptive (8 koşu, ~1,5 saat) | Deneme sonuçlarından ilk tablo ve grafikler |
| Rapor | Bölüm taslağı: seçilen konum, veri ve senaryo hazırlama | Bölüm taslağı: yazılım mimarisi, mimari diyagram (draw.io) |

**Hafta 3 çıktısı:** Parametreler donduruldu (bundan sonra değişmeyecek). Deneme sonuçları anlamlı.

---

## Hafta 4: 16 – 21 Aralık · Asıl deneyler ve sonuçlar

| | A | O |
|---|---|---|
| 16–17 Ara | **Tam deney:** 4 senaryo × 2 mod × 3 tohum = 24 koşu. SIM_END=3600 ve x5 hızla koşu başına ~12 dk, toplam ~5 saat. Bir gece koşturun. | Koşu sırasında Grafana'dan ekran görüntüleri; gecikme ve throughput kayıtları |
| 18–19 Ara | Sonuç tabloları ve yüzde iyileşme. Adaptif kontrolün kötüleştirdiği durumlar varsa nedenini araştır. | Sistem metrikleri tablosu: uçtan uca gecikme, kayıt/sn, süreklilik |
| 20–21 Ara | Rapor: threshold mantığı, deneysel tasarım, sonuçlar | Rapor: sistem metrikleri, tartışma bölümüne veri hattı sınırlamaları |

**Hafta 4 çıktısı:** Tüm sonuçlar elde. Raporun %80'i yazılmış.

---

## Hafta 5: 22 – 25 Aralık · Bitiş

| | A | O |
|---|---|---|
| 22 Pzt | Tartışma bölümü, rapor genel okuma | README son hali, çalıştırma adımları |
| 23 Sal | **Temiz makine testi:** üçüncü bir bilgisayarda veya Docker'ı sıfırlanmış bir makinede `git clone` → `cp .env.example .env` → `docker compose up`. Sadece README'yi takip edin. | Aynı test; çıkan sorunları düzelt |
| **24 Çar** | **TESLİM** | **TESLİM** |
| 25 Per 17:00 | Resmi son tarih (yedek gün) | |

---

## Sunum hazırlığı (26 Aralık sonrası)

- 10–12 slayt: problem, konum, mimari, threshold mantığı, deney tasarımı, sonuçlar, sınırlamalar.
- **Canlı demo:** `surge` senaryosu x10 hızla yaklaşık 6 dakika sürer. Grafana'da plan değişikliği görünür.
- **Yedek video:** Demo sırasında Docker çökerse diye önceden kaydedin.
- Çapraz hazırlık: A veri hattını, O kontrol mantığını anlatabilmeli. Birbirinize soru sorarak prova yapın.
- Muhtemel sorular:
  - "Sabit plan neden bu süreler?"
  - "Gecikmenin en büyük bileşeni ne?"
  - "Neden Spark içinde karar vermediniz?"
  - "Eşikleri nasıl seçtiniz?"
  - "Adaptif kontrol hangi senaryoda işe yaramadı ve neden?"

---

## Rapor bölümleri ve sorumlular

| Bölüm (belgede istenen) | Yazan | İnceleyen |
|---|---|---|
| Problem tanımı | A | O |
| Seçilen gerçek konum | A | O |
| Veri ve senaryo hazırlama | A | O |
| Yazılım mimarisi + veri akışı diyagramı | O | A |
| Threshold mantığı | A | O |
| Deneysel tasarım | A + O | — |
| Sonuçlar (tablo + grafik) | A + O | — |
| Tartışma | A + O | — |
| Kullanılan harita, veri, kütüphaneler | O | A |

---

## Riskler

| Risk | Etki | Önlem |
|---|---|---|
| OSM'den gelen kavşak geometrisi bozuk | Hafta 2 kayar | Basit bir koridor seçin. Hafta 1 sonunda ağ hazır değilse geçici ağla devam edip paralelde düzeltin. |
| Adaptif kontrol fayda sağlamıyor | Sonuç bölümü zayıf kalır | Bunu dürüstçe raporlayın ve nedenini analiz edin. Eşikleri sadece sonucu iyi göstermek için ayarlamayın. |
| Bir kişinin bilgisayarı yetersiz | Deneyler gecikir | Deneyleri güçlü olan makinede gece koşturun. Diğer kişi o sırada analiz ve rapor üzerinde çalışsın. |
| Commit dengesizliği | Not kaybı veya sıfır | Haftalık kontrol: `git shortlog -sn` |
| Son gün teslim sorunu | Sıfır | 24 Aralık'ta teslim edin. |
