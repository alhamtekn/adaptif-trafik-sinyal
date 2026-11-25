# Docker / WSL bellek ayarı (16 GB RAM'li Windows)

## 1. Önce teşhis: belleği gerçekten Docker mı kullanıyor?

Görev Yöneticisi'ni açın (Ctrl+Shift+Esc). Bellek sütununa göre sıralayın ve **VmmemWSL** (veya **Vmmem**) işlemine bakın.

- **VmmemWSL büyükse:** Belleği Docker'ın çalıştığı WSL sanal makinesi tutuyor. Adım 2'ye geçin.
- **VmmemWSL küçükse:** Sorun Docker değil. Tarayıcı, Teams gibi başlangıçta açılan uygulamalara bakın. Ayrıca Performans sekmesindeki "Önbelleğe alınmış" bellek gerçek kullanım değildir; Windows ihtiyaç olduğunda bu alanı geri verir.

PowerShell ile kontrol:

```powershell
wsl -l -v          # hangi WSL dağıtımları çalışıyor
docker stats       # konteyner başına bellek (Ctrl+C ile çıkın)
```

## 2. Neden şişiyor?

- WSL 2, sınır belirtilmezse Windows RAM'inin büyük bir kısmını (16 GB'ta yaklaşık yarısını) kullanabilir.
- Linux, dosya önbelleği için kullandığı belleği Windows'a hemen geri vermez. Bu yüzden konteyner çalışmasa bile VmmemWSL büyük görünebilir.

## 3. Sınır koyun: `%UserProfile%\.wslconfig`

Dosya yoksa Not Defteri ile oluşturun. Uzantının `.txt` olmadığından emin olun.

```ini
[wsl2]
memory=7GB
processors=4
swap=4GB

[experimental]
autoMemoryReclaim=gradual
sparseVhd=true
```

- `memory=7GB`: Bu projenin tüm servisleri toplamda en fazla ~5 GB kullanacak şekilde sınırlandı. 7 GB Docker'a yeter, Windows'a da 9 GB kalır.
- `autoMemoryReclaim=gradual`: Linux'un kullanmadığı önbellek belleğini zamanla Windows'a geri verir.
- `sparseVhd=true`: Sanal diskin boyutu kendiliğinden küçülebilir.

Ayarın uygulanması için:

```powershell
wsl --shutdown
```

Ardından Docker Desktop'ı yeniden açın.

`[experimental]` bölümündeki bir ayar için uyarı alırsanız önce `wsl --update` çalıştırın. Bu ayarların adları WSL sürümüne göre değişebiliyor; Microsoft'un "Advanced settings configuration in WSL" sayfasından kontrol edin.

## 4. Docker Desktop ayarları

- **Settings → Resources → Resource Saver:** Açık olsun. Konteyner çalışmadığında Docker'ın sanal makinesini durdurur.
- **Settings → General → Start Docker Desktop when you sign in:** Kapatın. Docker, proje üzerinde çalışmadığınız günlerde bellek kullanmasın.
- WSL 2 altyapısında bellek sınırı Docker Desktop'tan değil, `.wslconfig` dosyasından belirlenir.

## 5. Proje üzerinde çalışmadığınızda

```powershell
docker compose down     # proje klasöründe; veriler korunur
wsl --shutdown          # belleği hemen serbest bırakır
```

## Ölçülen değerler

Aşağıdaki değerler geliştirme ortamında, bir koşu sırasında ve hemen sonrasında ölçüldü:

| Servis | Kullanım | Üst sınır |
|---|---|---|
| spark | ~840 MB | 1.5 GB |
| kafka | ~410 MB | 768 MB |
| clickhouse | ~190 MB | 1.5 GB |
| grafana | ~50 MB | 384 MB |
| decision-service | ~25 MB | 256 MB |
| sumo-bridge (koşu sırasında) | — | 512 MB |
