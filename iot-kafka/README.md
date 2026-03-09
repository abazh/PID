# Hands-on IoT Streaming with Kafka, MQTT, ksqlDB, InfluxDB, and Grafana

## Ringkasan

Proyek ini menyiapkan *data pipeline* IoT end-to-end:

- **Perangkat/sensor** (disimulasikan) → **MQTT (Mosquitto)** → **Kafka**
- **Stream processing** dengan **ksqlDB** (alert & agregasi)
- **Sink** ke **InfluxDB** via **Telegraf**
- **Visualisasi** di **Grafana** dengan provisioning otomatis (datasource + dashboard)

Semua komponen dijalankan lokal via **Docker Compose**.

## Daftar isi

- [Ringkasan](#ringkasan)
- [Prasyarat](#1-prasyarat)
- [Cara Menjalankan](#2-cara-menjalankan)
- [Struktur Proyek](#3-struktur-proyek)
- [Penjelasan Alur](#4-penjelasan-alur)
- [Tugas / Evaluasi Mahasiswa](#5-tugas--evaluasi-mahasiswa)
- [Tips & Troubleshooting](#6-tips--troubleshooting)
- [Screenshots](#screenshots)
- [Lisensi](#7-lisensi)

## 1) Prasyarat

- Docker & Docker Compose
- (Opsional) Python 3.9+ bila ingin menjalankan publisher/consumer lokal
- (Opsional) **Python requirements lokal**: gunakan salah satu pendekatan di bawah untuk memasang dependency jika ingin menjalankan `mqtt_publisher.py`, `consumer.py`, atau skrip bridge/producer secara lokal.

  - Dengan virtual environment (direkomendasikan):

     ```bash
     python3 -m venv .venv
     source .venv/bin/activate   # macOS / Linux (zsh/bash)
     # Windows PowerShell: .\.venv\Scripts\Activate.ps1
     pip install -r requirements.txt
     ```

  - Atau (tanpa venv) satu baris cepat:

     ```bash
     pip3 install -r requirements.txt
     ```

---

## 2) Cara Menjalankan

### 2.1. Start stack

```bash
docker compose up -d
```

### 2.2. Buat topic Kafka

```bash
docker compose exec kafka kafka-topics.sh --create   --topic iot.sensors --bootstrap-server localhost:9092   --partitions 3 --replication-factor 1
```

### 2.3. Pilih sumber data

**Opsi A – Simulasi sensor → MQTT → (bridge) → Kafka**

1. Jalankan bridge dalam compose (sudah otomatis jalan).
2. Jalankan publisher lokal (opsional):

   ```bash
   python mqtt_publisher.py
   ```

   Lihat log bridge:

   ```bash
   docker compose logs -f bridge
   ```

**Opsi B – Simulasi sensor langsung ke Kafka (tanpa MQTT)**

```bash
docker compose logs -f producer   # container producer sudah jalan
```

**Opsi C – Producer lokal (tanpa container)**

```bash
pip install kafka-python
python producer/producer.py
```

### 2.4. Konsumsi & inspeksi

Consumer contoh:

```bash
python consumer.py
```

ksqlDB CLI (opsional):

```bash
docker run --rm -it --network host confluentinc/ksqldb-cli:0.29 ksql http://localhost:8088
# kemudian jalankan pernyataan di ksql/commands.sql
```

### 2.5. InfluxDB & Grafana

- InfluxDB: <http://localhost:8086>  
  user: `admin` / pass: `admin123` (Org: `iot-org`, Bucket: `iot`)

- Grafana: <http://localhost:3000>  
  user: `admin` / pass: `admin123`  
  Dashboard otomatis: **IoT Sensors Overview** (folder: *IoT*)

---

## 3) Struktur Proyek

```
grafana/
  provisioning/
    datasources/datasource.yml
    dashboards/dashboards.yml
    dashboards/json/iot-dashboard.json
telegraf/telegraf.conf
ksql/commands.sql
mosquitto/mosquitto.conf
bridge/
  Dockerfile
  requirements.txt
  mqtt_to_kafka.py
producer/
  Dockerfile
  requirements.txt
  producer.py
docker-compose.yml
consumer.py
mqtt_publisher.py
README.md
```

---

## 4) Penjelasan Alur

1. **Publisher (MQTT atau Kafka)** mengirim JSON: `event_id, device_id, timestamp, temperature, humidity, battery`.
2. **Bridge (MQTT→Kafka)** subscribe topik `sensors/+/metrics`, meneruskan payload ke Kafka topic `iot.sensors` (key = `device_id`).
3. **Telegraf** membaca Kafka, mengubah **device_id** menjadi **tag**, memastikan **temperature/humidity/battery** bertipe float, dan menulis ke **InfluxDB**.
4. **Grafana** menampilkan:
   - Timeseries temperature per device
   - Stat minimum battery (30 menit terakhir)
   - Tabel data mentah
5. **ksqlDB** mengeksekusi query streaming (alert suhu tinggi, rata-rata suhu per menit per device).

---

## 5) Tugas / Evaluasi Mahasiswa

> Tujuan: memastikan mahasiswa memahami arsitektur, alat, dan dapat menganalisis serta memodifikasi pipeline.

### Tugas A – Arsitektur & Observabilitas

1. Gambar ulang arsitektur end-to-end (sensor → MQTT → Kafka → Telegraf → InfluxDB → Grafana → ksqlDB).  
2. Jelaskan fungsi masing-masing komponen, termasuk **kenapa device_id dijadikan TAG** di Influx (keuntungan query & cardinality).  
3. Buktikan data mengalir: ambil **screenshot** dari:
   - Log `bridge` (MQTT→Kafka),
   - Query Flux di Grafana (temperature 15 menit),
   - Panel Stat minimum battery,
   - Hasil query ksql `SELECT * FROM HIGH_TEMP EMIT CHANGES LIMIT 3;`.

### Tugas B – Modifikasi Data & Dashboard

1. Tambahkan field baru **pressure** (kPa) di publisher (MQTT atau producer Kafka), pastikan ikut mengalir hingga Grafana (update Telegraf bila perlu).  
2. Tambahkan panel **Average Temperature per device (last 1h)** di dashboard bawaan.  
3. Buat **variable** di Grafana: dropdown `device_id` untuk memfilter panel.

### Tugas C – Stream Processing (ksqlDB)

1. Ubah ambang `HIGH_TEMP` menjadi 28.0°C dan tulis ke **topic baru** `iot.alerts`.  
2. Tampilkan jumlah alert per device per 5 menit (tumbling window) dalam tabel ksql.  
3. Diskusikan trade-off **tumbling vs hopping window** di kasus monitoring IoT.

### Tugas D – Reliability & Security

1. Ubah Telegraf agar **retry** lebih tahan gagal (cari opsi output Influx).  
2. Aktifkan **auth** Mosquitto (username/password) dan jelaskan perubahan yang diperlukan pada publisher & bridge.  
3. Jelaskan kapan perlu **SASL/SSL** di Kafka dan dampaknya ke komponen lain (ksqlDB, Telegraf).

> Kumpulkan **kode yang diubah**, **dashboard JSON** hasil modifikasi, serta **laporan PDF** ringkas (maks 5 halaman) yang menjawab pertanyaan di atas.

---

## 6) Tips & Troubleshooting

- Jika Grafana tidak memuat dashboard → cek folder provisioning dan environment `INFLUX_TOKEN`.  
- Jika Telegraf tidak menulis ke Influx → cek token & bucket; lihat log `docker compose logs -f telegraf`.  
- Jika Kafka tidak menerima data dari bridge → pastikan Mosquitto menerima pesan (`mqtt_publisher.py` berjalan), cek log `bridge`.

---

## 7) Lisensi

Untuk keperluan pendidikan & praktikum. Silakan modifikasi sesuai kebutuhan.

---

## Screenshots

### Influx

![Influx screenshot](https://github.com/user-attachments/assets/32a5c937-0fa6-4f61-bc57-c6496f3a0fea)

### Grafana

![Grafana screenshot](https://github.com/user-attachments/assets/e97afa14-4f4a-4d91-a10b-1dc68691e314)
