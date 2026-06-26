# Transjakarta ETL Pipeline

Pipeline ETL harian data transaksi Transjakarta menggunakan Apache Airflow, PostgreSQL, dan Docker Compose dengan arsitektur **Medallion (Bronze -> Silver -> Gold)**.

---

## Arsitektur Data

```
[Source Layer]
  CSV   : dummy_routes.csv, dummy_realisasi_bus.csv, dummy_shelter_corridor.
  PgSQL : dummy_transaksi_bus, dummy_transaksi_halte     

[Bronze / L0] stg_transaksi_raw    
[Staging Ref] ref_routes              
              ref_shelter_corridor     
              stg_realisasi_bus        

        | TRANSFORM SILVER 

[Silver / L1] stg_transaksi_clean  

        | TRANSFORM GOLD -

[Gold / L2]
  cube_by_card_type  -> tanggal, card_type_var, gate_in_boo
  cube_by_route      -> tanggal, route_code, route_name, gate_in_boo (BUS + HALTE)
  cube_by_tarif      -> tanggal, fare_int, gate_in_boo

  data/output/output_by_card_type.csv
  data/output/output_by_route.csv
  data/output/output_by_tarif.csv
```

## Struktur Folder

```
transjakarta-pipeline/
├── dags/
│   └── dag_datapelanggan.py       # Main DAG (schedule 07:00 WIB)
├── plugins/
│   └── etl_pipeline_factory.py    # ETL
├── sql/
│   ├── 01_init_source.sql         # DDL tabel source PostgreSQL
│   ├── 02_init_dw.sql             # DDL Bronze/Silver/Gold 
│   ├── 03_transform_silver.sql    # SQL pushdown Bronze ke Silver
│   └── 04_transform_gold.sql      # SQL pushdown Silver ke Gold cubes
├── data/
│   ├── input/                     # CSV files 
│   └── output/                    # Output CSV
├── scripts/
│   └── seed_data.py               
│                                   
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Cara Menjalankan (From Scratch)

### 1. Prerequisites
- Docker Desktop terinstall dan running
- Git

### 2. Clone / siapkan folder
```bash
ls data/input/
# dummy_routes.csv  dummy_shelter_corridor.csv  dummy_realisasi_bus.csv
# dummy_transaksi_bus.csv  dummy_transaksi_halte.csv
```

### 3. Jalankan Docker Compose
```bash
docker compose up -d
```
Cek dengan:
```bash
docker compose ps
```
Semua service harus status `healthy` atau `running`.

### 4. Seed data transaksi ke PostgreSQL (SEKALI SAJA)
```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/seed_data.py
```
> Ini load `dummy_transaksi_bus.csv` dan `dummy_transaksi_halte.csv` ke postgres_dw sebagai tabel source.

### 5. Setup Airflow Connection ke DW PostgreSQL
```bash
docker compose exec airflow-webserver airflow connections add transjakarta_dw \
  --conn-type postgres \
  --conn-host postgres_dw \
  --conn-login dwuser \
  --conn-password dwpassword \
  --conn-schema transjakarta_dw \
  --conn-port 5432
```

### 6. Buka Airflow UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 7. Aktifkan dan trigger DAG
1. Cari DAG `dag_datapelanggan`
2. Toggle **ON** (klik toggle di kiri nama DAG)
3. Klik tombol Trigger DAG untuk jalankan manual
4. Monitor progress di Graph View

### 8. Cek output
```bash
ls data/output/

# Atau query langsung ke PostgreSQL DW
docker compose exec postgres_dw psql -U dwuser -d transjakarta_dw \
  -c "SELECT * FROM cube_by_card_type LIMIT 10;"
```

---

## Schedule

DAG dijadwalkan `0 0 * * *` (UTC) = **07:00 WIB** setiap hari.

---

## Dependencies

| Package | Versi | Kegunaan |
|---|---|---|
| apache-airflow | 2.9.1 | Workflow orchestration |
| apache-airflow-providers-postgres | 5.11.1 | `PostgresOperator` untuk SQL pushdown |
| pandas | 2.2.2 | Extract CSV/PgSQL, staging Bronze & referensi |
| psycopg2-binary | 2.9.9 | Koneksi PostgreSQL |
| sqlalchemy | 2.0.30 | ORM / engine DB |
| pendulum | 3.0.0 | Datetime handling di DAG |

---

## Output CSV

| File | Grouping |
|---|---|
| `output_by_card_type.csv` | tanggal, card_type_var, gate_in_boo |
| `output_by_route.csv` | tanggal, route_code, route_name, gate_in_boo |
| `output_by_tarif.csv` | tanggal, fare_int, gate_in_boo |

Kolom output: `jumlah_pelanggan`, `total_amount`
> **Pelanggan** = transaksi dengan `status_var = 'S'`

---
