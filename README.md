# IMDb-ETE-Pipeline

## Project Goal
- Build an end-to-end pipeline that extracts data from **IMDb**, transforms it with **dbt**, and loads it into a **PostgreSQL DB** (cloud-ready).
- Centralize and clean data to allow smarter content recommendations.
- Automate everything via **Apache Airflow**, running in **Docker**.

---

## Roadmap
1. ✅ Sprint 1: Setup Airflow + Docker + test DAG (IMDb download).
2. ⏳ Sprint 2: Load IMDb data into PostgreSQL using Python + psycopg2.
3. 🔜 Sprint 3: Transform data with dbt.
4. 🔜 Sprint 4: Deploy to Cloud, test scalability + performance.

---

## Progress Overview
- ✅ Airflow DAG running, IMDb data successfully downloaded to local volume.
- ✅ Docker volume binding and container access confirmed.
- ⏳ PostgreSQL load task under development.
- 🔜 dbt model design planned for next sprint.

---

## Changelog
- [2024-03-16] Volume binding verified: Docker `/opt/airflow/dags/files` ↔ Local `./dags/files`.
- [2024-03-17] Added `psycopg2-binary` to Dockerfile for PostgreSQL integration.

---

## Tools & Versions
- Docker: 24.0.X
- Docker Compose: 2.20.X
- Python (inside container): 3.8.X
- Airflow: 2.7.X
- PostgreSQL: 12.X (Docker container)
- Python Packages: `requests`, `psycopg2-binary`, `boto3`

---

## Usage

### Setup
```bash
git clone <repo-url>
cd IMDb-ETE-Pipeline
docker compose up -d
```

### Access Airflow UI
- URL: [http://localhost:8080](http://localhost:8080)

### Trigger DAG Manually
```bash
docker exec -it webserver-1 bash
airflow dags trigger imdb_download_dag
```

---

## Folder Structure
```
.
├── dags/               # DAG scripts + downloaded files
│   ├── files/
│   └── imdb_download_dag.py
├── scripts/            # Entry scripts for Airflow
│   └── airflow-entrypoint.sh
├── airflow-logs/       # Airflow logs (auto-generated)
├── docker-compose.yml  # Service definitions
├── Dockerfile          # Custom Airflow image setup
├── requirements.txt    # Python packages (if needed)
└── README.md           # Project overview and instructions
```

---

## Next Steps
- [ ] Finalize and test PostgreSQL load task.
- [ ] Design and implement dbt transformations.
- [ ] Plan cloud deployment (e.g., AWS/GCP) and optimize performance.

---

