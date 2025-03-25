# IMDb-ETE-Pipeline

## Project Goal
- Build an end-to-end pipeline that extracts data from **IMDb**, transforms it with **dbt**, and loads it into a **PostgreSQL DB** (cloud-ready).
- Centralize and clean data to allow smarter content recommendations.
- Automate everything via **Apache Airflow**, running in **Docker**.

---

## Roadmap
1. Sprint 1: Setup Airflow + Docker + test DAG (IMDb download).
2. Sprint 2: Load IMDb data into PostgreSQL using Python + psycopg2.
3. Sprint 3: Transform data with dbt.
4. Sprint 4: Deploy to Cloud, test scalability + performance.

---

## Progress Overview
- Airflow DAG `imdb_download_and_load_dag` now downloads and loads 7 IMDb datasets into PostgreSQL.
- PostgreSQL load uses batch insert logic with idempotency: tables are truncated before each run.
- NULLs (`\N`) from IMDb data are safely handled.
- Logging is implemented to trace each phase of the load (download, truncate, insert, verify).
- Manual table drop was necessary for some conflicting table types.
- Heartbeat errors investigated and mitigated by optimizing memory usage and batch size.
- Custom DAG permissions enforced for users (e.g., airflow, andry).
- dbt model design planned for next sprint.

---

## Changelog
- [2024-03-16] Volume binding verified: Docker `/opt/airflow/dags/files` ↔ Local `./dags/files`.
- [2024-03-17] Added `psycopg2-binary` to Dockerfile for PostgreSQL integration.
- [2024-03-18] Integrated custom security manager for user-specific DAG access control.
- [2024-03-18] Improved Docker build efficiency by externalizing configuration.
- [2025-03-25] Implemented full batch load with idempotency, logging, and fault handling.

---

## Tools & Versions
- Docker: 24.0.X
- Docker Compose: 2.20.X
- Python (inside container): 3.8.X
- Airflow: 2.10.5
- PostgreSQL: 12.X (Docker container)
- Python Packages: `requests`, `psycopg2-binary`, `gzip`, `boto3`

---

## Usage

### Setup
```bash
git clone <repo-url>
cd IMDb-ETE-Pipeline
docker compose up --build
```

### Access Airflow UI
- URL: [http://localhost:8080](http://localhost:8080)
- Login: `admin / admin`

### Trigger DAG Manually
```bash
docker exec -it <webserver_container> bash
airflow dags trigger imdb_download_and_load_dag
```

---

## Current Idempotency Logic

- The DAG automatically checks if the file exists and skips download if already present.
- Each table is truncated before insert to prevent duplication.
- Data is inserted in batches of 10,000 rows for performance.
- A final check confirms number of inserted rows using `SELECT COUNT(*)`.

---

## Logging and Monitoring

- Logs are printed at:
  - File existence check
  - Download start & finish
  - Table creation & truncate
  - Batch inserts every 10k
  - Row count at end
- Logs are accessible in the Airflow UI per-task.
- Known issue: Airflow UI may not persist connections if defined via `.env` and docker-compose (Airflow <2.3).

---

## Custom DAG Permissions

- Controlled via `CustomSecurityManager` in `airflow/www/security/`.
- Mapping of user-DAG visibility is set in `config/security/dag_permissions.json`.
- Example:
  ```json
  {
    "airflow": ["dag1", "dag2"],
    "andry": ["imdb_download_and_load_dag"]
  }
  ```

---

## Folder Structure
```
.
├── dags/
│   ├── files/                     # IMDb data files
│   └── imdb_download_dag.py       # DAG logic
├── schemas/                       # PostgreSQL CREATE TABLE scripts
├── scripts/
│   └── airflow-entrypoint.sh
├── config/security/
│   └── dag_permissions.json
├── airflow/www/security/
│   └── CustomSecurityManager.py
├── airflow-logs/ (auto-generated)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Notes on Performance
- Batch size: 10,000 rows per insert — can be tuned.
- Process logging occurs every 100k lines.
- You may need to drop existing tables manually before running DAG the first time to avoid type conflicts.
- Heartbeat warning appears when a long-running insert stalls the scheduler — not fatal but worth monitoring.

---

## Next Steps
- [ ] Design and test dbt transformations (post-load cleanup & modeling).
- [ ] Add retry logic for failed downloads or partial loads.
- [ ] Optional: Add table existence check to skip table creation step.
- [ ] Deploy on cloud infra (e.g., GCP Composer or AWS MWAA).
