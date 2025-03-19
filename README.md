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
- Airflow DAG running, IMDb data successfully downloaded to local volume.
- PostgreSQL load working with manual row count verification.
- Custom DAG permissions enforced for users (e.g., airflow, andry).
- Optimized Docker build – faster rebuilds, clean architecture.
- dbt model design planned for next sprint.

---

## Changelog
- [2024-03-16] Volume binding verified: Docker `/opt/airflow/dags/files` ↔ Local `./dags/files`.
- [2024-03-17] Added `psycopg2-binary` to Dockerfile for PostgreSQL integration.
- [2024-03-18] Integrated custom security manager for user-specific DAG access control.
- [2024-03-18] Improved Docker build efficiency by externalizing configuration.

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

## Planned Idempotency Logic

- The IMDb dataset is refreshed daily, making it important to avoid data duplication.
- To ensure idempotent data loading, the following logic will be implemented:
  1. Check if new data files are available before proceeding.
  2. If files are present, truncate the destination table to clear old data.
  3. Insert fresh data safely, ensuring consistency across runs.

This logic will guarantee the database remains clean and accurate, regardless of re-runs or failed attempts. Currently, data is appended for testing purposes, and row counts are used for manual verification.

---

## Custom DAG Permissions

- Using a CustomSecurityManager, we control which DAGs are visible to specific users.
- The mapping between users and accessible DAGs is defined in `config/security/dag_permissions.json`, making it easy to update without modifying code.
- Example:
  ```json
  {
    "airflow": ["dag1", "dag2"],
    "andry": ["imdb_download_dag", "dag2"]
  }
  ```

- This enhances security and usability for multi-user environments.

---

## Folder Structure
```
.
├── dags/                          # DAG scripts + downloaded files
│   ├── files/                     # Downloaded IMDb data
│   └── imdb_download_dag.py       # DAG for download and load
├── scripts/
│   └── airflow-entrypoint.sh      # Airflow service startup script
├── config/security/
│   ├── dag_permissions.json       # DAG access control config
├── airflow/www/security/
│   └── CustomSecurityManager.py   # Custom DAG access logic
├── airflow-logs/                  # Airflow logs (auto-generated)
├── docker-compose.yml             # Service definitions
├── Dockerfile                     # Custom Airflow image setup
├── requirements.txt               # Python dependencies
└── README.md                      # Project overview and instructions
```

---

## Notes on Performance
- Configuration files (like `dag_permissions.json`) are mounted as volumes for easy updates without needing a full image rebuild.
- Docker build optimized by avoiding unnecessary rebuilds of unchanged files, resulting in faster setup.

---

## Next Steps
- [ ] Finalize and test PostgreSQL load task with idempotent logic.
- [ ] Design and implement dbt transformations.
- [ ] Plan cloud deployment (e.g., AWS/GCP) and optimize performance.
- [ ] Improve logging and monitoring of DAG runs.
---

