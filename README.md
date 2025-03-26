```markdown
# IMDb-ETE-Pipeline

## 1. Overview

This project automates the **download, ingestion, and transformation** of IMDb data into a PostgreSQL database. It uses **Docker** and **Apache Airflow** to orchestrate daily loads, and it is prepared for **dbt** modeling and eventual **cloud deployment** (e.g., GCP or AWS). The pipeline showcases real-world data engineering techniques like **batching**, **idempotent loads**, **security/permissions**, and **robust logging**.

## 2. Roadmap

1. **Sprint 1**: Setup Airflow + Docker + test DAG (IMDb download)  
2. **Sprint 2**: Load IMDb data into PostgreSQL (initially with batch inserts, then optimized using Postgres `COPY`)  
3. **Sprint 3**: Transform data with dbt (cleaning & modeling)  
4. **Sprint 4**: Deploy to Cloud, test scalability & performance

## 3. Current Progress

- **Airflow DAG**: `imdb_download_and_load_dag` downloads 7 IMDb datasets (title.basics, name.basics, etc.).
- **Data Loading**: Uses `TRUNCATE` + **batch inserts** or **Postgres `COPY`** for efficient ingestion. 
- **Null Handling**: IMDb’s `\N` fields are correctly mapped to `NULL`.
- **Logging & Monitoring**: Detailed logs trace file downloads, table creations, row counts, and batch progress.
- **Custom Security**: A `CustomSecurityManager` plus JSON config restrict certain DAGs to certain users.
- **Next Step**: Integrate **dbt** for post-load transformations and schema modeling.

## 4. Changelog

- **[2024-03-16]** Verified Docker volume binding (`/opt/airflow/dags/files` ↔ `./dags/files`).  
- **[2024-03-17]** Added `psycopg2-binary` for PostgreSQL integration.  
- **[2024-03-18]** Implemented custom DAG permissions via JSON + `CustomSecurityManager`.  
- **[2024-03-18]** Externalized config to `.env` for a more efficient Docker build.  
- **[2025-03-25]** Finalized batch loading with idempotency & logging. Investigated large-file memory usage.  
- **[2025-03-26]** Switched some loads to **Postgres `COPY`** for massive datasets (faster ingestion).

## 5. Tools & Versions

- **Docker**: 24.0.x  
- **Docker Compose**: 2.20.x  
- **Python (in container)**: 3.8.x  
- **Airflow**: 2.10.5  
- **PostgreSQL**: 12.x (via Docker)  
- **Python Packages**: `requests`, `psycopg2-binary`, `boto3`, `shutil`, etc.

## 6. Usage

### 6.1 Setup
```bash
git clone <repo-url>
cd IMDb-ETE-Pipeline
docker compose up --build
```
1. Airflow Web UI: [http://localhost:8080](http://localhost:8080)  
2. Default Credentials: `admin / admin` (customize in `.env`)

### 6.2 Trigger the DAG
Manual CLI trigger inside the Airflow container:
```bash
docker exec -it <webserver_container_name> bash
airflow dags trigger imdb_download_and_load_dag
```
Or click **Trigger DAG** in the Airflow UI.

## 7. How It Works

1. **Download Check**  
   - Each dataset is downloaded if **remote** is newer (comparing `Last-Modified` with local timestamps).  
   - Files are stored in `dags/files` (mounted in Docker).
2. **Table Creation & Truncate**  
   - Each table is created if needed using SQL in `schemas/`.  
   - Truncated each run → ensures idempotent loads.
3. **Data Loading**  
   - For moderate data sizes, we used **batch `executemany`**.  
   - For very large tables (millions of rows), we switched to **`COPY`** for speed, using `FORMAT TEXT`, tab delimiter, and `NULL '\N'`.  
   - Logs track insert/copy progress every 50k–100k lines.
4. **Verification**  
   - Final `SELECT COUNT(*)` for each table to confirm loaded rows match expectations.

## 8. Logging & Monitoring

- **Download**: start/end times, local vs. remote timestamps.  
- **Load**: table creation, truncation, batch or `COPY` progress, row counts.  
- **Failures**: immediate logs with stack traces in Airflow UI.

## 9. Security & Permissions

- **`CustomSecurityManager`**: Restricts which DAGs each user can see.  
- **`dag_permissions.json`**: Maps users to DAGs:
  ```json
  {
    "airflow": ["dag1", "dag2"],
    "andry": ["imdb_download_and_load_dag"]
  }
  ```

## 10. Folder Structure

```plaintext
.
├── dags/
│   ├── files/                # IMDb data files (TSV, etc.)
│   └── imdb_download_dag.py  # Main DAG
├── schemas/                  # CREATE TABLE scripts for Postgres
├── config/security/
│   └── dag_permissions.json  # DAG permission mapping
├── airflow/www/security/
│   └── CustomSecurityManager.py
├── scripts/
│   └── airflow-entrypoint.sh  # Docker entrypoint if needed
├── docker-compose.yml
├── Dockerfile
├── .env
├── requirements.txt
└── README.md
```

## 11. Performance Notes

- **Batch Size**: Usually 50k–100k is a sweet spot for `executemany`.  
- **`COPY`**: Much faster for tens of millions of rows, but can require data “cleanliness” (e.g., correct tab counts).  
- **Null Handling**: `\N` → `NULL` is recognized with `NULL '\\N'`.  
- **Scheduler Heartbeat**: For large loads, ensure frequent logging or adequate timeouts.

## 12. Next Steps

- [ ] **dbt Transformations**: Define sources pointing to `imdb_*` tables, build staging/analytics models.  
- [ ] **Retry Logic**: Re-download or reload on partial failures.  
- [ ] **Optional**: More graceful “table exists” checks, migrations for schema changes.  
- [ ] **Cloud Deployment**: Switch local volumes to S3/GCS, run on GCP Composer or AWS MWAA for production scale.
```