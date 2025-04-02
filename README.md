# IMDb-ETE-Pipeline

## 1. Overview

This project automates the **download, ingestion, and transformation** of IMDb data into a PostgreSQL database. It uses **Docker** and **Apache Airflow** to orchestrate daily loads and is fully configured for **dbt modeling** and eventual **cloud deployment** (e.g., GCP or AWS). The pipeline showcases real-world data engineering techniques like **COPY loading**, **idempotent DAGs**, **user-specific permissions**, and **robust logging**.

> **Why this project?**  
> I built this to simulate a production-ready ETL pipeline from scratch. The goal was to deepen my hands-on experience with orchestration (Airflow), bulk data ingestion (PostgreSQL `COPY`), project structure, and dbt-driven transformations — all inside a Dockerized and cloud-friendly setup.

## 2. Pipeline Architecture

```plaintext
+----------------+       +------------------------+       +-----------------------+       +---------------------+
| IMDb Public    |       | Airflow DAGs (Python)  |       | PostgreSQL (raw data) |       | dbt (TBD)           |
| .tsv.gz Files  |  -->  | download + load tasks  |  -->  | imdb_* tables created |  -->  | staging + models    |
+----------------+       +------------------------+       +-----------------------+       +---------------------+
        |                        |                              |                              |
        v                        v                              v                              v
Downloaded daily      Tables created via SQL        Data loaded using COPY         Transformations & cleaning
to `/dags/files/`     from `/schemas/*.sql`         or executemany fallback        with sources, staging, models
```

## 3. Roadmap

| Sprint | Goal                                      | Status |
|--------|-------------------------------------------|--------|
| 1      | Airflow + Docker setup                    | Done   |
| 2      | PostgreSQL loading via COPY               | Done   |
| 3      | dbt transformations (sources/models)      | Next   |
| 4      | Cloud deployment & dashboard integration  | Planned|

## 4. Current Progress

- Done **Airflow DAG** (`imdb_download_and_load_dag`) downloads 7 IMDb datasets.
- Done **Efficient Loading** using `COPY` for large files, fallback to `executemany` for smaller datasets.
- Done **Schema Creation** from versioned SQL files in `/schemas`.
- Done **Idempotency** with `TRUNCATE` before each load.
- Done **Detailed Logging** (download, row counts, insert progress).
- Done **Custom DAG Permissions** with JSON control.
- Done **dbt Configured** inside Docker, ready for modeling.

## 5. Changelog

- **[2024-03-16]** Volume binding verified: `/opt/airflow/dags/files ↔ ./dags/files`
- **[2024-03-18]** Added `CustomSecurityManager` + JSON DAG restriction
- **[2025-03-25]** Batch load implemented with `executemany`
- **[2025-03-26]** Switched to PostgreSQL `COPY` for large datasets
- **[2025-03-27]** Dockerfile updated with git + dbt setup
- **[2025-03-27]** dbt debug passed inside container

## 6. Tools & Versions

- **Docker**: 24.0.x  
- **Docker Compose**: 2.20.x  
- **Python (Airflow containers)**: 3.8.x  
- **Airflow**: 2.10.5  
- **PostgreSQL**: 12.x  
- **dbt**: 1.8.7 (inside container)  
- **Python Libs**: `requests`, `psycopg2-binary`, `dbt-core`, `dbt-postgres`, etc.

## 7. Setup

### Local Docker Environment

```bash
git clone <repo-url>
cd apache-airflow-dev-server
docker compose up --build
```

### Access Airflow UI
- [http://localhost:8080](http://localhost:8080)  
- Default credentials: `admin / admin`

## 8. How the DAG Works

1. **Download Logic**
   - Files downloaded only if remote version is newer (`Last-Modified` header).
   - Files stored in `dags/files/` (Docker volume).

2. **Table Handling**
   - Tables are created from `/schemas/*.sql` if not already present.
   - Each load truncates existing data (idempotency).

3. **Data Loading**
   - For small/moderate files → `executemany` with 50000 batch size.
   - For large tables (millions of rows) → optimized `COPY` with null & tab handling.
   - Logs print progress every 100000 lines.

4. **Validation**
   - Final row count is checked via `SELECT COUNT(*)`.

## 9. Logging & Monitoring

All phases are logged:
- File download start/end and size
- Table creation, truncate
- Progress in lines and percentage
- COPY success or errors
- Row count comparison

Logs are viewable in the **Airflow UI** under each task.

## 10. DAG Permissions

Controlled via:
- `CustomSecurityManager.py` in `airflow/config/auth/`
- `dag_permissions.json` for mapping:

```json
{
  "airflow": ["*"],
  "andry": ["imdb_download_and_load_dag"]
}
```

## 11. Folder Structure

```plaintext
.
├── dags/                           # Airflow DAGs
│   ├── imdb_download_dag.py        # Main DAG file
│   └── files/                      # Downloaded data files
├── schemas/                        # SQL CREATE TABLE files
│   ├── imdb_title_basics.sql
│   ├── imdb_title_akas.sql
│   ├── imdb_title_ratings.sql
│   └── ...
├── logs/                           # Logs volume
├── config/
│   └── auth/
│       ├── dag_permissions.json
│       └── CustomSecurityManager.py
├── dbt/                            # dbt project
│   ├── dbt_project.yml            # Main dbt configuration file
│   ├── profiles.yml               # Connection profiles
│   ├── models/                    # dbt models directory
│   │   ├── staging/              # Staging models
│   │   │   └── imdb/            # IMDb staging schemas
│   │   ├── marts/               # Business-specific models
│   │   └── core/                # Core models
│   ├── macros/                   # Custom SQL macros
│   ├── seeds/                    # Static reference data
│   ├── snapshots/                # Slowly changing dimensions
│   ├── analyses/                 # SQL explorations
│   └── tests/                    # Custom dbt tests
├── docker-compose.yml
├── Dockerfile
├── .env
├── requirements.txt
├── SCHEMA.md                      # Database schema documentation
└── README.md
```

## 12. dbt Status

- Done Installed inside container
- Done `dbt debug` successfully connects to Postgres
- Done `profiles.yml` and `dbt_project.yml` are under `/opt/airflow/dbt/`
- Next: define `sources`, `staging`, `models`

## 13. Performance Notes

- `COPY` is **10x faster** than batch insert for huge files.
- `\N` handled correctly as `NULL` in PostgreSQL.
- Progress bars and row validation implemented.
- Airflow `execution_timeout` extended to handle large batches.

## 14. Next Steps

- [ ] Create initial **staging models** in dbt for each IMDb table  
- [ ] Use `sources` in `schema.yml` for dbt best practices  
- [ ] Explore **dbt tests**, `unique`, `not_null`, etc.  
- [ ] Optionally: connect **Metabase** or **Superset** to PostgreSQL  
- [ ] Prepare for **cloud deployment** (e.g. GCS volumes, GCP Composer)

## 15. Future Improvements

- [ ] Add retry logic for flaky downloads  
- [ ] Add test DAG for verifying dbt transformations  
- [ ] CI/CD pipeline for DAG + dbt deployment  
- [ ] Archive old raw files to cloud storage

## 16. Database Schema

For detailed information about the IMDb database schema including table structures, column descriptions, relationships, and usage examples, please refer to the [SCHEMA.md](SCHEMA.md) file in this repository.

Key tables in the IMDb dataset:

| Table | Description | Primary Key | Notable Columns |
|-------|-------------|------------|----------------|
| imdb_title_basics | Core title data | tconst | titleType, primaryTitle, startYear |
| imdb_title_ratings | User ratings | tconst | averageRating, numVotes |
| imdb_name_basics | Person information | nconst | primaryName, birthYear, knownForTitles |
| imdb_title_crew | Directors and writers | tconst | directors, writers |
| imdb_title_principals | Cast and crew | tconst + ordering | nconst, category |
| imdb_title_akas | Alternative titles | titleId + ordering | title, region, language |
| imdb_title_episode | TV episode data | tconst | parentTconst, seasonNumber, episodeNumber |

The schema is designed for performant queries with appropriate indexes on join columns and frequent filter conditions.