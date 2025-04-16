# IMDb-ETE-Pipeline

## 1. Overview

This project automates the **download, ingestion, and transformation** of IMDb data into a PostgreSQL database. It uses **Docker**, **Apache Airflow**, and **dbt** to orchestrate daily loads and transformations. The pipeline is designed to simulate a production-ready ETL process with robust logging, idempotent DAGs, and efficient data handling.

> **Why this project?**  
> This project was built to deepen hands-on experience with orchestration (Airflow), bulk data ingestion (PostgreSQL `COPY`), and dbt-driven transformations — all inside a Dockerized and cloud-friendly setup.

## 2. Pipeline Architecture

```plaintext
+----------------+       +------------------------+       +-----------------------+       +---------------------+
| IMDb Public    |       | Airflow DAGs (Python)  |       | PostgreSQL (raw data) |       | dbt (staging/models)|
| .tsv.gz Files  |  -->  | download + load tasks  |  -->  | imdb_* tables created |  -->  | transformations     |
+----------------+       +------------------------+       +-----------------------+       +---------------------+
        |                        |                              |                              |
        v                        v                              v                              v
Downloaded daily      Tables created via SQL        Data loaded using COPY         Transformations & cleaning
to `/data/files/`     from `/schemas/*.sql`         or executemany fallback        with sources, staging, models
```

## 3. Roadmap

| Sprint | Goal                                      | Status      |
|--------|-------------------------------------------|-------------|
| 1      | Airflow + Docker setup                    | Done        |
| 2      | PostgreSQL loading via COPY               | Done        |
| 3      | dbt transformations (sources/models)      | Done        |
| 4      | Cloud deployment & dashboard integration  | Planned     |

## 4. Current Progress

- **Airflow DAG** (`imdb_download_and_load_dag`) downloads 7 IMDb datasets daily with smart incremental fetch based on `Last-Modified` header checks.
- **Efficient Loading** using PostgreSQL `COPY` command for bulk data ingestion with detailed error handling and validation.
- **Schema Creation** from versioned SQL files in `/schemas` with proper data types.
- **Idempotency** with `TRUNCATE` before each load to ensure clean data.
- **Detailed Logging** with download status, file sizes, row counts, and data validation.
- **Custom Security Manager** for role-based DAG access control via JSON configuration.
- **Docker Setup** with properly isolated services for PostgreSQL, Airflow webserver, and scheduler.

### dbt Progress
- **Sources Defined**: All 7 IMDb raw tables are defined in `sources.yml` with proper documentation.
- **Staging Models Implemented**: 
  - `stg_title_basics` - Core title data with genre splitting into primary/secondary/third
  - `stg_name_basics` - Person information with profession/known titles normalization
  - `stg_title_crew_directors` - Normalized directors from array to individual rows
  - `stg_title_crew_writers` - Normalized writers from array to individual rows
  - `stg_title_episode` - TV episode data with proper column naming
  - `stg_title_principals` - Cast and crew information
  - `stg_title_ratings` - User ratings data
- **Testing Framework**: Comprehensive tests including `not_null`, `unique`, `accepted_values`, and combination tests.
- **Documentation**: Detailed model documentation in `schema.yml` for all columns and models.

## 5. Tools & Versions

- **Docker**: 24.0.x  
- **Docker Compose**: 2.20.x  
- **Python (Airflow containers)**: 3.8.x (apache/airflow:latest-python3.8)
- **Airflow**: 2.10.5  
- **PostgreSQL**: 12.x  
- **dbt**: 1.8.7 (dbt-core and dbt-postgres packages)
- **Python Libraries**: `requests`, `psycopg2-binary`, `pandas`, `boto3`

## 6. Setup & Usage

### Local Docker Environment

```bash
# Clone the repository
git clone <repo-url>
cd apache-airflow-dev-server

# Build and start the Docker environment
docker compose up --build
```

### Configuration
The project uses environment variables defined in `.env` for configuration:
- Database connections
- Airflow core settings
- Security settings
- Custom file paths for data and schemas

### Access Components

- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (credentials: `admin / admin`)
- **PostgreSQL**: Available on host at port 5434
  - Username: airflow
  - Password: airflow
  - Database: airflow

### Execute the Pipeline

1. In the Airflow UI, enable the `imdb_download_and_load_dag` DAG
2. The DAG will:
   - Check if IMDb data needs to be downloaded (based on Last-Modified header)
   - Download files only if local copy is outdated
   - Create tables if they don't exist using schemas from `/schemas/`
   - TRUNCATE tables and use PostgreSQL COPY for fast loading
   - Log detailed information about row counts and validation

### Run dbt Commands

From inside the Docker container:

```bash
cd /opt/airflow/dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Or for specific models:

```bash
dbt run --profiles-dir . --select stg_title_basics
dbt test --profiles-dir . --select stg_title_basics
```

## 7. Folder Structure

```
apache-airflow-dev-server/
├── .env                          # Environment variables
├── .gitignore                    # Git ignore patterns
├── .dockerignore                 # Docker build exclusions
├── Dockerfile                    # Docker image definition
├── docker-compose.yml            # Service configuration
├── requirements.txt              # Python dependencies
├── README.md                     # Project documentation
├── SCHEMA.md                     # Database schema documentation
├── airflow/
│   ├── config/auth/              # Custom security manager
│   │   ├── CustomSecurityManager.py  # DAG-level access control
│   │   └── dag_permissions.json      # User-to-DAG permissions
│   ├── dags/                     # Airflow DAG definitions
│   │   └── imdb_download_dag.py      # Main IMDb pipeline
│   ├── logs/                     # Airflow logs
│   └── scripts/                  # Startup and entrypoint scripts
│       └── airflow-entrypoint.sh     # Container entrypoint
├── data/
│   └── files/                    # Downloaded IMDb .tsv.gz files
├── dbt/
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   │   ├── stg_name_basics.sql       # Person information
│   │   │   ├── stg_title_basics.sql      # Core title data
│   │   │   ├── stg_title_crew_directors.sql  # Normalized directors
│   │   │   ├── stg_title_crew_writers.sql    # Normalized writers
│   │   │   ├── stg_title_episode.sql     # TV episode data
│   │   │   ├── stg_title_principals.sql  # Cast and crew
│   │   │   ├── stg_title_ratings.sql     # User ratings
│   │   │   ├── schema.yml                # Model documentation
│   │   │   └── sources.yml               # Source definitions
│   │   ├── intermediate/           # Currently empty
│   │   └── marts/                  # Currently empty
│   ├── analyses/                 # dbt analyses (SQL)
│   ├── macros/                   # Custom dbt macros
│   ├── snapshots/                # dbt snapshots
│   ├── seeds/                    # dbt seed files
│   ├── tests/                    # Custom dbt tests
│   ├── packages.yml              # dbt package dependencies
│   ├── package-lock.yml          # Locked package versions
│   ├── profiles.yml              # dbt connection profiles
│   ├── dbt_project.yml           # dbt project configuration
│   └── README.md                 # dbt project documentation
└── schemas/                      # SQL table definitions
    ├── imdb_name_basics.sql      # Person table schema
    ├── imdb_title_akas.sql       # Alternative titles schema
    ├── imdb_title_basics.sql     # Core title data schema
    ├── imdb_title_crew.sql       # Directors/writers schema
    ├── imdb_title_episode.sql    # TV episode schema
    ├── imdb_title_principals.sql # Cast and crew schema
    └── imdb_title_ratings.sql    # Ratings schema
```

## 8. Key Technical Features

### Efficient IMDb Data Loading
- **Incremental Downloads**: Uses HTTP header `Last-Modified` to only download changed files
- **Bulk Loading**: PostgreSQL `COPY` command for optimal performance
- **Error Handling**: Detailed logging and validation of row counts
- **Data Cleaning**: Transformations in dbt staging models

### Custom Security
- Role-based authorization for DAG access
- Configurable through JSON permissions file
- Inheritance from AirflowSecurityManager

### Data Transformation
- Normalized nested arrays (directors, writers, genres)
- Data type conversions in staging models
- Boolean conversion from text ('0'/'1')

## 9. Next Steps

- [ ] Add intermediate and mart layer dbt models for analytics
- [ ] Implement data quality checks and monitoring
- [ ] Add dashboard integration with Superset or Metabase
- [ ] Deploy to cloud environment (AWS/GCP/Azure)
- [ ] Add CI/CD pipeline for testing and deployment

## 10. Database Schema

For detailed information about the IMDb database schema, please refer to [SCHEMA.md](SCHEMA.md).

### Key Tables Overview

| Table                | Description           | Primary Key          | Notable Columns                  |
|----------------------|----------------------|-----------------------|----------------------------------|
| imdb_title_basics    | Core title data      | tconst                | titleType, primaryTitle, startYear |
| imdb_title_ratings   | User ratings         | tconst                | averageRating, numVotes          |
| imdb_name_basics     | Person information   | nconst                | primaryName, birthYear, knownForTitles |
| imdb_title_crew      | Directors and writers| tconst                | directors, writers               |
| imdb_title_principals| Cast and crew        | tconst + ordering     | nconst, category, job            |
| imdb_title_akas      | Alternative titles   | titleId + ordering    | title, region, language          |
| imdb_title_episode   | TV episode data      | tconst                | parentTconst, seasonNumber, episodeNumber |

### Data Relationships
- **Movies & TV Shows**: Stored in `imdb_title_basics`
- **People**: Actors, directors, writers in `imdb_name_basics`
- **Connections**: `imdb_title_principals` links people to titles
- **TV Structure**: `imdb_title_episode` links episodes to series

## 11. Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 12. License

This project is licensed under the MIT License - see the LICENSE file for details.